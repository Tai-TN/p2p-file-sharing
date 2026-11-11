import os
import json
import time
import socket
import base64
import threading
from typing import Dict, List, Optional
from storage.piece_manager import PieceManager
from protocol.messages import (
    MessageType,
    send_msg,
    recv_msg,
    create_announce_msg,
    create_get_peers_msg,
)
from .selection import choose_next_piece
from .connection import PeerConnection
from .peer_selector import PeerSelector
from .stats import RateMeter

# Peer bị ngắt do chết / gửi sai dữ liệu sẽ bị bỏ qua trong khoảng thời gian này (giây).
FAILED_PEER_COOLDOWN = 120.0
# Số lần gửi sai dữ liệu tối đa trước khi ngắt kết nối peer.
MAX_BAD_DATA_STRIKES = 4


class TransferEngine:
    """
    Điều phối quá trình tải file P2P.

    Lớp này kết nối Tracker, khám phá các Peer, tạo worker tải song song
    và phối hợp với PeerNode để cập nhật đóng góp cho Tit-for-Tat.
    Đồng thời phát hiện peer chết / gửi sai dữ liệu và báo cáo lên Tracker.
    """
    def __init__(self, peer_id: str, tracker_host: str, tracker_port: int,
                 listen_port: int, piece_manager: PieceManager,
                 max_workers: int = 4, my_ip: str = "",
                 strategy: str = "rarest_first"):
        self.peer_id = peer_id
        self.tracker_host = tracker_host
        self.tracker_port = tracker_port
        self.listen_port = listen_port
        self.piece_manager = piece_manager
        self.max_workers = max_workers
        self.my_ip = my_ip          # IP thực của máy — báo cho Tracker để peer khác kết nối đúng chỗ
        # Chiến lược chọn mảnh: "rarest_first" (mặc định), "random" hoặc "sequential".
        self.strategy = strategy
        self.info_hash = piece_manager.metainfo["info_hash"]

        self.is_running = False  # Cờ điều khiển toàn bộ worker và luồng announce.
        self.lock = threading.Lock()  # Bảo vệ danh sách Peer và piece đang được tải.

        # Bảng thông tin các Peer đã biết từ Tracker:
        # peer_id -> địa chỉ TCP, bitfield hiện tại và số lần gửi sai dữ liệu.
        self.connected_peers: Dict[str, Dict] = {}
        # Một selector dùng chung cho mọi worker để đếm tải theo từng peer.
        self.peer_selector = PeerSelector()

        # Blacklist tạm: peer_id -> thời điểm bị ngắt.
        self.failed_peers: Dict[str, float] = {}
        # Peer đã chết, chờ được báo cáo lên Tracker ở lần ANNOUNCE tiếp theo.
        self.pending_bad_reports: set = set()

        # Các piece đã được một worker nhận xử lý; tránh nhiều worker tải trùng.
        self.in_flight_pieces: set = set()
        # Các connection lâu dài do worker tạo; dùng để đóng ngay khi engine dừng.
        self._worker_connections: set[PeerConnection] = set()

        # File tạm lưu bitfield để có thể tiếp tục download sau khi khởi động lại.
        self.progress_file = f"{piece_manager.target_file_path}.progress.json"

        # Số liệu cho giao diện: tốc độ tải tổng và theo từng peer.
        self.download_meter = RateMeter()
        self.peer_meters: Dict[str, RateMeter] = {}
        # Tracker trả về BLOCKED nếu admin đã chặn peer này.
        self.blocked_by_tracker = False
        # Lần cuối liên lạc Tracker thành công (None = chưa từng / đang lỗi).
        self.tracker_ok: Optional[bool] = None

    def start_download(self, peer_node=None) -> bool:
        """Khởi động các worker và tải các piece còn thiếu song song."""
        self.is_running = True
        self.peer_node = peer_node
        # Khôi phục tiến độ trước khi tạo worker, tránh tải lại piece đã hợp lệ.
        self._load_progress()

        print(f"[TransferEngine {self.peer_id}] Khởi động tải tập tin: {self.piece_manager.info['name']}")

        # Đăng ký bản thân với Tracker và dùng luôn danh sách Peer trả về.
        peers = self._announce_to_tracker(event="started")
        if peers:
            self._update_peer_connections(peers)

        # Luồng nền giữ announce sống và cập nhật Peer mới trong swarm.
        announce_thread = threading.Thread(target=self._periodic_announce, daemon=True)
        announce_thread.start()

        # Mỗi worker có connection riêng, nhưng trạng thái piece vẫn dùng chung.
        worker_threads = []
        for i in range(self.max_workers):
            t = threading.Thread(target=self._worker_loop, args=(i,), daemon=True)
            t.start()
            worker_threads.append(t)

        time_start = time.time()

        # Luồng chính chờ hoàn tất; định kỳ lưu bitfield để hỗ trợ resume.
        last_save = 0
        while self.is_running and not self.piece_manager.is_complete():
            time.sleep(0.5)
            now = time.time()
            if now - last_save >= 10.0:
                self._save_progress()
                last_save = now

        if self.piece_manager.is_complete():
            elapsed   = time.time() - time_start
            total_MB  = self.piece_manager.file_size / 1_048_576
            speed_MBs = total_MB / elapsed if elapsed > 0 else 0
            speed_Mbs = speed_MBs * 8
            print(f"[TransferEngine {self.peer_id}] TẢI HOÀN TẤT 100%! Bắt đầu chuyển sang chế độ Seeder.")
            print(f"  Thời gian : {elapsed:.2f} giây")
            print(f"  Kích thước: {total_MB:.2f} MB")
            print(f"  Tốc độ TB : {speed_MBs:.2f} MB/s  ({speed_Mbs:.1f} Mbps)")
            self._announce_to_tracker(event="completed")
            self._remove_progress_file()
            return True

        return False

    def _announce_to_tracker(self, event: str = "started") -> List[Dict]:
        """Thông báo trạng thái (kèm danh sách peer chết) với Tracker rồi lấy danh sách Peer."""
        bad_to_report: List[str] = []
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5.0)
            sock.connect((self.tracker_host, self.tracker_port))

            # Tracker chỉ cần biết tiến độ tổng quan, không tham gia choking.
            downloaded_bytes = int((self.piece_manager.progress_percentage() / 100.0) * self.piece_manager.file_size)
            left_bytes = self.piece_manager.file_size - downloaded_bytes

            # Lấy danh sách peer chết cần báo cáo và xóa khỏi hàng chờ
            with self.lock:
                bad_to_report = list(self.pending_bad_reports)
                self.pending_bad_reports.clear()

            msg = create_announce_msg(
                info_hash=self.info_hash,
                peer_id=self.peer_id,
                port=self.listen_port,
                ip=self.my_ip,          # IP thực để Tracker lưu đúng địa chỉ
                event=event,
                downloaded=downloaded_bytes,
                left=left_bytes,
                bad_peers=bad_to_report,  # Báo cáo peer chết
                name=self.piece_manager.info.get("name", ""),
            )
            if bad_to_report:
                print(f"[TransferEngine {self.peer_id}] Báo cáo {len(bad_to_report)} peer chết lên Tracker: {bad_to_report}")
            send_msg(sock, msg)
            announce_resp = recv_msg(sock)
            sock.close()
            bad_to_report = []  # Đã gửi thành công, không cần đưa lại vào hàng chờ
            self.tracker_ok = True

            if announce_resp.get("status") == "BLOCKED":
                if not self.blocked_by_tracker:
                    print(f"[TransferEngine {self.peer_id}] Lỗi: Tracker đã chặn peer này.")
                self.blocked_by_tracker = True
                return []
            self.blocked_by_tracker = False

            # Sau announce, mở một request riêng để lấy danh sách Peer.
            sock2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock2.settimeout(5.0)
            sock2.connect((self.tracker_host, self.tracker_port))
            get_p_msg = create_get_peers_msg(self.info_hash, self.peer_id)
            send_msg(sock2, get_p_msg)
            p_resp = recv_msg(sock2)
            sock2.close()

            if p_resp and p_resp.get("peers"):
                return p_resp["peers"]
            return []
        except Exception:
            self.tracker_ok = False
            # Gửi báo cáo thất bại → đưa lại vào hàng chờ để lần sau gửi tiếp
            if bad_to_report:
                with self.lock:
                    self.pending_bad_reports.update(bad_to_report)
            return []

    def start_seeding(self) -> None:
        """Khởi động chế độ Seeding (duy trì announce lên Tracker cho file đã có)."""
        self.is_running = True
        self._announce_to_tracker(event="started")
        announce_thread = threading.Thread(target=self._periodic_announce, daemon=True)
        announce_thread.start()

    def _periodic_announce(self) -> None:
        """Định kỳ gửi heartbeat để Tracker không loại Peer khỏi swarm."""
        while self.is_running:
            time.sleep(20)
            if not self.is_running:
                break
            event = "completed" if self.piece_manager.is_complete() else "started"
            peers = self._announce_to_tracker(event=event)
            if peers and not self.piece_manager.is_complete():
                self._update_peer_connections(peers)

    def _update_peer_connections(self, peers_list: List[Dict]) -> None:
        """Lấy bitfield của Peer mới và lưu thông tin kết nối vào bộ nhớ."""
        for p_info in peers_list:
            p_id = p_info.get("peer_id")
            ip   = p_info.get("ip")
            port = p_info.get("port")

            if not p_id or not ip or not port:
                continue

            # Kiểm tra nhanh trong lock (không làm network ở đây)
            with self.lock:
                if p_id == self.peer_id or p_id in self.connected_peers:
                    continue
                # Peer vừa bị ngắt do chết/gửi sai dữ liệu → bỏ qua trong thời gian cooldown
                failed_at = self.failed_peers.get(p_id)
                if failed_at is not None:
                    if time.time() - failed_at < FAILED_PEER_COOLDOWN:
                        continue
                    del self.failed_peers[p_id]

            # Connection probe chỉ dùng lấy bitfield, worker sẽ tạo connection dài hạn riêng.
            connection = self._create_connection(p_id, ip, port)
            if connection is None:
                continue

            bitfield = list(connection.bitfield or [])
            connection.close()

            with self.lock:
                if p_id not in self.connected_peers:
                    # Không lưu socket dùng chung; mỗi worker tự quản lý socket của mình.
                    self.connected_peers[p_id] = {
                        "bitfield":       bitfield,
                        "ip":             ip,
                        "port":           port,
                        "bad_data_count": 0,
                    }
                    have_count = sum(1 for b in bitfield if b)
                    print(f"[TransferEngine {self.peer_id}] Biết Peer {p_id} ({ip}:{port}) "
                          f"| {have_count}/{len(bitfield)} mảnh")

    def _on_peer_have(self, remote_peer_id: str, piece_index: int) -> None:
        """Cập nhật bitfield khi nhận thông báo HAVE từ peer từ xa."""
        with self.lock:
            if remote_peer_id in self.connected_peers:
                bf = self.connected_peers[remote_peer_id]["bitfield"]
                if 0 <= piece_index < len(bf):
                    bf[piece_index] = True

    def _create_connection(self, peer_id: str, ip: str, port: int) -> Optional[PeerConnection]:
        """Tạo PeerConnection và thực hiện handshake với Peer từ xa.

        recv_msg/send_msg giờ ném exception khi lỗi, nên bọc try để
        lỗi mạng không làm chết luồng gọi (worker / announce).
        """
        connection = PeerConnection(
            local_peer_id=self.peer_id,
            remote_peer_id=peer_id,
            info_hash=self.info_hash,
            ip=ip,
            port=port,
            on_have=self._on_peer_have,
        )
        try:
            if connection.connect():
                return connection
        except Exception:
            pass
        try:
            connection.close()
        except Exception:
            pass
        return None

    def _worker_loop(self, worker_id: int) -> None:
        """Worker chọn piece, tải piece và tự quản lý các socket của mình."""
        # Connection riêng của worker: { peer_id: PeerConnection }.
        # Choking vẫn tính theo peer_id, không tính theo số socket.
        my_conns: Dict[str, PeerConnection] = {}

        while self.is_running and not self.piece_manager.is_complete():
            with self.lock:
                peer_bitfields = {p_id: info["bitfield"] for p_id, info in self.connected_peers.items()}
                missing = [p for p in self.piece_manager.missing_pieces() if p not in self.in_flight_pieces]

            if not peer_bitfields:
                peers = self._announce_to_tracker()
                if peers:
                    self._update_peer_connections(peers)
                time.sleep(2.0)
                continue

            if not missing:
                # Tất cả mảnh còn thiếu đang được các worker khác tải (in-flight)
                time.sleep(0.2)
                continue

            # Chỉ chọn mảnh mà ít nhất một peer ĐANG DÙNG ĐƯỢC có giữ. Nếu không,
            # mảnh chỉ nằm ở peer đã rời mạng / đang bị backoff sẽ bị chọn lại liên
            # tục và chặn các mảnh khác (head-of-line blocking, rõ nhất với Sequential).
            # Độ hiếm vẫn đếm trên mọi peer đã biết.
            usable = self.peer_selector.available_peers(list(peer_bitfields))
            missing = [
                p for p in missing
                if any(p < len(peer_bitfields[q]) and peer_bitfields[q][p] for q in usable)
            ]
            target_piece = choose_next_piece(missing, peer_bitfields, strategy=self.strategy)
            if target_piece is None:
                time.sleep(0.3)
                continue

            # Nhận quyền tải piece trong cùng lock để hai worker không lấy trùng.
            with self.lock:
                already_taken = target_piece in self.in_flight_pieces
                if not already_taken:
                    self.in_flight_pieces.add(target_piece)
            if already_taken:
                time.sleep(0.05)
                continue

            # Bị CHOKE chỉ làm request này thất bại; Peer vẫn được giữ lại để thử sau.
            try:
                success = self._download_piece(target_piece, my_conns)
            except Exception as e:
                # Lưới an toàn: lỗi bất ngờ không được làm chết worker
                print(f"[Worker-{worker_id}] Lỗi không mong muốn khi tải piece {target_piece}: {e}")
                success = False

            with self.lock:
                self.in_flight_pieces.discard(target_piece)

            if not success:
                # Peer lỗi/CHOKE đã bị PeerSelector tạm loại bằng cooldown/backoff.
                time.sleep(0.05)

        # Đóng các connection do worker sở hữu khi worker kết thúc.
        for connection in my_conns.values():
            connection.close()
            with self.lock:
                self._worker_connections.discard(connection)

    def _download_piece(self, piece_index: int, my_conns: Dict[str, PeerConnection]) -> bool:
        """
        Tải một piece bằng connection riêng của worker.

        ``my_conns`` có dạng ``{peer_id: PeerConnection}`` và không được
        dùng chung giữa các worker; slot choking vẫn được tính theo peer_id.

        Mỗi request đã reserve trong PeerSelector được kết thúc ĐÚNG MỘT lần
        bằng record_success / record_choke / record_failure (qua _fail).
        """
        with self.lock:
            available_peers = [
                (p_id, info) for p_id, info in self.connected_peers.items()
                if piece_index < len(info["bitfield"]) and info["bitfield"][piece_index]
            ]

        if not available_peers:
            return False

        # Selector chọn trong các peer có piece này và reserve một request.
        piece_size = self._piece_size(piece_index)
        selected = self.peer_selector.select_peer(available_peers, piece_size)
        if selected is None:
            return False
        p_id, p_info = selected
        ip   = p_info["ip"]
        port = p_info["port"]

        # Tái sử dụng connection của worker; nếu chưa có thì handshake và báo interested.
        connection = my_conns.get(p_id)
        if connection is None:
            connection = self._create_connection(p_id, ip, port)
            if connection is None:
                return self._fail(p_id, my_conns)
            try:
                interested_ok = connection.send_interested()
            except Exception:
                interested_ok = False
            if not interested_ok:
                connection.close()
                return self._fail(p_id, my_conns)
            my_conns[p_id] = connection
            with self.lock:
                self._worker_connections.add(connection)

        # ── Giai đoạn 1+2: Gửi REQUEST và nhận phản hồi ──────────────────
        try:
            connection.set_timeout(45.0)
            # perf_counter() chính xác tới micro giây trên mọi hệ điều hành
            # (time.monotonic() trên Windows chỉ ~15,6 ms → piece nhanh đo ra 0s).
            request_started = time.perf_counter()
            resp = connection.request_piece(piece_index)
            request_elapsed = max(time.perf_counter() - request_started, 1e-6)
        except socket.timeout:
            # Peer chậm nhưng chưa chắc đã chết: chỉ bỏ connection này để lần sau tạo lại.
            return self._fail(p_id, my_conns, drop_conn=True)
        except (EOFError, OSError):
            # Peer đóng kết nối / lỗi mạng → coi là peer chết, báo lên Tracker.
            return self._fail(p_id, my_conns, drop_conn=True, dead=True)
        except Exception:
            # JSON hỏng / sai định dạng → peer gửi dữ liệu sai, luồng tin không còn tin cậy.
            return self._fail(p_id, my_conns, drop_conn=True, bad_data=True)

        if not resp:
            return self._fail(p_id, my_conns, drop_conn=True, dead=True)

        # ── Giai đoạn 3: Kiểm tra loại phản hồi ──────────────────────────
        resp_type = resp.get("type")

        if resp_type == MessageType.CHOKE.value:
            # CHOKE là trạng thái tạm thời của Tit-for-Tat, không phải lỗi.
            self.peer_selector.record_choke(p_id)
            return False

        if resp_type == MessageType.ERROR.value:
            # Peer không đọc được piece này ở phía họ; không tính là gửi sai dữ liệu.
            return self._fail(p_id, my_conns)

        if resp_type != MessageType.PIECE.value:
            return self._fail(p_id, my_conns, bad_data=True)

        if resp.get("piece_index") != piece_index:
            # Luồng tin bị lệch → bỏ connection này
            return self._fail(p_id, my_conns, drop_conn=True, bad_data=True)

        # ── Giai đoạn 4: Kiểm tra dữ liệu Base64 ─────────────────────────
        raw_b64 = resp.get("data")
        if not isinstance(raw_b64, str):
            return self._fail(p_id, my_conns, bad_data=True)
        try:
            piece_data = base64.b64decode(raw_b64, validate=True)
        except ValueError:
            return self._fail(p_id, my_conns, bad_data=True)

        # ── Giai đoạn 5: Ghi mảnh (kèm kiểm tra SHA-256) ─────────────────
        try:
            written = self.piece_manager.write_piece(piece_index, piece_data)
        except OSError as e:
            # Lỗi ổ đĩa phía mình, không phải lỗi của peer.
            print(f"[TransferEngine {self.peer_id}] Lỗi ghi piece {piece_index}: {e}")
            return self._fail(p_id, my_conns)

        if not written:
            # SHA-256 không khớp → peer gửi dữ liệu hỏng
            return self._fail(p_id, my_conns, bad_data=True)

        # ── Thành công ───────────────────────────────────────────────────
        # Chỉ cập nhật tốc độ sau khi dữ liệu đã ghi và xác minh thành công.
        self.peer_selector.record_success(p_id, len(piece_data), request_elapsed)
        self.download_meter.add(len(piece_data))
        with self.lock:
            meter = self.peer_meters.setdefault(p_id, RateMeter())
        meter.add(len(piece_data))
        if self.peer_node:
            # PeerNode dùng số liệu này để xếp hạng regular unchoking.
            self.peer_node.record_download(p_id, len(piece_data))
        print(f"[Worker-{self.peer_id}] Piece {piece_index} ✓ ← {p_id} "
              f"({self.piece_manager.progress_percentage():.1f}%)")
        if self.peer_node:
            self.peer_node.broadcast_have(piece_index)
        return True

    def _fail(self, p_id: str, my_conns: Dict[str, PeerConnection], *,
              drop_conn: bool = False, bad_data: bool = False, dead: bool = False) -> bool:
        """
        Kết thúc một request thất bại (release reserve trong PeerSelector đúng một lần).

        drop_conn: đóng connection của worker tới peer này (lần sau tạo lại).
        bad_data : tính 1 lần gửi sai dữ liệu; đủ ngưỡng thì ngắt peer.
        dead     : peer chết → ngắt ngay và báo cáo lên Tracker.
        Luôn trả về False để có thể viết `return self._fail(...)`.
        """
        self.peer_selector.record_failure(p_id)
        if drop_conn:
            self._drop_worker_connection(p_id, my_conns)
        if dead:
            self._disconnect_peer(p_id)
        elif bad_data:
            self._record_bad_data(p_id)
        return False

    def _drop_worker_connection(self, p_id: str, my_conns: Dict[str, PeerConnection]) -> None:
        """Đóng connection của worker hiện tại tới peer p_id."""
        connection = my_conns.pop(p_id, None)
        if connection is not None:
            try:
                connection.close()
            except Exception:
                pass
            with self.lock:
                self._worker_connections.discard(connection)

    def _piece_size(self, piece_index: int) -> int:
        """Trả về kích thước thực tế của piece, kể cả piece cuối."""
        if piece_index == self.piece_manager.num_pieces - 1:
            # Piece cuối có thể nhỏ hơn piece_length chuẩn.
            return self.piece_manager.file_size - (
                piece_index * self.piece_manager.piece_length
            )
        return self.piece_manager.piece_length

    def _save_progress(self) -> None:
        """Lưu bitfield hiện tại vào file progress để hỗ trợ resume."""
        try:
            data = {
                "info_hash": self.info_hash,
                "bitfield": self.piece_manager.get_bitfield(),
                "timestamp": time.time()
            }
            with open(self.progress_file, "w") as f:
                json.dump(data, f)
        except Exception:
            pass

    def _remove_progress_file(self) -> None:
        """Xóa file progress sau khi file đích đã tải hoàn chỉnh."""
        if os.path.exists(self.progress_file):
            try:
                os.remove(self.progress_file)
            except Exception:
                pass

    def _load_progress(self) -> None:
        """
        Khôi phục tiến độ theo ba trường hợp:
        progress hợp lệ, file đã tồn tại nhưng chưa có progress, hoặc file mới.
        """
        target = self.piece_manager.target_file_path

        # Trường hợp 1: Có progress JSON hợp lệ thì khôi phục trực tiếp bitfield.
        if os.path.exists(self.progress_file):
            try:
                with open(self.progress_file, "r") as f:
                    data = json.load(f)

                if data.get("info_hash") != self.info_hash:
                    raise ValueError("info_hash không khớp")

                saved_bitfield = data.get("bitfield", [])
                if len(saved_bitfield) != self.piece_manager.num_pieces:
                    raise ValueError("Số mảnh không khớp")

                # Không cần quét lại toàn bộ file, vì bitfield đã được lưu trước đó.
                self.piece_manager.bitfield = saved_bitfield
                count = sum(1 for b in saved_bitfield if b)
                print(f"[TransferEngine] Resume từ JSON: {count}/{self.piece_manager.num_pieces} mảnh "
                      f"({self.piece_manager.progress_percentage():.1f}%)")
                return

            except Exception:
                # JSON hỏng hoặc không khớp thì quay về quét checksum trên đĩa.
                pass

        # Trường hợp 2: Có file cũ từ trước nhưng chưa có JSON → xác minh từng piece.
        if not getattr(self.piece_manager, "is_new_file", False) and os.path.exists(target) and os.path.getsize(target) > 0:
            print(f"[TransferEngine] Không có file progress, đang quét lại SHA-256... (có thể mất vài phút)")
            self.piece_manager.verify_all_pieces()
            count = sum(1 for b in self.piece_manager.bitfield if b)
            if count > 0:
                print(f"[TransferEngine] Quét xong: tìm thấy {count}/{self.piece_manager.num_pieces} mảnh hợp lệ")

        # Trường hợp 3: File mới → bitfield mặc định toàn False là đúng.

    def snapshot(self) -> Dict:
        """Ảnh chụp trạng thái tải cho giao diện (chỉ đọc, không đổi logic tải)."""
        pm = self.piece_manager
        have = pm.completed_pieces_count()
        # Piece cuối có thể nhỏ hơn; tính byte đã có chính xác theo từng piece.
        downloaded_bytes = sum(self._piece_size(i) for i, ok in enumerate(pm.bitfield) if ok)
        speed = self.download_meter.rate()
        left_bytes = pm.file_size - downloaded_bytes
        eta = (left_bytes / speed) if speed > 0 and left_bytes > 0 else None

        with self.lock:
            peers = []
            for p_id, info in self.connected_peers.items():
                bf = info.get("bitfield") or []
                meter = self.peer_meters.get(p_id)
                peers.append({
                    "peer_id": p_id,
                    "ip": info.get("ip"),
                    "port": info.get("port"),
                    "have": sum(1 for b in bf if b),
                    "rate": meter.rate() if meter else 0.0,
                    "received": meter.total if meter else 0,
                    "bad_data": info.get("bad_data_count", 0),
                })
            in_flight = len(self.in_flight_pieces)

        peers.sort(key=lambda p: (-p["rate"], p["peer_id"]))
        return {
            "info_hash": self.info_hash,
            "name": pm.info.get("name", ""),
            "file_size": pm.file_size,
            "num_pieces": pm.num_pieces,
            "have_pieces": have,
            "progress": round(pm.progress_percentage(), 1),
            "downloaded_bytes": downloaded_bytes,
            "download_rate": speed,
            "eta_seconds": eta,
            "peers": peers,
            "in_flight": in_flight,
            "running": self.is_running,
            "blocked": self.blocked_by_tracker,
            "tracker_ok": self.tracker_ok,
        }

    def stop(self) -> None:
        """Dừng worker logic và xóa danh sách Peer đang được quản lý."""
        was_running = self.is_running
        self.is_running = False
        # Lưu bitfield ngay khi dừng để lần "Tiếp tục" không phải tải lại mảnh đã có.
        if was_running and not self.piece_manager.is_complete():
            self._save_progress()
        with self.lock:
            for connection in tuple(self._worker_connections):
                connection.close()
            self._worker_connections.clear()
            self.connected_peers.clear()
        try:
            self.piece_manager.close()
        except Exception:
            pass

    def _record_bad_data(self, p_id: str, max_strikes: int = MAX_BAD_DATA_STRIKES) -> None:
        """
        Ghi nhận 1 lần peer gửi sai dữ liệu (sai type, sai piece_index, sai Base64, SHA-256 fail...).
        Khi đạt ngưỡng max_strikes thì tự động ngắt kết nối peer đó.
        """
        with self.lock:
            info = self.connected_peers.get(p_id)
            if info is None:
                return  # Peer đã bị xóa trước đó
            info["bad_data_count"] = info.get("bad_data_count", 0) + 1
            count = info["bad_data_count"]

        if count >= max_strikes:
            print(
                f"[TransferEngine {self.peer_id}] Peer {p_id} gửi sai dữ liệu "
                f"{count} lần (ngưỡng {max_strikes}) → Ngắt kết nối."
            )
            self._disconnect_peer(p_id)
        else:
            print(
                f"[TransferEngine {self.peer_id}] Peer {p_id} gửi sai dữ liệu "
                f"(lần {count}/{max_strikes})."
            )

    def _disconnect_peer(self, p_id: str) -> None:
        """
        Xóa peer khỏi danh sách đang dùng, đưa vào blacklist tạm (failed_peers)
        và hàng chờ báo cáo lên Tracker ở lần ANNOUNCE tiếp theo.

        Connection của từng worker tới peer này sẽ tự bị đóng khi worker
        dùng lại và gặp lỗi, hoặc khi worker kết thúc.
        """
        with self.lock:
            if self.connected_peers.pop(p_id, None) is None:
                return  # Đã bị worker khác ngắt trước đó
            self.failed_peers[p_id] = time.time()
            self.pending_bad_reports.add(p_id)
        print(f"[TransferEngine {self.peer_id}] Ngắt Peer {p_id}, sẽ báo cáo lên Tracker.")