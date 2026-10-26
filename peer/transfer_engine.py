import os
import json
import time
import socket
import base64
import random
import threading
from typing import Dict, List, Optional
from storage.piece_manager import PieceManager
from protocol.messages import (
    MessageType,
    send_msg,
    recv_msg,
    create_announce_msg,
    create_get_peers_msg,
    create_request_msg,
)
from .selection import choose_next_piece
from .connection import PeerConnection

class TransferEngine:
    """
    Điều phối quá trình tải file P2P.

    Lớp này kết nối Tracker, khám phá các Peer, tạo worker tải song song
    và phối hợp với PeerNode để cập nhật đóng góp cho Tit-for-Tat.
    """
    def __init__(self, peer_id: str, tracker_host: str, tracker_port: int,
                 listen_port: int, piece_manager: PieceManager,
                 max_workers: int = 4, my_ip: str = ""):
        self.peer_id = peer_id
        self.tracker_host = tracker_host
        self.tracker_port = tracker_port
        self.listen_port = listen_port
        self.piece_manager = piece_manager
        self.max_workers = max_workers
        self.my_ip = my_ip          # IP thực của máy — báo cho Tracker để peer khác kết nối đúng chỗ
        self.info_hash = piece_manager.metainfo["info_hash"]

        self.is_running = False  # Cờ điều khiển toàn bộ worker và luồng announce.
        self.lock = threading.Lock()  # Bảo vệ danh sách Peer và piece đang được tải.

        # Bảng thông tin các Peer đã biết từ Tracker:
        # peer_id -> địa chỉ TCP và bitfield hiện tại.
        self.connected_peers: Dict[str, Dict] = {}

        # Các piece đã được một worker nhận xử lý; tránh nhiều worker tải trùng.
        self.in_flight_pieces: set = set()
        # Các connection lâu dài do worker tạo; dùng để đóng ngay khi engine dừng.
        self._worker_connections: set[PeerConnection] = set()

        # File tạm lưu bitfield để có thể tiếp tục download sau khi khởi động lại.
        self.progress_file = f"{piece_manager.target_file_path}.progress.json"

    def start_download(self, peer_node=None) -> bool:
        """Khởi động các worker và tải các piece còn thiếu song song."""
        self.is_running = True
        self.peer_node = peer_node
        # Khôi phục tiến độ trước khi tạo worker, tránh tải lại piece đã hợp lệ.
        self._load_progress()

        print(f"[TransferEngine {self.peer_id}] Khởi động tải tập tin: {self.piece_manager.info['name']}")

        # Đăng ký bản thân với Tracker và lấy danh sách Peer hiện có.
        self._announce_to_tracker(event="started")

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
            print(f"[TransferEngine {self.peer_id}] TẢI HOÀN TẤT 100%! Bắt đầu chuyển sang chế độ Seeder.")
            elapsed   = time.time() - time_start
            total_MB  = self.piece_manager.file_size / 1_048_576
            speed_MBs = total_MB / elapsed if elapsed > 0 else 0
            speed_Mbs = speed_MBs * 8
            print(f"[TransferEngine {self.peer_id}] TẢI HOÀN TẤT 100%!")
            print(f"  Thời gian : {elapsed:.2f} giây")
            print(f"  Kích thước: {total_MB:.2f} MB")
            print(f"  Tốc độ TB : {speed_MBs:.2f} MB/s  ({speed_Mbs:.1f} Mbps)")
            self._announce_to_tracker(event="completed")
            self._remove_progress_file()
            return True
        
        return False

    def _announce_to_tracker(self, event: str = "started") -> List[Dict]:
        """Thông báo trạng thái với Tracker rồi lấy danh sách Peer khả dụng."""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5.0)
            sock.connect((self.tracker_host, self.tracker_port))

            # Tracker chỉ cần biết tiến độ tổng quan, không tham gia choking.
            downloaded_bytes = int((self.piece_manager.progress_percentage() / 100.0) * self.piece_manager.file_size)
            left_bytes = self.piece_manager.file_size - downloaded_bytes

            msg = create_announce_msg(
                info_hash=self.info_hash,
                peer_id=self.peer_id,
                port=self.listen_port,
                ip=self.my_ip,          # IP thực để Tracker lưu đúng địa chỉ
                event=event,
                downloaded=downloaded_bytes,
                left=left_bytes
            )
            send_msg(sock, msg)
            recv_msg(sock)
            sock.close()

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

            # Chỉ đọc bảng trong lock; không giữ lock trong lúc kết nối mạng.
            with self.lock:
                if p_id in self.connected_peers or p_id == self.peer_id:
                    continue

            # Connection probe chỉ dùng lấy bitfield, worker sẽ tạo connection dài hạn riêng.
            connection = self._create_connection(p_id, ip, port)
            if connection is None:
                continue

            bitfield = connection.bitfield
            connection.close()

            with self.lock:
                if p_id not in self.connected_peers:
                    # Không lưu socket dùng chung; mỗi worker tự quản lý socket của mình.
                    self.connected_peers[p_id] = {
                        "ip":      ip,
                        "port":    port,
                        "bitfield": bitfield
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
        """Tạo PeerConnection và thực hiện handshake với Peer từ xa."""
        connection = PeerConnection(
            local_peer_id=self.peer_id,
            remote_peer_id=peer_id,
            info_hash=self.info_hash,
            ip=ip,
            port=port,
            on_have=self._on_peer_have,
        )
        if not connection.connect():
            return None
        return connection

    def _worker_loop(self, worker_id: int) -> None:
        """Worker chọn piece, tải piece và tự quản lý các socket của mình."""
        # Socket riêng của worker: { peer_id: socket }.
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

            target_piece = choose_next_piece(missing, peer_bitfields, strategy="rarest_first")
            if target_piece is None:
                time.sleep(0.3)
                continue

            # Nhận quyền tải piece trong cùng lock để hai worker không lấy trùng.
            with self.lock:
                if target_piece in self.in_flight_pieces:
                    time.sleep(0.05)
                    continue
                self.in_flight_pieces.add(target_piece)

            # Bị CHOKE chỉ làm request này thất bại; Peer vẫn được giữ lại để thử sau.
            success = self._download_piece(target_piece, my_conns)

            with self.lock:
                self.in_flight_pieces.discard(target_piece)

            if not success:
                time.sleep(0.5)

        # Đóng các socket do worker sở hữu khi worker kết thúc.
        for connection in my_conns.values():
            connection.close()
            with self.lock:
                self._worker_connections.discard(connection)

    def _download_piece(self, piece_index: int, my_conns: Dict[str, PeerConnection]) -> bool:
        """
        Tải một piece bằng socket riêng của worker.

        ``my_conns`` có dạng ``{peer_id: PeerConnection}`` và không được
        dùng chung giữa các worker; slot choking vẫn được tính theo peer_id.
        """
        with self.lock:
            available_peers = [
                (p_id, info) for p_id, info in self.connected_peers.items()
                if piece_index < len(info["bitfield"]) and info["bitfield"][piece_index]
            ]

        if not available_peers:
            return False

        p_id, p_info = random.choice(available_peers)
        ip   = p_info["ip"]
        port = p_info["port"]

        # Tái sử dụng connection của worker; nếu chưa có thì handshake và báo interested.
        connection = my_conns.get(p_id)
        if connection is None:
            connection = self._create_connection(p_id, ip, port)
            if connection is None:
                return False
            if not connection.send_interested():
                connection.close()
                return False
            # INTERESTED là tín hiệu cho server biết worker đang cần piece.
            my_conns[p_id] = connection
            with self.lock:
                self._worker_connections.add(connection)

        try:
            connection.set_timeout(45.0)

            # PeerConnection tự gửi REQUEST, xử lý các message trung gian và trả về
            # PIECE hoặc CHOKE cho TransferEngine.
            resp = connection.request_piece(piece_index)
            if not resp:
                raise ConnectionError("Peer đóng connection khi đang chờ message")
            if resp.get("type") == MessageType.CHOKE.value:
                # CHOKE là trạng thái tạm thời, không phải lỗi kết nối.
                return False

            if resp.get("type") == MessageType.PIECE.value:
                if resp.get("piece_index") != piece_index:
                    raise ConnectionError("PIECE sai số mảnh, luồng tin bị lệch")
                if resp.get("piece_index") == piece_index:
                    piece_data = base64.b64decode(resp["data"])
                    if self.piece_manager.write_piece(piece_index, piece_data):
                        # Chỉ ghi nhận đóng góp sau khi checksum và ghi đĩa thành công.
                        # PeerNode dùng số liệu này để xếp hạng regular unchoking.
                        if self.peer_node:
                            self.peer_node.record_download(p_id, len(piece_data))
                        print(f"[Worker-{self.peer_id}] Piece {piece_index} ✓ ← {p_id} "
                              f"({self.piece_manager.progress_percentage():.1f}%)")
                        if self.peer_node:
                            self.peer_node.broadcast_have(piece_index)
                        return True

        except Exception as e:
            # Connection lỗi hoặc timeout: đóng kết nối hiện tại của worker này để lần sau tạo lại socket mới
            connection = my_conns.pop(p_id, None)
            if connection is not None:
                connection.close()
                with self.lock:
                    self._worker_connections.discard(connection)

        return False

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


    def stop(self) -> None:
        """Dừng worker logic và xóa danh sách Peer đang được quản lý."""

        self.is_running = False
        with self.lock:
            for connection in tuple(self._worker_connections):
                connection.close()
            self._worker_connections.clear()
            self.connected_peers.clear()
        try:
            self.piece_manager.close()
        except Exception:
            pass
