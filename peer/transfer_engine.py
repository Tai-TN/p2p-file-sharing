import os
import json
import time
import socket
import base64
import binascii
import threading
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from typing import Dict, List, Optional, Tuple, Any
from storage.piece_manager import PieceManager
from protocol.messages import (
    MessageType,
    send_msg,
    recv_msg,
    create_announce_msg,
    create_get_peers_msg,
    create_handshake_msg,
    create_request_msg
)
from .selection import choose_next_piece
from .connection import PeerConnection
from .peer_selector import PeerSelector
from nat.stun_client import STUNClient, NATInfo, NATType
from nat.relay import open_relay
from protocol.auth import connect_tracker

FAILED_PEER_COOLDOWN = 120.0
MAX_BAD_DATA_STRIKES = 4

class TransferEngine:
    """
    Động cơ chuyển tải file P2P: Quản lý việc kết nối đến Tracker, kết nối trực tiếp đến các Peer khác,
    và điều phối các Worker Thread để tải song song các mảnh (Pieces) còn thiếu.
    """
    HEARTBEAT_INTERVAL = 20.0
    PUNCH_RETRY_INTERVAL = 30.0
    DIRECT_PROBE_TIMEOUT = 100.0

    def __init__(self, peer_id: str, tracker_host: str, tracker_port: int,
                 listen_port: int, piece_manager: PieceManager,
                 max_workers: int = 4, my_ip: str = "",
                 nat_info: Optional[NATInfo] = None,
                 signal_channel: Optional[Any] = None,
                 transport: str = "tcp", udp_transfer: Optional[Any] = None,
                 udp_parallel: int = 4, strategy: str = "rarest_first"):
        if not isinstance(udp_parallel, int) or not 1 <= udp_parallel <= 4:
            raise ValueError("udp_parallel phải trong khoảng 1–4")
        self.peer_id = peer_id
        self.tracker_host = tracker_host
        self.tracker_port = tracker_port
        self.listen_port = listen_port
        self.piece_manager = piece_manager
        self.max_workers = max_workers
        self.my_ip = my_ip          # IP LAN của máy — báo cho Tracker để peer khác kết nối đúng chỗ
        self.info_hash = piece_manager.metainfo["info_hash"]

        # Thông tin NAT từ STUN (None = chưa chạy STUN hoặc STUN thất bại)
        self.nat_info: Optional[NATInfo] = nat_info
        # Kênh signaling với Tracker để yêu cầu đục lỗ NAT khi kết nối trực tiếp thất bại
        self.signal_channel = signal_channel
        self.transport = transport
        self.udp_transfer = udp_transfer
        self.udp_parallel = udp_parallel
        self.strategy = strategy
        self._udp_session_cancel = threading.Event()

        self.is_running = False
        self.lock = threading.Lock()
        self._announce_stop = threading.Event()
        self._announce_thread: Optional[threading.Thread] = None
        self._last_tracker_error: Optional[str] = None
        self._next_punch_attempt: Dict[str, float] = {}
        self.current_path: Optional[str] = None
        self._auto_sock: Optional[socket.socket] = None
        self._worker_threads: List[threading.Thread] = []

        # Quản lý danh sách các peer đã kết nối
        self.connected_peers: Dict[str, Dict] = {}
        self._selectors = {path: PeerSelector() for path in
                           ("DIRECT_TCP", "DIRECT_UDP", "RELAY")}
        self.peer_selector = self._selectors["DIRECT_TCP"]
        self.failed_peers: Dict[str, float] = {}
        self.pending_bad_reports: set[str] = set()
        self._worker_connections: set[PeerConnection] = set()

        # Đánh dấu các piece đang được tải (tránh 2 luồng tải trùng piece)
        self.in_flight_pieces: set = set()

        # Đường dẫn file lưu vết tiến độ resume
        self.progress_file = f"{piece_manager.target_file_path}.progress.json"

    def start_download(self, peer_node=None) -> bool:
        """Bắt đầu quá trình tải tập tin song song."""
        self.peer_node = peer_node
        self._load_progress() # Dùng load_progress tránh việc verify lại toàn bộ hash -> Lâu lắc
        verified_before = sum(self.piece_manager._get_piece_bounds(i)[1]
                              for i in range(self.piece_manager.num_pieces)
                              if self.piece_manager.has_piece(i))
        time_start = time.monotonic()

        print(f"[TransferEngine {self.peer_id}] Khởi động tải tập tin: {self.piece_manager.info['name']}")

        self.start_announcing()


        # Tạo và chạy luồng Worker Pool tải song song
        worker_threads = []
        worker_count = 1 if self.transport in ("auto", "udp") else self.max_workers
        for i in range(worker_count):
            target = (self._auto_worker_loop if self.transport == "auto" else
                      self._udp_worker_loop if self.transport == "udp" else self._worker_loop)
            args = (i,) if self.transport == "tcp" else ()
            t = threading.Thread(target=target, args=args, daemon=True)
            t.start()
            worker_threads.append(t)
        self._worker_threads = worker_threads


        # Chờ cho tới khi tải xong 100% hoặc bị dừng
        # Lưu progress mỗi 10 giây thay vì mỗi 0.5s — tránh ghi JSON lớn liên tục
        last_save = 0
        while self.is_running and not self.piece_manager.is_complete():
            time.sleep(0.5)
            now = time.time()
            if now - last_save >= 10.0:
                self._save_progress()
                last_save = now

        if self.piece_manager.is_complete():
            print(f"[TransferEngine {self.peer_id}] TẢI HOÀN TẤT 100%! Bắt đầu chuyển sang chế độ Seeder.")
            elapsed = time.monotonic() - time_start
            new_bytes = self.piece_manager.file_size - verified_before
            total_MiB = self.piece_manager.file_size / 1_048_576
            speed_MiBs = new_bytes / 1_048_576 / elapsed if elapsed > 0 else 0
            speed_Mbps = new_bytes * 8 / 1_000_000 / elapsed if elapsed > 0 else 0
            print(f"[TransferEngine {self.peer_id}] TẢI HOÀN TẤT 100%!")
            print(f"  Thời gian : {elapsed:.2f} giây")
            print(f"  Kích thước: {total_MiB:.2f} MiB")
            print(f"  Đã tải lượt này: {new_bytes / 1_048_576:.2f} MiB")
            print(f"  Tốc độ TB : {speed_MiBs:.2f} MiB/s  ({speed_Mbps:.1f} Mbps)")
            self._announce_to_tracker(event="completed")
            self._remove_progress_file()
            return True
        
        return False

    def start_announcing(self) -> None:
        """Đăng ký file và duy trì heartbeat, kể cả khi đang seeding."""
        if self.is_running:
            return
        if self._announce_stop.is_set():
            return
        self._announce_to_tracker(event="started", raise_on_error=True)
        self.is_running = True
        print(f"[TransferEngine {self.peer_id}] Đã đăng ký với tracker {self.tracker_host}:{self.tracker_port}")
        self._announce_thread = threading.Thread(
            target=self._periodic_announce, daemon=True
        )
        self._announce_thread.start()

    def _tracker_request(self, message: Dict, response_type: MessageType) -> Dict:
        with connect_tracker(self.tracker_host, self.tracker_port, timeout=5.0) as sock:
            if not send_msg(sock, message):
                raise ConnectionError("Không gửi được yêu cầu")
            response = recv_msg(sock)
        if not isinstance(response, dict) or response.get("type") != response_type.value:
            raise ConnectionError(f"Phản hồi tracker không hợp lệ: {response!r}")
        return response

    def _announce_to_tracker(self, event: str = "started", raise_on_error: bool = False) -> List[Dict]:
        """Gửi ANNOUNCE tới Tracker và lấy về danh sách các Peer khả dụng."""
        bad_to_report: List[str] = []
        try:
            downloaded_bytes = int((self.piece_manager.progress_percentage() / 100.0) * self.piece_manager.file_size)
            left_bytes = self.piece_manager.file_size - downloaded_bytes

            # Đính kèm UDP endpoint từ STUN nếu có
            udp_port  = self.nat_info.public_port if self.nat_info else 0
            public_ip = self.nat_info.public_ip   if self.nat_info else ""
            with self.lock:
                bad_to_report = list(self.pending_bad_reports)
                self.pending_bad_reports.clear()

            msg = create_announce_msg(
                info_hash=self.info_hash,
                peer_id=self.peer_id,
                port=self.listen_port,
                ip=self.my_ip,          # IP LAN — Tracker dùng làm fallback
                event=event,
                downloaded=downloaded_bytes,
                left=left_bytes,
                udp_port=udp_port,
                public_ip=public_ip,
                bad_peers=bad_to_report,
            )
            resp = self._tracker_request(msg, MessageType.ANNOUNCE_RESPONSE)
            if resp.get("status") != "OK":
                raise ConnectionError(f"Tracker từ chối ANNOUNCE: {resp!r}")
            bad_to_report = []

            with self.lock:
                self._last_tracker_error = None

            if event == "stopped" or self.piece_manager.is_complete():
                return []

            # Tiếp theo xin danh sách peers
            get_p_msg = create_get_peers_msg(self.info_hash, self.peer_id)
            p_resp = self._tracker_request(get_p_msg, MessageType.GET_PEERS_RESPONSE)

            if p_resp and p_resp.get("peers"):
                return p_resp["peers"]
            return []
        except Exception as exc:
            if bad_to_report:
                with self.lock:
                    self.pending_bad_reports.update(bad_to_report)
            error = f"Tracker {self.tracker_host}:{self.tracker_port}: {exc}"
            if raise_on_error:
                raise ConnectionError(error) from exc
            with self.lock:
                should_log = error != self._last_tracker_error
                self._last_tracker_error = error
            if should_log:
                print(f"[TransferEngine {self.peer_id}] Lỗi ANNOUNCE: {error}")
            return []

    def _periodic_announce(self) -> None:
        """Gửi ANNOUNCE cập nhật định kỳ 20 giây một lần."""
        while not self._announce_stop.wait(self.HEARTBEAT_INTERVAL):
            event = "completed" if self.piece_manager.is_complete() else "started"
            peers = self._announce_to_tracker(event=event)
            if peers and self.transport == "tcp" and not self.piece_manager.is_complete():
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

    def _udp_worker_loop(self) -> None:
        """Một seeder mỗi lượt tải; không dùng TCP để lấy mảnh."""
        if not self.udp_transfer or not self.signal_channel:
            print(f"[TransferEngine {self.peer_id}] UDP cần STUN và signal channel với tracker")
            self.is_running = False
            return
        deadline = time.monotonic() + 100.0
        attempts = {}
        seeder_ids: List[str] = []
        while self.is_running and not self.piece_manager.is_complete() and not seeder_ids:
            peers = self._announce_to_tracker()
            for peer in peers:
                peer_id = peer.get("peer_id")
                if not peer_id or peer_id == self.peer_id or peer.get("left") != 0:
                    continue
                if self.udp_transfer.manager.get_endpoint(peer_id):
                    seeder_ids.append(peer_id)
                    continue
                now = time.monotonic()
                if (peer.get("udp_port") and attempts.get(peer_id, 0) < 3
                        and now >= self._next_punch_attempt.get(peer_id, 0.0)):
                    self._next_punch_attempt[peer_id] = now + self.PUNCH_RETRY_INTERVAL
                    attempts[peer_id] = attempts.get(peer_id, 0) + 1
                    print(f"[TransferEngine {self.peer_id}] Kiểm tra ứng viên UDP "
                          f"{peer_id}, lượt {attempts[peer_id]}/3")
                    self.signal_channel.request_punch(peer_id)
            if not seeder_ids:
                if time.monotonic() >= deadline:
                    print(f"[TransferEngine {self.peer_id}] Không tạo được đường UDP trực tiếp "
                          "sau 100 giây; cặp mạng này cần mở cổng hoặc relay")
                    self.is_running = False
                    return
                time.sleep(2.0)

        print(f"[TransferEngine {self.peer_id}] Tải UDP trực tiếp từ {seeder_ids}")
        if not self._download_udp_session(seeder_ids) and self.is_running:
            self.is_running = False

    def _connect_and_handshake(self, ip: str, port: int) -> Optional[socket.socket]:
        """Thực hiện kết nối socket TCP và thực hiện Bắt tay (Handshake) với Peer khác."""
        sock = None
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5.0)
            sock.connect((ip, port))

            hs_msg = create_handshake_msg(self.info_hash, self.peer_id)
            if not send_msg(sock, hs_msg):
                sock.close()
                return None

            resp = recv_msg(sock)
            if resp and resp.get("type") == MessageType.HANDSHAKE_RESPONSE.value:
                if resp.get("info_hash") == self.info_hash:
                    return sock
            sock.close()
            return None
        except Exception:
            if sock is not None:
                sock.close()
            return None

    def _handshake_existing_socket(self, sock: socket.socket) -> bool:
        if not send_msg(sock, create_handshake_msg(self.info_hash, self.peer_id)):
            return False
        response = recv_msg(sock)
        return (isinstance(response, dict)
                and response.get("type") == MessageType.HANDSHAKE_RESPONSE.value
                and response.get("info_hash") == self.info_hash)

    def _download_tcp_session(self, sock: socket.socket, peer_id: str, path: str) -> bool:
        """Tải các mảnh peer có qua một socket direct hoặc relay."""
        self.current_path = path
        self._auto_sock = sock
        connection = PeerConnection(self.peer_id, peer_id, self.info_hash, "", 0)
        selector = self._selectors[path]
        try:
            connection.attach_handshaken_socket(sock)
            if len(connection.bitfield) > self.piece_manager.num_pieces:
                return False
            connection.send_interested()
            print(f"[TransferEngine {self.peer_id}] Đường {path} tới {peer_id} sẵn sàng")
            choke_deadline = time.monotonic() + 20.0
            for piece_index in self.piece_manager.missing_pieces():
                if not self.is_running:
                    return False
                if piece_index >= len(connection.bitfield) or not connection.bitfield[piece_index]:
                    continue
                while self.is_running:
                    chosen = selector.select_peer([(peer_id, {})], self._piece_size(piece_index))
                    if chosen is None:
                        if time.monotonic() >= choke_deadline:
                            return False
                        time.sleep(0.1)
                        continue
                    try:
                        connection.set_timeout(15.0)
                        started = time.perf_counter()
                        response = connection.request_piece(piece_index)
                        elapsed = max(time.perf_counter() - started, 1e-6)
                    except (OSError, EOFError, ValueError):
                        selector.record_failure(peer_id)
                        raise
                    if response and response.get("type") == MessageType.CHOKE.value:
                        selector.record_choke(peer_id)
                        if time.monotonic() >= choke_deadline:
                            return False
                        continue
                    if (not response or response.get("type") != MessageType.PIECE.value
                            or response.get("piece_index") != piece_index
                            or not isinstance(response.get("data"), str)):
                        selector.record_failure(peer_id)
                        return False
                    try:
                        data = base64.b64decode(response["data"], validate=True)
                    except (ValueError, binascii.Error):
                        selector.record_failure(peer_id)
                        return False
                    if not self.piece_manager.write_piece(piece_index, data):
                        selector.record_failure(peer_id)
                        return False
                    selector.record_success(peer_id, len(data), elapsed)
                    if self.peer_node:
                        self.peer_node.record_download(peer_id, len(data))
                    break
                print(f"[TransferEngine {self.peer_id}] Piece {piece_index} ✓ {path} "
                      f"({self.piece_manager.progress_percentage():.1f}%)")
                if self.peer_node:
                    self.peer_node.broadcast_have(piece_index)
            return self.piece_manager.is_complete()
        except (OSError, EOFError, ValueError) as exc:
            print(f"[TransferEngine {self.peer_id}] Đường {path} lỗi: {exc}")
            return False
        finally:
            if self._auto_sock is sock:
                self._auto_sock = None
            connection.close()
            sock.close()

    def _find_udp_peer(self, peers: List[Dict]) -> Optional[str]:
        if not (self.nat_info and self.udp_transfer and self.signal_channel
                and self.signal_channel.is_connected):
            return None
        candidates = [p for p in peers if p.get("left") == 0
                      and isinstance(p.get("udp_port"), int) and p["udp_port"] > 0]
        if not candidates:
            return None
        deadline = min(time.monotonic() + self.DIRECT_PROBE_TIMEOUT,
                       getattr(self, "_route_deadline", float("inf")))
        attempts: Dict[str, int] = {}
        while self.is_running and time.monotonic() < deadline:
            for peer in candidates:
                peer_id = peer.get("peer_id")
                if not isinstance(peer_id, str):
                    continue
                if self.udp_transfer.manager.get_endpoint(peer_id):
                    return peer_id
                now = time.monotonic()
                if (attempts.get(peer_id, 0) < 3
                        and now >= self._next_punch_attempt.get(peer_id, 0)):
                    attempts[peer_id] = attempts.get(peer_id, 0) + 1
                    self._next_punch_attempt[peer_id] = now + self.PUNCH_RETRY_INTERVAL
                    print(f"[TransferEngine {self.peer_id}] Thử DIRECT_UDP tới {peer_id} "
                          f"{attempts[peer_id]}/3")
                    self.signal_channel.request_punch(peer_id)
            if not self.signal_channel.is_connected:
                break
            time.sleep(min(2.0, max(0.0, deadline - time.monotonic())))
        print(f"[TransferEngine {self.peer_id}] DIRECT_UDP không khả dụng sau "
              f"tối đa {self.DIRECT_PROBE_TIMEOUT:.0f} giây; chuyển RELAY")
        return None

    def _download_udp_session(self, peer_id: str | List[str]) -> bool:
        if self.udp_transfer is None:
            return False
        peer_ids = [peer_id] if isinstance(peer_id, str) else list(dict.fromkeys(peer_id))
        if not peer_ids:
            return False
        self.current_path = "DIRECT_UDP"
        print(f"[TransferEngine {self.peer_id}] DIRECT_UDP: {len(peer_ids)} peer, "
              f"{self.udp_parallel} mảnh song song")
        selector = self._selectors["DIRECT_UDP"]
        missing = iter(self.piece_manager.missing_pieces())
        started = time.monotonic()
        verified_bytes = 0
        failed = False
        active = {}

        with ThreadPoolExecutor(max_workers=self.udp_parallel,
                                thread_name_prefix="udp-piece") as pool:
            def submit(piece_index: int, attempt: int) -> bool:
                size = self._piece_size(piece_index)
                candidates = [(pid, {}) for pid in peer_ids]
                selected = None
                deadline = time.monotonic() + 2.0
                while self.is_running and time.monotonic() < deadline:
                    selected = selector.select_peer(candidates, size)
                    if selected is not None:
                        break
                    time.sleep(0.05)
                if selected is None:
                    return False
                chosen_id = selected[0]
                future = pool.submit(self.udp_transfer.request_piece,
                                     chosen_id, self.info_hash, piece_index, size,
                                     cancel_event=self._udp_session_cancel)
                active[future] = (piece_index, attempt, chosen_id, time.perf_counter())
                return True

            for _ in range(self.udp_parallel):
                piece_index = next(missing, None)
                if piece_index is None:
                    break
                if not submit(piece_index, 1):
                    failed = True
                    break

            while active and self.is_running and not self._udp_session_cancel.is_set():
                done, _ = wait(active, timeout=0.2, return_when=FIRST_COMPLETED)
                for future in done:
                    piece_index, attempt, chosen_id, request_started = active.pop(future)
                    try:
                        data = future.result()
                    except Exception as exc:
                        print(f"[TransferEngine {self.peer_id}] DIRECT_UDP mảnh "
                              f"{piece_index} lỗi: {exc}")
                        data = None
                    if not self.is_running or self._udp_session_cancel.is_set():
                        selector.record_failure(chosen_id)
                        break
                    if data is not None and self.piece_manager.write_piece(piece_index, data):
                        elapsed = max(time.perf_counter() - request_started, 1e-6)
                        selector.record_success(chosen_id, len(data), elapsed)
                        verified_bytes += len(data)
                        if self.peer_node:
                            self.peer_node.record_download(chosen_id, len(data))
                            self.peer_node.broadcast_have(piece_index)
                        print(f"[TransferEngine {self.peer_id}] Piece {piece_index} ✓ DIRECT_UDP "
                              f"({self.piece_manager.progress_percentage():.1f}%)")
                        next_piece = next(missing, None)
                        if next_piece is not None and not submit(next_piece, 1):
                            failed = True
                    else:
                        selector.record_failure(chosen_id)
                        if attempt < 3:
                            print(f"[TransferEngine {self.peer_id}] DIRECT_UDP mảnh "
                                  f"{piece_index} thất bại, thử {attempt}/3")
                            if not submit(piece_index, attempt + 1):
                                failed = True
                        else:
                            failed = True
                            self._udp_session_cancel.set()
                            break
                if failed:
                    self._udp_session_cancel.set()
                    break
            if active:
                self._udp_session_cancel.set()
                for future, (_, _, chosen_id, _) in list(active.items()):
                    future.cancel()
                    selector.record_failure(chosen_id)
                active.clear()

        elapsed = time.monotonic() - started
        rate = verified_bytes * 8 / 1_000_000 / elapsed if elapsed > 0 else 0
        print(f"[TransferEngine {self.peer_id}] DIRECT_UDP đã xác minh "
              f"{verified_bytes / 1_048_576:.2f} MiB trong {elapsed:.2f} giây "
              f"({rate:.1f} Mbps)")
        if failed and self.transport == "auto":
            print(f"[TransferEngine {self.peer_id}] DIRECT_UDP bị đứt; chuyển RELAY")
        return not failed and self.is_running and self.piece_manager.is_complete()

    def _auto_worker_loop(self) -> None:
        """Thử direct TCP, direct UDP (100 giây), rồi relay theo từng kết nối."""
        self._route_deadline = time.monotonic() + self.DIRECT_PROBE_TIMEOUT
        while self.is_running and not self.piece_manager.is_complete():
            peers = self._announce_to_tracker()
            if peers:
                break
            time.sleep(2.0)
        if not self.is_running or self.piece_manager.is_complete():
            return

        first_piece = next(iter(self.piece_manager.missing_pieces()), 0)
        piece_size = max(1, self._piece_size(first_piece))
        ranked_tcp = self._selectors["DIRECT_TCP"].rank_peers(
            [(p.get("peer_id"), p) for p in peers if isinstance(p.get("peer_id"), str)],
            piece_size)
        for _, peer in ranked_tcp:
            if not self.is_running:
                return
            if time.monotonic() >= self._route_deadline:
                break
            peer_id, ip, port = peer.get("peer_id"), peer.get("ip"), peer.get("port")
            if not (isinstance(peer_id, str) and isinstance(ip, str)
                    and isinstance(port, int) and 1 <= port <= 65535):
                continue
            sock = self._connect_and_handshake(ip, port)
            if sock and self._download_tcp_session(sock, peer_id, "DIRECT_TCP"):
                return

        if self.nat_info and self.signal_channel and self.signal_channel.is_connected:
            udp_peer = self._find_udp_peer(peers)
            udp_peers = ([udp_peer] + [p.get("peer_id") for p in peers
                         if p.get("peer_id") != udp_peer
                         and self.udp_transfer.manager.get_endpoint(p.get("peer_id", ""))]
                         if udp_peer else [])
            if udp_peers and self._download_udp_session(udp_peers):
                return
        else:
            print(f"[TransferEngine {self.peer_id}] Không có đường UDP; chuyển RELAY")

        no_progress = 0
        while self.is_running and not self.piece_manager.is_complete() and no_progress < 10:
            before = self.piece_manager.completed_pieces_count()
            peers = self._announce_to_tracker()
            ranked_relay = self._selectors["RELAY"].rank_peers(
                [(p.get("peer_id"), p) for p in peers if isinstance(p.get("peer_id"), str)],
                piece_size)
            for _, peer in ranked_relay:
                peer_id = peer.get("peer_id")
                if not isinstance(peer_id, str):
                    continue
                try:
                    self.current_path = "RELAY"
                    sock = open_relay(self.tracker_host, self.tracker_port,
                                      self.peer_id, peer_id, self.info_hash,
                                      on_socket=lambda connected: setattr(self, "_auto_sock", connected))
                    if not self.is_running:
                        sock.close()
                        return
                    if not self._handshake_existing_socket(sock):
                        sock.close()
                        raise ConnectionError("Handshake qua relay thất bại")
                    if self._download_tcp_session(sock, peer_id, "RELAY"):
                        return
                except (OSError, ConnectionError, PermissionError) as exc:
                    print(f"[TransferEngine {self.peer_id}] RELAY tới {peer_id} lỗi: {exc}")
                finally:
                    if self._auto_sock is not None and self._auto_sock.fileno() == -1:
                        self._auto_sock = None
            after = self.piece_manager.completed_pieces_count()
            no_progress = 0 if after > before else no_progress + 1
            if not self.piece_manager.is_complete():
                time.sleep(2.0)
        if not self.piece_manager.is_complete():
            print(f"[TransferEngine {self.peer_id}] Không tải được qua RELAY sau 10 lượt")
            self.is_running = False

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
        self.current_path = "DIRECT_TCP"
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

    def _save_progress(self) -> None:
        """Lưu tiến độ tải (.progress.json) hỗ trợ Resume download."""
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
        if os.path.exists(self.progress_file):
            try:
                os.remove(self.progress_file)
            except Exception:
                pass
    
    def _load_progress(self) -> None:
        """
        Khôi phục trạng thái tải, ưu tiên tốc độ: => có 3 trường hợp, có file progress, không có file progress
        => Verify all piecce, mới khởi taoj => Mảng mặc định là False
        """
        self.piece_manager.verify_all_pieces()
        count = self.piece_manager.completed_pieces_count()
        print(f"[TransferEngine] Đã xác minh trên đĩa: {count}/{self.piece_manager.num_pieces} mảnh")


    def stop(self) -> None:
        self.is_running = False
        self._udp_session_cancel.set()
        self._announce_stop.set()
        if self._auto_sock is not None:
            try:
                self._auto_sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            self._auto_sock.close()
        if self._announce_thread and self._announce_thread is not threading.current_thread():
            self._announce_thread.join()
        with self.lock:
            connections = list(self._worker_connections)
            self._worker_connections.clear()
            self.connected_peers.clear()
        for connection in connections:
            connection.close()
        for thread in self._worker_threads:
            if thread is not threading.current_thread():
                thread.join(timeout=2.0)
