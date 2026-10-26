import os
import json
import time
import socket
import base64
import random
import threading
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
from nat.stun_client import STUNClient, NATInfo, NATType

class TransferEngine:
    """
    Động cơ chuyển tải file P2P: Quản lý việc kết nối đến Tracker, kết nối trực tiếp đến các Peer khác,
    và điều phối các Worker Thread để tải song song các mảnh (Pieces) còn thiếu.
    """
    HEARTBEAT_INTERVAL = 20.0
    PUNCH_RETRY_INTERVAL = 30.0

    def __init__(self, peer_id: str, tracker_host: str, tracker_port: int,
                 listen_port: int, piece_manager: PieceManager,
                 max_workers: int = 4, my_ip: str = "",
                 nat_info: Optional[NATInfo] = None,
                 signal_channel: Optional[Any] = None):
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

        self.is_running = False
        self.lock = threading.Lock()
        self._announce_stop = threading.Event()
        self._announce_thread: Optional[threading.Thread] = None
        self._last_tracker_error: Optional[str] = None
        self._next_punch_attempt: Dict[str, float] = {}

        # Quản lý danh sách các peer đã kết nối
        self.connected_peers: Dict[str, Dict] = {}

        # Đánh dấu các piece đang được tải (tránh 2 luồng tải trùng piece)
        self.in_flight_pieces: set = set()

        # Đường dẫn file lưu vết tiến độ resume
        self.progress_file = f"{piece_manager.target_file_path}.progress.json"

    def start_download(self, peer_node=None) -> bool:
        """Bắt đầu quá trình tải tập tin song song."""
        self.peer_node = peer_node
        self._load_progress() # Dùng load_progress tránh việc verify lại toàn bộ hash -> Lâu lắc

        print(f"[TransferEngine {self.peer_id}] Khởi động tải tập tin: {self.piece_manager.info['name']}")

        self.start_announcing()


        # Tạo và chạy luồng Worker Pool tải song song
        worker_threads = []
        for i in range(self.max_workers):
            t = threading.Thread(target=self._worker_loop, args=(i,), daemon=True)
            t.start()
            worker_threads.append(t)


        time_start = time.time()

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
        with socket.create_connection((self.tracker_host, self.tracker_port), timeout=5.0) as sock:
            if not send_msg(sock, message):
                raise ConnectionError("Không gửi được yêu cầu")
            response = recv_msg(sock)
        if not isinstance(response, dict) or response.get("type") != response_type.value:
            raise ConnectionError(f"Phản hồi tracker không hợp lệ: {response!r}")
        return response

    def _announce_to_tracker(self, event: str = "started", raise_on_error: bool = False) -> List[Dict]:
        """Gửi ANNOUNCE tới Tracker và lấy về danh sách các Peer khả dụng."""
        try:
            downloaded_bytes = int((self.piece_manager.progress_percentage() / 100.0) * self.piece_manager.file_size)
            left_bytes = self.piece_manager.file_size - downloaded_bytes

            # Đính kèm UDP endpoint từ STUN nếu có
            udp_port  = self.nat_info.public_port if self.nat_info else 0
            public_ip = self.nat_info.public_ip   if self.nat_info else ""

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
            )
            resp = self._tracker_request(msg, MessageType.ANNOUNCE_RESPONSE)
            if resp.get("status") != "OK":
                raise ConnectionError(f"Tracker từ chối ANNOUNCE: {resp!r}")

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
            if peers and not self.piece_manager.is_complete():
                self._update_peer_connections(peers)

    def _update_peer_connections(self, peers_list: List[Dict]) -> None:
        """Kết nối tới các Peer mới trong danh sách thu thập từ Tracker."""
        for p_info in peers_list:
            p_id = p_info.get("peer_id")
            ip   = p_info.get("ip")
            port = p_info.get("port")

            if not p_id or not ip or not port:
                continue

            # Kiểm tra nhanh trong lock (không làm network ở đây)
            with self.lock:
                if p_id in self.connected_peers or p_id == self.peer_id:
                    continue

            # Làm network NGOÀI lock — worker khác vẫn tải bình thường trong lúc này
            sock = self._connect_and_handshake(ip, port)

            # Nếu kết nối trực tiếp thất bại và ta có signal channel, thử đục lỗ NAT qua Tracker
            if not sock and self.signal_channel and getattr(self.signal_channel, "is_connected", False):
                udp_port = p_info.get("udp_port", 0)
                if udp_port:
                    now = time.monotonic()
                    with self.lock:
                        should_request = now >= self._next_punch_attempt.get(p_id, 0.0)
                        if should_request:
                            self._next_punch_attempt[p_id] = now + self.PUNCH_RETRY_INTERVAL
                    if should_request:
                        print(f"[TransferEngine {self.peer_id}] Kết nối TCP tới {p_id} ({ip}:{port}) thất bại. Gửi PUNCH_REQUEST...")
                        try:
                            self.signal_channel.request_punch(p_id)
                        except Exception as e:
                            print(f"[TransferEngine {self.peer_id}] Lỗi khi yêu cầu đục lỗ NAT tới {p_id}: {e}")

            if not sock:
                continue

            bitfield_msg = recv_msg(sock)
            if not bitfield_msg or bitfield_msg.get("type") != MessageType.BITFIELD.value:
                sock.close()
                continue

            bitfield = bitfield_msg.get("bitfield", [])

            # Thêm peer vào danh sách (trong lock, nhanh)
            with self.lock:
                if p_id not in self.connected_peers:
                    self.connected_peers[p_id] = {
                        "sock":      sock,
                        "sock_lock": threading.Lock(),
                        "bitfield":  bitfield,
                        "ip":        ip,
                        "port":      port
                    }
                    print(f"[TransferEngine {self.peer_id}] Kết nối P2P thành công tới Peer {p_id} ({ip}:{port})")
                else:
                    sock.close()  # Worker khác vừa kết nối peer này → đóng socket thừa

    def _connect_and_handshake(self, ip: str, port: int) -> Optional[socket.socket]:
        """Thực hiện kết nối socket TCP và thực hiện Bắt tay (Handshake) với Peer khác."""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5.0)
            sock.connect((ip, port))

            hs_msg = create_handshake_msg(self.info_hash, self.peer_id)
            send_msg(sock, hs_msg)

            resp = recv_msg(sock)
            if resp and resp.get("type") == MessageType.HANDSHAKE_RESPONSE.value:
                if resp.get("info_hash") == self.info_hash:
                    return sock
            sock.close()
            return None
        except Exception:
            return None

    def _worker_loop(self, worker_id: int) -> None:
        """Vòng lặp làm việc của Worker Thread: Chọn mảnh hiếm nhất và xin dữ liệu từ Peer."""
        while self.is_running and not self.piece_manager.is_complete():
            # Kiểm tra và cập nhật danh sách peer
            with self.lock:
                peer_bitfields = {p_id: info["bitfield"] for p_id, info in self.connected_peers.items()}
                missing = [p for p in self.piece_manager.missing_pieces() if p not in self.in_flight_pieces]

            if not missing or not peer_bitfields:
                # Nếu chưa có peer kết nối, thử hỏi lại Tracker
                peers = self._announce_to_tracker()
                if peers:
                    self._update_peer_connections(peers)
                time.sleep(2.0)
                continue

            # Sử dụng thuật toán Rarest-First để chọn mảnh tiếp theo
            target_piece = choose_next_piece(missing, peer_bitfields, strategy="rarest_first")
            if target_piece is None:
                time.sleep(0.5)
                continue

            # Đánh dấu mảnh này đang được luồng worker_id xử lý
            with self.lock:
                self.in_flight_pieces.add(target_piece)

            # Tìm Peer sở hữu piece này để xin
            success = self._download_piece_from_any_peer(target_piece)
            
            with self.lock:
                self.in_flight_pieces.remove(target_piece)

            if not success:
                time.sleep(0.5)

    def _download_piece_from_any_peer(self, piece_index: int) -> bool:
        """Tìm Peer có mảnh piece_index và gửi tin nhắn REQUEST."""
        with self.lock:
            available_peers = [
                (p_id, info) for p_id, info in self.connected_peers.items()
                if piece_index < len(info["bitfield"]) and info["bitfield"][piece_index]
            ]

        if not available_peers:
            return False

        # Chọn ngẫu nhiên 1 peer trong danh sách (phân tải đều giữa nhiều Seeder)
        p_id, p_info = random.choice(available_peers)
        sock = p_info["sock"]
        sock_lock = p_info["sock_lock"]

        # Khóa socket của peer này — chỉ 1 worker được gửi/nhận tại 1 thời điểm
        acquired = sock_lock.acquire(timeout=10.0)
        if not acquired:
            return False  # Peer đang bận, thử lại sau

        try:
            req_msg = create_request_msg(piece_index)
            sock.settimeout(15.0)
            
            if not send_msg(sock, req_msg):
                return False

            resp = recv_msg(sock)
            if resp and resp.get("type") == MessageType.PIECE.value:
                if resp.get("piece_index") == piece_index:
                    raw_b64 = resp.get("data")
                    piece_data = base64.b64decode(raw_b64)

                    # Ghi mảnh xuống đĩa và kiểm tra SHA-256
                    if self.piece_manager.write_piece(piece_index, piece_data):
                        print(f"[TransferEngine {self.peer_id}] Piece {piece_index} ✓ từ Peer {p_id} ({self.piece_manager.progress_percentage():.1f}%)")
                        
                        # Phát tin nhắn HAVE đến tất cả các peer kết nối nếu có peer_node
                        if self.peer_node:
                            self.peer_node.broadcast_have(piece_index)
                        return True
        except Exception as e:
            # Ngắt kết nối peer bị lỗi
            with self.lock:
                if p_id in self.connected_peers:
                    try:
                        self.connected_peers[p_id]["sock"].close()
                    except Exception:
                        pass
                    del self.connected_peers[p_id]
        finally:
            # Chỉ release nếu đã acquire thành công
            if acquired:
                sock_lock.release()
        return False

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
        target = self.piece_manager.target_file_path

        # Trường hợp 1: Có file .progress.json
        if os.path.exists(self.progress_file):
            try:
                with open(self.progress_file, "r") as f:
                    data = json.load(f)

                if data.get("info_hash") != self.info_hash:
                    raise ValueError("info_hash không khớp")

                saved_bitfield = data.get("bitfield", [])
                if len(saved_bitfield) != self.piece_manager.num_pieces:
                    raise ValueError("Số mảnh không khớp")

                # Khôi phục bitfield từ JSON — không cần đọc lại đĩa
                self.piece_manager.bitfield = saved_bitfield
                count = sum(1 for b in saved_bitfield if b)
                print(f"[TransferEngine] Resume từ JSON: {count}/{self.piece_manager.num_pieces} mảnh "
                      f"({self.piece_manager.progress_percentage():.1f}%)")
                return

            except Exception:
                # JSON lỗi → rơi xuống trường hợp 2
                pass

        # Trường hợp 2: Không có JSON nhưng file đã tồn tại → quét lại
        if os.path.exists(target) and os.path.getsize(target) > 0:
            print(f"[TransferEngine] Không có file progress, đang quét lại SHA-256... (có thể mất vài phút)")
            self.piece_manager.verify_all_pieces()
            count = sum(1 for b in self.piece_manager.bitfield if b)
            if count > 0:
                print(f"[TransferEngine] Quét xong: tìm thấy {count}/{self.piece_manager.num_pieces} mảnh hợp lệ")

        # Trường hợp 3: File mới rỗng → không cần làm gì (bitfield toàn False)


    def stop(self) -> None:
        self.is_running = False
        self._announce_stop.set()
        if self._announce_thread and self._announce_thread is not threading.current_thread():
            self._announce_thread.join()
        with self.lock:
            for p_info in self.connected_peers.values():
                try:
                    p_info["sock"].close()
                except Exception:
                    pass
            self.connected_peers.clear()
