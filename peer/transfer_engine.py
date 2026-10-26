import os
import json
import time
import socket
import base64
import random
import threading
from typing import Dict, List, Optional, Tuple
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

class TransferEngine:
    """
    Động cơ chuyển tải file P2P: Quản lý việc kết nối đến Tracker, kết nối trực tiếp đến các Peer khác,
    và điều phối các Worker Thread để tải song song các mảnh (Pieces) còn thiếu.
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

        self.is_running = False
        self.lock = threading.Lock()

        # Quản lý danh sách các peer đã kết nối
        self.connected_peers: Dict[str, Dict] = {}
        self.failed_peers: Dict[str, float] = {}

        # Peer đã chết, chờ được báo cáo lên Tracker ở lần ANNOUNCE tiếp theo
        self.pending_bad_reports: set = set()

        # Đánh dấu các piece đang được tải (tránh 2 luồng tải trùng piece)
        self.in_flight_pieces: set = set()

        # Đường dẫn file lưu vết tiến độ resume
        self.progress_file = f"{piece_manager.target_file_path}.progress.json"

    def start_download(self, peer_node=None) -> bool:
        """Bắt đầu quá trình tải tập tin song song."""
        self.is_running = True
        self.peer_node = peer_node
        self._load_progress() # Dùng load_progress tránh việc verify lại toàn bộ hash -> Lâu lắc

        print(f"[TransferEngine {self.peer_id}] Khởi động tải tập tin: {self.piece_manager.info['name']}")

        # Gửi ANNOUNCE ban đầu lên Tracker
        self._announce_to_tracker(event="started")

        # Bắt đầu luồng heartbeat announce định kỳ
        announce_thread = threading.Thread(target=self._periodic_announce, daemon=True)
        announce_thread.start()


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

    def _announce_to_tracker(self, event: str = "started") -> List[Dict]:
        """Gửi ANNOUNCE tới Tracker và lấy về danh sách các Peer khả dụng."""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5.0)
            sock.connect((self.tracker_host, self.tracker_port))

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
                bad_peers=bad_to_report  # Báo cáo peer chết
            )
            if bad_to_report:
                print(f"[TransferEngine {self.peer_id}] Báo cáo {len(bad_to_report)} peer chết lên Tracker: {bad_to_report}")
            send_msg(sock, msg)
            resp = recv_msg(sock)
            sock.close()

            # Tiếp theo xin danh sách peers
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
        except Exception as e:
            return []

    def _periodic_announce(self) -> None:
        """Gửi ANNOUNCE cập nhật định kỳ 20 giây một lần."""
        while self.is_running and not self.piece_manager.is_complete():
            time.sleep(20)
            peers = self._announce_to_tracker(event="started")
            if peers:
                self._update_peer_connections(peers)

    def _update_peer_connections(self, peers_list: List[Dict]) -> None:
        """Khám phá Peer mới từ Tracker và lưu thông tin bitfield."""
        for p_info in peers_list:
            p_id = p_info.get("peer_id")
            ip   = p_info.get("ip")
            port = p_info.get("port")

            if not p_id or not ip or not port:
                continue

            if p_id in self.failed_peers:
                if time.time() - self.failed_peers[p_id] < 120: 
                    continue

            # Kiểm tra nhanh trong lock (không làm network ở đây)
            with self.lock:
                if p_id in self.connected_peers or p_id == self.peer_id:
                    continue

            # Kết nối probe NGOÀI lock — chỉ để lấy bitfield
            sock = self._connect_and_handshake(ip, port)
            if not sock:
                continue

            try:
                bitfield_msg = recv_msg(sock)
                sock.close()
                if not bitfield_msg or bitfield_msg.get("type") != MessageType.BITFIELD.value:
                    continue
            except Exception:
                sock.close()
                continue

            bitfield = bitfield_msg.get("bitfield", [])

            with self.lock:
                if p_id not in self.connected_peers:
                    # Chỉ lưu ip/port/bitfield — không lưu socket (mỗi worker tự kết nối riêng)
                    self.connected_peers[p_id] = {
                        "bitfield":      bitfield,
                        "ip":            ip,
                        "port":          port,
                        "bad_data_count": 0   
                    }
                    have_count = sum(1 for b in bitfield if b)
                    print(f"[TransferEngine {self.peer_id}] Biết Peer {p_id} ({ip}:{port}) "
                          f"| {have_count}/{len(bitfield)} mảnh")

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
        """Vòng lặp của Worker Thread. Mỗi worker có bộ connection riêng — không tranh socket với nhau."""
        # Dict connection riêng của worker này: { peer_id: socket }
        my_conns: Dict[str, socket.socket] = {}

        while self.is_running and not self.piece_manager.is_complete():
            with self.lock:
                peer_bitfields = {p_id: info["bitfield"] for p_id, info in self.connected_peers.items()}
                missing = [p for p in self.piece_manager.missing_pieces() if p not in self.in_flight_pieces]

            if not missing or not peer_bitfields:
                peers = self._announce_to_tracker()
                if peers:
                    self._update_peer_connections(peers)
                time.sleep(2.0)
                continue

            target_piece = choose_next_piece(missing, peer_bitfields, strategy="rarest_first")
            if target_piece is None:
                time.sleep(0.5)
                continue

            # Claim piece — check + add trong cùng 1 lock
            with self.lock:
                if target_piece in self.in_flight_pieces:
                    continue
                self.in_flight_pieces.add(target_piece)

            # Tải mảnh bằng connection riêng của worker này
            success = self._download_piece(target_piece, my_conns)

            with self.lock:
                self.in_flight_pieces.discard(target_piece)

            if not success:
                time.sleep(0.5)

        # Dọn dẹp connection khi worker thoát
        for sock in my_conns.values():
            try:
                sock.close()
            except Exception:
                pass

    def _download_piece(self, piece_index: int, my_conns: Dict[str, "socket.socket"]) -> bool:
        """
        Tải 1 mảnh từ Peer bất kỳ sử dụng connection riêng của worker (độc lập, không bị block).
        my_conns: dict local của worker { peer_id: socket } — không dùng chung với worker khác.
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

        # Lấy hoặc tạo connection riêng của worker này tới peer này
        sock = my_conns.get(p_id)
        if sock is None:
            sock = self._connect_and_handshake(ip, port)
            if not sock:
                return False
            recv_msg(sock)   # Tiêu thụ BITFIELD mà Seeder gửi ngay sau handshake
            my_conns[p_id] = sock
        sock_lock = threading.Thread.Lock()  # Lock riêng cho socket này để tránh 2 workers cùng dùng socket
        acquired = sock_lock.acquire(timeout=10.0)
        if not acquired:
            return False
        try:
            # Giai đoạn 1: Gửi yêu cầu
            req_msg = create_request_msg(piece_index)
            sock.settimeout(15.0)
            try:
                send_msg(sock, req_msg)
            except OSError:
                self._disconnect_peer(p_id, sock)
                return False

            # Giai đoạn 2: Nhận phản hồi
            try:
                resp = recv_msg(sock)
            except (EOFError, socket.timeout, OSError):
                self._disconnect_peer(p_id, sock)
                return False
            except Exception:
                return False

            # Giai đoạn 3: Kiểm tra phản hồi
            if resp.get("type") != MessageType.PIECE.value:
                self._record_bad_data(p_id, sock)
                return False

            if resp.get("piece_index") != piece_index:
                self._record_bad_data(p_id, sock)
                return False

            # Giai đoạn 4: Kiểm tra dữ liệu Base64
            raw_b64 = resp.get("data")

            if not isinstance(raw_b64, str):
                self._record_bad_data(p_id, sock)
                return False

            piece_data = base64.b64decode(raw_b64)

            # Giai đoạn 5: Ghi mảnh
            try:
                written = self.piece_manager.write_piece(piece_index, piece_data)
            except OSError as e:
                print(f"Lỗi ghi piece {piece_index}: {e}")
                return False

            if not written:
                # SHA-256 không khớp → peer gửi dữ liệu hỏng, ghi nhận lỗi
                self._record_bad_data(p_id, sock)
                return False

            print(
                f"[TransferEngine {self.peer_id}] "
                f"Piece {piece_index} ✓ từ Peer {p_id} "
                f"({self.piece_manager.progress_percentage():.1f}%)"
            )
            if self.peer_node:
                self.peer_node.broadcast_have(piece_index)

            return True

        finally:
            # Luôn release lock dù thành công hay thất bại
            if acquired:
                sock_lock.release()

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
        with self.lock:
            for p_info in self.connected_peers.values():
                try:
                    p_info["sock"].close()
                except Exception:
                    pass
            self.connected_peers.clear()

    def _record_bad_data(self, p_id: str, sock, max_strikes: int = 4) -> None:
        """
        Ghi nhận 1 lần peer gửi sai dữ liệu (sai type, sai piece_index, sai Base64, SHA-256 fail...).
        Khi đạt ngưỡng max_strikes thì tự động ngắt kết nối peer đó.
        """
        with self.lock:
            info = self.connected_peers.get(p_id)
            if info is None or info.get("sock") is not sock:
                return  # Peer đã bị xóa trước đó

            info["bad_data_count"] += 1
            count = info["bad_data_count"]

        if count >= max_strikes:
            print(
                f"[TransferEngine {self.peer_id}] Peer {p_id} gửi sai dữ liệu "
                f"{count} lần (ngưỡng {max_strikes}) → Ngắt kết nối."
            )
            self._disconnect_peer(p_id, sock)
        else:
            print(
                f"[TransferEngine {self.peer_id}] Peer {p_id} gửi sai dữ liệu "
                f"(lần {count}/{max_strikes})."
            )

    def _disconnect_peer(self, p_id, sock):
        """Chỉ ngắt kết nối nếu peer vẫn đang dùng đúng socket này.
        Đồng thời ghi nhận peer này vào hàng chờ báo cáo lên Tracker.
        """
        with self.lock:
            current = self.connected_peers.get(p_id)

            if current is None or current.get("sock") is not sock:
                return

            try:
                sock.close()
            except OSError:
                pass

            del self.connected_peers[p_id]

            # Ghi vào failed_peers (blacklist tạm) và hàng chờ báo cáo Tracker
            self.failed_peers[p_id] = time.time()
            self.pending_bad_reports.add(p_id)
            self.failed_peers[p_id] = time.time()