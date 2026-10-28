import os
import json
import time
import socket
import base64
import hashlib
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
from protocol.ipv6 import normalize_ipv6_address

class TransferEngine:
    """
    Động cơ chuyển tải file P2P: Quản lý việc kết nối đến Tracker, kết nối trực tiếp đến các Peer khác,
    và điều phối các Worker Thread để tải song song các mảnh (Pieces) còn thiếu.
    """
    HEARTBEAT_INTERVAL = 20.0
    DISCOVERY_INTERVAL = 2.0
    PEER_RETRY_INTERVAL = 120.0
    MAX_BAD_DATA = 4

    def __init__(self, peer_id: str, tracker_host: str, tracker_port: int,
                 listen_port: int, piece_manager: PieceManager,
                 max_workers: int = 4, my_ip: str = ""):
        self.peer_id = peer_id
        self.tracker_host = normalize_ipv6_address(tracker_host)
        self.tracker_port = tracker_port
        self.listen_port = listen_port
        self.piece_manager = piece_manager
        self.max_workers = max_workers
        self.my_ip = normalize_ipv6_address(my_ip) if my_ip else ""
        self.announced_ip = ""
        self.info_hash = piece_manager.metainfo["info_hash"]

        self.is_running = False
        self.lock = threading.Lock()
        self._work_available = threading.Condition(self.lock)
        self._announce_stop = threading.Event()
        self._announce_thread: Optional[threading.Thread] = None
        self._worker_threads: List[threading.Thread] = []
        self._pending_sockets: set[socket.socket] = set()

        # Mỗi worker có một socket riêng tới từng peer, tối đa max_workers/socket peer.
        self.connected_peers: Dict[str, Dict] = {}
        self.failed_peers: Dict[str, float] = {}
        self.pending_bad_reports: set[str] = set()

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
        for i in range(self.max_workers):
            t = threading.Thread(target=self._worker_loop, args=(i,),
                                 name=f"piece-worker-{self.peer_id}-{i}", daemon=True)
            t.start()
            self._worker_threads.append(t)


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
        self.is_running = True
        peers = self._announce_to_tracker(event="started")
        if peers:
            self._update_peer_connections(peers)
        self._announce_thread = threading.Thread(
            target=self._periodic_announce,
            name=f"peer-discovery-{self.peer_id}", daemon=True
        )
        self._announce_thread.start()

    def _announce_to_tracker(self, event: str = "started") -> List[Dict]:
        """Gửi ANNOUNCE tới Tracker và lấy về danh sách các Peer khả dụng."""
        try:
            downloaded_bytes = int((self.piece_manager.progress_percentage() / 100.0) * self.piece_manager.file_size)
            left_bytes = self.piece_manager.file_size - downloaded_bytes
            with self.lock:
                bad_to_report = list(self.pending_bad_reports)[:256]

            msg = create_announce_msg(
                info_hash=self.info_hash,
                peer_id=self.peer_id,
                port=self.listen_port,
                ip=self.my_ip,          # Rỗng để Tracker dùng IPv6 nguồn của kết nối TCP.
                event=event,
                downloaded=downloaded_bytes,
                left=left_bytes,
                bad_peers=bad_to_report,
            )
            with socket.socket(socket.AF_INET6, socket.SOCK_STREAM) as sock:
                sock.settimeout(5.0)
                sock.connect((self.tracker_host, self.tracker_port))
                send_msg(sock, msg)
                resp = recv_msg(sock)

            if not isinstance(resp, dict) or resp.get("type") != MessageType.ANNOUNCE_RESPONSE.value:
                return []
            if resp.get("status") != "OK":
                print(f"[TransferEngine {self.peer_id}] Tracker từ chối ANNOUNCE: {resp.get('error', 'không rõ lỗi')}")
                return []
            with self.lock:
                self.pending_bad_reports.difference_update(bad_to_report)
            reported_ip = resp.get("ip")
            if reported_ip and reported_ip != self.announced_ip:
                self.announced_ip = reported_ip
                print(f"[TransferEngine {self.peer_id}] Tracker đã công bố IPv6 [{reported_ip}]:{self.listen_port}")

            if event == "stopped" or self.piece_manager.is_complete():
                return []

            return self._get_peers_from_tracker()
        except (EOFError, OSError, ValueError) as exc:
            print(f"[TransferEngine {self.peer_id}] Không kết nối được tracker IPv6: {exc}")
            return []

    def _get_peers_from_tracker(self) -> List[Dict]:
        """Chỉ luồng điều phối hỏi tracker; các worker không gọi tracker."""
        try:
            with socket.socket(socket.AF_INET6, socket.SOCK_STREAM) as sock:
                sock.settimeout(5.0)
                sock.connect((self.tracker_host, self.tracker_port))
                send_msg(sock, create_get_peers_msg(self.info_hash, self.peer_id))
                response = recv_msg(sock)
            if isinstance(response, dict) and response.get("type") == MessageType.GET_PEERS_RESPONSE.value:
                peers = response.get("peers")
                return peers if isinstance(peers, list) else []
        except (EOFError, OSError, ValueError) as exc:
            print(f"[TransferEngine {self.peer_id}] Không lấy được danh sách peer: {exc}")
        return []

    def _periodic_announce(self) -> None:
        """Một luồng duy trì heartbeat và tìm peer cho toàn bộ worker."""
        last_announce = time.monotonic()
        while self.is_running:
            interval = (self.HEARTBEAT_INTERVAL if self.piece_manager.is_complete()
                        else min(self.DISCOVERY_INTERVAL, self.HEARTBEAT_INTERVAL))
            if self._announce_stop.wait(interval):
                break
            now = time.monotonic()
            if self.piece_manager.is_complete() or now - last_announce >= self.HEARTBEAT_INTERVAL:
                event = "completed" if self.piece_manager.is_complete() else "started"
                peers = self._announce_to_tracker(event=event)
                last_announce = now
            else:
                peers = self._get_peers_from_tracker()
            if peers and not self.piece_manager.is_complete():
                self._update_peer_connections(peers)

    def _update_peer_connections(self, peers_list: List[Dict]) -> None:
        """Kết nối tới các Peer mới trong danh sách thu thập từ Tracker."""
        for p_info in peers_list:
            if not isinstance(p_info, dict):
                continue
            p_id = p_info.get("peer_id")
            ip   = p_info.get("ip")
            port = p_info.get("port")

            if (not isinstance(p_id, str) or not p_id or not isinstance(ip, str)
                    or not isinstance(port, int) or isinstance(port, bool)
                    or not 1 <= port <= 65535):
                continue
            try:
                ip = normalize_ipv6_address(ip)
            except ValueError:
                continue

            # Kiểm tra nhanh trong lock (không làm network ở đây)
            with self.lock:
                if not self.is_running:
                    break
                failed_at = self.failed_peers.get(p_id)
                if failed_at is not None:
                    if time.monotonic() - failed_at < self.PEER_RETRY_INTERVAL:
                        continue
                    del self.failed_peers[p_id]
                current = self.connected_peers.get(p_id)
                if p_id == self.peer_id or (current and (current["ip"], current["port"]) == (ip, port)):
                    continue

            # Kết nối thăm dò để lấy BITFIELD, không giữ lock trong lúc chờ mạng.
            session = self._open_peer_session(ip, port)
            if not session:
                continue
            sock, bitfield = session

            with self._work_available:
                previous = self.connected_peers.get(p_id)
                old_sockets = list(previous["sockets"].values()) if previous else []
                if previous and previous["spare_socket"] is not None:
                    old_sockets.append(previous["spare_socket"])
                if self.is_running:
                    self.connected_peers[p_id] = {
                        "bitfield": bitfield, "ip": ip, "port": port,
                        "sockets": {}, "spare_socket": sock, "bad_data_count": 0,
                    }
                    self._work_available.notify_all()
                else:
                    old_sockets.append(sock)
            for old_sock in old_sockets:
                self._close_socket(old_sock)
            if self.is_running:
                print(f"[TransferEngine {self.peer_id}] Kết nối P2P thành công tới Peer {p_id} ([{ip}]:{port})")

    def _open_peer_session(self, ip: str, port: int) -> Optional[Tuple[socket.socket, List[bool]]]:
        """Mở một TCP session độc lập và nhận bitfield của peer."""
        sock = None
        try:
            sock = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
            sock.settimeout(5.0)
            with self.lock:
                running = self.is_running
                if running:
                    self._pending_sockets.add(sock)
            if not running:
                sock.close()
                return None
            sock.connect((ip, port))
            send_msg(sock, create_handshake_msg(self.info_hash, self.peer_id))
            resp = recv_msg(sock)
            if not isinstance(resp, dict) or resp.get("type") != MessageType.HANDSHAKE_RESPONSE.value or resp.get("info_hash") != self.info_hash:
                raise ValueError("HANDSHAKE_RESPONSE không hợp lệ")
            bitfield_msg = recv_msg(sock)
            if not isinstance(bitfield_msg, dict) or bitfield_msg.get("type") != MessageType.BITFIELD.value:
                raise ValueError("Thiếu BITFIELD")
            bitfield = bitfield_msg.get("bitfield")
            if (not isinstance(bitfield, list) or len(bitfield) != self.piece_manager.num_pieces
                    or not all(isinstance(value, bool) for value in bitfield)):
                raise ValueError("BITFIELD không hợp lệ")
            return sock, bitfield
        except (EOFError, OSError, ValueError) as exc:
            if sock is not None:
                sock.close()
            if self.is_running:
                print(f"[TransferEngine {self.peer_id}] Không mở được phiên [{ip}]:{port}: {exc}")
            return None
        finally:
            if sock is not None:
                with self.lock:
                    self._pending_sockets.discard(sock)

    @staticmethod
    def _close_socket(sock: socket.socket) -> None:
        try:
            sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        sock.close()

    def _reserve_piece_locked(self) -> Optional[Tuple[int, str, str, int]]:
        """Chọn peer và giữ chỗ mảnh trong cùng một lần giữ self.lock."""
        peer_bitfields = {peer_id: info["bitfield"] for peer_id, info in self.connected_peers.items()}
        missing = [piece for piece in self.piece_manager.missing_pieces()
                   if piece not in self.in_flight_pieces]
        piece_index = choose_next_piece(missing, peer_bitfields, strategy="rarest_first")
        if piece_index is None:
            return None
        owners = [(peer_id, info) for peer_id, info in self.connected_peers.items()
                  if info["bitfield"][piece_index]]
        if not owners:
            return None
        peer_id, info = random.choice(owners)
        self.in_flight_pieces.add(piece_index)
        return piece_index, peer_id, info["ip"], info["port"]

    def _worker_loop(self, worker_id: int) -> None:
        """Mỗi worker dùng socket riêng và không tự hỏi tracker."""
        try:
            while self.is_running and not self.piece_manager.is_complete():
                with self._work_available:
                    reservation = self._reserve_piece_locked()
                    if reservation is None:
                        self._work_available.wait(timeout=0.5)
                        continue
                piece_index, peer_id, ip, port = reservation
                try:
                    success = self._download_piece_from_peer(piece_index, peer_id, ip, port, worker_id)
                finally:
                    with self._work_available:
                        self.in_flight_pieces.discard(piece_index)
                        self._work_available.notify_all()
                if not success:
                    self._announce_stop.wait(0.5)
        finally:
            self._close_worker_sockets(worker_id)

    def _worker_socket(self, worker_id: int, peer_id: str, ip: str,
                       port: int) -> Optional[socket.socket]:
        with self.lock:
            peer = self.connected_peers.get(peer_id)
            if not peer or (peer["ip"], peer["port"]) != (ip, port):
                return None
            sock = peer["sockets"].get(worker_id)
            if sock is not None:
                return sock
            spare = peer["spare_socket"]
            if spare is not None:
                peer["spare_socket"] = None
                peer["sockets"][worker_id] = spare
                return spare

        session = self._open_peer_session(ip, port)
        if not session:
            if self.is_running:
                self._disconnect_peer(peer_id, endpoint=(ip, port))
            return None
        sock, bitfield = session
        with self._work_available:
            peer = self.connected_peers.get(peer_id)
            if self.is_running and peer and (peer["ip"], peer["port"]) == (ip, port):
                peer["sockets"][worker_id] = sock
                peer["bitfield"] = bitfield
                self._work_available.notify_all()
                return sock
        self._close_socket(sock)
        return None

    def _discard_worker_socket(self, worker_id: int, peer_id: str,
                               sock: socket.socket) -> None:
        with self.lock:
            peer = self.connected_peers.get(peer_id)
            if peer and peer["sockets"].get(worker_id) is sock:
                del peer["sockets"][worker_id]
        self._close_socket(sock)

    def _close_worker_sockets(self, worker_id: int) -> None:
        with self.lock:
            sockets = [peer["sockets"].pop(worker_id) for peer in self.connected_peers.values()
                       if worker_id in peer["sockets"]]
        for sock in sockets:
            self._close_socket(sock)

    def _disconnect_peer(self, peer_id: str, sock=None, endpoint=None) -> None:
        """Quarantine the current peer and close all of its worker sessions."""
        with self._work_available:
            peer = self.connected_peers.get(peer_id)
            if peer is None:
                return
            if endpoint is not None and (peer["ip"], peer["port"]) != endpoint:
                return
            sockets = list(peer["sockets"].values())
            if peer["spare_socket"] is not None:
                sockets.append(peer["spare_socket"])
            if sock is not None and sock not in sockets:
                return  # A stale session must not remove a new registration.
            del self.connected_peers[peer_id]
            self.failed_peers[peer_id] = time.monotonic()
            self.pending_bad_reports.add(peer_id)
            self._work_available.notify_all()
        for session in sockets:
            self._close_socket(session)
        print(f"[TransferEngine {self.peer_id}] Tạm ngắt peer {peer_id}; chờ báo cáo lên tracker")

    def _record_bad_data(self, peer_id: str, sock, worker_id: int) -> None:
        with self.lock:
            peer = self.connected_peers.get(peer_id)
            if peer is None or sock not in peer["sockets"].values():
                return
            peer["bad_data_count"] += 1
            strikes = peer["bad_data_count"]
        print(f"[TransferEngine {self.peer_id}] Peer {peer_id} gửi dữ liệu lỗi ({strikes}/{self.MAX_BAD_DATA})")
        if strikes >= self.MAX_BAD_DATA:
            self._disconnect_peer(peer_id, sock)
        else:
            self._discard_worker_socket(worker_id, peer_id, sock)

    def _download_piece_from_peer(self, piece_index: int, peer_id: str, ip: str,
                                  port: int, worker_id: int) -> bool:
        sock = self._worker_socket(worker_id, peer_id, ip, port)
        if sock is None:
            return False
        try:
            sock.settimeout(15.0)
            send_msg(sock, create_request_msg(piece_index))
            resp = recv_msg(sock)
            if not isinstance(resp, dict) or resp.get("type") != MessageType.PIECE.value or resp.get("piece_index") != piece_index:
                raise ValueError("Thiếu hoặc sai phản hồi PIECE")
            piece_data = base64.b64decode(resp.get("data"), validate=True)
            try:
                written = self.piece_manager.write_piece(piece_index, piece_data)
            except OSError as exc:
                print(f"[TransferEngine {self.peer_id}] Lỗi ghi mảnh {piece_index}: {exc}")
                return False
            if not written:
                pm = self.piece_manager
                length = min(pm.piece_length, pm.file_size - piece_index * pm.piece_length)
                if (len(piece_data) != length
                        or hashlib.sha256(piece_data).hexdigest() != pm.expected_hashes[piece_index]):
                    raise ValueError("Mảnh sai độ dài hoặc SHA-256")
                print(f"[TransferEngine {self.peer_id}] Không ghi được mảnh {piece_index} xuống đĩa")
                return False  # Local storage errors must not penalize the remote peer.
            print(f"[TransferEngine {self.peer_id}] Piece {piece_index} ✓ từ Peer {peer_id} ({self.piece_manager.progress_percentage():.1f}%)")
            if self.peer_node:
                self.peer_node.broadcast_have(piece_index)
            return True
        except (EOFError, OSError) as exc:
            print(f"[TransferEngine {self.peer_id}] Mất kết nối tải mảnh {piece_index} từ {peer_id}: {exc}")
            if self.is_running:
                self._disconnect_peer(peer_id, sock)
            return False
        except (ValueError, TypeError) as exc:
            print(f"[TransferEngine {self.peer_id}] Lỗi mảnh {piece_index} từ {peer_id}: {exc}")
            self._record_bad_data(peer_id, sock, worker_id)
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
        with self._work_available:
            sockets = [sock for peer in self.connected_peers.values()
                       for sock in peer["sockets"].values()]
            sockets.extend(peer["spare_socket"] for peer in self.connected_peers.values()
                           if peer["spare_socket"] is not None)
            sockets.extend(self._pending_sockets)
            self._pending_sockets.clear()
            self.connected_peers.clear()
            self._work_available.notify_all()
        for sock in sockets:
            self._close_socket(sock)
        if self._announce_thread and self._announce_thread is not threading.current_thread():
            self._announce_thread.join(timeout=6.0)
        deadline = time.monotonic() + 6.0
        for worker in self._worker_threads:
            if worker is not threading.current_thread():
                worker.join(timeout=max(0.0, deadline - time.monotonic()))
