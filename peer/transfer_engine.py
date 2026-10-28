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
<<<<<<< Updated upstream
=======
from .connection import PeerConnection
from .peer_selector import PeerSelector
>>>>>>> Stashed changes

class TransferEngine:
    """
    Động cơ chuyển tải file P2P: Quản lý việc kết nối đến Tracker, kết nối trực tiếp đến các Peer khác,
    và điều phối các Worker Thread để tải song song các mảnh (Pieces) còn thiếu.
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

        self.is_running = False
        self.lock = threading.Lock()

        # Quản lý danh sách các peer đã kết nối
        self.connected_peers: Dict[str, Dict] = {}
        # Một selector dùng chung cho mọi worker để đếm tải theo từng peer.
        self.peer_selector = PeerSelector()

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

<<<<<<< Updated upstream
        # Gửi ANNOUNCE ban đầu lên Tracker
        self._announce_to_tracker(event="started")
=======
        # Đăng ký bản thân với Tracker và lấy danh sách Peer hiện có.
        # Dùng luôn danh sách trả về; trước đây bị bỏ đi nên worker phải chờ 2s
        # rồi mới hỏi lại Tracker.
        peers = self._announce_to_tracker(event="started")
        if peers:
            self._update_peer_connections(peers)
>>>>>>> Stashed changes

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

            # Kiểm tra nhanh trong lock
            with self.lock:
                if p_id in self.connected_peers or p_id == self.peer_id:
                    continue

            # Kết nối probe NGOÀI lock — chỉ để lấy bitfield
            sock = self._connect_and_handshake(ip, port)
            if not sock:
                continue

            bitfield_msg = recv_msg(sock)
            sock.close()   # Đóng ngay — mỗi worker sẽ tự tạo connection riêng của mình

            if not bitfield_msg or bitfield_msg.get("type") != MessageType.BITFIELD.value:
                continue

            bitfield = bitfield_msg.get("bitfield", [])

            with self.lock:
                if p_id not in self.connected_peers:
                    # Chỉ lưu ip/port/bitfield — không lưu socket (mỗi worker tự kết nối riêng)
                    self.connected_peers[p_id] = {
                        "ip":      ip,
                        "port":    port,
                        "bitfield": bitfield
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

<<<<<<< Updated upstream
            target_piece = choose_next_piece(missing, peer_bitfields, strategy="rarest_first")
=======
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
>>>>>>> Stashed changes
            if target_piece is None:
                time.sleep(0.5)
                continue

            # Claim piece — check + add trong cùng 1 lock
            with self.lock:
<<<<<<< Updated upstream
                if target_piece in self.in_flight_pieces:
                    continue
                self.in_flight_pieces.add(target_piece)
=======
                already_taken = target_piece in self.in_flight_pieces
                if not already_taken:
                    self.in_flight_pieces.add(target_piece)
            if already_taken:
                # Ngủ NGOÀI lock (trước đây ngủ trong lock, chặn các worker khác).
                time.sleep(0.05)
                continue
>>>>>>> Stashed changes

            # Tải mảnh bằng connection riêng của worker này
            success = self._download_piece(target_piece, my_conns)

            with self.lock:
                self.in_flight_pieces.discard(target_piece)

            if not success:
                # Nghỉ ngắn. Peer lỗi/CHOKE đã bị PeerSelector tạm loại bằng
                # cooldown/backoff, nên không cần chặn cả worker 0.5s ở đây.
                time.sleep(0.05)

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

        # Selector chọn trong các peer có piece này và reserve một request.
        piece_size = self._piece_size(piece_index)
        selected = self.peer_selector.select_peer(available_peers, piece_size)
        if selected is None:
            return False
        p_id, p_info = selected
        ip   = p_info["ip"]
        port = p_info["port"]

<<<<<<< Updated upstream
        # Lấy hoặc tạo connection riêng của worker này tới peer này
        sock = my_conns.get(p_id)
        if sock is None:
            sock = self._connect_and_handshake(ip, port)
            if not sock:
                return False
            recv_msg(sock)   # Tiêu thụ BITFIELD mà Seeder gửi ngay sau handshake
            my_conns[p_id] = sock
=======
        # Tái sử dụng connection của worker; nếu chưa có thì handshake và báo interested.
        connection = my_conns.get(p_id)
        if connection is None:
            connection = self._create_connection(p_id, ip, port)
            if connection is None:
                # Đã reserve nhưng không tạo được connection nên phải release.
                self.peer_selector.record_failure(p_id)
                return False
            if not connection.send_interested():
                connection.close()
                # Gửi INTERESTED thất bại cũng được tính là request lỗi.
                self.peer_selector.record_failure(p_id)
                return False
            # INTERESTED là tín hiệu cho server biết worker đang cần piece.
            my_conns[p_id] = connection
            with self.lock:
                self._worker_connections.add(connection)
>>>>>>> Stashed changes

        request_settled = False
        try:
            req_msg = create_request_msg(piece_index)
            sock.settimeout(30.0)

<<<<<<< Updated upstream
            if not send_msg(sock, req_msg):
                raise ConnectionError("Gửi REQUEST thất bại")
=======
            # PeerConnection tự gửi REQUEST, xử lý các message trung gian và trả về
            # PIECE hoặc CHOKE cho TransferEngine.
            # Chỉ đo thời gian truyền request, không tính thời gian handshake.
            # perf_counter() có độ phân giải micro giây trên mọi hệ điều hành.
            # time.monotonic() trên Windows (Python <= 3.12) chỉ chính xác ~15,6 ms:
            # mảnh tải nhanh trên localhost sẽ đo ra 0 giây, record_success() báo lỗi
            # và lần tải THÀNH CÔNG bị tính là peer lỗi (bị phạt, backoff, đóng socket).
            request_started = time.perf_counter()
            resp = connection.request_piece(piece_index)
            # Chặn dưới 1 micro giây để không bao giờ chia cho 0.
            request_elapsed = max(time.perf_counter() - request_started, 1e-6)
            if not resp:
                raise ConnectionError("Peer đóng connection khi đang chờ message")
            if resp.get("type") == MessageType.CHOKE.value:
                # CHOKE là trạng thái tạm thời, không phải lỗi kết nối.
                self.peer_selector.record_choke(p_id)
                request_settled = True
                return False
>>>>>>> Stashed changes

            resp = recv_msg(sock)
            if resp and resp.get("type") == MessageType.PIECE.value:
                if resp.get("piece_index") == piece_index:
                    piece_data = base64.b64decode(resp["data"])
                    if self.piece_manager.write_piece(piece_index, piece_data):
<<<<<<< Updated upstream
=======
                        # Chỉ cập nhật tốc độ sau khi dữ liệu đã ghi và xác minh thành công.
                        self.peer_selector.record_success(
                            p_id,
                            len(piece_data),
                            request_elapsed,
                        )
                        request_settled = True
                        # Chỉ ghi nhận đóng góp sau khi checksum và ghi đĩa thành công.
                        # PeerNode dùng số liệu này để xếp hạng regular unchoking.
                        if self.peer_node:
                            self.peer_node.record_download(p_id, len(piece_data))
>>>>>>> Stashed changes
                        print(f"[Worker-{self.peer_id}] Piece {piece_index} ✓ ← {p_id} "
                              f"({self.piece_manager.progress_percentage():.1f}%)")
                        if self.peer_node:
                            self.peer_node.broadcast_have(piece_index)
                        return True

<<<<<<< Updated upstream
        except Exception:
            # Connection bị đứt — xóa khỏi my_conns để worker sẽ tạo lại lần sau
            try:
                my_conns.pop(p_id).close()
            except Exception:
                pass
            # Xóa peer khỏi connected_peers nếu tất cả worker đều gặp lỗi (doạn dưới)
            with self.lock:
                if p_id in self.connected_peers:
                    del self.connected_peers[p_id]
=======
            self.peer_selector.record_failure(p_id)
            request_settled = True
        except Exception as e:
            if not request_settled:
                # Mọi lỗi bất ngờ phải release pending đúng một lần.
                self.peer_selector.record_failure(p_id)
            # Connection lỗi hoặc timeout: đóng kết nối hiện tại của worker này để lần sau tạo lại socket mới
            connection = my_conns.pop(p_id, None)
            if connection is not None:
                connection.close()
                with self.lock:
                    self._worker_connections.discard(connection)
>>>>>>> Stashed changes

        return False

    def _piece_size(self, piece_index: int) -> int:
        """Trả về kích thước thực tế của piece, kể cả piece cuối."""
        if piece_index == self.piece_manager.num_pieces - 1:
            # Piece cuối có thể nhỏ hơn piece_length chuẩn.
            return self.piece_manager.file_size - (
                piece_index * self.piece_manager.piece_length
            )
        return self.piece_manager.piece_length

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
