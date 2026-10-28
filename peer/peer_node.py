import socket
import threading
import time
from typing import Dict, Optional, Tuple

from storage.piece_manager import PieceManager
from protocol.messages import (
    MessageType,
    send_msg,
    recv_msg,
    create_bitfield_msg,
    create_piece_msg,
    create_choke_msg,
    create_unchoke_msg,
)
from .choking import ChokingManager


class PeerNode:
    """
    Server TCP lắng nghe kết nối từ các Peer khác để phục vụ upload (chia sẻ mảnh).

    Thay vì chỉ phục vụ 1 file như cũ, giờ PeerNode lưu một bảng:
        { info_hash -> PieceManager }
    Khi Peer khác kết nối đến và gửi HANDSHAKE(info_hash), PeerNode tra bảng
    để tìm đúng PieceManager tương ứng và phục vụ yêu cầu mảnh của file đó.
    """

    def __init__(self, peer_id: str, host: str, port: int,
                 piece_manager: Optional[PieceManager] = None,
                 regular_slots: int = 2,
                 optimistic_slots: int = 1,
                 choking_interval: float = 20.0,
                 measurement_window: float = 30.0,
                 upload_rate_limit: Optional[float] = None):
        self.peer_id = peer_id
        self.host = host
        self.port = port

        # Bảng quản lý file: { info_hash: PieceManager }
        # Dùng lock riêng vì có thể được cập nhật từ nhiều thread
        self._piece_managers: Dict[str, PieceManager] = {}
        self._pm_lock = threading.Lock()

        # Nếu truyền vào sẵn 1 file thì đăng ký luôn
        if piece_manager is not None:
            info_hash = piece_manager.metainfo["info_hash"]
            self._piece_managers[info_hash] = piece_manager

        # Giới hạn tốc độ upload (byte/giây) để giả lập mạng thật khi thí nghiệm.
        # None = không giới hạn. Mọi kết nối dùng chung một "đường truyền", nên
        # gửi tuần tự qua _upload_lock (giống một card mạng có băng thông cố định).
        self.upload_rate_limit = upload_rate_limit
        self._upload_lock = threading.Lock()

        self.server_socket: Optional[socket.socket] = None
        self.is_running = False

        # Danh sách socket đang kết nối đến từ Peer khác: { addr: socket }
        self._active_sockets: Dict[Tuple[str, int], socket.socket] = {}
        self._sock_lock = threading.Lock()
        # Mỗi socket cần một lock vì thread choking có thể gửi CHOKE/UNCHOKE
        # cùng lúc với thread upload đang gửi PIECE.
        self._send_locks: Dict[socket.socket, threading.Lock] = {}
        # Ánh xạ peer_id -> nhiều socket; nhiều worker không được chiếm nhiều slot.
        self._socket_peers: Dict[str, set[socket.socket]] = {}
        # Trạng thái CHOKE/UNCHOKE cuối cùng đã gửi trên từng socket.
        # None nghĩa là socket chưa nhận thông báo trạng thái nào.
        self._sent_choking_states: Dict[socket.socket, Optional[bool]] = {}
        # Chính sách quyết định Peer nào được upload và đo đóng góp của từng Peer.
        self._choking = ChokingManager(
            regular_slots=regular_slots,
            optimistic_slots=optimistic_slots,
            choking_interval=choking_interval,
            measurement_window=measurement_window,
        )

    # ── Quản lý file ──────────────────────────────────────────────────── #

    def add_file(self, info_hash: str, piece_manager: PieceManager) -> None:
        """Đăng ký một file mới vào PeerNode để phục vụ upload."""
        with self._pm_lock:
            self._piece_managers[info_hash] = piece_manager

    def remove_file(self, info_hash: str) -> bool:
        """Hủy đăng ký một file. Trả về True nếu file tồn tại và bị xóa."""
        with self._pm_lock:
            if info_hash in self._piece_managers:
                del self._piece_managers[info_hash]
                return True
            return False

    # ── Khởi động / Dừng ─────────────────────────────────────────────── #

    def start_listening(self) -> None:
        """Mở cổng TCP và bắt đầu chấp nhận kết nối ngầm."""
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(64)
        self.is_running = True

        print(f"[PeerNode {self.peer_id}] Lắng nghe tại {self.host}:{self.port}")

        t = threading.Thread(target=self._accept_loop, daemon=True)
        t.start()
        choking_thread = threading.Thread(target=self._choking_loop, daemon=True)
        choking_thread.start()

    def stop(self) -> None:
        """Đóng tất cả kết nối và server socket."""
        self.is_running = False

        with self._sock_lock:
            for sock in self._active_sockets.values():
                try:
                    sock.close()
                except Exception:
                    pass
            self._active_sockets.clear()
            self._send_locks.clear()
            self._socket_peers.clear()
            self._sent_choking_states.clear()

        if self.server_socket:
            try:
                self.server_socket.close()
            except Exception:
                pass

        print(f"[PeerNode {self.peer_id}] Đã dừng.")

    # ── Vòng lặp chấp nhận kết nối ───────────────────────────────────── #

    def _accept_loop(self) -> None:
        """Liên tục chấp nhận kết nối TCP mới và trao mỗi kết nối cho 1 thread xử lý."""
        while self.is_running:
            try:
                conn, addr = self.server_socket.accept()
                t = threading.Thread(
                    target=self._handle_peer_connection,
                    args=(conn, addr),
                    daemon=True,
                )
                t.start()
            except OSError:
                break  # server_socket đã bị đóng (do stop())
            except Exception:
                if not self.is_running:
                    break

    # ── Xử lý từng kết nối Peer ─────────────────────────────────────── #

    def _handle_peer_connection(self, conn: socket.socket, addr: Tuple[str, int]) -> None:
        """
        Phục vụ 1 kết nối P2P theo giao thức:
          1. Nhận HANDSHAKE → tra info_hash → tìm PieceManager
          2. Gửi HANDSHAKE_RESPONSE + BITFIELD
          3. Lặp nhận REQUEST → gửi PIECE (hoặc bỏ qua KEEP_ALIVE)
        """
        conn.settimeout(60.0)  # Tự đóng nếu Peer ngắt kết nối hoặc im lặng quá 60 giây

        with self._sock_lock:
            self._active_sockets[addr] = conn
            self._send_locks[conn] = threading.Lock()
            self._sent_choking_states[conn] = None

        remote_peer_id = None
        try:
            # Bước 1: Nhận HANDSHAKE
            msg = recv_msg(conn)
            if not msg or msg.get("type") != MessageType.HANDSHAKE.value:
                return

            info_hash = msg.get("info_hash")
            remote_peer_id = msg.get("peer_id")
            if not remote_peer_id:
                return

            self._choking.register_peer(remote_peer_id)
            with self._sock_lock:
                self._socket_peers.setdefault(remote_peer_id, set()).add(conn)

            # Tra bảng tìm PieceManager tương ứng với file được yêu cầu
            with self._pm_lock:
                pm = self._piece_managers.get(info_hash)

            if pm is None:
                return  # Không có file này → đóng kết nối

            # Bước 2: Phản hồi handshake và gửi BITFIELD
            self._send(conn, {
                "type": MessageType.HANDSHAKE_RESPONSE.value,
                "info_hash": info_hash,
                "peer_id": self.peer_id,
            })
            self._send(conn, create_bitfield_msg(pm.get_bitfield()))

            # Bước 3: Vòng lặp phục vụ REQUEST
            while self.is_running:
                req = recv_msg(conn)
                if not req:
                    break  # Peer đóng kết nối hoặc timeout

                req_type = req.get("type")

                if req_type == MessageType.REQUEST.value:
                    piece_idx = req.get("piece_index")
                    # REQUEST ngầm thể hiện Peer đang interested. Trạng thái này
                    # được quản lý theo peer_id, không theo từng connection.
                    self._choking.set_interested(remote_peer_id, True)
                    self._choking.recalculate()
                    is_unchoked = self._choking.is_unchoked(remote_peer_id)
                    if not is_unchoked:
                        if self._send(conn, create_choke_msg()):
                            with self._sock_lock:
                                 self._sent_choking_states[conn] = False
                        continue

                    # Chỉ gửi UNCHOKE khi trạng thái trên socket thay đổi
                    # hoặc socket chưa từng nhận trạng thái; không gửi lặp
                    # trong mọi REQUEST.
                    self._send_choking_state(conn, True)
                    if piece_idx is not None:
                        data = pm.read_piece(piece_idx)
                        if data:
                            if self.upload_rate_limit:
                                with self._upload_lock:
                                    time.sleep(len(data) / self.upload_rate_limit)
                            # Chỉ tính đóng góp sau khi sendall thành công; không
                            # tính request hoặc piece đọc lỗi là upload hợp lệ.
                            if self._send(conn, create_piece_msg(piece_idx, data)):
                                self._choking.record_upload(remote_peer_id, len(data))
                        else:
                            self._send(conn, {"type": MessageType.ERROR.value, "piece_index": piece_idx})
                elif req_type == MessageType.INTERESTED.value:
                    self._choking.set_interested(remote_peer_id, True)
                    self._choking.recalculate()

                elif req_type == MessageType.NOT_INTERESTED.value:
                    self._choking.set_interested(remote_peer_id, False)

                elif req_type == MessageType.KEEP_ALIVE.value:
                    pass  # Không làm gì, chỉ để giữ kết nối sống

        except socket.timeout:
            pass  # Timeout bình thường
        except Exception:
            pass
        finally:
            if remote_peer_id:
                with self._sock_lock:
                    peer_sockets = self._socket_peers.get(remote_peer_id)
                    if peer_sockets is not None:
                        peer_sockets.discard(conn)
                        if not peer_sockets:
                            self._socket_peers.pop(remote_peer_id, None)
                            self._choking.unregister_peer(remote_peer_id)
            with self._sock_lock:
                self._active_sockets.pop(addr, None)
                self._send_locks.pop(conn, None)
                self._sent_choking_states.pop(conn, None)
            try:
                conn.close()
            except Exception:
                pass

    def _send(self, sock: socket.socket, message: Dict) -> bool:
        """Serialize frames sent concurrently on the same TCP connection."""
        with self._sock_lock:
            send_lock = self._send_locks.get(sock)
        if send_lock is None:
            return False
        with send_lock:
            return send_msg(sock, message)

    def _choking_loop(self) -> None:
        """Định kỳ tính lại regular/optimistic slots và báo thay đổi cho Peer."""
        while self.is_running:
            time_to_wait = self._choking.choking_interval
            for _ in range(int(time_to_wait * 10)):
                if not self.is_running:
                    return
                threading.Event().wait(0.1)
            changed = self._choking.recalculate()
            self._sync_choking_messages(changed)

    def _send_choking_state(
        self,
        sock: socket.socket,
        is_unchoked: bool,
    ) -> None:
        """Gửi CHOKE/UNCHOKE một lần khi trạng thái của socket thay đổi."""
        with self._sock_lock:
            previous_state = self._sent_choking_states.get(sock)
        if previous_state is not None and previous_state == is_unchoked:
            return

        message = create_unchoke_msg() if is_unchoked else create_choke_msg()
        if self._send(sock, message):
            with self._sock_lock:
                self._sent_choking_states[sock] = is_unchoked

    def _sync_choking_messages(self, peer_ids: set[str]) -> None:
        """Chỉ chủ động báo UNCHOKE; CHOKE được trả lời khi Peer gửi REQUEST."""
        # Không gửi CHOKE chủ động: nếu CHOKE chen vào giữa REQUEST và PIECE,
        # phía tải sẽ bỏ đi và đọc nhầm PIECE cũ ở lần request sau.
        unchoked_ids = [p for p in peer_ids if self._choking.is_unchoked(p)]
        with self._sock_lock:
            sockets = {
                peer_id: tuple(self._socket_peers.get(peer_id, ()))
                for peer_id in unchoked_ids
            }
        message = create_unchoke_msg()
        for peer_sockets in sockets.values():
            for sock in peer_sockets:
                if self._send(sock, message):
                    with self._sock_lock:
                        self._sent_choking_states[sock] = True

    def record_download(self, peer_id: str, byte_count: int) -> None:
        """Ghi nhận số byte nhận được từ Peer để tính điểm Tit-for-Tat."""
        self._choking.record_download(peer_id, byte_count)

    # ── Broadcast HAVE ────────────────────────────────────────────────── #

    def broadcast_have(self, piece_index: int) -> None:
        """Thông báo đến mọi Peer đang kết nối rằng ta vừa có thêm 1 piece mới."""
        have_msg = {
            "type": MessageType.HAVE.value,
            "piece_index": piece_index,
        }
        with self._sock_lock:
            sockets = list(self._active_sockets.values())
        for sock in sockets:
            try:
                self._send(sock, have_msg)
            except Exception:
                pass
