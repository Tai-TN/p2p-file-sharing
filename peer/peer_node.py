import socket
import threading
from typing import Dict, Optional, Tuple

from storage.piece_manager import PieceManager
from protocol.messages import (
    MessageType,
    send_msg,
    recv_msg,
    create_bitfield_msg,
    create_piece_msg,
)


class PeerNode:
    """
    Server TCP lắng nghe kết nối từ các Peer khác để phục vụ upload (chia sẻ mảnh).

    Thay vì chỉ phục vụ 1 file như cũ, giờ PeerNode lưu một bảng:
        { info_hash -> PieceManager }
    Khi Peer khác kết nối đến và gửi HANDSHAKE(info_hash), PeerNode tra bảng
    để tìm đúng PieceManager tương ứng và phục vụ yêu cầu mảnh của file đó.
    """

    def __init__(self, peer_id: str, host: str, port: int,
                 piece_manager: Optional[PieceManager] = None):
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

        self.server_socket: Optional[socket.socket] = None
        self.is_running = False

        # Danh sách socket đang kết nối đến từ Peer khác: { addr: socket }
        self._active_sockets: Dict[Tuple[str, int], socket.socket] = {}
        self._sock_lock = threading.Lock()

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

        try:
            # Bước 1: Nhận HANDSHAKE
            msg = recv_msg(conn)
            if not msg or msg.get("type") != MessageType.HANDSHAKE.value:
                return

            info_hash = msg.get("info_hash")

            # Tra bảng tìm PieceManager tương ứng với file được yêu cầu
            with self._pm_lock:
                pm = self._piece_managers.get(info_hash)

            if pm is None:
                return  # Không có file này → đóng kết nối

            # Bước 2: Phản hồi handshake và gửi BITFIELD
            send_msg(conn, {
                "type": MessageType.HANDSHAKE_RESPONSE.value,
                "info_hash": info_hash,
                "peer_id": self.peer_id,
            })
            send_msg(conn, create_bitfield_msg(pm.get_bitfield()))

            # Bước 3: Vòng lặp phục vụ REQUEST
            while self.is_running:
                req = recv_msg(conn)
                if not req:
                    break  # Peer đóng kết nối hoặc timeout

                req_type = req.get("type")

                if req_type == MessageType.REQUEST.value:
                    piece_idx = req.get("piece_index")
                    if piece_idx is not None:
                        data = pm.read_piece(piece_idx)
                        if data:
                            send_msg(conn, create_piece_msg(piece_idx, data))

                elif req_type == MessageType.KEEP_ALIVE.value:
                    pass  # Không làm gì, chỉ để giữ kết nối sống

        except (EOFError,OSError,socket.timeout):
            pass  # Timeout bình thường
        except Exception:
            pass
        finally:
            with self._sock_lock:
                self._active_sockets.pop(addr, None)
            try:
                conn.close()
            except Exception:
                pass

    # ── Broadcast HAVE ────────────────────────────────────────────────── #

    def broadcast_have(self, piece_index: int) -> None:
        """Thông báo đến mọi Peer đang kết nối rằng ta vừa có thêm 1 piece mới."""
        have_msg = {
            "type": MessageType.HAVE.value,
            "piece_index": piece_index,
        }
        with self._sock_lock:
            for sock in list(self._active_sockets.values()):
                try:
                    send_msg(sock, have_msg)
                except Exception:
                    pass
