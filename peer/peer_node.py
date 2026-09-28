import socket
import time
import threading
import base64
from collections import deque
from typing import Dict, List, Optional, Tuple
from storage.piece_manager import PieceManager
from protocol.messages import (
    MessageType,
    send_msg,
    recv_msg,
    create_handshake_msg,
    create_bitfield_msg,
    create_piece_msg
)

class PeerNode:
    """
    Nút Peer đóng vai trò Server lắng nghe các kết nối từ Peer khác đến để phục vụ chia sẻ tập tin (Seeding).
    """
    def __init__(self, peer_id: str, host: str, port: int, piece_manager: PieceManager):
        self.peer_id = peer_id
        self.host = host
        self.port = port
        self.piece_manager = piece_manager
        self.info_hash = piece_manager.metainfo["info_hash"]
        
        self.server_socket: Optional[socket.socket] = None
        self.is_running = False
        
        # Danh sách các socket active đang mở đến peer khác: { (ip, port): socket }
        self.active_sockets: Dict[Tuple[str, int], socket.socket] = {}
        self.sockets_lock = threading.Lock()

        # Log phục vụ Web UI (ghi lại sự kiện kết nối và upload piece)
        self.serve_log: deque = deque(maxlen=200)

    def start_listening(self) -> None:
        """Khởi động lắng nghe kết nối TCP."""
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(64)
        self.is_running = True

        print(f"[Peer Listener {self.peer_id}] Đang lắng nghe tại {self.host}:{self.port}")
        
        accept_thread = threading.Thread(target=self._accept_loop, daemon=True)
        accept_thread.start()

    def _accept_loop(self) -> None:
        while self.is_running:
            try:
                conn, addr = self.server_socket.accept()
                handler_thread = threading.Thread(target=self._handle_peer_connection, args=(conn, addr), daemon=True)
                handler_thread.start()
            except Exception:
                if not self.is_running:
                    break

    def _handle_peer_connection(self, conn: socket.socket, addr: Tuple[str, int]) -> None:
        """Xử lý kết nối từ một Peer khác đến để xin piece dữ liệu."""
        peer_addr_key = addr
        with self.sockets_lock:
            self.active_sockets[peer_addr_key] = conn

        try:
            # Nhận HANDSHAKE từ Peer kết nối đến
            msg = recv_msg(conn)
            if not msg or msg.get("type") != MessageType.HANDSHAKE.value:
                return

            client_info_hash = msg.get("info_hash")
            if client_info_hash != self.info_hash:
                print(f"[Peer Server {self.peer_id}] Lỗi Handshake: Sai info_hash từ {addr}")
                return

            # Gửi HANDSHAKE_RESPONSE và BITFIELD cho Peer
            hs_resp = {
                "type": MessageType.HANDSHAKE_RESPONSE.value,
                "info_hash": self.info_hash,
                "peer_id": self.peer_id
            }
            send_msg(conn, hs_resp)
            
            bitfield_msg = create_bitfield_msg(self.piece_manager.get_bitfield())
            send_msg(conn, bitfield_msg)

            # Log: peer kết nối thành công
            self.serve_log.append({
                "time": time.strftime("%H:%M:%S"),
                "type": "peer_connected",
                "addr": f"{addr[0]}:{addr[1]}",
            })

            # Vòng lặp nhận và phục vụ các yêu cầu (REQUEST / KEEP_ALIVE)
            while self.is_running:
                req = recv_msg(conn)
                if not req:
                    break

                req_type = req.get("type")
                if req_type == MessageType.REQUEST.value:
                    piece_idx = req.get("piece_index")
                    if piece_idx is not None:
                        data = self.piece_manager.read_piece(piece_idx)
                        if data:
                            piece_response = create_piece_msg(piece_idx, data)
                            send_msg(conn, piece_response)
                            # Log: đã gửi piece cho peer
                            self.serve_log.append({
                                "time": time.strftime("%H:%M:%S"),
                                "type": "piece_served",
                                "piece_index": piece_idx,
                                "to_addr": f"{addr[0]}:{addr[1]}",
                            })
                elif req_type == MessageType.KEEP_ALIVE.value:
                    pass # Nếu chỉ là yêu càu duy trì kết nối thì paasss -> Để giữa kết nối 
 
        except Exception as e:
            pass
        finally:
            with self.sockets_lock:
                if peer_addr_key in self.active_sockets:
                    del self.active_sockets[peer_addr_key]
            try:
                conn.close()
            except Exception:
                pass

    def broadcast_have(self, piece_index: int) -> None:
        """Gửi thông báo HAVE(piece_index) cho tất cả các Peer đang kết nối với mình."""
        have_msg = {
            "type": MessageType.HAVE.value,
            "piece_index": piece_index
        }
        with self.sockets_lock:
            for addr, sock in list(self.active_sockets.items()):
                try:
                    send_msg(sock, have_msg)
                except Exception:
                    pass

    def stop(self) -> None:
        self.is_running = False
        with self.sockets_lock:
            for sock in self.active_sockets.values():
                try:
                    sock.close()
                except Exception:
                    pass
            self.active_sockets.clear()

        if self.server_socket:
            try:
                self.server_socket.close()
            except Exception:
                pass
        print(f"[Peer Listener {self.peer_id}] Đã dừng.")
