import socket
import threading
import time
from typing import Optional
from protocol.messages import (
    MessageType,
    send_msg,
    recv_msg
)
from .state import TrackerState

class TrackerServer:
    """
    Máy chủ Tracker đa luồng lắng nghe các yêu cầu ANNOUNCE và GET_PEERS từ các nút Peer.
    """
    def __init__(self, host: str = "127.0.0.1", port: int = 8000, peer_timeout: float = 60.0):
        self.host = host
        self.port = port
        self.state = TrackerState(peer_timeout=peer_timeout)
        self.server_socket: Optional[socket.socket] = None
        self.is_running = False
        self.accept_thread: Optional[threading.Thread] = None
        self.cleanup_thread: Optional[threading.Thread] = None

    def start(self) -> None:
        """Khởi động máy chủ Tracker Server và luồng dọn dẹp định kỳ."""
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) #Ipv4, TCP
        self.server_socket.bind((self.host, self.port)) 
        self.server_socket.listen(128) # # -> Listening socket, tối đa 128 người chờ
        self.is_running = True

        print(f"[Tracker Server] Đang khởi chạy và lắng nghe tại {self.host}:{self.port}")

        # Khởi chạy luồng dọn dẹp Peer dead định kỳ (Background daemon thread)
        self.cleanup_thread = threading.Thread(target=self._periodic_cleanup, daemon=True)
        self.cleanup_thread.start()

        # Khởi chạy luồng chấp nhận kết nối (Accept thread)
        self.accept_thread = threading.Thread(target=self._accept_loop, daemon=True)
        self.accept_thread.start()

    def _accept_loop(self) -> None:
        """Vòng lặp chấp nhận kết nối TCP từ các Peer."""
        while self.is_running:
            try:
                conn, addr = self.server_socket.accept()
                # Mỗi Peer kết nối đến được xử lý trên 1 Worker Thread riêng biệt
                client_thread = threading.Thread(target=self._handle_client, args=(conn, addr), daemon=True)
                client_thread.start()
            except Exception as e:
                if not self.is_running:
                    break
                print(f"[Tracker Server] Lỗi chấp nhận kết nối: {e}")

    def _handle_client(self, conn: socket.socket, addr: tuple) -> None:
        """Xử lý yêu cầu từ một Peer kết nối đến."""
        try:
            msg = recv_msg(conn)
            if not msg:
                return

            msg_type = msg.get("type")
            client_ip = addr[0]

            if msg_type == MessageType.ANNOUNCE.value:
                info_hash = msg.get("info_hash")
                peer_id = msg.get("peer_id")
                peer_port = msg.get("port")
                event = msg.get("event", "started")
                downloaded = msg.get("downloaded", 0)
                left = msg.get("left", 0)

                # Đăng ký thông tin peer
                self.state.register_peer(
                    info_hash=info_hash,
                    peer_id=peer_id,
                    ip=client_ip,
                    port=peer_port,
                    downloaded=downloaded,
                    left=left,
                    event=event
                )

                # Trả về phản hồi thành công
                response = {
                    "type": MessageType.ANNOUNCE_RESPONSE.value,
                    "status": "OK",
                    "interval": 30  # Peer nên announce lại mỗi 30 giây
                }
                send_msg(conn, response)

            elif msg_type == MessageType.GET_PEERS.value:
                info_hash = msg.get("info_hash")
                request_peer_id = msg.get("peer_id")

                peers_list = self.state.get_peers(info_hash, exclude_peer_id=request_peer_id)
                
                response = {
                    "type": MessageType.GET_PEERS_RESPONSE.value,
                    "info_hash": info_hash,
                    "peers": peers_list
                }
                send_msg(conn, response)

        except Exception as e:
            print(f"[Tracker Server] Lỗi xử lý client {addr}: {e}")
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def _periodic_cleanup(self) -> None:
        """Vòng lặp dọn dẹp các Peer không phản hồi định kỳ."""
        while self.is_running:
            time.sleep(10)
            self.state.cleanup_stale_peers()

    def stop(self) -> None:
        """Dừng Tracker Server an toàn."""
        self.is_running = False
        if self.server_socket:
            try:
                self.server_socket.close()
            except Exception:
                pass
        print("[Tracker Server] Đã dừng thành công.")
