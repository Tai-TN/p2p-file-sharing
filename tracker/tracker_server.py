import socket
import threading
import time
from typing import Optional
from protocol.messages import MessageType, send_msg, recv_msg
from protocol.ipv6 import normalize_ipv6_address
from .state import TrackerState


class TrackerServer:
    """
    Máy chủ Tracker đa luồng lắng nghe các yêu cầu ANNOUNCE và GET_PEERS từ các nút Peer.
    """

    def __init__(
        self, host: str = "::1", port: int = 8000, peer_timeout: float = 60.0
    ):
        self.host = normalize_ipv6_address(host)
        self.port = port
        self.state = TrackerState(peer_timeout=peer_timeout)
        self.server_socket: Optional[socket.socket] = None
        self.is_running = False
        self.accept_thread: Optional[threading.Thread] = None
        self.cleanup_thread: Optional[threading.Thread] = None

    def start(self) -> None:
        """Khởi động máy chủ Tracker Server và luồng dọn dẹp định kỳ."""
        self.server_socket = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
        self.server_socket.setsockopt(
            socket.SOL_SOCKET, socket.SO_REUSEADDR, 1
        )
        self.server_socket.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(128)  # # -> Listening socket, tối đa 128 người chờ
        self.is_running = True

        print(
            f"[Tracker Server] Đang khởi chạy và lắng nghe tại [{self.host}]:{self.port}"
        )

        # Khởi chạy luồng dọn dẹp Peer dead định kỳ (Background daemon thread)
        self.cleanup_thread = threading.Thread(
            target=self._periodic_cleanup, daemon=True
        )
        self.cleanup_thread.start()

        # Khởi chạy luồng chấp nhận kết nối (Accept thread)
        self.accept_thread = threading.Thread(target=self._accept_loop, daemon=True)
        self.accept_thread.start()

    def _accept_loop(self) -> None:
        """Vòng lặp chấp nhận kết nối TCP từ các Peer."""
        while self.is_running:
            try:
                conn, addr = self.server_socket.accept()  # block
                # Mỗi Peer kết nối đến được xử lý trên 1 Worker Thread riêng biệt
                client_thread = threading.Thread(
                    target=self._handle_client, args=(conn, addr), daemon=True
                )
                client_thread.start()
            except Exception as e:
                if not self.is_running:
                    break
                print(f"[Tracker Server] Lỗi chấp nhận kết nối: {e}")

    def _handle_client(self, conn: socket.socket, addr: tuple) -> None:
        """Xử lý yêu cầu từ một Peer kết nối đến."""
        try:
            conn.settimeout(5.0)
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
                bad_peers = msg.get("bad_peers", [])

                announced_ip = msg.get("ip", "")
                if (not isinstance(announced_ip, str)
                        or not isinstance(peer_port, int) or isinstance(peer_port, bool)
                        or not 1 <= peer_port <= 65535
                        or not isinstance(info_hash, str) or not info_hash
                        or not isinstance(peer_id, str) or not peer_id
                        or not isinstance(bad_peers, list) or len(bad_peers) > 256
                        or not all(isinstance(peer, str) and peer for peer in bad_peers)):
                    send_msg(conn, {"type": MessageType.ANNOUNCE_RESPONSE.value,
                                    "status": "ERROR", "error": "ANNOUNCE không hợp lệ"})
                    return
                try:
                    peer_ip = normalize_ipv6_address(announced_ip or client_ip)
                except ValueError:
                    send_msg(conn, {"type": MessageType.ANNOUNCE_RESPONSE.value,
                                    "status": "ERROR", "error": "Địa chỉ IPv6 không hợp lệ"})
                    return

                # Đăng ký thông tin peer
                self.state.register_peer(
                    info_hash=info_hash,
                    peer_id=peer_id,
                    ip=peer_ip,
                    port=peer_port,
                    downloaded=downloaded,
                    left=left,
                    event=event,
                )

                # Xử lý báo cáo peer chết từ Leecher
                if bad_peers:
                    self.state.report_bad_peers(
                        info_hash=info_hash,
                        reporter_id=peer_id,
                        bad_peer_ids=bad_peers
                    )

                # Trả về phản hồi thành công
                response = {
                    "type": MessageType.ANNOUNCE_RESPONSE.value,
                    "status": "OK",
                    "interval": 30,  # Peer nên announce lại mỗi 30 giây
                    "ip": peer_ip,
                }
                send_msg(conn, response)

            elif msg_type == MessageType.GET_PEERS.value:
                info_hash = msg.get("info_hash")
                request_peer_id = msg.get("peer_id")

                peers_list = self.state.get_peers(
                    info_hash, exclude_peer_id=request_peer_id
                )

                response = {
                    "type": MessageType.GET_PEERS_RESPONSE.value,
                    "info_hash": info_hash,
                    "peers": peers_list,
                }
                send_msg(conn, response)

        except EOFError:
            pass  # A connectivity check or client may close without sending a frame.
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
            time.sleep(120)
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
