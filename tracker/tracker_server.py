import socket
import threading
import time
import ipaddress
import os
from typing import Optional
from protocol.auth import authenticate_client
from protocol.messages import (
    MessageType, send_msg, recv_msg,
    create_punch_signal_msg, create_punch_failed_msg,
)
from .state import TrackerState
from .relay import RelayManager


class TrackerServer:
    """
    Máy chủ Tracker đa luồng lắng nghe các yêu cầu ANNOUNCE và GET_PEERS từ các nút Peer.
    """

    def __init__(
        self, host: str = "127.0.0.1", port: int = 8000,
        peer_timeout: float = 60.0, signal_idle_timeout: float = 90.0
    ):
        self.host = host
        self.port = port
        self.state = TrackerState(peer_timeout=peer_timeout)
        self.auth_secret = os.environ.get("P2P_TRACKER_SECRET", "")
        self.relay = RelayManager(self.state)
        # Client gửi KEEP_ALIVE mỗi 30 giây; cần chờ lâu hơn khoảng đó.
        self.signal_idle_timeout = signal_idle_timeout
        self.server_socket: Optional[socket.socket] = None
        self.is_running = False
        self.accept_thread: Optional[threading.Thread] = None
        self.cleanup_thread: Optional[threading.Thread] = None
        self.start_time: Optional[float] = None
        self._stop_event = threading.Event()
        self._clients = set()
        self._clients_lock = threading.Lock()
        self._connection_slots = threading.BoundedSemaphore(256)

    def start(self) -> None:
        """Khởi động máy chủ Tracker Server và luồng dọn dẹp định kỳ."""
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(
            socket.SOL_SOCKET, socket.SO_REUSEADDR, 1
        )  # Ipv4, TCP
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(128)  # # -> Listening socket, tối đa 128 người chờ
        self.port = self.server_socket.getsockname()[1]
        self.is_running = True
        self.start_time = time.time()  # Dùng để hiển thị thời gian chạy trên app admin
        self._stop_event.clear()

        print(
            f"[Tracker Server] Đang khởi chạy và lắng nghe tại {self.host}:{self.port}"
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
                if not self._connection_slots.acquire(blocking=False):
                    conn.close()
                    continue
                with self._clients_lock:
                    self._clients.add(conn)
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
        """
        Xử lý yêu cầu từ một Peer kết nối đến.
        Phân biệt 2 loại kết nối qua message đầu tiên:
          - ANNOUNCE / GET_PEERS  → xử lý ngắn, đóng kết nối sau.
          - SIGNAL_REGISTER       → mở kênh persistent, vòng lặp chờ PUNCH_REQUEST.
        """
        try:
            conn.settimeout(5.0)
            if not authenticate_client(conn, self.auth_secret):
                return
            msg = recv_msg(conn)
            if not isinstance(msg, dict):
                return

            msg_type = msg.get("type")
            client_ip = addr[0]

            if msg_type == MessageType.SIGNAL_REGISTER.value:
                # Kênh signaling persistent — không đóng sau lần đầu, chuyển sang loop
                self._handle_signal_channel(conn, addr, msg)
                return   # _handle_signal_channel tự đóng khi xong

            if msg_type == MessageType.RELAY_OPEN.value:
                self.relay.open(conn, msg)
                return

            if msg_type == MessageType.RELAY_JOIN.value:
                self.relay.join(conn, msg)
                return

            if msg_type == MessageType.ANNOUNCE.value:
                info_hash = msg.get("info_hash")
                peer_id = msg.get("peer_id")
                peer_port = msg.get("port")
                event = msg.get("event", "started")
                downloaded = msg.get("downloaded", 0)
                left = msg.get("left", 0)

                # Trường mới từ STUN (0 / rỗng nếu peer chưa chạy STUN)
                udp_port  = msg.get("udp_port", 0)
                public_ip = msg.get("public_ip", "")

                # Chọn IP tốt nhất để lưu vào state:
                # Ưu tiên: public_ip (STUN) > ip (LAN tự báo) > client_ip (TCP socket)
                announced_ip = msg.get("ip", "")
                if (not isinstance(info_hash, str) or len(info_hash) != 64
                        or not isinstance(peer_id, str) or not 1 <= len(peer_id) <= 64
                        or not isinstance(peer_port, int) or not 1 <= peer_port <= 65535
                        or not isinstance(udp_port, int) or not 0 <= udp_port <= 65535
                        or not isinstance(announced_ip, str)
                        or not isinstance(public_ip, str)
                        or not isinstance(downloaded, int) or downloaded < 0
                        or not isinstance(left, int) or left < 0
                        or event not in ("started", "completed", "stopped")):
                    send_msg(conn, {"type": MessageType.ANNOUNCE_RESPONSE.value,
                                    "status": "ERROR"})
                    return
                announced_ip = announced_ip.strip()
                public_ip = public_ip.strip()
                try:
                    if announced_ip:
                        announced_ip = str(ipaddress.IPv4Address(announced_ip))
                    if public_ip:
                        public_ip = str(ipaddress.IPv4Address(public_ip))
                except ipaddress.AddressValueError:
                    send_msg(conn, {"type": MessageType.ANNOUNCE_RESPONSE.value,
                                    "status": "ERROR"})
                    return
                peer_ip = announced_ip if announced_ip else client_ip

                # Đăng ký thông tin peer (trả về False nếu peer bị admin chặn)
                accepted = self.state.register_peer(
                    info_hash=info_hash,
                    peer_id=peer_id,
                    ip=peer_ip,
                    port=peer_port,
                    downloaded=downloaded,
                    left=left,
                    event=event,
                    udp_port=udp_port,
                    public_ip=public_ip,
                    name=str(msg.get("name", "") or "")[:255],
                )
                if not accepted:
                    send_msg(conn, {
                        "type": MessageType.ANNOUNCE_RESPONSE.value,
                        "status": "BLOCKED",
                        "interval": 30,
                    })
                    return
                if event != "stopped":
                    self.state.report_bad_peers(info_hash, peer_id,
                                                msg.get("bad_peers", []))

                # Trả về phản hồi thành công
                response = {
                    "type": MessageType.ANNOUNCE_RESPONSE.value,
                    "status": "OK",
                    "interval": 30,  # Peer nên announce lại mỗi 30 giây
                }
                send_msg(conn, response)

            elif msg_type == MessageType.GET_PEERS.value:
                info_hash = msg.get("info_hash")
                request_peer_id = msg.get("peer_id")

                if not isinstance(info_hash, str) or len(info_hash) != 64:
                    return

                # Peer bị chặn không được nhận danh sách peer.
                if self.state.is_blocked(request_peer_id or "", client_ip):
                    peers_list = []
                else:
                    peers_list = self.state.get_peers(
                        info_hash, exclude_peer_id=request_peer_id
                    )

                response = {
                    "type": MessageType.GET_PEERS_RESPONSE.value,
                    "info_hash": info_hash,
                    "peers": peers_list,
                }
                send_msg(conn, response)

        except Exception as e:
            print(f"[Tracker Server] Lỗi xử lý client {addr}: {e}")
        finally:
            with self._clients_lock:
                self._clients.discard(conn)
            self._connection_slots.release()
            try:
                conn.close()
            except Exception:
                pass

    # ── Signal channel handler (persistent) ──────────────────────────────── #

    def _handle_signal_channel(self, conn: socket.socket, addr: tuple,
                                register_msg: dict) -> None:
        """
        Vòng lặp persistent cho kênh signaling.
        - Nhận SIGNAL_REGISTER → xác nhận SIGNAL_READY → lưu channel vào state.
        - Lặp nhận PUNCH_REQUEST → relay PUNCH_SIGNAL cho target peer.
        - Tự dọn dẹp khi peer ngắt kết nối.
        """
        peer_id  = register_msg.get("peer_id", "")
        ip       = register_msg.get("ip", addr[0])
        udp_port = register_msg.get("udp_port", 0)
        tcp_port = register_msg.get("tcp_port", 0)
        candidates = self._valid_candidates(register_msg.get("candidates", []))

        if (not isinstance(peer_id, str) or not 1 <= len(peer_id) <= 64
                or not isinstance(ip, str) or not isinstance(udp_port, int)
                or not 0 <= udp_port <= 65535 or not isinstance(tcp_port, int)
                or not 1 <= tcp_port <= 65535):
            return
        # Peer bị admin chặn không được mở kênh signaling (đục lỗ / relay).
        if self.state.is_blocked(peer_id, addr[0]):
            return

        # Bật TCP keepalive để OS phát hiện khi client đứt đột ngột
        conn.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        conn.settimeout(self.signal_idle_timeout)

        # Đăng ký channel
        self.state.register_signal_channel(peer_id, conn, ip, udp_port, tcp_port,
                                           candidates)

        # Xác nhận với peer
        if not self.state.deliver_signal(peer_id,
                {"type": MessageType.SIGNAL_READY.value, "peer_id": peer_id}):
            self.state.unregister_signal_channel(peer_id, conn)
            return

        try:
            while self.is_running:
                try:
                    msg = recv_msg(conn)
                except socket.timeout:
                    continue   # poll lại — kiểm tra is_running
                except Exception:
                    break

                if not msg:
                    break   # peer đóng kết nối (EOF)

                msg_type = msg.get("type")

                if msg_type == MessageType.PUNCH_REQUEST.value:
                    self._relay_punch(requesting_peer_id=peer_id,
                                      requesting_conn=conn,
                                      target_peer_id=msg.get("target_peer_id", ""))

                elif msg_type == MessageType.KEEP_ALIVE.value:
                    pass    # reset timeout, không làm gì

        except Exception as e:
            if self.is_running:
                print(f"[Tracker Server] Signal channel lỗi ({peer_id}): {e}")
        finally:
            self.state.unregister_signal_channel(peer_id, conn)
            try:
                conn.close()
            except Exception:
                pass

    def _relay_punch(self, requesting_peer_id: str, requesting_conn: socket.socket,
                     target_peer_id: str) -> None:
        """
        Relay PUNCH_SIGNAL cho cả 2 peer khi một bên gửi PUNCH_REQUEST.

        Quy trình:
          1. Tra thông tin signal channel của target.
          2. Gửi PUNCH_SIGNAL(requester's endpoint) → target.
          3. Gửi PUNCH_SIGNAL(target's endpoint)    → requester.
        Nếu target không có signal channel → gửi PUNCH_FAILED về requester.
        """
        if not target_peer_id:
            self.state.deliver_signal(requesting_peer_id, create_punch_failed_msg("target_peer_id bị thiếu"))
            return

        requester_info = self.state.get_signal_info(requesting_peer_id)
        target_info    = self.state.get_signal_info(target_peer_id)

        if not target_info:
            self.state.deliver_signal(requesting_peer_id,
                     create_punch_failed_msg(f"Peer {target_peer_id} không có signal channel"))
            print(f"[Tracker Server] PUNCH_FAILED: {requesting_peer_id} → {target_peer_id} "
                  f"(target offline hoặc không hỗ trợ)")
            return

        if not requester_info:
            # Không nên xảy ra vì chính requester đã có channel — phòng thủ
            self.state.deliver_signal(requesting_peer_id, create_punch_failed_msg("Lỗi nội bộ tracker"))
            return

        # Gửi PUNCH_SIGNAL(requester's info) → target
        self.state.deliver_punch_signal(
            target_peer_id,
            create_punch_signal_msg(
                peer_id=requesting_peer_id,
                ip=requester_info["ip"],
                udp_port=requester_info["udp_port"],
                tcp_port=requester_info["tcp_port"],
                candidates=requester_info["candidates"],
            )
        )

        # Gửi PUNCH_SIGNAL(target's info) → requester (qua conn hiện tại)
        self.state.deliver_signal(
            requesting_peer_id,
            create_punch_signal_msg(
                peer_id=target_peer_id,
                ip=target_info["ip"],
                udp_port=target_info["udp_port"],
                tcp_port=target_info["tcp_port"],
                candidates=target_info["candidates"],
            )
        )

        print(f"[Tracker Server] PUNCH_SIGNAL relay: {requesting_peer_id} ↔ {target_peer_id}")

    @staticmethod
    def _valid_candidates(value) -> list:
        """Giới hạn địa chỉ ứng viên mà tracker nhận từ peer không tin cậy."""
        if not isinstance(value, list) or len(value) > 8:
            return []
        result = []
        for item in value:
            if not isinstance(item, dict):
                continue
            ip, port = item.get("ip"), item.get("port")
            try:
                address = ipaddress.IPv4Address(ip)
                if address.is_unspecified or not isinstance(port, int) or not 1 <= port <= 65535:
                    continue
            except (ipaddress.AddressValueError, TypeError):
                continue
            if len(result) < 8:
                result.append({"ip": str(address), "port": port,
                               "type": "host" if item.get("type") == "host" else "srflx"})
        return result

    def _periodic_cleanup(self) -> None:
        """Vòng lặp dọn dẹp các Peer không phản hồi định kỳ."""
        while self.is_running:
            if self._stop_event.wait(120):
                break
            self.state.cleanup_stale_peers()

    def stop(self) -> None:
        """Dừng Tracker Server an toàn."""
        self.is_running = False
        self._stop_event.set()
        if self.server_socket:
            try:
                self.server_socket.close()
            except Exception:
                pass

        self.relay.close()
        with self._clients_lock:
            clients = list(self._clients)
        for conn in clients:
            try:
                conn.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            conn.close()
        for thread in (self.accept_thread, self.cleanup_thread):
            if thread and thread is not threading.current_thread():
                thread.join(timeout=1.0)

        print("[Tracker Server] Đã dừng thành công.")
