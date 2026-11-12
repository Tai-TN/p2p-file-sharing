"""
nat/hole_punch.py — Kỹ thuật UDP Hole Punching & NAT Traversal.

Chức năng:
  - UDPHolePuncher: Đục lỗ NAT bằng UDP probe/ack gói tin hai chiều đồng thời.
  - Hỗ trợ reverse connection và simultaneous connection attempt.
  - Phối hợp với TrackerSignalChannel để đồng bộ timing giữa 2 peer.

Cách hoạt động:
  1. Khi 2 peer nhận PUNCH_SIGNAL từ Tracker, cả hai đồng thời khởi chạy UDPHolePuncher.
  2. Mỗi bên gửi liên tục các gói UDP probe tới (public_ip, udp_port) của bên kia.
  3. Khi gói tin chạm vào NAT của đối phương, NAT mở outbound mapping.
  4. Gói tin tiếp theo đi xuyên qua thành công → 2 bên trao đổi ACK xác nhận đục lỗ thành công.
"""

import time
import socket
import logging
import threading
import queue
import ipaddress
from typing import Optional, Tuple, Dict, Any

from nat.stun_client import _build_binding_request, _build_transaction_id

logger = logging.getLogger(__name__)

# Tiêu đề gói tin probe và ack để đục lỗ NAT
PROBE_PREFIX = b"P2P_PUNCH:"
ACK_PREFIX   = b"P2P_ACK:"


class UDPHolePuncher:
    """
    Thực hiện đục lỗ NAT qua UDP giữa máy cục bộ và peer từ xa.
    Dùng chung local_port mà STUN đã dùng để tận dụng NAT mapping hiện có.
    """

    def __init__(self, local_port: int, my_peer_id: str,
                 shared_socket: Optional[socket.socket] = None,
                 incoming: Optional[queue.Queue] = None):
        self.local_port = local_port
        self.my_peer_id = my_peer_id
        self.shared_socket = shared_socket
        self.incoming = incoming
        self.last_target_addr: Optional[Tuple[str, int]] = None

    def punch(self, remote_ip: str, remote_udp_port: int,
              remote_peer_id: str = "", timeout: float = 3.0,
              candidates: Optional[list] = None) -> Tuple[bool, Optional[socket.socket]]:
        """
        Bắt đầu gửi các gói UDP probe dồn dập đến remote endpoint để mở lỗ NAT.
        Đồng thời lắng nghe phản hồi ACK từ remote peer.

        Args:
            remote_ip: IP public của peer đối diện (nhận từ STUN / Tracker).
            remote_udp_port: UDP port public của peer đối diện.
            remote_peer_id: ID của peer đối diện (để verify gói tin).
            timeout: Thời gian tối đa thử đục lỗ (giây).

        Returns:
            (success: bool, sock: Optional[socket.socket])
            Nếu thành công, trả về True và socket UDP đã thông suốt (có thể tiếp tục dùng).
        """
        if not remote_ip or not remote_udp_port:
            logger.warning("[UDPHolePuncher] Thiếu remote_ip hoặc remote_udp_port")
            return False, None

        sock = self.shared_socket
        owns_socket = sock is None
        if sock is None:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            try:
                sock.bind(("0.0.0.0", self.local_port))
            except OSError as e:
                logger.error(f"[UDPHolePuncher] Không thể bind UDP port {self.local_port}: {e}")
                sock.close()
                return False, None

        previous_timeout = sock.gettimeout()
        if self.incoming is None:
            sock.settimeout(0.08)  # chỉ puncher độc lập đọc trực tiếp từ socket

        probe_payload = f"{PROBE_PREFIX.decode()}{self.my_peer_id}".encode("utf-8")
        ack_payload   = f"{ACK_PREFIX.decode()}{self.my_peer_id}".encode("utf-8")
        expected_probe = PROBE_PREFIX + remote_peer_id.encode("utf-8") if remote_peer_id else None
        expected_ack = ACK_PREFIX + remote_peer_id.encode("utf-8") if remote_peer_id else None

        targets = [(remote_ip, remote_udp_port)]
        for candidate in (candidates or [])[:8]:
            try:
                ip = str(ipaddress.IPv4Address(candidate["ip"]))
                port = candidate["port"]
                if isinstance(port, int) and 1 <= port <= 65535 and (ip, port) not in targets:
                    targets.append((ip, port))
            except (KeyError, TypeError, ipaddress.AddressValueError):
                continue
        target_addr = targets[0]
        self.last_target_addr = None
        deadline = time.time() + timeout

        got_peer_probe = False
        confirmed_ack = False
        keep_socket = False
        sent_probes = 0
        send_error = None

        logger.info(f"[UDPHolePuncher {self.my_peer_id}] Bắt đầu đục lỗ UDP tới {remote_ip}:{remote_udp_port}...")

        try:
            while time.time() < deadline:
                # 1. Bắn probe ra ngoài (tạo outbound mapping trên NAT của mình)
                try:
                    for address in targets:
                        sock.sendto(probe_payload, address)
                        sent_probes += 1
                    if got_peer_probe:
                        # Nếu đã thấy probe của đối phương, bắn thêm ACK dồn dập
                        sock.sendto(ack_payload, target_addr)
                except Exception as e:
                    send_error = e
                    logger.debug(f"[UDPHolePuncher] Lỗi gửi probe: {e}")

                # 2. Lắng nghe gói tin từ bên kia
                try:
                    if self.incoming is None:
                        data, addr = sock.recvfrom(512)
                    else:
                        data, addr = self.incoming.get(timeout=0.08)
                    if not data:
                        continue

                    # Kiểm tra xem có phải gói từ đối phương không
                    if (data == expected_probe if expected_probe else data.startswith(PROBE_PREFIX)):
                        if not got_peer_probe:
                            logger.info(f"[UDPHolePuncher {self.my_peer_id}] Đã nhận probe UDP từ {addr}")
                        got_peer_probe = True
                        target_addr = addr  # peer có thể dùng port khác endpoint STUN dự đoán
                        if addr not in targets:
                            targets.append(addr)
                        # Gửi ACK phản hồi ngay lập tức
                        sock.sendto(ack_payload, target_addr)
                    elif (data == expected_ack if expected_ack else data.startswith(ACK_PREFIX)):
                        confirmed_ack = True
                        # Đối phương đã nhận được probe của mình và trả lời ACK
                        logger.info(f"[UDPHolePuncher {self.my_peer_id}] Đục lỗ UDP THÀNH CÔNG tới {addr}!")
                        sock.sendto(ack_payload, addr)
                        keep_socket = True
                        self.last_target_addr = addr
                        return True, sock

                except (socket.timeout, queue.Empty):
                    pass
                except Exception as e:
                    logger.debug(f"[UDPHolePuncher] Lỗi khi nhận: {e}")

                time.sleep(0.04)  # nghỉ nhẹ 40ms giữa các đợt bắn

            if got_peer_probe and not confirmed_ack:
                # Đã nhận được probe nhưng chưa nhận ACK (có thể ACK bị rớt trên mạng),
                # nhưng lỗ NAT 1 chiều đã mở, gửi thêm loạt ACK vét
                for _ in range(3):
                    sock.sendto(ack_payload, target_addr)
                logger.info(f"[UDPHolePuncher {self.my_peer_id}] Chỉ nhận được probe từ đối phương; chưa xác nhận đường UDP hai chiều")
                return False, None

            logger.warning(f"[UDPHolePuncher {self.my_peer_id}] Timeout đục lỗ UDP tới "
                           f"{remote_ip}:{remote_udp_port}; đã gửi {sent_probes} probe, "
                           f"không nhận được probe/ACK hợp lệ"
                           + (f"; lỗi gửi cuối: {send_error}" if send_error else ""))
            return False, None

        except Exception as e:
            logger.error(f"[UDPHolePuncher] Lỗi ngoại lệ trong quá trình punch: {e}")
            return False, None
        finally:
            if owns_socket:
                if not keep_socket:
                    sock.close()
            elif self.incoming is None:
                sock.settimeout(previous_timeout)


class HolePunchManager:
    """
    Quản lý điều phối Hole Punching cấp cao:
    - Tiếp nhận sự kiện PUNCH_SIGNAL từ Tracker.
    - Kích hoạt đục lỗ UDP và thử kết nối TCP hai chiều đồng thời (Simultaneous / Reverse connect).
    """

    def __init__(self, my_peer_id: str, local_port: int):
        self.my_peer_id = my_peer_id
        self.local_port = local_port
        self.puncher = UDPHolePuncher(local_port=local_port, my_peer_id=my_peer_id)
        self._socket_lock = threading.Lock()
        self._shared_socket: Optional[socket.socket] = None
        self._keepalive_stop = threading.Event()
        self._keepalive_thread: Optional[threading.Thread] = None
        self._receive_thread: Optional[threading.Thread] = None
        self._inboxes: Dict[str, queue.Queue] = {}
        self._inboxes_lock = threading.Lock()
        self._logged_probes: set[str] = set()
        self._connected_endpoints: Dict[str, Tuple[str, int]] = {}
        self.on_data_packet = None

    def set_socket(self, sock: socket.socket,
                   stun_server: Optional[Tuple[str, int]] = None) -> None:
        """Nhận socket đã dùng cho STUN; giữ ánh xạ UDP trong lúc chờ peer."""
        with self._socket_lock:
            self._shared_socket = sock
            self.puncher.shared_socket = sock
            sock.settimeout(0.2)
        self._keepalive_stop.clear()
        self._receive_thread = threading.Thread(
            target=self._receive_loop, daemon=True,
            name=f"udp-receive-{self.my_peer_id}",
        )
        self._receive_thread.start()
        if stun_server:
            self._keepalive_thread = threading.Thread(
                target=self._keepalive_loop, args=(stun_server,), daemon=True,
                name=f"udp-keepalive-{self.my_peer_id}",
            )
            self._keepalive_thread.start()

    def _receive_loop(self) -> None:
        """Luôn đọc UDP để tránh đầy hàng đợi và trả ACK cả lúc chưa punch."""
        ack = ACK_PREFIX + self.my_peer_id.encode("utf-8")
        while not self._keepalive_stop.is_set():
            sock = self._shared_socket
            if sock is None:
                return
            try:
                data, addr = sock.recvfrom(2048)
            except socket.timeout:
                continue
            except OSError:
                if not self._keepalive_stop.is_set():
                    logger.warning(f"[HolePunchManager {self.my_peer_id}] Socket UDP đã đóng")
                return

            is_probe = data.startswith(PROBE_PREFIX)
            if is_probe:
                peer_bytes = data[len(PROBE_PREFIX):]
            elif data.startswith(ACK_PREFIX):
                peer_bytes = data[len(ACK_PREFIX):]
            else:
                if self.on_data_packet is not None:
                    try:
                        self.on_data_packet(data, addr)
                    except Exception as e:
                        logger.warning(f"[HolePunchManager] Gói dữ liệu UDP lỗi: {e}")
                continue  # phản hồi STUN hoặc gói dữ liệu
            if not (1 <= len(peer_bytes) <= 64 and all(
                48 <= b <= 57 or 65 <= b <= 90 or 97 <= b <= 122 or b in (45, 95)
                for b in peer_bytes
            )):
                continue
            peer_id = peer_bytes.decode("ascii")

            if is_probe:
                if peer_id not in self._logged_probes and len(self._logged_probes) < 64:
                    print(f"[HolePunchManager {self.my_peer_id}] Đã nhận probe UDP từ {peer_id} ({addr[0]}:{addr[1]})")
                    self._logged_probes.add(peer_id)
                try:
                    sock.sendto(ack, addr)
                except OSError:
                    continue

            with self._inboxes_lock:
                inbox = self._inboxes.get(peer_id)
                if inbox is not None:
                    try:
                        inbox.put_nowait((data, addr))
                    except queue.Full:
                        # Ưu tiên gói mới thay vì giữ probe cũ trong hàng đợi.
                        try:
                            inbox.get_nowait()
                            inbox.put_nowait((data, addr))
                        except queue.Empty:
                            pass

    def _keepalive_loop(self, stun_server: Tuple[str, int]) -> None:
        while not self._keepalive_stop.wait(15.0):
            try:
                server_addr = (socket.gethostbyname(stun_server[0]), stun_server[1])
                request = _build_binding_request(_build_transaction_id())
                with self._socket_lock:
                    if self._shared_socket is None:
                        return
                    self._shared_socket.sendto(request, server_addr)
            except OSError as e:
                logger.warning(f"[HolePunchManager] UDP keepalive thất bại: {e}")

    def close(self) -> None:
        self._keepalive_stop.set()
        with self._socket_lock:
            if self._shared_socket:
                self._shared_socket.close()
                self._shared_socket = None
                self.puncher.shared_socket = None
        if self._keepalive_thread and self._keepalive_thread is not threading.current_thread():
            self._keepalive_thread.join(timeout=1.0)
        if self._receive_thread and self._receive_thread is not threading.current_thread():
            self._receive_thread.join(timeout=1.0)

    def get_endpoint(self, peer_id: str) -> Optional[Tuple[str, int]]:
        with self._inboxes_lock:
            return self._connected_endpoints.get(peer_id)

    def has_endpoint(self, addr: Tuple[str, int]) -> bool:
        with self._inboxes_lock:
            return addr in self._connected_endpoints.values()

    def handle_punch_signal(self, signal_msg: Dict[str, Any],
                            connect_tcp_callback=None) -> Dict[str, Any]:
        """
        Xử lý khi nhận được PUNCH_SIGNAL từ Tracker.

        Args:
            signal_msg: Dict chứa { peer_id, ip, udp_port, tcp_port } của đối phương.
            connect_tcp_callback: Hàm callback gọi để thử kết nối TCP (nếu có).

        Returns:
            Dict kết quả { "udp_success": bool, "tcp_connected": bool, "peer_id": str }
        """
        remote_peer_id = signal_msg.get("peer_id", "")
        remote_ip      = signal_msg.get("ip", "")
        remote_udp     = int(signal_msg.get("udp_port", 0) or 0)
        remote_tcp     = int(signal_msg.get("tcp_port", 0) or 0)

        result = {
            "peer_id": remote_peer_id,
            "udp_success": False,
            "tcp_connected": False,
        }

        # 1. Khởi chạy đục lỗ UDP trong luồng hoặc trực tiếp
        if remote_udp > 0:
            with self._socket_lock:
                inbox = queue.Queue(maxsize=128) if self._shared_socket else None
                if inbox is not None:
                    with self._inboxes_lock:
                        self._inboxes[remote_peer_id] = inbox
                    self.puncher.incoming = inbox
                try:
                    success, udp_sock = self.puncher.punch(
                        remote_ip=remote_ip,
                        remote_udp_port=remote_udp,
                        remote_peer_id=remote_peer_id,
                    timeout=5.0,
                    candidates=signal_msg.get("candidates", []),
                    )
                finally:
                    self.puncher.incoming = None
                    if inbox is not None:
                        with self._inboxes_lock:
                            self._inboxes.pop(remote_peer_id, None)
            result["udp_success"] = success
            if success and self.puncher.last_target_addr:
                with self._inboxes_lock:
                    self._connected_endpoints[remote_peer_id] = self.puncher.last_target_addr
            if udp_sock and udp_sock is not self._shared_socket:
                try:
                    udp_sock.close()
                except Exception:
                    pass

        # 2. Thử kết nối TCP tới đối phương (Reverse Connect / Simultaneous connect)
        if connect_tcp_callback and remote_tcp > 0:
            try:
                tcp_ok = connect_tcp_callback(remote_ip, remote_tcp, remote_peer_id)
                result["tcp_connected"] = bool(tcp_ok)
            except Exception as e:
                logger.debug(f"[HolePunchManager] Lỗi thử TCP connect: {e}")

        return result

