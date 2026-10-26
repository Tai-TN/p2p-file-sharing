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
from typing import Optional, Tuple, Dict, Any

logger = logging.getLogger(__name__)

# Tiêu đề gói tin probe và ack để đục lỗ NAT
PROBE_PREFIX = b"P2P_PUNCH:"
ACK_PREFIX   = b"P2P_ACK:"


class UDPHolePuncher:
    """
    Thực hiện đục lỗ NAT qua UDP giữa máy cục bộ và peer từ xa.
    Dùng chung local_port mà STUN đã dùng để tận dụng NAT mapping hiện có.
    """

    def __init__(self, local_port: int, my_peer_id: str):
        self.local_port = local_port
        self.my_peer_id = my_peer_id

    def punch(self, remote_ip: str, remote_udp_port: int,
              remote_peer_id: str = "", timeout: float = 3.0) -> Tuple[bool, Optional[socket.socket]]:
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

        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            # Trên Linux, SO_REUSEPORT cho phép nhiều tiến trình/thread bind cùng port nếu cần
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        except Exception:
            pass

        try:
            sock.bind(("0.0.0.0", self.local_port))
        except Exception as e:
            # Nếu port đã bị chiếm không bind được, để OS cấp port ngẫu nhiên
            logger.debug(f"[UDPHolePuncher] Không bind được {self.local_port}, fallback bind port 0: {e}")
            try:
                sock.bind(("0.0.0.0", 0))
            except Exception as e2:
                logger.error(f"[UDPHolePuncher] Không thể bind socket UDP: {e2}")
                sock.close()
                return False, None

        sock.settimeout(0.08)  # timeout ngắn cho mỗi lần recv để liên tục gửi probe

        probe_payload = f"{PROBE_PREFIX.decode()}{self.my_peer_id}".encode("utf-8")
        ack_payload   = f"{ACK_PREFIX.decode()}{self.my_peer_id}".encode("utf-8")

        target_addr = (remote_ip, remote_udp_port)
        deadline = time.time() + timeout

        got_peer_probe = False
        confirmed_ack = False

        logger.info(f"[UDPHolePuncher {self.my_peer_id}] Bắt đầu đục lỗ UDP tới {remote_ip}:{remote_udp_port}...")

        try:
            while time.time() < deadline:
                # 1. Bắn probe ra ngoài (tạo outbound mapping trên NAT của mình)
                try:
                    sock.sendto(probe_payload, target_addr)
                    if got_peer_probe:
                        # Nếu đã thấy probe của đối phương, bắn thêm ACK dồn dập
                        sock.sendto(ack_payload, target_addr)
                except Exception as e:
                    logger.debug(f"[UDPHolePuncher] Lỗi gửi probe: {e}")

                # 2. Lắng nghe gói tin từ bên kia
                try:
                    data, addr = sock.recvfrom(512)
                    if not data:
                        continue

                    # Kiểm tra xem có phải gói từ đối phương không
                    if data.startswith(PROBE_PREFIX):
                        got_peer_probe = True
                        # Gửi ACK phản hồi ngay lập tức
                        sock.sendto(ack_payload, target_addr)
                    elif data.startswith(ACK_PREFIX):
                        confirmed_ack = True
                        # Đối phương đã nhận được probe của mình và trả lời ACK
                        logger.info(f"[UDPHolePuncher {self.my_peer_id}] Đục lỗ UDP THÀNH CÔNG tới {addr}!")
                        return True, sock

                except socket.timeout:
                    pass
                except Exception as e:
                    logger.debug(f"[UDPHolePuncher] Lỗi khi nhận: {e}")

                time.sleep(0.04)  # nghỉ nhẹ 40ms giữa các đợt bắn

            if got_peer_probe and not confirmed_ack:
                # Đã nhận được probe nhưng chưa nhận ACK (có thể ACK bị rớt trên mạng),
                # nhưng lỗ NAT 1 chiều đã mở, gửi thêm loạt ACK vét
                for _ in range(3):
                    sock.sendto(ack_payload, target_addr)
                logger.info(f"[UDPHolePuncher {self.my_peer_id}] Nhận được probe từ đối phương (bán thành công)")
                return True, sock

            logger.warning(f"[UDPHolePuncher {self.my_peer_id}] Timeout đục lỗ UDP tới {remote_ip}:{remote_udp_port}")
            sock.close()
            return False, None

        except Exception as e:
            logger.error(f"[UDPHolePuncher] Lỗi ngoại lệ trong quá trình punch: {e}")
            sock.close()
            return False, None


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
            success, udp_sock = self.puncher.punch(
                remote_ip=remote_ip,
                remote_udp_port=remote_udp,
                remote_peer_id=remote_peer_id,
                timeout=2.5
            )
            result["udp_success"] = success
            if udp_sock:
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

