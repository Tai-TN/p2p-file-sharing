"""
nat/stun_client.py — STUN client theo RFC 5389.

Chức năng:
- Lấy public IP và UDP port của máy qua STUN server.
- Phân loại NAT type (Full-Cone, Restricted-Cone, Port-Restricted, Symmetric, No NAT).
- Không phụ thuộc thư viện ngoài, chỉ dùng thư viện chuẩn Python.

Cách dùng:
    from nat.stun_client import STUNClient
    client = STUNClient()
    result = client.get_nat_info()
    print(result)
    # => NATInfo(public_ip='1.2.3.4', public_port=12345, nat_type=NATType.FULL_CONE, local_port=54321)
"""

import os
import socket
import struct
import hmac
import hashlib
import logging
import random
from dataclasses import dataclass
from enum import Enum
from typing import Optional, Tuple

logger = logging.getLogger(__name__)

# ─── Hằng số STUN RFC 5389 ───────────────────────────────────────────────────

STUN_MAGIC_COOKIE = 0x2112A442          # Bắt buộc theo RFC 5389
STUN_BINDING_REQUEST  = 0x0001
STUN_BINDING_RESPONSE = 0x0101
STUN_BINDING_ERROR    = 0x0111

# STUN Attribute types
ATTR_MAPPED_ADDRESS     = 0x0001
ATTR_XOR_MAPPED_ADDRESS = 0x0020
ATTR_ERROR_CODE         = 0x0009
ATTR_CHANGE_REQUEST     = 0x0003        # Dùng để test NAT type (RFC 3489 style)

FAMILY_IPV4 = 0x01
FAMILY_IPV6 = 0x02

# Các STUN server công cộng dự phòng (không cần đăng ký, luôn sẵn sàng)
DEFAULT_STUN_SERVERS = [
    ("stun.l.google.com", 19302),
    ("stun1.l.google.com", 19302),
    ("stun.cloudflare.com", 3478),
    ("stun.ekiga.net", 3478),
]
    
# STUN server thứ 2 (cùng server nhưng port khác, để test CHANGE_REQUEST)
STUN_SERVER_ALT_PORT = 19303            # Google STUN server có port phụ này


# ─── Enum NAT type ────────────────────────────────────────────────────────────

class NATType(Enum):
    """
    Phân loại NAT theo RFC 3489.
    - OPEN: Không có NAT (máy có public IP trực tiếp).
    - FULL_CONE: NAT mở rộng nhất — hole punch dễ nhất.
    - RESTRICTED_CONE: Lọc theo IP nguồn — hole punch được.
    - PORT_RESTRICTED_CONE: Lọc theo IP+Port nguồn — hole punch được nhưng khó hơn.
    - SYMMETRIC: Mỗi đích khác nhau → NAT map sang port khác — KHÔNG hole punch được.
    - BLOCKED: UDP bị chặn hoàn toàn (firewall).
    - UNKNOWN: Không xác định được (test không đủ server).
    """
    OPEN              = "open"
    FULL_CONE         = "full_cone"
    RESTRICTED_CONE   = "restricted_cone"
    PORT_RESTRICTED   = "port_restricted_cone"
    SYMMETRIC         = "symmetric"
    BLOCKED           = "blocked"
    UNKNOWN           = "unknown"

    def can_hole_punch(self) -> bool:
        """Trả về True nếu NAT type này có thể hole punch được."""
        return self in (NATType.OPEN, NATType.FULL_CONE,
                        NATType.RESTRICTED_CONE, NATType.PORT_RESTRICTED)

    def __str__(self) -> str:
        labels = {
            NATType.OPEN:             "Open (Không có NAT)",
            NATType.FULL_CONE:        "Full Cone NAT (có thể hole punch)",
            NATType.RESTRICTED_CONE:  "Restricted Cone NAT (có thể hole punch)",
            NATType.PORT_RESTRICTED:  "Port-Restricted Cone NAT (có thể hole punch)",
            NATType.SYMMETRIC:        "Symmetric NAT (KHÔNG hole punch được — cần relay)",
            NATType.BLOCKED:          "Blocked (UDP bị chặn hoàn toàn)",
            NATType.UNKNOWN:          "Không xác định",
        }
        return labels.get(self, self.value)


@dataclass
class NATInfo:
    """Kết quả đầy đủ sau khi kiểm tra NAT."""
    public_ip:   str
    public_port: int
    local_ip:    str
    local_port:  int
    nat_type:    NATType

    def is_behind_nat(self) -> bool:
        return self.public_ip != self.local_ip

    def endpoint(self) -> Tuple[str, int]:
        """Public (IP, UDP port) để báo cho Tracker và các peer khác."""
        return (self.public_ip, self.public_port)

    def __str__(self) -> str:
        behind = "sau NAT" if self.is_behind_nat() else "public IP trực tiếp"
        return (
            f"NATInfo(\n"
            f"  Public endpoint : {self.public_ip}:{self.public_port}\n"
            f"  Local  endpoint : {self.local_ip}:{self.local_port}\n"
            f"  NAT type        : {self.nat_type}\n"
            f"  Vị trí          : {behind}\n"
            f"  Hole punch      : {'CÓ THỂ ✓' if self.nat_type.can_hole_punch() else 'KHÔNG ✗'}\n"
            f")"
        )


# ─── Build / Parse STUN message ───────────────────────────────────────────────

def _build_transaction_id() -> bytes:
    """Tạo Transaction ID 12 bytes ngẫu nhiên (RFC 5389)."""
    return os.urandom(12)


def _build_binding_request(transaction_id: bytes,
                            change_ip: bool = False,
                            change_port: bool = False) -> bytes:
    """
    Tạo STUN Binding Request.
    - Nếu change_ip=True hoặc change_port=True, thêm CHANGE_REQUEST attribute
      (dùng để detect NAT type theo RFC 3489).
    """
    attributes = b""

    if change_ip or change_port:
        # CHANGE_REQUEST: bit 2 = change IP, bit 1 = change port
        flags = 0
        if change_ip:
            flags |= 0x04
        if change_port:
            flags |= 0x02
        attr_value = struct.pack(">I", flags)
        attributes += struct.pack(">HH", ATTR_CHANGE_REQUEST, len(attr_value)) + attr_value

    # Header: Type (2) + Length (2) + Magic Cookie (4) + Transaction ID (12) = 20 bytes
    header = struct.pack(
        ">HHI12s",
        STUN_BINDING_REQUEST,
        len(attributes),
        STUN_MAGIC_COOKIE,
        transaction_id,
    )
    return header + attributes


def _parse_stun_response(data: bytes, transaction_id: bytes) -> Optional[Tuple[str, int]]:
    """
    Parse STUN Binding Response, trả về (public_ip, public_port).
    Ưu tiên XOR-MAPPED-ADDRESS (RFC 5389), fallback sang MAPPED-ADDRESS (RFC 3489).
    Trả về None nếu response không hợp lệ hoặc transaction_id không khớp.
    """
    if len(data) < 20:
        return None

    msg_type, msg_len, magic, txn_id = struct.unpack(">HHI12s", data[:20])

    # Xác minh transaction ID để tránh nhận nhầm response cũ hoặc packet giả
    if txn_id != transaction_id:
        logger.debug("STUN response transaction ID không khớp — bỏ qua")
        return None

    if msg_type == STUN_BINDING_ERROR:
        logger.debug("STUN server trả về error response")
        return None
    if msg_type != STUN_BINDING_RESPONSE:
        return None

    # Parse từng attribute
    offset = 20
    mapped_addr = None
    xor_mapped_addr = None

    while offset + 4 <= len(data):
        attr_type, attr_len = struct.unpack(">HH", data[offset:offset + 4])
        offset += 4
        attr_value = data[offset:offset + attr_len]
        # Padding STUN attributes về bội số 4
        offset += (attr_len + 3) & ~3

        if attr_type == ATTR_MAPPED_ADDRESS and len(attr_value) >= 8:
            family = attr_value[1]
            if family == FAMILY_IPV4:
                port = struct.unpack(">H", attr_value[2:4])[0]
                ip = socket.inet_ntoa(attr_value[4:8])
                mapped_addr = (ip, port)

        elif attr_type == ATTR_XOR_MAPPED_ADDRESS and len(attr_value) >= 8:
            family = attr_value[1]
            if family == FAMILY_IPV4:
                # XOR port với 2 byte cao của magic cookie
                x_port = struct.unpack(">H", attr_value[2:4])[0]
                port = x_port ^ (STUN_MAGIC_COOKIE >> 16)
                # XOR IP với toàn bộ magic cookie
                x_ip_bytes = attr_value[4:8]
                magic_bytes = struct.pack(">I", STUN_MAGIC_COOKIE)
                ip_bytes = bytes(a ^ b for a, b in zip(x_ip_bytes, magic_bytes))
                ip = socket.inet_ntoa(ip_bytes)
                xor_mapped_addr = (ip, port)

    # Ưu tiên XOR-MAPPED-ADDRESS vì chính xác hơn (RFC 5389)
    return xor_mapped_addr or mapped_addr


def _stun_request(sock: socket.socket,
                  server_host: str,
                  server_port: int,
                  change_ip: bool = False,
                  change_port: bool = False,
                  timeout: float = 3.0) -> Optional[Tuple[str, int]]:
    """
    Gửi STUN Binding Request đến server và trả về (public_ip, public_port).
    Thử lại tối đa 3 lần nếu không nhận được response.
    """
    transaction_id = _build_transaction_id()
    request = _build_binding_request(transaction_id, change_ip, change_port)

    sock.settimeout(timeout)
    for attempt in range(3):
        try:
            sock.sendto(request, (server_host, server_port))
            data, _ = sock.recvfrom(2048)
            result = _parse_stun_response(data, transaction_id)
            if result:
                return result
        except socket.timeout:
            logger.debug(f"STUN timeout (lần {attempt + 1}/3) tới {server_host}:{server_port}")
        except OSError as e:
            logger.debug(f"STUN OSError: {e}")
            break
    return None


# ─── STUNClient class ─────────────────────────────────────────────────────────

class STUNClient:
    """
    STUN client xác định public endpoint và NAT type của máy cục bộ.

    Thuật toán detect NAT type (theo RFC 3489, đơn giản hóa):

    Test 1: Gửi request tới STUN server chính, cổng chuẩn
      → Không nhận được → BLOCKED
      → Public IP == Local IP → OPEN (không có NAT)

    Test 2: Gửi request với CHANGE_REQUEST (đổi cả IP lẫn port)
      → Nhận được response từ IP khác → FULL_CONE
      → Không nhận được → tiếp tục Test 3

    Test 3: Gửi request tới IP thứ hai của server (server_alt_ip), cổng gốc
      → So sánh public port trả về với Test 1:
        Khác nhau → SYMMETRIC
        Giống nhau → tiếp tục Test 4

    Test 4: Gửi request với CHANGE_REQUEST (chỉ đổi port)
      → Nhận được → RESTRICTED_CONE
      → Không nhận được → PORT_RESTRICTED_CONE
    """

    def __init__(self,
                 stun_servers: list = None,
                 local_port: int = 0,
                 timeout: float = 3.0):
        """
        Args:
            stun_servers: Danh sách (host, port) STUN server. Dùng mặc định nếu None.
            local_port:   UDP port cục bộ muốn bind (0 = hệ điều hành tự chọn).
            timeout:      Timeout mỗi request STUN (giây).
        """
        self.stun_servers = stun_servers or DEFAULT_STUN_SERVERS
        self.local_port = local_port
        self.timeout = timeout
        self.last_server: Optional[Tuple[str, int]] = None

    def get_public_endpoint(self, sock: Optional[socket.socket] = None) -> Optional[NATInfo]:
        """
        Lấy public IP và UDP port. Không cần detect NAT type đầy đủ.
        Nhanh hơn get_nat_info() — dùng khi chỉ cần endpoint để báo cho Tracker.

        Returns:
            NATInfo với nat_type=UNKNOWN nếu không test thêm, hoặc None nếu STUN thất bại.
        """
        owns_socket = sock is None
        if sock is None:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            if owns_socket:
                sock.bind(("0.0.0.0", self.local_port))
            local_port = sock.getsockname()[1]   # lấy port OS đã cấp
            local_ip = self._get_local_ip()       # IP thực trong LAN

            for host, port in self.stun_servers:
                try:
                    result = _stun_request(sock, host, port, timeout=self.timeout)
                    if result:
                        pub_ip, pub_port = result
                        self.last_server = (host, port)
                        logger.info(f"Public endpoint: {pub_ip}:{pub_port} (local: {local_ip}:{local_port})")
                        return NATInfo(
                            public_ip=pub_ip,
                            public_port=pub_port,
                            local_ip=local_ip,
                            local_port=local_port,
                            nat_type=NATType.UNKNOWN,
                        )
                except Exception as e:
                    logger.debug(f"STUN server {host}:{port} thất bại: {e}")

            logger.warning("Tất cả STUN server đều thất bại")
            return None
        finally:
            if owns_socket:
                sock.close()

    def get_nat_info(self) -> NATInfo:
        """
        Xác định đầy đủ NAT type theo thuật toán 4-test RFC 3489.
        Mất khoảng 3–12 giây tùy mạng (vì cần nhiều request).

        Returns:
            NATInfo đầy đủ, bao gồm nat_type.
        """
        # Dùng 1 UDP socket duy nhất xuyên suốt để giữ cùng local port
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.bind(("0.0.0.0", self.local_port))
            local_port = sock.getsockname()[1]
            local_ip = self._get_local_ip()

            nat_type, pub_ip, pub_port = self._run_nat_tests(sock, local_ip)

            if pub_ip is None:
                pub_ip = ""
                pub_port = 0

            return NATInfo(
                public_ip=pub_ip,
                public_port=pub_port,
                local_ip=local_ip,
                local_port=local_port,
                nat_type=nat_type,
            )

    # ── Internal test logic ────────────────────────────────────────────────── #

    def _run_nat_tests(self, sock: socket.socket,
                       local_ip: str) -> Tuple[NATType, Optional[str], Optional[int]]:
        """
        Chạy 4 test STUN để xác định NAT type.
        Returns: (nat_type, public_ip, public_port)
        """
        server_host, server_port = self.stun_servers[0]

        # ── Test 1: Kết nối cơ bản ─────────────────────────────────────── #
        result1 = _stun_request(sock, server_host, server_port, timeout=self.timeout)

        if result1 is None:
            # Thử server dự phòng trước khi kết luận BLOCKED
            for alt_host, alt_port in self.stun_servers[1:]:
                result1 = _stun_request(sock, alt_host, alt_port, timeout=self.timeout)
                if result1:
                    server_host, server_port = alt_host, alt_port
                    break

        if result1 is None:
            logger.warning("Test 1 thất bại — UDP có thể bị chặn")
            return NATType.BLOCKED, None, None

        pub_ip1, pub_port1 = result1
        logger.debug(f"Test 1: public endpoint = {pub_ip1}:{pub_port1}")

        # Nếu public IP trùng local IP → Không có NAT (OPEN)
        if pub_ip1 == local_ip:
            return NATType.OPEN, pub_ip1, pub_port1

        # ── Test 2: CHANGE_REQUEST (đổi cả IP lẫn port) ───────────────── #
        # Nếu server phản hồi từ IP/port khác → Full Cone
        result2 = _stun_request(
            sock, server_host, server_port,
            change_ip=True, change_port=True,
            timeout=self.timeout
        )

        if result2 is not None:
            logger.debug("Test 2 thành công → Full Cone NAT")
            return NATType.FULL_CONE, pub_ip1, pub_port1

        # ── Test 3: Gửi tới server thứ hai (IP khác), lấy mapped port ─── #
        # Dùng server dự phòng để lấy mapping từ IP server khác
        alt_server = None
        for h, p in self.stun_servers[1:]:
            if h != server_host:
                alt_server = (h, p)
                break

        if alt_server:
            result3 = _stun_request(sock, alt_server[0], alt_server[1], timeout=self.timeout)
            if result3:
                pub_ip3, pub_port3 = result3
                logger.debug(f"Test 3: endpoint từ alt server = {pub_ip3}:{pub_port3}")
                if pub_port3 != pub_port1:
                    # Port mapping khác nhau với mỗi server → Symmetric NAT
                    logger.debug("Port mapping thay đổi → Symmetric NAT")
                    return NATType.SYMMETRIC, pub_ip1, pub_port1
            # Nếu Test 3 thất bại, giả định không phải Symmetric (không đủ thông tin)

        # ── Test 4: CHANGE_REQUEST chỉ đổi port ───────────────────────── #
        result4 = _stun_request(
            sock, server_host, server_port,
            change_ip=False, change_port=True,
            timeout=self.timeout
        )

        if result4 is not None:
            logger.debug("Test 4 thành công → Restricted Cone NAT")
            return NATType.RESTRICTED_CONE, pub_ip1, pub_port1
        else:
            logger.debug("Test 4 thất bại → Port-Restricted Cone NAT")
            return NATType.PORT_RESTRICTED, pub_ip1, pub_port1

    @staticmethod
    def _get_local_ip() -> str:
        """Lấy IP cục bộ của máy trong mạng (heuristic qua UDP connect)."""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                s.connect(("8.8.8.8", 80))
                return s.getsockname()[0]
        except Exception:
            return "127.0.0.1"


# ─── CLI standalone ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    import time

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s"
    )

    print("=" * 60)
    print("  STUN NAT Type Detector")
    print("=" * 60)

    client = STUNClient()

    # Nếu chạy với --quick, chỉ lấy public endpoint (nhanh hơn)
    quick_mode = "--quick" in sys.argv

    if quick_mode:
        print("\n[Mode: Quick — chỉ lấy public endpoint]\n")
        t0 = time.time()
        info = client.get_public_endpoint()
        elapsed = time.time() - t0
    else:
        print("\n[Mode: Full — detect NAT type (mất 5–15 giây)]\n")
        t0 = time.time()
        info = client.get_nat_info()
        elapsed = time.time() - t0

    if info:
        print(info)
        print(f"\n  Thời gian kiểm tra: {elapsed:.2f} giây")
    else:
        print("Không thể kết nối STUN server — kiểm tra kết nối Internet.")
        sys.exit(1)

