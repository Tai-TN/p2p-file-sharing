"""
tests/test_stun_client.py — Unit test cho nat/stun_client.py

Tách biệt hoàn toàn với mạng: mock socket, test parse/build logic.
"""

import struct
import socket
import unittest
from unittest.mock import patch, MagicMock, call

from nat.stun_client import (
    _build_binding_request,
    _build_transaction_id,
    _parse_stun_response,
    _stun_request,
    NATType,
    NATInfo,
    STUNClient,
    STUN_MAGIC_COOKIE,
    STUN_BINDING_RESPONSE,
    ATTR_XOR_MAPPED_ADDRESS,
    ATTR_MAPPED_ADDRESS,
    FAMILY_IPV4,
)


def _make_xor_mapped_addr_attr(ip: str, port: int) -> bytes:
    """Tạo XOR-MAPPED-ADDRESS attribute bytes cho test."""
    magic_bytes = struct.pack(">I", STUN_MAGIC_COOKIE)
    x_port = port ^ (STUN_MAGIC_COOKIE >> 16)
    ip_bytes = socket.inet_aton(ip)
    x_ip = bytes(a ^ b for a, b in zip(ip_bytes, magic_bytes))
    attr_value = struct.pack(">BBH", 0, FAMILY_IPV4, x_port) + x_ip
    return struct.pack(">HH", ATTR_XOR_MAPPED_ADDRESS, len(attr_value)) + attr_value


def _make_mapped_addr_attr(ip: str, port: int) -> bytes:
    """Tạo MAPPED-ADDRESS attribute bytes cho test."""
    ip_bytes = socket.inet_aton(ip)
    attr_value = struct.pack(">BBH", 0, FAMILY_IPV4, port) + ip_bytes
    return struct.pack(">HH", ATTR_MAPPED_ADDRESS, len(attr_value)) + attr_value


def _make_stun_response(attributes: bytes, txn_id: bytes = b"\x00" * 12) -> bytes:
    """Tạo STUN Binding Response hoàn chỉnh với txn_id cho sẵn."""
    header = struct.pack(
        ">HHI12s",
        STUN_BINDING_RESPONSE,
        len(attributes),
        STUN_MAGIC_COOKIE,
        txn_id,
    )
    return header + attributes


class TestBuildBindingRequest(unittest.TestCase):
    def test_length_without_change_request(self):
        txn = b"\x01" * 12
        req = _build_binding_request(txn)
        # Header 20 bytes, không có attribute
        self.assertEqual(len(req), 20)
        msg_type, msg_len, magic = struct.unpack(">HHI", req[:8])
        self.assertEqual(msg_type, 0x0001)
        self.assertEqual(msg_len, 0)
        self.assertEqual(magic, STUN_MAGIC_COOKIE)

    def test_with_change_request_adds_attribute(self):
        txn = b"\x02" * 12
        req = _build_binding_request(txn, change_ip=True, change_port=True)
        # Header 20 + attr header 4 + attr value 4 = 28 bytes
        self.assertEqual(len(req), 28)
        msg_type, msg_len = struct.unpack(">HH", req[:4])
        self.assertEqual(msg_type, 0x0001)
        self.assertEqual(msg_len, 8)  # attr header(4) + value(4)

    def test_transaction_id_embedded(self):
        txn = b"\xAB\xCD\xEF" * 4
        req = _build_binding_request(txn)
        _, _, _, embedded_txn = struct.unpack(">HHI12s", req[:20])
        self.assertEqual(embedded_txn, txn)

    def test_change_port_only_flag(self):
        """change_port=True, change_ip=False → flag bit 1 set, bit 2 clear."""
        txn = b"\x00" * 12
        req = _build_binding_request(txn, change_ip=False, change_port=True)
        # 20 header + 4 attr header + 4 attr value = 28
        self.assertEqual(len(req), 28)
        flags = struct.unpack(">I", req[24:28])[0]
        self.assertEqual(flags & 0x02, 0x02)   # port bit set
        self.assertEqual(flags & 0x04, 0x00)   # ip bit clear


class TestParseStunResponse(unittest.TestCase):
    def test_parse_xor_mapped_address(self):
        ip, port = "1.2.3.4", 12345
        txn = b"\x11" * 12
        attrs = _make_xor_mapped_addr_attr(ip, port)
        response = _make_stun_response(attrs, txn_id=txn)
        result = _parse_stun_response(response, txn)
        self.assertIsNotNone(result)
        self.assertEqual(result, (ip, port))

    def test_parse_mapped_address_fallback(self):
        ip, port = "5.6.7.8", 54321
        txn = b"\x22" * 12
        attrs = _make_mapped_addr_attr(ip, port)
        response = _make_stun_response(attrs, txn_id=txn)
        result = _parse_stun_response(response, txn)
        self.assertIsNotNone(result)
        self.assertEqual(result, (ip, port))

    def test_xor_preferred_over_mapped(self):
        """XOR-MAPPED-ADDRESS phải được ưu tiên hơn MAPPED-ADDRESS."""
        txn = b"\x33" * 12
        ip_xor, port_xor = "10.0.0.1", 1111
        ip_map, port_map = "10.0.0.2", 2222
        attrs = _make_mapped_addr_attr(ip_map, port_map) + _make_xor_mapped_addr_attr(ip_xor, port_xor)
        response = _make_stun_response(attrs, txn_id=txn)
        result = _parse_stun_response(response, txn)
        self.assertEqual(result, (ip_xor, port_xor))

    def test_returns_none_on_short_data(self):
        self.assertIsNone(_parse_stun_response(b"\x01\x01\x00\x00", b"\x00" * 12))

    def test_returns_none_on_error_response(self):
        # Type 0x0111 = STUN Binding Error
        txn = b"\x00" * 12
        header = struct.pack(">HHI12s", 0x0111, 0, STUN_MAGIC_COOKIE, txn)
        self.assertIsNone(_parse_stun_response(header, txn))

    def test_returns_none_on_wrong_transaction_id(self):
        """Bug fix: response với transaction ID sai phải bị từ chối."""
        txn_sent = b"\xAA" * 12
        txn_other = b"\xBB" * 12
        attrs = _make_xor_mapped_addr_attr("9.9.9.9", 9999)
        response = _make_stun_response(attrs, txn_id=txn_other)   # response của request khác
        result = _parse_stun_response(response, txn_sent)
        self.assertIsNone(result, "Phải từ chối response có transaction ID không khớp")

    def test_accepts_matching_transaction_id(self):
        """Response với đúng transaction ID phải được chấp nhận."""
        txn = b"\xCC" * 12
        attrs = _make_xor_mapped_addr_attr("203.0.113.1", 5555)
        response = _make_stun_response(attrs, txn_id=txn)
        result = _parse_stun_response(response, txn)
        self.assertIsNotNone(result)
        self.assertEqual(result, ("203.0.113.1", 5555))


class TestNATType(unittest.TestCase):
    def test_can_hole_punch_open(self):
        self.assertTrue(NATType.OPEN.can_hole_punch())

    def test_can_hole_punch_full_cone(self):
        self.assertTrue(NATType.FULL_CONE.can_hole_punch())

    def test_can_hole_punch_restricted(self):
        self.assertTrue(NATType.RESTRICTED_CONE.can_hole_punch())

    def test_can_hole_punch_port_restricted(self):
        self.assertTrue(NATType.PORT_RESTRICTED.can_hole_punch())

    def test_cannot_hole_punch_symmetric(self):
        self.assertFalse(NATType.SYMMETRIC.can_hole_punch())

    def test_cannot_hole_punch_blocked(self):
        self.assertFalse(NATType.BLOCKED.can_hole_punch())


class TestNATInfo(unittest.TestCase):
    def test_is_behind_nat_true(self):
        info = NATInfo("1.2.3.4", 1234, "192.168.1.10", 5678, NATType.FULL_CONE)
        self.assertTrue(info.is_behind_nat())

    def test_is_behind_nat_false(self):
        info = NATInfo("1.2.3.4", 1234, "1.2.3.4", 1234, NATType.OPEN)
        self.assertFalse(info.is_behind_nat())

    def test_endpoint(self):
        info = NATInfo("9.9.9.9", 9999, "192.168.0.1", 1234, NATType.RESTRICTED_CONE)
        self.assertEqual(info.endpoint(), ("9.9.9.9", 9999))


class TestSTUNClientMocked(unittest.TestCase):
    """Test STUNClient logic với mock socket — không cần mạng thật."""

    def _make_response_bytes(self, ip: str, port: int, txn_id: bytes = b"\x00" * 12) -> bytes:
        attrs = _make_xor_mapped_addr_attr(ip, port)
        return _make_stun_response(attrs, txn_id=txn_id)

    @patch("nat.stun_client.socket.socket")
    def test_get_public_endpoint_success(self, mock_socket_class):
        """get_public_endpoint() trả về NATInfo khi STUN server phản hồi."""
        mock_sock = MagicMock()
        mock_socket_class.return_value.__enter__ = MagicMock(return_value=mock_sock)
        mock_socket_class.return_value.__exit__ = MagicMock(return_value=False)
        mock_sock.getsockname.return_value = ("0.0.0.0", 44444)

        # _stun_request tạo transaction_id ngẫu nhiên bên trong → mock toàn bộ _stun_request
        client = STUNClient(stun_servers=[("fake.stun.test", 19302)])
        with patch("nat.stun_client._stun_request", return_value=("203.0.113.5", 44444)):
            with patch.object(STUNClient, "_get_local_ip", return_value="10.0.0.1"):
                info = client.get_public_endpoint()

        self.assertIsNotNone(info)
        self.assertEqual(info.public_ip, "203.0.113.5")
        self.assertEqual(info.public_port, 44444)
        self.assertEqual(info.nat_type, NATType.UNKNOWN)

    @patch("nat.stun_client.socket.socket")
    def test_get_public_endpoint_all_fail(self, mock_socket_class):
        """get_public_endpoint() trả về None khi mọi server đều timeout."""
        mock_sock = MagicMock()
        mock_socket_class.return_value.__enter__ = MagicMock(return_value=mock_sock)
        mock_socket_class.return_value.__exit__ = MagicMock(return_value=False)
        mock_sock.getsockname.return_value = ("0.0.0.0", 11111)

        client = STUNClient(stun_servers=[("fake.stun.test", 19302)])
        with patch("nat.stun_client._stun_request", return_value=None):
            with patch.object(STUNClient, "_get_local_ip", return_value="10.0.0.1"):
                info = client.get_public_endpoint()

        self.assertIsNone(info)

    @patch("nat.stun_client.socket.socket")
    def test_get_public_endpoint_tries_fallback_server(self, mock_socket_class):
        """Nếu server đầu thất bại, thử server dự phòng."""
        mock_sock = MagicMock()
        mock_socket_class.return_value.__enter__ = MagicMock(return_value=mock_sock)
        mock_socket_class.return_value.__exit__ = MagicMock(return_value=False)
        mock_sock.getsockname.return_value = ("0.0.0.0", 5000)

        servers = [("server1.test", 19302), ("server2.test", 3478)]
        client = STUNClient(stun_servers=servers)
        # server1 → None, server2 → thành công
        with patch("nat.stun_client._stun_request", side_effect=[None, ("1.1.1.1", 5000)]):
            with patch.object(STUNClient, "_get_local_ip", return_value="192.168.0.1"):
                info = client.get_public_endpoint()

        self.assertIsNotNone(info)
        self.assertEqual(info.public_ip, "1.1.1.1")


class TestRunNATTests(unittest.TestCase):
    """
    Test thuật toán 4-test phân loại NAT type trong _run_nat_tests().
    Mock _stun_request để mô phỏng từng kịch bản NAT mà không cần mạng.
    """

    def _make_client(self):
        """Tạo STUNClient với 2 server khác tên để Test 3 hoạt động."""
        return STUNClient(
            stun_servers=[("stun1.test", 19302), ("stun2.test", 3478)],
            timeout=1.0,
        )

    def _run(self, side_effects):
        """Chạy _run_nat_tests với danh sách kết quả mock cho _stun_request."""
        client = self._make_client()
        mock_sock = MagicMock()
        with patch("nat.stun_client._stun_request", side_effect=side_effects):
            return client._run_nat_tests(mock_sock, local_ip="192.168.1.10")

    # ── Test 1 thất bại → BLOCKED ─────────────────────────────────────── #
    def test_blocked_when_all_servers_fail(self):
        """Test 1: tất cả server không phản hồi → BLOCKED."""
        nat_type, ip, port = self._run([None, None, None, None])
        self.assertEqual(nat_type, NATType.BLOCKED)
        self.assertIsNone(ip)

    def test_blocked_uses_fallback_server_first(self):
        """Test 1: server chính fail, server dự phòng cũng fail → BLOCKED."""
        nat_type, _, _ = self._run([None, None])
        self.assertEqual(nat_type, NATType.BLOCKED)

    # ── Public IP == Local IP → OPEN ──────────────────────────────────── #
    def test_open_when_public_equals_local(self):
        """Test 1: public IP trùng local IP → OPEN (không có NAT)."""
        # local_ip="192.168.1.10" nhưng STUN trả về cùng IP → OPEN
        nat_type, ip, port = self._run([("192.168.1.10", 12345)])
        self.assertEqual(nat_type, NATType.OPEN)
        self.assertEqual(ip, "192.168.1.10")
        self.assertEqual(port, 12345)

    # ── Test 2 thành công → FULL_CONE ─────────────────────────────────── #
    def test_full_cone_when_test2_succeeds(self):
        """Test 1 OK (public != local) + Test 2 (CHANGE_REQUEST) OK → FULL_CONE."""
        nat_type, ip, port = self._run([
            ("1.2.3.4", 10000),   # Test 1: thành công
            ("1.2.3.4", 10000),   # Test 2: CHANGE_REQUEST → server phản hồi → Full Cone
        ])
        self.assertEqual(nat_type, NATType.FULL_CONE)
        self.assertEqual(ip, "1.2.3.4")

    # ── Test 2 fail + Test 3 port khác → SYMMETRIC ────────────────────── #
    def test_symmetric_when_ports_differ(self):
        """Test 1 OK + Test 2 fail + Test 3 (alt server) trả về port khác → SYMMETRIC."""
        nat_type, ip, port = self._run([
            ("5.5.5.5", 1000),    # Test 1: thành công, port=1000
            None,                  # Test 2: CHANGE_REQUEST fail
            ("5.5.5.5", 2000),    # Test 3: alt server → port khác (2000 != 1000) → Symmetric
        ])
        self.assertEqual(nat_type, NATType.SYMMETRIC)

    # ── Test 2 fail + Test 3 port giống + Test 4 OK → RESTRICTED_CONE ── #
    def test_restricted_cone_when_test4_succeeds(self):
        """Test 1 OK + Test 2 fail + Test 3 port giống + Test 4 OK → RESTRICTED_CONE."""
        nat_type, ip, port = self._run([
            ("6.6.6.6", 3000),    # Test 1
            None,                  # Test 2: fail
            ("6.6.6.6", 3000),    # Test 3: cùng port → không phải Symmetric
            ("6.6.6.6", 3000),    # Test 4: CHANGE_REQUEST(port only) → server trả lời → Restricted
        ])
        self.assertEqual(nat_type, NATType.RESTRICTED_CONE)

    # ── Test 2 fail + Test 3 port giống + Test 4 fail → PORT_RESTRICTED ─ #
    def test_port_restricted_when_test4_fails(self):
        """Test 1 OK + Test 2 fail + Test 3 port giống + Test 4 fail → PORT_RESTRICTED."""
        nat_type, ip, port = self._run([
            ("7.7.7.7", 4000),    # Test 1
            None,                  # Test 2: fail
            ("7.7.7.7", 4000),    # Test 3: cùng port → không phải Symmetric
            None,                  # Test 4: fail → Port-Restricted
        ])
        self.assertEqual(nat_type, NATType.PORT_RESTRICTED)

    # ── Test 2 fail + Test 3 fail (alt server timeout) + Test 4 OK ────── #
    def test_restricted_cone_when_test3_unavailable(self):
        """Test 3 timeout nhưng Test 4 OK → RESTRICTED_CONE (không đủ thông tin kết luận Symmetric)."""
        nat_type, _, _ = self._run([
            ("8.8.8.8", 5000),    # Test 1
            None,                  # Test 2: fail
            None,                  # Test 3: alt server cũng timeout
            ("8.8.8.8", 5000),    # Test 4: OK → Restricted Cone
        ])
        self.assertEqual(nat_type, NATType.RESTRICTED_CONE)


if __name__ == "__main__":
    unittest.main(verbosity=2)

