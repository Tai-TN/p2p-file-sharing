"""
tests/test_signaling.py — Test kênh signaling NAT traversal (Bước 3).

Kiểm tra toàn bộ luồng SIGNAL_REGISTER → SIGNAL_READY → PUNCH_REQUEST → PUNCH_SIGNAL
với Tracker và TrackerSignalChannel chạy thực sự trên localhost.
"""

import socket
import threading
import time
import unittest
from unittest.mock import patch

from tracker.tracker_server import TrackerServer
from nat.signaling import TrackerSignalChannel
from protocol.messages import (
    MessageType,
    send_msg, recv_msg,
    create_signal_register_msg,
    create_punch_request_msg,
    create_punch_signal_msg,
    create_punch_failed_msg,
)


def _free_port() -> int:
    """Lấy 1 port trống trên localhost."""
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class TestSignalMessages(unittest.TestCase):
    """Test các builder functions cho signaling messages."""

    def test_signal_register_msg(self):
        msg = create_signal_register_msg("p1", "1.2.3.4", 5000, 6881)
        self.assertEqual(msg["type"], MessageType.SIGNAL_REGISTER.value)
        self.assertEqual(msg["peer_id"], "p1")
        self.assertEqual(msg["ip"], "1.2.3.4")
        self.assertEqual(msg["udp_port"], 5000)
        self.assertEqual(msg["tcp_port"], 6881)

    def test_punch_request_msg(self):
        msg = create_punch_request_msg("peer_B")
        self.assertEqual(msg["type"], MessageType.PUNCH_REQUEST.value)
        self.assertEqual(msg["target_peer_id"], "peer_B")

    def test_punch_signal_msg(self):
        msg = create_punch_signal_msg("peer_A", "9.9.9.9", 4444, 6881)
        self.assertEqual(msg["type"], MessageType.PUNCH_SIGNAL.value)
        self.assertEqual(msg["peer_id"], "peer_A")
        self.assertEqual(msg["ip"], "9.9.9.9")
        self.assertEqual(msg["udp_port"], 4444)
        self.assertEqual(msg["tcp_port"], 6881)

    def test_punch_failed_msg(self):
        msg = create_punch_failed_msg("peer offline")
        self.assertEqual(msg["type"], MessageType.PUNCH_FAILED.value)
        self.assertIn("reason", msg)


class TestTrackerSignalChannel(unittest.TestCase):
    """
    Test end-to-end signaling giữa TrackerSignalChannel và TrackerServer
    chạy thực sự trên localhost.
    """

    def setUp(self):
        self.port = _free_port()
        self.tracker = TrackerServer(host="127.0.0.1", port=self.port)
        self.tracker.start()
        time.sleep(0.05)  # chờ tracker bind

    def tearDown(self):
        self.tracker.stop()
        time.sleep(0.05)

    def _make_channel(self, peer_id: str, udp_port: int = 10000,
                      tcp_port: int = 6881) -> TrackerSignalChannel:
        return TrackerSignalChannel(
            tracker_host="127.0.0.1",
            tracker_port=self.port,
            peer_id=peer_id,
            ip="127.0.0.1",
            udp_port=udp_port,
            tcp_port=tcp_port,
            connect_timeout=3.0,
        )

    # ── Đăng ký channel ──────────────────────────────────────────────────── #

    def test_register_and_ready(self):
        """SIGNAL_REGISTER gửi lên → Tracker trả về SIGNAL_READY."""
        ch = self._make_channel("peer_A", udp_port=11111)
        try:
            ok = ch.start()
            self.assertTrue(ok, "start() phải trả về True khi Tracker online")
            self.assertTrue(ch.is_connected)
            # Kiểm tra Tracker đã lưu channel
            time.sleep(0.1)
            info = self.tracker.state.get_signal_info("peer_A")
            self.assertIsNotNone(info, "Tracker phải lưu signal channel của peer_A")
            self.assertEqual(info["udp_port"], 11111)
        finally:
            ch.stop()

    def test_channel_stored_after_register(self):
        """Tracker lưu đúng ip, udp_port, tcp_port của peer."""
        ch = self._make_channel("peer_B", udp_port=22222, tcp_port=7000)
        try:
            ch.start()
            time.sleep(0.1)
            info = self.tracker.state.get_signal_info("peer_B")
            self.assertIsNotNone(info)
            self.assertEqual(info["udp_port"], 22222)
            self.assertEqual(info["tcp_port"], 7000)
        finally:
            ch.stop()

    # ── PUNCH_REQUEST → PUNCH_SIGNAL relay ───────────────────────────────── #

    def test_punch_signal_relay_between_two_peers(self):
        """
        Luồng đầy đủ:
          A và B đều đăng ký signal channel.
          A gửi PUNCH_REQUEST(target=B).
          → B nhận PUNCH_SIGNAL với endpoint của A.
          → A nhận PUNCH_SIGNAL với endpoint của B.
        """
        signal_received_by_b = threading.Event()
        signal_received_by_a = threading.Event()
        b_received_msg = {}
        a_received_msg = {}

        def on_signal_b(msg):
            b_received_msg.update(msg)
            signal_received_by_b.set()

        def on_signal_a(msg):
            a_received_msg.update(msg)
            signal_received_by_a.set()

        ch_a = self._make_channel("peer_A", udp_port=31111, tcp_port=6001)
        ch_b = self._make_channel("peer_B", udp_port=32222, tcp_port=6002)
        ch_a.on_punch_signal = on_signal_a
        ch_b.on_punch_signal = on_signal_b

        try:
            self.assertTrue(ch_a.start())
            self.assertTrue(ch_b.start())
            time.sleep(0.1)   # chờ cả 2 channel được đăng ký

            # A yêu cầu punch với B
            ok = ch_a.request_punch("peer_B")
            self.assertTrue(ok)

            # Chờ cả 2 nhận signal (timeout 3 giây)
            got_b = signal_received_by_b.wait(timeout=3.0)
            got_a = signal_received_by_a.wait(timeout=3.0)

            self.assertTrue(got_b, "peer_B phải nhận PUNCH_SIGNAL")
            self.assertTrue(got_a, "peer_A phải nhận PUNCH_SIGNAL")

            # peer_B nhận thông tin của A
            self.assertEqual(b_received_msg.get("peer_id"), "peer_A")
            self.assertEqual(b_received_msg.get("udp_port"), 31111)

            # peer_A nhận thông tin của B
            self.assertEqual(a_received_msg.get("peer_id"), "peer_B")
            self.assertEqual(a_received_msg.get("udp_port"), 32222)

        finally:
            ch_a.stop()
            ch_b.stop()

    def test_punch_request_to_offline_peer_returns_failed(self):
        """PUNCH_REQUEST tới peer không có signal channel → nhận PUNCH_FAILED."""
        failed_reason = []
        received_failed = threading.Event()

        def on_failed(reason):
            failed_reason.append(reason)
            received_failed.set()

        ch_a = self._make_channel("peer_A2", udp_port=41111)
        ch_a.on_punch_failed = on_failed

        try:
            ch_a.start()
            time.sleep(0.1)
            ch_a.request_punch("peer_NONEXISTENT")

            got = received_failed.wait(timeout=3.0)
            self.assertTrue(got, "Phải nhận PUNCH_FAILED khi target offline")
            self.assertTrue(len(failed_reason) > 0)
        finally:
            ch_a.stop()

    # ── Cleanup khi peer ngắt kết nối ──────────────────────────────────────#

    def test_channel_removed_after_stop(self):
        """Sau khi stop(), Tracker không còn lưu signal channel của peer."""
        ch = self._make_channel("peer_C")
        ch.start()
        time.sleep(0.1)
        self.assertIsNotNone(self.tracker.state.get_signal_info("peer_C"))

        ch.stop()
        # Chờ Tracker listener thread phát hiện EOF và unregister (tối đa 2 giây)
        deadline = time.time() + 2.0
        while time.time() < deadline:
            if self.tracker.state.get_signal_info("peer_C") is None:
                break
            time.sleep(0.05)
        self.assertIsNone(self.tracker.state.get_signal_info("peer_C"))

    def test_keepalive_prevents_idle_disconnect(self):
        self.tracker.signal_idle_timeout = 0.2
        ch = self._make_channel("peer_keepalive")
        try:
            with patch("nat.signaling._KEEPALIVE_INTERVAL", 0.05):
                self.assertTrue(ch.start())
                time.sleep(0.5)
                self.assertTrue(ch.is_connected)
                self.assertIsNotNone(self.tracker.state.get_signal_info("peer_keepalive"))
        finally:
            ch.stop()

    def test_channel_reconnects_after_connection_drops(self):
        ch = self._make_channel("peer_reconnect")
        try:
            with patch("nat.signaling._RECONNECT_DELAY", 0.05):
                self.assertTrue(ch.start())
                old_sock = self.tracker.state.get_signal_info("peer_reconnect")["sock"]
                old_sock.shutdown(socket.SHUT_RDWR)
                old_sock.close()

                deadline = time.monotonic() + 2.0
                while time.monotonic() < deadline:
                    info = self.tracker.state.get_signal_info("peer_reconnect")
                    if info and info["sock"] is not old_sock and ch.is_connected:
                        break
                    time.sleep(0.02)
                else:
                    self.fail("Signal channel không kết nối lại")
                self.assertIsNotNone(self.tracker.state.get_signal_info("peer_reconnect"))
        finally:
            ch.stop()


class TestTrackerStateSignalChannel(unittest.TestCase):
    """Unit test cho TrackerState signal channel management (không cần mạng)."""

    def setUp(self):
        from tracker.state import TrackerState
        self.state = TrackerState()

    def test_register_and_get(self):
        mock_sock = object()   # object giả làm socket
        self.state.register_signal_channel("p1", mock_sock, "1.1.1.1", 5000, 6881)
        info = self.state.get_signal_info("p1")
        self.assertIsNotNone(info)
        self.assertEqual(info["ip"], "1.1.1.1")
        self.assertEqual(info["udp_port"], 5000)
        self.assertEqual(info["tcp_port"], 6881)

    def test_get_nonexistent_returns_none(self):
        self.assertIsNone(self.state.get_signal_info("nonexistent"))

    def test_unregister(self):
        mock_sock = object()
        self.state.register_signal_channel("p2", mock_sock, "2.2.2.2", 6000, 7000)
        self.state.unregister_signal_channel("p2")
        self.assertIsNone(self.state.get_signal_info("p2"))

    def test_get_returns_copy(self):
        """get_signal_info() trả bản sao — mutation không ảnh hưởng internal state."""
        mock_sock = object()
        self.state.register_signal_channel("p3", mock_sock, "3.3.3.3", 7000, 8000)
        info = self.state.get_signal_info("p3")
        info["ip"] = "MUTATED"
        # Internal state không bị thay đổi
        info2 = self.state.get_signal_info("p3")
        self.assertEqual(info2["ip"], "3.3.3.3")


if __name__ == "__main__":
    unittest.main(verbosity=2)
