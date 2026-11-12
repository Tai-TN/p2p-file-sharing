"""Kiểm tra tracker relay, xác thực và đường tải auto trên localhost."""

import hashlib
import os
import socket
import tempfile
import time
import unittest
from unittest.mock import patch

from nat.signaling import TrackerSignalChannel
from peer.client import P2PClient
from peer.transfer_engine import TransferEngine
from nat.stun_client import NATInfo, NATType
from protocol.auth import connect_tracker
from protocol.messages import MessageType, recv_msg, send_msg
from tracker.tracker_server import TrackerServer


class RelayTests(unittest.TestCase):
    def setUp(self):
        self.secret_patch = patch.dict(os.environ, {"P2P_TRACKER_SECRET": "test-secret"})
        self.secret_patch.start()
        self.tracker = TrackerServer("127.0.0.1", 0)
        self.tracker.start()

    def tearDown(self):
        self.tracker.stop()
        self.secret_patch.stop()

    def _request(self, message):
        with connect_tracker("127.0.0.1", self.tracker.port) as sock:
            self.assertTrue(send_msg(sock, message))
            return recv_msg(sock)

    def test_wrong_secret_is_rejected(self):
        with patch.dict(os.environ, {"P2P_TRACKER_SECRET": "wrong-secret"}):
            with self.assertRaises(PermissionError):
                connect_tracker("127.0.0.1", self.tracker.port)

    def test_invalid_join_and_session_limit(self):
        info_hash = "ab" * 32
        self.tracker.state.register_peer(info_hash, "leecher", "127.0.0.1", 5001)
        self.tracker.state.register_peer(info_hash, "seeder", "127.0.0.1", 5002)
        channel = TrackerSignalChannel("127.0.0.1", self.tracker.port, "seeder",
                                       "127.0.0.1", 0, 5002)
        self.assertTrue(channel.start())
        try:
            bad = self._request({"type": MessageType.RELAY_JOIN.value,
                                 "peer_id": "seeder", "session_id": "0" * 32,
                                 "join_token": "1" * 32})
            self.assertEqual(bad["type"], MessageType.RELAY_ERROR.value)
            with patch.object(self.tracker.relay, "MAX_SESSIONS", 0):
                limited = self._request({"type": MessageType.RELAY_OPEN.value,
                                         "peer_id": "leecher", "target_peer_id": "seeder",
                                         "info_hash": info_hash})
            self.assertIn("giới hạn", limited["reason"])
        finally:
            channel.stop()

    def test_pending_session_expires_and_cannot_join(self):
        info_hash = "cd" * 32
        self.tracker.state.register_peer(info_hash, "leecher", "127.0.0.1", 5001)
        self.tracker.state.register_peer(info_hash, "seeder", "127.0.0.1", 5002)
        invite = []
        channel = TrackerSignalChannel("127.0.0.1", self.tracker.port, "seeder",
                                       "127.0.0.1", 0, 5002)
        channel.on_relay_invite = invite.append
        self.assertTrue(channel.start())
        try:
            with patch.object(self.tracker.relay, "JOIN_TIMEOUT", 0.1):
                expired = self._request({"type": MessageType.RELAY_OPEN.value,
                                         "peer_id": "leecher", "target_peer_id": "seeder",
                                         "info_hash": info_hash})
            self.assertEqual(expired["type"], MessageType.RELAY_ERROR.value)
            self.assertIn("Hết hạn", expired["reason"])
            self.assertEqual(len(invite), 1)
            stale = self._request({"type": MessageType.RELAY_JOIN.value,
                                   "peer_id": "seeder",
                                   "session_id": invite[0]["session_id"],
                                   "join_token": invite[0]["join_token"]})
            self.assertEqual(stale["type"], MessageType.RELAY_ERROR.value)
        finally:
            channel.stop()

    def test_auto_falls_back_to_relay_without_stun_and_matches_hash(self):
        data = os.urandom(80 * 1024)
        with tempfile.TemporaryDirectory() as temp_dir:
            source = os.path.join(temp_dir, "source.bin")
            meta_path = os.path.join(temp_dir, "source.bin.meta.json")
            with open(source, "wb") as handle:
                handle.write(data)
            with patch("peer.client.STUNClient.get_public_endpoint", return_value=None):
                seeder = P2PClient("seeder", listen_port=0,
                                   tracker_host="127.0.0.1", tracker_port=self.tracker.port)
                leecher = P2PClient("leecher", listen_port=0,
                                    tracker_host="127.0.0.1", tracker_port=self.tracker.port)
                seeder.start()
                leecher.start()
            try:
                info_hash = seeder.share_file(source, meta_path)
                with patch.object(TransferEngine, "_connect_and_handshake", return_value=None):
                    leecher.download_file(meta_path, os.path.join(temp_dir, "downloads"))
                    deadline = time.monotonic() + 12.0
                    while time.monotonic() < deadline:
                        if leecher._tasks[info_hash]["pm"].is_complete():
                            break
                        time.sleep(0.05)
                self.assertTrue(leecher._tasks[info_hash]["pm"].is_complete())
                self.assertEqual(leecher._tasks[info_hash]["engine"].current_path, "RELAY")
                with open(os.path.join(temp_dir, "downloads", "source.bin"), "rb") as handle:
                    self.assertEqual(hashlib.sha256(handle.read()).digest(),
                                     hashlib.sha256(data).digest())
            finally:
                leecher.stop()
                seeder.stop()

    def test_auto_tries_udp_then_relays_after_probe_timeout(self):
        data = os.urandom(24 * 1024)
        with tempfile.TemporaryDirectory() as temp_dir:
            source = os.path.join(temp_dir, "source.bin")
            meta_path = os.path.join(temp_dir, "source.bin.meta.json")
            with open(source, "wb") as handle:
                handle.write(data)

            def fake_stun(_client, udp_sock):
                port = udp_sock.getsockname()[1]
                return NATInfo("127.0.0.1", port, "127.0.0.1", port, NATType.OPEN)

            with patch("peer.client.STUNClient.get_public_endpoint", autospec=True,
                       side_effect=fake_stun), \
                 patch("peer.client.STUNClient.gather_candidates", return_value=[]):
                seeder = P2PClient("seeder-udp", listen_port=0,
                                   tracker_host="127.0.0.1", tracker_port=self.tracker.port)
                leecher = P2PClient("leecher-udp", listen_port=0,
                                    tracker_host="127.0.0.1", tracker_port=self.tracker.port)
                seeder.start()
                leecher.start()
            try:
                info_hash = seeder.share_file(source, meta_path)
                with patch.object(TransferEngine, "_connect_and_handshake", return_value=None), \
                     patch.object(TransferEngine, "DIRECT_PROBE_TIMEOUT", 0.2), \
                     patch.object(TrackerSignalChannel, "request_punch", return_value=True) as punch:
                    leecher.download_file(meta_path, os.path.join(temp_dir, "downloads"))
                    deadline = time.monotonic() + 10.0
                    while time.monotonic() < deadline:
                        if leecher._tasks[info_hash]["pm"].is_complete():
                            break
                        time.sleep(0.05)
                self.assertTrue(punch.called)
                self.assertTrue(leecher._tasks[info_hash]["pm"].is_complete())
                self.assertEqual(leecher._tasks[info_hash]["engine"].current_path, "RELAY")
                with open(os.path.join(temp_dir, "downloads", "source.bin"), "rb") as handle:
                    self.assertEqual(handle.read(), data)
            finally:
                leecher.stop()
                seeder.stop()

    def test_auto_relays_after_three_failed_udp_piece_requests(self):
        data = os.urandom(40 * 1024)
        with tempfile.TemporaryDirectory() as temp_dir:
            source = os.path.join(temp_dir, "source.bin")
            meta_path = os.path.join(temp_dir, "source.bin.meta.json")
            with open(source, "wb") as handle:
                handle.write(data)

            def fake_stun(_client, udp_sock):
                port = udp_sock.getsockname()[1]
                return NATInfo("127.0.0.1", port, "127.0.0.1", port, NATType.OPEN)

            with patch("peer.client.STUNClient.get_public_endpoint", autospec=True,
                       side_effect=fake_stun), \
                 patch("peer.client.STUNClient.gather_candidates", return_value=[]):
                seeder = P2PClient("seeder-fallback", listen_port=0,
                                   tracker_host="127.0.0.1", tracker_port=self.tracker.port)
                leecher = P2PClient("leecher-fallback", listen_port=0,
                                    tracker_host="127.0.0.1", tracker_port=self.tracker.port)
                seeder.start()
                leecher.start()
            try:
                info_hash = seeder.share_file(source, meta_path)
                with patch.object(TransferEngine, "_connect_and_handshake", return_value=None), \
                     patch.object(TransferEngine, "_find_udp_peer", return_value="seeder-fallback"), \
                     patch.object(leecher._udp_transfer, "request_piece", return_value=None) as request:
                    leecher.download_file(meta_path, os.path.join(temp_dir, "downloads"))
                    deadline = time.monotonic() + 10.0
                    while time.monotonic() < deadline:
                        if leecher._tasks[info_hash]["pm"].is_complete():
                            break
                        time.sleep(0.05)
                self.assertEqual(request.call_count, 3)
                self.assertTrue(leecher._tasks[info_hash]["pm"].is_complete())
                self.assertEqual(leecher._tasks[info_hash]["engine"].current_path, "RELAY")
                with open(os.path.join(temp_dir, "downloads", "source.bin"), "rb") as handle:
                    self.assertEqual(hashlib.sha256(handle.read()).digest(),
                                     hashlib.sha256(data).digest())
            finally:
                leecher.stop()
                seeder.stop()

    def test_auto_uses_direct_tcp_when_reachable(self):
        data = os.urandom(12 * 1024)
        with tempfile.TemporaryDirectory() as temp_dir:
            source = os.path.join(temp_dir, "source.bin")
            meta_path = os.path.join(temp_dir, "source.bin.meta.json")
            with open(source, "wb") as handle:
                handle.write(data)
            with patch("peer.client.STUNClient.get_public_endpoint", return_value=None):
                seeder = P2PClient("direct-seeder", listen_port=0,
                                   tracker_host="127.0.0.1", tracker_port=self.tracker.port,
                                   advertise_ip="127.0.0.1")
                leecher = P2PClient("direct-leecher", listen_port=0,
                                    tracker_host="127.0.0.1", tracker_port=self.tracker.port,
                                    advertise_ip="127.0.0.1")
                seeder.start()
                leecher.start()
            try:
                info_hash = seeder.share_file(source, meta_path)
                leecher.download_file(meta_path, os.path.join(temp_dir, "downloads"))
                deadline = time.monotonic() + 8.0
                while time.monotonic() < deadline:
                    if leecher._tasks[info_hash]["pm"].is_complete():
                        break
                    time.sleep(0.05)
                self.assertTrue(leecher._tasks[info_hash]["pm"].is_complete())
                self.assertEqual(leecher._tasks[info_hash]["engine"].current_path, "DIRECT_TCP")
                with open(os.path.join(temp_dir, "downloads", "source.bin"), "rb") as handle:
                    self.assertEqual(handle.read(), data)
            finally:
                leecher.stop()
                seeder.stop()


if __name__ == "__main__":
    unittest.main()
