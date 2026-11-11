"""Kiểm thử danh tính peer: peer_id uuid4 cố định, Tracker từ chối id trùng, xác minh HANDSHAKE."""
import os
import shutil
import socket
import tempfile
import time
import unittest
from unittest.mock import patch

from peer.connection import PeerConnection
from peer.identity import is_valid_peer_id, load_or_create_peer_id, new_peer_id
from peer.peer_node import PeerNode
from protocol.messages import (
    MessageType,
    create_announce_msg,
    create_handshake_msg,
    recv_msg,
    send_msg,
)
from storage.metainfo import create_metainfo
from storage.piece_manager import PieceManager
from tracker.state import REGISTER_DUPLICATE_ID, REGISTER_OK, TrackerState
from tracker.tracker_server import TrackerServer


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class PeerIdTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="p2p_id_")

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_new_peer_id_format_and_uniqueness(self):
        ids = {new_peer_id() for _ in range(2000)}
        self.assertEqual(len(ids), 2000)
        for value in list(ids)[:5]:
            self.assertRegex(value, r"^peer_[0-9a-f]{12}$")
            self.assertTrue(is_valid_peer_id(value))

    def test_id_is_persisted_across_restarts(self):
        path = os.path.join(self.tmp, "sub", ".peer_id")
        first = load_or_create_peer_id(path)
        self.assertEqual(load_or_create_peer_id(path), first)

    def test_corrupt_id_file_is_regenerated(self):
        path = os.path.join(self.tmp, ".peer_id")
        with open(path, "w", encoding="utf-8") as f:
            f.write("có khoảng trắng !!")
        value = load_or_create_peer_id(path)
        self.assertTrue(is_valid_peer_id(value))
        self.assertEqual(load_or_create_peer_id(path), value)


class DuplicateIdTrackerTest(unittest.TestCase):
    def setUp(self):
        self.state = TrackerState(peer_timeout=60.0)
        self.assertEqual(self.state.register_peer("h1", "peer_x", "10.0.0.1", 6881), REGISTER_OK)

    def test_same_id_from_other_ip_is_rejected_everywhere(self):
        self.assertEqual(self.state.register_peer("h1", "peer_x", "10.0.0.2", 6881), REGISTER_DUPLICATE_ID)
        # Kể cả ở swarm khác: một id chỉ thuộc về một máy.
        self.assertEqual(self.state.register_peer("h2", "peer_x", "10.0.0.2", 6881), REGISTER_DUPLICATE_ID)
        peers = self.state.get_peers("h1")
        self.assertEqual([(p["peer_id"], p["ip"]) for p in peers], [("peer_x", "10.0.0.1")])
        self.assertEqual(self.state.snapshot()["events"][0]["kind"], "REJECTED")

    def test_same_machine_restart_with_new_port_is_accepted(self):
        self.assertEqual(self.state.register_peer("h1", "peer_x", "10.0.0.1", 7000), REGISTER_OK)
        self.assertEqual(self.state.get_peers("h1")[0]["port"], 7000)

    def test_stale_entry_does_not_block_new_owner(self):
        self.state.swarms["h1"]["peer_x"]["last_seen"] = time.time() - 120
        self.assertEqual(self.state.register_peer("h1", "peer_x", "10.0.0.2", 6881), REGISTER_OK)
        self.assertEqual(self.state.get_peers("h1")[0]["ip"], "10.0.0.2")

    def test_stopped_from_other_ip_cannot_kick_real_peer(self):
        result = self.state.register_peer("h1", "peer_x", "10.0.0.9", 6881, event="stopped")
        self.assertEqual(result, REGISTER_DUPLICATE_ID)
        self.assertEqual(len(self.state.get_peers("h1")), 1)

    def test_server_answers_duplicate_id_status(self):
        port = free_port()
        server = TrackerServer(host="127.0.0.1", port=port)
        server.start()
        try:
            server.state.register_peer("hx", "peer_dup", "10.1.1.1", 6881)
            with socket.create_connection(("127.0.0.1", port), timeout=3) as sock:
                send_msg(sock, create_announce_msg("hx", "peer_dup", 6881, ip="10.1.1.2"))
                resp = recv_msg(sock)
            self.assertEqual(resp["status"], "DUPLICATE_ID")
        finally:
            server.stop()


class HandshakeIdentityTest(unittest.TestCase):
    """PeerNode chỉ từ chối khi peer_id đã được Tracker ghi nhận ở một IP khác."""

    def setUp(self):
        self.node = PeerNode("peer_me", "127.0.0.1", 0)
        self.node.update_known_peers([{"peer_id": "peer_good", "ip": "192.168.1.20", "port": 6881}])

    def test_known_id_from_its_own_ip_is_accepted(self):
        self.assertTrue(self.node._handshake_identity_ok("peer_good", "h", "192.168.1.20"))

    def test_known_id_from_other_ip_is_rejected(self):
        self.assertFalse(self.node._handshake_identity_ok("peer_good", "h", "192.168.1.66"))

    def test_unknown_id_is_accepted(self):
        self.assertTrue(self.node._handshake_identity_ok("peer_new", "h", "192.168.1.66"))

    def test_resolver_is_asked_for_unknown_ids(self):
        calls = []

        def resolver(info_hash):
            calls.append(info_hash)
            return [{"peer_id": "peer_far", "ip": "192.168.1.30", "port": 6881}]

        self.node.peer_resolver = resolver
        self.assertFalse(self.node._handshake_identity_ok("peer_far", "h", "192.168.1.66"))
        self.assertEqual(calls, ["h"])

    def test_resolver_refresh_accepts_peer_that_changed_ip(self):
        self.node.peer_resolver = lambda h: [{"peer_id": "peer_good", "ip": "192.168.1.21", "port": 6881}]
        self.assertTrue(self.node._handshake_identity_ok("peer_good", "h", "192.168.1.21"))


class HandshakeEndToEndTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="p2p_hs_")
        path = os.path.join(self.tmp, "a.bin")
        with open(path, "wb") as f:
            f.write(os.urandom(1000))
        self.meta = create_metainfo(path)
        self.pm = PieceManager(path, self.meta)
        self.pm.verify_all_pieces()
        self.port = free_port()
        self.node = PeerNode("peer_seed", "127.0.0.1", self.port, piece_manager=self.pm)
        self.node.start_listening()

    def tearDown(self):
        self.node.stop()
        self.pm.close()
        time.sleep(0.1)
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _handshake(self, claimed_id):
        with socket.create_connection(("127.0.0.1", self.port), timeout=3) as sock:
            send_msg(sock, create_handshake_msg(self.meta["info_hash"], claimed_id))
            try:
                return recv_msg(sock)
            except (EOFError, OSError):
                return None

    def test_spoofed_id_gets_connection_closed(self):
        self.node.update_known_peers([{"peer_id": "peer_good", "ip": "10.9.9.9"}])
        # Trong test mọi kết nối đều từ 127.0.0.1; tắt ngoại lệ "cùng máy" để mô phỏng máy khác.
        with patch.object(PeerNode, "_is_loopback", staticmethod(lambda ip: False)):
            self.assertIsNone(self._handshake("peer_good"))
            self.assertEqual(self.node.rejected_handshakes, 1)
            resp = self._handshake("peer_honest")
        self.assertEqual(resp["type"], MessageType.HANDSHAKE_RESPONSE.value)
        self.assertEqual(resp["peer_id"], "peer_seed")

    def test_connection_rejects_responder_with_wrong_id(self):
        wrong = PeerConnection("peer_me", "peer_someone_else", self.meta["info_hash"], "127.0.0.1", self.port)
        self.assertFalse(wrong.connect())
        right = PeerConnection("peer_me", "peer_seed", self.meta["info_hash"], "127.0.0.1", self.port)
        self.assertTrue(right.connect())
        right.close()


if __name__ == "__main__":
    unittest.main()
