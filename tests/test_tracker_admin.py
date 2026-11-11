"""Kiểm thử các thao tác quản trị Tracker: tên file, xóa / chặn peer, snapshot."""
import socket
import time
import unittest

from protocol.messages import (
    MessageType,
    create_announce_msg,
    create_get_peers_msg,
    recv_msg,
    send_msg,
)
from tracker.state import REGISTER_BLOCKED, REGISTER_DUPLICATE_ID, REGISTER_OK, TrackerState
from tracker.tracker_server import TrackerServer


class TrackerStateAdminTest(unittest.TestCase):
    def setUp(self):
        self.state = TrackerState(peer_timeout=60.0)
        self.state.register_peer("h1", "peer_a", "10.0.0.1", 6881, downloaded=100, left=0, name="video.mp4")
        self.state.register_peer("h1", "peer_b", "10.0.0.2", 6882, downloaded=40, left=60)
        self.state.register_peer("h2", "peer_b", "10.0.0.2", 6882, left=10, name="nhac.mp3")

    def test_file_name_is_stored_from_announce(self):
        snap = self.state.snapshot()
        names = {s["info_hash"]: s["name"] for s in snap["swarms"]}
        self.assertEqual(names["h1"], "video.mp4")
        self.assertEqual(names["h2"], "nhac.mp3")

    def test_snapshot_reports_progress_and_roles(self):
        swarm = next(s for s in self.state.snapshot()["swarms"] if s["info_hash"] == "h1")
        self.assertEqual(swarm["seeders"], 1)
        self.assertEqual(swarm["leechers"], 1)
        peer_b = next(p for p in swarm["peers"] if p["peer_id"] == "peer_b")
        self.assertEqual(peer_b["progress"], 40.0)
        self.assertFalse(peer_b["is_seeder"])

    def test_remove_peer_only_affects_one_swarm(self):
        self.assertTrue(self.state.remove_peer("h1", "peer_b"))
        self.assertEqual([p["peer_id"] for p in self.state.get_peers("h1")], ["peer_a"])
        self.assertEqual([p["peer_id"] for p in self.state.get_peers("h2")], ["peer_b"])
        self.assertFalse(self.state.remove_peer("h1", "peer_b"))

    def test_blocked_peer_is_removed_and_cannot_rejoin(self):
        removed = self.state.block_peer("peer_b")
        self.assertEqual(removed, 2)
        self.assertEqual(self.state.register_peer("h1", "peer_b", "10.0.0.2", 6882), REGISTER_BLOCKED)
        self.assertNotIn("peer_b", [p["peer_id"] for p in self.state.get_peers("h1")])

    def test_block_by_ip_rejects_new_peer_id_from_same_ip(self):
        self.state.block_peer("peer_b", ip="10.0.0.2")
        self.assertEqual(self.state.register_peer("h1", "peer_new", "10.0.0.2", 7000), REGISTER_BLOCKED)
        self.assertTrue(self.state.is_blocked("anything", "10.0.0.2"))

    def test_unblock_allows_rejoin(self):
        self.state.block_peer("peer_b")
        self.assertTrue(self.state.unblock_peer("peer_b"))
        self.assertEqual(self.state.register_peer("h1", "peer_b", "10.0.0.2", 6882), REGISTER_OK)

    def test_events_are_logged_newest_first(self):
        self.state.block_peer("peer_a")
        events = self.state.snapshot()["events"]
        self.assertEqual(events[0]["kind"], "ADMIN")
        self.assertTrue(any(e["kind"] == "ANNOUNCE" for e in events))


class TrackerServerBlockTest(unittest.TestCase):
    def setUp(self):
        with socket.socket() as s:
            s.bind(("127.0.0.1", 0))
            self.port = s.getsockname()[1]
        self.server = TrackerServer(host="127.0.0.1", port=self.port)
        self.server.start()
        time.sleep(0.1)

    def tearDown(self):
        self.server.stop()

    def _request(self, msg):
        with socket.create_connection(("127.0.0.1", self.port), timeout=3) as sock:
            send_msg(sock, msg)
            return recv_msg(sock)

    def test_announce_carries_name_and_blocked_peer_gets_blocked_status(self):
        resp = self._request(create_announce_msg("hx", "peer_x", 6881, ip="10.1.1.1", name="a.bin"))
        self.assertEqual(resp["status"], "OK")
        self.assertEqual(self.server.state.snapshot()["swarms"][0]["name"], "a.bin")

        self.server.state.block_peer("peer_x")
        resp = self._request(create_announce_msg("hx", "peer_x", 6881, ip="10.1.1.1"))
        self.assertEqual(resp["status"], "BLOCKED")

        self._request(create_announce_msg("hx", "peer_y", 6882, ip="10.1.1.2"))
        resp = self._request(create_get_peers_msg("hx", "peer_x"))
        self.assertEqual(resp["type"], MessageType.GET_PEERS_RESPONSE.value)
        self.assertEqual(resp["peers"], [])


if __name__ == "__main__":
    unittest.main()
