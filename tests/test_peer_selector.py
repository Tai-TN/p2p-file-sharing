import random
import unittest

from peer.peer_selector import PeerSelector


class TestPeerSelector(unittest.TestCase):
    """Kiểm thử thuật toán chọn peer độc lập với networking."""

    def setUp(self):
        self.peers = [
            ("peer_A", {"bitfield": [True]}),
            ("peer_B", {"bitfield": [True]}),
        ]

    def _measure(self, selector, peer_id, byte_count, elapsed):
        # Reserve rồi báo thành công để tạo tốc độ ban đầu cho peer.
        selected = selector.select_peer([(peer_id, {})], byte_count)
        self.assertIsNotNone(selected)
        selector.record_success(peer_id, byte_count, elapsed)

    def test_selects_peer_with_lowest_eta(self):
        """Peer có ETA thấp hơn phải được ưu tiên."""
        selector = PeerSelector(
            explore_rate=0,
            eta_tie_threshold=0,
            rng=random.Random(1),
        )
        self._measure(selector, "peer_A", 100, 1)
        self._measure(selector, "peer_B", 50, 1)

        selected = selector.select_peer(self.peers, 100)

        self.assertEqual(selected[0], "peer_A")

    def test_pending_requests_are_included_in_eta(self):
        """Pending request làm ETA tăng và có thể khiến peer bị đổi lựa chọn."""
        selector = PeerSelector(
            explore_rate=0,
            eta_tie_threshold=0,
            rng=random.Random(1),
        )
        self._measure(selector, "peer_A", 100, 1)
        self._measure(selector, "peer_B", 60, 1)

        selector.select_peer(self.peers, 100)
        selected = selector.select_peer(self.peers, 100)

        self.assertEqual(selected[0], "peer_B")

    def test_unmeasured_peers_use_bootstrap_rate(self):
        """Peer chưa đo vẫn được chọn nhờ tốc độ bootstrap."""
        selector = PeerSelector(explore_rate=0, default_bootstrap_rate=100)

        selected = selector.select_peer(self.peers, 100)

        self.assertIsNotNone(selected)
        self.assertEqual(
            selector.get_peer_stats(selected[0]).pending_requests,
            1,
        )

    def test_choked_peer_is_skipped_until_cooldown_expires(self):
        """Peer bị CHOKE bị loại tạm thời rồi được chọn lại sau cooldown."""
        selector = PeerSelector(
            explore_rate=0,
            eta_tie_threshold=0,
            choke_cooldown=10,
        )
        selector.select_peer([self.peers[0]], 100)
        selector.record_choke("peer_A", now=100)

        selected = selector.select_peer(self.peers, 100, now=105)
        self.assertEqual(selected[0], "peer_B")
        self.assertIsNone(selector.select_peer([self.peers[0]], 100, now=105))
        self.assertIsNotNone(
            selector.select_peer([self.peers[0]], 100, now=111)
        )

    def test_failure_penalizes_rate_and_releases_pending(self):
        """Lỗi phải giảm rate và giải phóng pending request."""
        selector = PeerSelector(explore_rate=0)
        self._measure(selector, "peer_A", 100, 1)
        selector.select_peer([("peer_A", {})], 100)

        before = selector.get_peer_stats("peer_A").estimated_rate
        selector.record_failure("peer_A")
        state = selector.get_peer_stats("peer_A")

        self.assertEqual(state.estimated_rate, before * 0.5)
        self.assertEqual(state.pending_requests, 0)
        self.assertEqual(state.failure_count, 1)

    def test_exploration_can_be_disabled(self):
        """explore_rate bằng 0 phải tắt hoàn toàn exploration."""
        selector = PeerSelector(
            explore_rate=0,
            eta_tie_threshold=0,
            rng=random.Random(1),
        )
        self._measure(selector, "peer_A", 100, 1)
        self._measure(selector, "peer_B", 10, 1)

        selected = selector.select_peer(self.peers, 100)

        self.assertEqual(selected[0], "peer_A")


if __name__ == "__main__":
    unittest.main()
