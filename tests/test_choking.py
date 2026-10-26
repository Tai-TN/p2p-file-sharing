import random
import unittest

from peer.choking import ChokingManager


class TestChokingManager(unittest.TestCase):
    def test_regular_slots_prefer_recent_contributors(self):
        manager = ChokingManager(
            regular_slots=1,
            optimistic_slots=0,
            choking_interval=1,
            measurement_window=30,
            rng=random.Random(1),
        )
        manager.set_interested("fast", True)
        manager.set_interested("slow", True)
        manager.record_upload("fast", 1000, now=10)
        manager.record_upload("slow", 100, now=10)

        manager.recalculate(now=10, force=True)

        self.assertEqual(manager.unchoked_peers(now=10), {"fast"})

    def test_optimistic_slot_selects_interested_peer(self):
        manager = ChokingManager(
            regular_slots=1,
            optimistic_slots=1,
            choking_interval=1,
            measurement_window=30,
            rng=random.Random(3),
        )
        for peer_id in ("fast", "slow", "new"):
            manager.set_interested(peer_id, True)
        manager.record_upload("fast", 1000, now=10)
        manager.record_upload("slow", 100, now=10)

        manager.recalculate(now=10, force=True)

        self.assertEqual(manager.unchoked_peers(now=10), {"fast", "slow"})
        self.assertEqual(manager.optimistic_peers(), {"slow"})

    def test_uninterested_peer_is_removed_from_slots(self):
        manager = ChokingManager(
            regular_slots=1,
            optimistic_slots=0,
            choking_interval=1,
        )
        manager.set_interested("peer", True)
        manager.recalculate(now=10, force=True)
        manager.set_interested("peer", False)

        self.assertFalse(manager.is_unchoked("peer", now=11))

    def test_negative_transfer_is_rejected(self):
        manager = ChokingManager()
        with self.assertRaises(ValueError):
            manager.record_upload("peer", -1)

    def test_free_rider_only_gets_optimistic_slot(self):
        """Peer không đóng góp không được chiếm regular slot."""
        manager = ChokingManager(
            regular_slots=1,
            optimistic_slots=1,
            choking_interval=1,
            measurement_window=30,
            rng=random.Random(1),
        )
        manager.set_interested("cooperator", True)
        manager.set_interested("free_rider", True)
        manager.record_upload("cooperator", 1000, now=10)

        manager.recalculate(now=10, force=True)

        self.assertIn("cooperator", manager.unchoked_peers(now=10))
        self.assertIn("free_rider", manager.optimistic_peers())
        self.assertEqual(manager.unchoked_peers(now=10), {"cooperator", "free_rider"})


if __name__ == "__main__":
    unittest.main()
