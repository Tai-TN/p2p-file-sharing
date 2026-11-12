"""Kiểm tra ghép lựa chọn peer vào NAT và báo cáo peer lỗi."""

import hashlib
import os
import tempfile
import threading
import time
import unittest

from peer.transfer_engine import TransferEngine
from storage.metainfo import create_metainfo
from storage.piece_manager import PieceManager
from tracker.state import TrackerState


class MergedRoutesTests(unittest.TestCase):
    def test_route_measurements_are_independent(self):
        with tempfile.TemporaryDirectory() as temp:
            source = os.path.join(temp, "source.bin")
            with open(source, "wb") as handle:
                handle.write(os.urandom(8192))
            meta = create_metainfo(source, piece_length=1024)
            pm = PieceManager(os.path.join(temp, "target.bin"), meta)
            try:
                engine = TransferEngine("leecher", "localhost", 8000, 6881, pm)
                relay = engine._selectors["RELAY"]
                udp = engine._selectors["DIRECT_UDP"]
                relay.record_success("slow", 1024, 1.0)
                relay.record_success("fast", 1024, 0.1)
                self.assertEqual(relay.rank_peers([("slow", {}), ("fast", {})], 1024)[0][0],
                                 "fast")
                self.assertIsNone(udp.get_peer_stats("fast").estimated_rate)
            finally:
                pm.close()

    def test_udp_multiple_seeders_verify_sha_without_duplicate_requests(self):
        with tempfile.TemporaryDirectory() as temp:
            source = os.path.join(temp, "source.bin")
            data = os.urandom(16 * 1024)
            with open(source, "wb") as handle:
                handle.write(data)
            meta = create_metainfo(source, piece_length=1024)
            pm = PieceManager(os.path.join(temp, "target.bin"), meta)

            class FakeUDP:
                def __init__(self):
                    self.requests = []
                    self.lock = threading.Lock()

                def request_piece(self, peer_id, info_hash, index, length, **kwargs):
                    with self.lock:
                        self.requests.append((peer_id, index))
                    time.sleep(0.01)
                    return data[index * 1024:index * 1024 + length]

            udp = FakeUDP()
            try:
                engine = TransferEngine("leecher", "localhost", 8000, 6881, pm,
                                        transport="udp", udp_transfer=udp, udp_parallel=4)
                engine.is_running = True
                engine.peer_node = None
                self.assertTrue(engine._download_udp_session(["s1", "s2"]))
                self.assertEqual(len(udp.requests), pm.num_pieces)
                self.assertEqual({index for _, index in udp.requests}, set(range(pm.num_pieces)))
                self.assertEqual({peer for peer, _ in udp.requests}, {"s1", "s2"})
                self.assertEqual(sum(engine._selectors["DIRECT_UDP"].get_peer_stats(peer).pending_requests
                                     for peer in ("s1", "s2")), 0)
                with open(pm.target_file_path, "rb") as handle:
                    self.assertEqual(hashlib.sha256(handle.read()).digest(),
                                     hashlib.sha256(data).digest())
            finally:
                pm.close()

    def test_tracker_requires_two_current_swarm_reporters(self):
        state = TrackerState()
        info_hash = "a" * 64
        for peer in ("bad", "a", "b"):
            state.register_peer(info_hash, peer, "127.0.0.1", 6881)
        self.assertEqual(state.report_bad_peers(info_hash, "outsider", ["bad"]), [])
        self.assertEqual(state.report_bad_peers(info_hash, "a", ["bad"]), [])
        self.assertEqual(state.report_bad_peers(info_hash, "a", ["bad"]), [])
        self.assertEqual(state.report_bad_peers(info_hash, "b", ["bad"]), ["bad"])
        self.assertFalse(state.has_peer(info_hash, "bad"))


if __name__ == "__main__":
    unittest.main()
