import hashlib
import os
import socket
import struct
import tempfile
import threading
import unittest
from unittest.mock import Mock, patch

from peer.peer_node import PeerNode
from peer.transfer_engine import TransferEngine
from protocol.messages import MessageType, create_piece_msg, send_msg
from storage.metainfo import create_metainfo
from storage.piece_manager import PieceManager
from tracker.state import TrackerState
from tracker.tracker_server import TrackerServer


class TestTransferErrors(unittest.TestCase):
    def test_two_distinct_reporters_are_required(self):
        state = TrackerState()
        for peer in ("bad", "reporter-a", "reporter-b"):
            state.register_peer("file", peer, "::1", 6881)
        self.assertEqual(state.report_bad_peers("file", "reporter-a", ["bad"]), [])
        self.assertEqual(state.report_bad_peers("file", "reporter-a", ["bad"]), [])
        self.assertEqual(state.report_bad_peers("file", "reporter-b", ["bad"]), ["bad"])
        self.assertNotIn("bad", [p["peer_id"] for p in state.get_peers("file")])

    def test_download_recovers_from_eof_bad_json_and_bad_hash(self):
        for mode in ("eof", "json", "hash"):
            with self.subTest(mode=mode):
                self.run_recovery_case(mode)

    def run_recovery_case(self, mode):
        with tempfile.TemporaryDirectory() as root:
            source = os.path.join(root, "source.dat")
            data = os.urandom(8 * 128 * 1024)
            with open(source, "wb") as stream:
                stream.write(data)
            meta = create_metainfo(source, piece_length=128 * 1024)
            tracker = TrackerServer(host="::1", port=0)
            tracker.start()
            tracker_port = tracker.server_socket.getsockname()[1]
            nodes, seeders = [], []
            leecher = None
            thread = None
            try:
                for peer_id in ("bad", "good"):
                    pm = PieceManager(source, meta)
                    pm.verify_all_pieces()
                    node = PeerNode(peer_id, "::1", 0, pm)
                    node.start_listening()
                    nodes.append(node)
                    port = node.server_socket.getsockname()[1]
                    seeder = TransferEngine(peer_id, "::1", tracker_port, port, pm)
                    seeders.append(seeder)
                    seeder._announce_to_tracker()
                bad_port = nodes[0].server_socket.getsockname()[1]
                bad_requests = []

                def faulty_send(sock, message):
                    if (message.get("type") == MessageType.PIECE.value
                            and sock.getsockname()[1] == bad_port):
                        bad_requests.append(message["piece_index"])
                        if mode == "eof":
                            sock.sendall(struct.pack(">I", 1000) + b'{"type":')
                            sock.shutdown(socket.SHUT_WR)
                            return True
                        if mode == "json":
                            sock.sendall(struct.pack(">I", 8) + b'{"type":')
                            return True
                        return send_msg(sock, create_piece_msg(
                            message["piece_index"], b"x" * meta["info"]["piece_length"]))
                    return send_msg(sock, message)

                def prefer_bad(options):
                    if isinstance(options[0], tuple):
                        return next((item for item in options if item[0] == "bad"), options[0])
                    return options[0]

                target = os.path.join(root, "download.dat")
                pm = PieceManager(target, meta)
                leecher = TransferEngine("leecher", "::1", tracker_port, 6882, pm, max_workers=2)
                outcome = []
                thread = threading.Thread(target=lambda: outcome.append(leecher.start_download()), daemon=True)
                with patch("peer.peer_node.send_msg", side_effect=faulty_send), \
                     patch("peer.transfer_engine.random.choice", side_effect=prefer_bad):
                    thread.start()
                    thread.join(timeout=10)
                    self.assertFalse(thread.is_alive(), "Worker died or download got stuck")
                self.assertEqual(outcome, [True])
                self.assertTrue(bad_requests)
                self.assertIn("bad", leecher.failed_peers)
                if mode != "eof":
                    self.assertGreaterEqual(len(bad_requests), leecher.MAX_BAD_DATA)
                self.assertEqual(tracker.state._bad_reports[leecher.info_hash]["bad"], {"leecher"})
                with open(target, "rb") as stream:
                    self.assertEqual(hashlib.sha256(stream.read()).digest(), hashlib.sha256(data).digest())
                self.assertEqual(leecher.in_flight_pieces, set())
            finally:
                if leecher:
                    leecher.stop()
                if thread:
                    thread.join(timeout=2)
                for seeder in seeders:
                    seeder.stop()
                for node in nodes:
                    node.stop()
                tracker.stop()

    def test_report_is_retried_until_tracker_acknowledges(self):
        with tempfile.TemporaryDirectory() as root:
            source = os.path.join(root, "sample.dat")
            with open(source, "wb") as stream:
                stream.write(b"test report")
            pm = PieceManager(source, create_metainfo(source))
            pm.verify_all_pieces()
            tracker = TrackerServer(host="::1", port=0)
            tracker.start()
            port = tracker.server_socket.getsockname()[1]
            engine = TransferEngine("reporter", "::1", port, 6881, pm)
            try:
                tracker.state.register_peer(engine.info_hash, "bad", "::1", 6882)
                engine.pending_bad_reports.add("bad")
                with patch("peer.transfer_engine.recv_msg", side_effect=EOFError("no ACK")):
                    engine._announce_to_tracker()
                self.assertEqual(engine.pending_bad_reports, {"bad"})
                engine._announce_to_tracker()
                self.assertEqual(engine.pending_bad_reports, set())
                self.assertEqual(tracker.state._bad_reports[engine.info_hash]["bad"], {"reporter"})
            finally:
                engine.stop()
                tracker.stop()

    def test_local_write_failure_does_not_penalize_peer(self):
        with tempfile.TemporaryDirectory() as root:
            source = os.path.join(root, "file.dat")
            with open(source, "wb") as stream:
                stream.write(b"valid piece")
            pm = PieceManager(source, create_metainfo(source))
            engine = TransferEngine("leecher", "::1", 8000, 6882, pm)
            sock = Mock()
            engine.is_running = True
            engine.connected_peers["good"] = {
                "ip": "::1", "port": 6881, "bitfield": [True],
                "sockets": {0: sock}, "spare_socket": None, "bad_data_count": 0,
            }
            response = create_piece_msg(0, b"valid piece")
            with patch.object(engine, "_worker_socket", return_value=sock), \
                 patch("peer.transfer_engine.send_msg"), \
                 patch("peer.transfer_engine.recv_msg", return_value=response):
                for failure in (False, OSError("disk full")):
                    with self.subTest(failure=failure):
                        options = {"side_effect": failure} if isinstance(failure, Exception) else {"return_value": failure}
                        with patch.object(pm, "write_piece", **options):
                            self.assertFalse(engine._download_piece_from_peer(0, "good", "::1", 6881, 0))
                        self.assertEqual(engine.failed_peers, {})
                        self.assertEqual(engine.pending_bad_reports, set())
                        self.assertEqual(engine.connected_peers["good"]["bad_data_count"], 0)
                        self.assertIs(engine.connected_peers["good"]["sockets"][0], sock)
            engine.stop()
