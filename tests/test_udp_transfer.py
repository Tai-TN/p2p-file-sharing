import hashlib
import json
import os
import queue
import tempfile
import threading
import time
import unittest
import socket
from unittest.mock import patch

from nat.udp_transfer import UDPTransfer, HEADER, MAGIC, DATA, ACK
from nat.stun_client import NATInfo, NATType, STUNClient
from nat.hole_punch import UDPHolePuncher, ACK_PREFIX
from peer.client import P2PClient
from peer.transfer_engine import TransferEngine
from storage.metainfo import create_metainfo, save_metainfo
from storage.piece_manager import PieceManager
from tracker.tracker_server import TrackerServer


class FakeSocket:
    def __init__(self, address):
        self.address = address
        self.remote = None
        self.drop_once = True
        self.drop_ack_once = False

    def sendto(self, data, address):
        if address != self.remote.address:
            raise OSError("unexpected destination")
        if len(data) >= HEADER.size:
            magic, kind, _, _, seq = HEADER.unpack_from(data)
            if magic == MAGIC and kind == DATA and seq == 3 and self.drop_once:
                self.drop_once = False
                return len(data)
            if magic == MAGIC and kind == ACK and seq == 3 and self.drop_ack_once:
                self.drop_ack_once = False
                return len(data)
        self.remote.manager.on_data_packet(data, self.address)
        return len(data)


class FakeManager:
    def __init__(self, address, remote_address):
        self.address = address
        self.remote_address = remote_address
        self._shared_socket = FakeSocket(address)
        self._shared_socket.manager = self
        self.on_data_packet = None

    def get_endpoint(self, _peer_id):
        return self.remote_address

    def has_endpoint(self, address):
        return address == self.remote_address


class TestUDPTransfer(unittest.TestCase):
    def test_candidate_gathering_keeps_distinct_stun_mappings(self):
        class BoundSocket:
            def getsockname(self):
                return ("0.0.0.0", 6881)

        client = STUNClient(stun_servers=[("first", 3478), ("second", 3478)])
        client.last_server = ("first", 3478)
        primary = NATInfo("198.51.100.10", 10001, "192.168.1.10", 6881,
                          NATType.UNKNOWN)
        with patch("nat.stun_client._stun_request", return_value=("198.51.100.10", 20002)):
            candidates = client.gather_candidates(BoundSocket(), primary)
        self.assertEqual([(c["ip"], c["port"]) for c in candidates], [
            ("192.168.1.10", 6881), ("198.51.100.10", 10001),
            ("198.51.100.10", 20002),
        ])

    def test_punch_tries_alternate_candidate(self):
        class SendSocket:
            def __init__(self):
                self.sent = []

            def gettimeout(self):
                return 0.2

            def sendto(self, data, address):
                self.sent.append(address)

        sock = SendSocket()
        inbox = queue.Queue()
        alternate = ("203.0.113.2", 40002)
        inbox.put((ACK_PREFIX + b"peer_B", alternate))
        puncher = UDPHolePuncher(6881, "peer_A", shared_socket=sock, incoming=inbox)
        success, _ = puncher.punch("203.0.113.2", 30001, "peer_B", 0.5,
                                   candidates=[{"ip": alternate[0], "port": alternate[1]}])
        self.assertTrue(success)
        self.assertIn(alternate, sock.sent)
        self.assertEqual(puncher.last_target_addr, alternate)

    def test_local_tracker_seeder_leecher_use_udp_for_file(self):
        """Đi qua tracker, punch, gói UDP và SHA-256 mà không nối TCP tới peer."""
        with tempfile.TemporaryDirectory() as folder:
            source = os.path.join(folder, "source.bin")
            meta_path = os.path.join(folder, "source.bin.meta.json")
            target_dir = os.path.join(folder, "downloads")
            original = os.urandom(100_000)
            with open(source, "wb") as handle:
                handle.write(original)
            save_metainfo(create_metainfo(source, piece_length=16_384), meta_path)
            with socket.socket() as port_socket:
                port_socket.bind(("127.0.0.1", 0))
                tracker_port = port_socket.getsockname()[1]
            tracker = TrackerServer("127.0.0.1", tracker_port)
            tracker.start()
            seeder = P2PClient("seeder_udp", 0, "127.0.0.1", tracker_port,
                               transport="udp")
            leecher = P2PClient("leecher_udp", 0, "127.0.0.1", tracker_port,
                                transport="udp")

            def local_stun(_stun, sock):
                port = sock.getsockname()[1]
                return NATInfo("127.0.0.1", port, "127.0.0.1", port, NATType.OPEN)

            def local_candidates(_stun, sock, _primary):
                return [{"ip": "127.0.0.1", "port": sock.getsockname()[1],
                         "type": "host"}]

            try:
                with patch.object(STUNClient, "get_public_endpoint", local_stun), \
                     patch.object(STUNClient, "gather_candidates", local_candidates):
                    seeder.start()
                    leecher.start()
                seeder.share_file(source, meta_path)
                leecher.download_file(meta_path, target_dir)
                info_hash = create_metainfo(source, piece_length=16_384)["info_hash"]
                deadline = time.monotonic() + 15
                while time.monotonic() < deadline:
                    if leecher._tasks[info_hash]["pm"].is_complete():
                        break
                    time.sleep(0.1)
                self.assertTrue(leecher._tasks[info_hash]["pm"].is_complete())
                with open(os.path.join(target_dir, "source.bin"), "rb") as handle:
                    self.assertEqual(hashlib.sha256(handle.read()).digest(),
                                     hashlib.sha256(original).digest())
            finally:
                leecher.stop()
                seeder.stop()
                tracker.stop()

    def test_piece_survives_lost_datagram_and_matches_sha256(self):
        data = os.urandom(70_000)
        info_hash = "ab" * 32
        meta = {"info_hash": info_hash, "info": {
            "name": "sample.bin", "file_size": len(data),
            "piece_length": len(data), "pieces": [hashlib.sha256(data).hexdigest()],
        }}
        with tempfile.TemporaryDirectory() as folder:
            source = os.path.join(folder, "source.bin")
            target = os.path.join(folder, "target.bin")
            with open(source, "wb") as handle:
                handle.write(data)
            seeder_pm = PieceManager(source, meta)
            seeder_pm.verify_all_pieces()
            leecher_pm = PieceManager(target, meta)
            a = FakeManager(("127.0.0.1", 11001), ("127.0.0.1", 11002))
            b = FakeManager(("127.0.0.1", 11002), ("127.0.0.1", 11001))
            a._shared_socket.remote = b._shared_socket
            b._shared_socket.remote = a._shared_socket
            b._shared_socket.drop_ack_once = True
            upload = UDPTransfer(a, lambda key: seeder_pm if key == info_hash else None)
            download = UDPTransfer(b, lambda _: None)
            try:
                received = download.request_piece("seeder", info_hash, 0, len(data), timeout=5)
                self.assertEqual(received, data)
                self.assertFalse(a._shared_socket.drop_once)
                self.assertFalse(b._shared_socket.drop_ack_once)
                self.assertTrue(leecher_pm.write_piece(0, received))
                deadline = time.monotonic() + 3
                while upload._uploads and time.monotonic() < deadline:
                    time.sleep(0.05)
                self.assertFalse(upload._uploads, "ACK cuối mất phải được xác nhận khi gói gửi lại")
                with open(target, "rb") as handle:
                    self.assertEqual(hashlib.sha256(handle.read()).digest(),
                                     hashlib.sha256(data).digest())
            finally:
                upload.close()
                download.close()

    def test_resume_rechecks_disk_instead_of_trusting_progress_json(self):
        data = b"good data"
        meta = {"info_hash": "cd" * 32, "info": {
            "name": "sample.bin", "file_size": len(data),
            "piece_length": len(data), "pieces": [hashlib.sha256(data).hexdigest()],
        }}
        with tempfile.TemporaryDirectory() as folder:
            target = os.path.join(folder, "sample.bin")
            pm = PieceManager(target, meta)
            engine = TransferEngine("leecher", "127.0.0.1", 8000, 6882, pm)
            with open(engine.progress_file, "w") as handle:
                json.dump({"info_hash": meta["info_hash"], "bitfield": [True]}, handle)
            engine._load_progress()
            self.assertFalse(pm.has_piece(0))

    def test_tracker_rejects_bad_candidate_list(self):
        valid = [{"ip": "192.168.1.2", "port": 6881, "type": "host"}]
        self.assertEqual(TrackerServer._valid_candidates(valid), valid)
        self.assertEqual(TrackerServer._valid_candidates(valid * 9), [])
        self.assertEqual(TrackerServer._valid_candidates([
            {"ip": "not-ip", "port": 1}, {"ip": "1.2.3.4", "port": 70000}
        ]), [])


if __name__ == "__main__":
    unittest.main()
