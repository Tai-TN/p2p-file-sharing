import os
import tempfile
import time
import unittest
from unittest.mock import patch

from peer.client import P2PClient
from peer.transfer_engine import TransferEngine
from run_client import parse_args
from tracker.tracker_server import TrackerServer


class TestClientNetwork(unittest.TestCase):
    def test_cli_keeps_old_arguments_and_accepts_advertised_ipv4(self):
        old_args = parse_args(["6882", "192.168.1.10"])
        self.assertEqual(old_args.listen_port, 6882)
        self.assertEqual(old_args.tracker_host, "192.168.1.10")
        self.assertIsNone(old_args.advertise_ip)

        vpn_args = parse_args(
            ["6882", "100.108.79.47", "--advertise-ip", "100.82.132.18"]
        )
        self.assertEqual(vpn_args.advertise_ip, "100.82.132.18")

    def test_seeder_advertises_override_and_stays_registered(self):
        with tempfile.TemporaryDirectory() as test_dir:
            source = os.path.join(test_dir, "sample.dat")
            meta_path = os.path.join(test_dir, "sample.dat.meta.json")
            with open(source, "wb") as file:
                file.write(os.urandom(1024))

            tracker = TrackerServer(host="127.0.0.1", port=0, peer_timeout=0.12)
            tracker.start()
            client = P2PClient(
                peer_id="seeder-test",
                listen_port=0,
                tracker_host="127.0.0.1",
                tracker_port=tracker.server_socket.getsockname()[1],
                advertise_ip="127.0.0.23",
            )
            try:
                client.start()
                client.listen_port = client.node.server_socket.getsockname()[1]
                with patch.object(TransferEngine, "HEARTBEAT_INTERVAL", 0.03):
                    info_hash = client.share_file(source, meta_path)
                    time.sleep(0.25)  # Lâu hơn peer_timeout; heartbeat phải gia hạn.
                    peers = tracker.state.get_peers(info_hash)
                    self.assertEqual(len(peers), 1)
                    self.assertEqual(peers[0]["ip"], "127.0.0.23")
                    self.assertEqual(peers[0]["port"], client.listen_port)
                    client.stop()
                    self.assertEqual(tracker.state.get_peers(info_hash), [])
            finally:
                if client.node.is_running:
                    client.stop()
                tracker.stop()


if __name__ == "__main__":
    unittest.main()
