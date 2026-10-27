import contextlib
import io
import json
import os
import socket
import tempfile
import time
import unittest
from unittest.mock import patch

from peer.client import P2PClient
from peer.transfer_engine import TransferEngine
from protocol.messages import create_announce_msg, recv_msg, send_msg
from run_client import main, parse_args
from tracker.tracker_server import TrackerServer


class TestClientNetwork(unittest.TestCase):
    def test_cli_accepts_ipv6_and_rejects_ipv4(self):
        args = parse_args(["6882", "2001:db8::10", "--advertise-ip", "2001:db8::20"])
        self.assertEqual(args.tracker_host, "2001:db8::10")
        self.assertEqual(args.advertise_ip, "2001:db8::20")
        self.assertEqual(parse_args([]).tracker_host, "::1")

        for argv in (["6881", "192.168.1.10"],
                     ["--advertise-ip", "100.82.132.18"],
                     ["--tailscale"]):
            with self.subTest(argv=argv), contextlib.redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit):
                    parse_args(argv)

    def test_main_uses_auto_advertisement_by_default(self):
        with patch("run_client.P2PClient") as client_class, \
             patch("run_client.os.makedirs"), \
             patch("builtins.input", return_value="4"), \
             contextlib.redirect_stdout(io.StringIO()):
            with patch("run_client.parse_args", return_value=parse_args(["6881", "::1"])):
                main()
        self.assertIsNone(client_class.call_args.kwargs["advertise_ip"])
        client_class.return_value.stop.assert_called_once()

    def test_ipv4_is_rejected_by_network_components(self):
        with self.assertRaisesRegex(ValueError, "IPv6"):
            TrackerServer(host="127.0.0.1")
        with self.assertRaisesRegex(ValueError, "IPv6"):
            P2PClient(tracker_host="127.0.0.1")

    def test_tracker_uses_ipv6_source_and_validates_override(self):
        tracker = TrackerServer(host="::1", port=0)
        tracker.start()
        port = tracker.server_socket.getsockname()[1]

        def announce(peer_id, advertised_ip, peer_port=6881):
            with socket.socket(socket.AF_INET6, socket.SOCK_STREAM) as sock:
                sock.settimeout(2)
                sock.connect(("::1", port))
                self.assertTrue(send_msg(sock, create_announce_msg(
                    "test-hash", peer_id, peer_port, ip=advertised_ip)))
                return recv_msg(sock)

        try:
            response = announce("automatic", "")
            self.assertEqual(response["status"], "OK")
            self.assertEqual(response["ip"], "::1")
            self.assertEqual(tracker.state.get_peers("test-hash")[0]["ip"], "::1")

            response = announce("manual", "2001:db8::20")
            self.assertEqual(response["status"], "OK")
            self.assertEqual(response["ip"], "2001:db8::20")

            response = announce("invalid", "192.0.2.10")
            self.assertEqual(response["status"], "ERROR")
            self.assertNotIn("invalid", [peer["peer_id"] for peer in
                                         tracker.state.get_peers("test-hash")])
            response = announce("bad-port", "", peer_port=0)
            self.assertEqual(response["status"], "ERROR")
        finally:
            tracker.stop()

    def test_seeder_auto_advertises_and_stays_registered(self):
        with tempfile.TemporaryDirectory() as test_dir:
            source = os.path.join(test_dir, "sample.dat")
            meta_path = os.path.join(test_dir, "sample.dat.meta.json")
            with open(source, "wb") as file:
                file.write(os.urandom(1024))

            tracker = TrackerServer(host="::1", port=0, peer_timeout=0.12)
            tracker.start()
            client = P2PClient(
                peer_id="seeder-test", listen_port=0, tracker_host="::1",
                tracker_port=tracker.server_socket.getsockname()[1],
            )
            try:
                client.start()
                client.listen_port = client.node.server_socket.getsockname()[1]
                with patch.object(TransferEngine, "HEARTBEAT_INTERVAL", 0.03):
                    info_hash = client.share_file(source, meta_path)
                    with open(meta_path, encoding="utf-8") as file:
                        self.assertEqual(json.load(file)["tracker"],
                                         f"[::1]:{client.tracker_port}")
                    time.sleep(0.25)
                    peers = tracker.state.get_peers(info_hash)
                    self.assertEqual(len(peers), 1)
                    self.assertEqual(peers[0]["ip"], "::1")
                    self.assertEqual(peers[0]["port"], client.listen_port)
                    self.assertEqual(client._tasks[info_hash]["engine"].announced_ip, "::1")
                    client.stop()
                    self.assertEqual(tracker.state.get_peers(info_hash), [])
            finally:
                if client.node.is_running:
                    client.stop()
                tracker.stop()


if __name__ == "__main__":
    unittest.main()
