import argparse
import contextlib
import io
import os
import subprocess
import tempfile
import time
import unittest
from unittest.mock import call, patch

from peer.client import P2PClient
from peer.transfer_engine import TransferEngine
from run_client import get_tailscale_ip, main, parse_args, show_tailscale_status
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

        tailscale_args = parse_args(["6881", "100.108.79.47", "--tailscale"])
        self.assertTrue(tailscale_args.tailscale)
        with contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit):
                parse_args(["--tailscale", "--advertise-ip", "100.108.79.47"])

    def test_tailscale_cli_supplies_advertised_ip(self):
        outputs = [
            subprocess.CompletedProcess([], 0, '{"BackendState":"Running"}', ""),
            subprocess.CompletedProcess([], 0, "100.108.79.47\n", ""),
        ]
        with patch("run_client.subprocess.run", side_effect=outputs) as run:
            self.assertEqual(get_tailscale_ip(), "100.108.79.47")
        self.assertEqual(run.call_args_list, [
            call(["tailscale", "status", "--json"], capture_output=True,
                 text=True, timeout=5, check=True),
            call(["tailscale", "ip", "-4"], capture_output=True,
                 text=True, timeout=5, check=True),
        ])

    def test_tailscale_cli_reports_missing_or_disconnected(self):
        with patch("run_client.subprocess.run", side_effect=FileNotFoundError()):
            with self.assertRaisesRegex(RuntimeError, "Không tìm thấy CLI tailscale"):
                get_tailscale_ip()

        stopped = subprocess.CompletedProcess([], 0, '{"BackendState":"Stopped"}', "")
        with patch("run_client.subprocess.run", return_value=stopped) as run:
            with self.assertRaisesRegex(RuntimeError, "Tailscale chưa kết nối"):
                get_tailscale_ip()
            self.assertEqual(run.call_count, 1)

        failed = subprocess.CalledProcessError(1, ["tailscale", "status"], stderr="daemon down")
        with patch("run_client.subprocess.run", side_effect=failed):
            with self.assertRaisesRegex(RuntimeError, "daemon down"):
                get_tailscale_ip()

    def test_tailscale_cli_rejects_invalid_ip_and_shows_path(self):
        outputs = [
            subprocess.CompletedProcess([], 0, '{"BackendState":"Running"}', ""),
            subprocess.CompletedProcess([], 0, "not-an-ip", ""),
        ]
        with patch("run_client.subprocess.run", side_effect=outputs):
            with self.assertRaisesRegex(RuntimeError, "IPv4 không hợp lệ"):
                get_tailscale_ip()

        with patch("run_client.subprocess.run", return_value=subprocess.CompletedProcess(
            [], 0, "100.82.132.18 android active; direct 27.71.211.220:24556", ""
        )):
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                show_tailscale_status()
            self.assertIn("direct 27.71.211.220:24556", output.getvalue())

    def test_main_uses_tailscale_ip_for_client(self):
        args = argparse.Namespace(listen_port=6881, tracker_host="100.108.79.47",
                                  tailscale=True, advertise_ip=None)
        with patch("run_client.parse_args", return_value=args), \
             patch("run_client.get_tailscale_ip", return_value="100.108.79.47"), \
             patch("run_client.P2PClient") as client_class, \
             patch("run_client.os.makedirs"), \
             patch("builtins.input", return_value="4"), \
             contextlib.redirect_stdout(io.StringIO()):
            main()
        self.assertEqual(client_class.call_args.kwargs["advertise_ip"], "100.108.79.47")
        client_class.return_value.stop.assert_called_once()

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
