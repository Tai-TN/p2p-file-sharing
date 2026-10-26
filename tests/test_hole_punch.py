"""
tests/test_hole_punch.py — Unit test cho nat/hole_punch.py.

Kiểm tra:
- Khả năng đục lỗ UDP 2 chiều giữa 2 puncher trên localhost.
- Xử lý timeout khi đối phương không phản hồi.
- HolePunchManager điều phối signal và TCP callback.
"""

import time
import socket
import unittest
import threading
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from unittest.mock import Mock, patch

from nat.hole_punch import UDPHolePuncher, HolePunchManager, PROBE_PREFIX, ACK_PREFIX
from peer.client import P2PClient
from peer.transfer_engine import TransferEngine


def _free_udp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class TestHolePunch(unittest.TestCase):

    def test_parallel_workers_send_one_punch_request(self):
        pm = SimpleNamespace(metainfo={"info_hash": "sample"}, target_file_path="/tmp/sample")
        signal = SimpleNamespace(is_connected=True, request_punch=Mock(return_value=True))
        engine = TransferEngine("leecher", "127.0.0.1", 8000, 6882, pm,
                                signal_channel=signal)
        peer = {"peer_id": "seeder", "ip": "203.0.113.2", "port": 6881,
                "udp_port": 28410}
        with patch.object(engine, "_connect_and_handshake", return_value=None):
            with ThreadPoolExecutor(max_workers=4) as pool:
                list(pool.map(lambda _: engine._update_peer_connections([peer]), range(4)))
            engine._update_peer_connections([peer])
        signal.request_punch.assert_called_once_with("seeder")

    def test_duplicate_punch_signals_start_one_worker(self):
        client = P2PClient(peer_id="seeder", advertise_ip="127.0.0.1")
        entered = threading.Event()
        release = threading.Event()

        def slow_punch(_):
            entered.set()
            release.wait(timeout=2.0)
            return {"udp_success": False}

        msg = {"peer_id": "leecher", "ip": "203.0.113.2", "udp_port": 28410,
               "tcp_port": 6882}
        with patch.object(client._hole_punch_mgr, "handle_punch_signal",
                          side_effect=slow_punch) as punch:
            client._on_punch_signal(msg)
            self.assertTrue(entered.wait(timeout=1.0))
            client._on_punch_signal(msg)
            release.set()
            punch.assert_called_once_with(msg)

    def test_constants(self):
        self.assertTrue(PROBE_PREFIX.startswith(b"P2P_PUNCH:"))
        self.assertTrue(ACK_PREFIX.startswith(b"P2P_ACK:"))

    def test_udp_hole_punch_two_peers_success(self):
        """Mô phỏng 2 peer cùng đục lỗ UDP tới nhau."""
        port_a = _free_udp_port()
        port_b = _free_udp_port()

        puncher_a = UDPHolePuncher(local_port=port_a, my_peer_id="peer_A")
        puncher_b = UDPHolePuncher(local_port=port_b, my_peer_id="peer_B")

        res_a = {}
        res_b = {}

        def run_a():
            ok, sock = puncher_a.punch(remote_ip="127.0.0.1", remote_udp_port=port_b, remote_peer_id="peer_B", timeout=2.0)
            res_a["ok"] = ok
            if sock:
                sock.close()

        def run_b():
            ok, sock = puncher_b.punch(remote_ip="127.0.0.1", remote_udp_port=port_a, remote_peer_id="peer_A", timeout=2.0)
            res_b["ok"] = ok
            if sock:
                sock.close()

        t1 = threading.Thread(target=run_a)
        t2 = threading.Thread(target=run_b)
        t1.start()
        t2.start()
        t1.join(timeout=3.0)
        t2.join(timeout=3.0)

        self.assertTrue(res_a.get("ok"), "Peer A phải đục lỗ thành công")
        self.assertTrue(res_b.get("ok"), "Peer B phải đục lỗ thành công")

    def test_udp_hole_punch_timeout(self):
        """Nếu đối phương không online, puncher phải kết thúc và trả về False sau timeout."""
        unused_port = _free_udp_port()
        local_port = _free_udp_port()

        puncher = UDPHolePuncher(local_port=local_port, my_peer_id="peer_alone")
        t0 = time.time()
        ok, sock = puncher.punch(remote_ip="127.0.0.1", remote_udp_port=unused_port, timeout=0.3)
        elapsed = time.time() - t0

        self.assertFalse(ok)
        self.assertIsNone(sock)
        self.assertGreaterEqual(elapsed, 0.25)

    def test_hole_punch_manager_coordination(self):
        """HolePunchManager gọi callback và xử lý signal đúng."""
        port = _free_udp_port()
        mgr = HolePunchManager(my_peer_id="peer_mgr", local_port=port)

        tcp_callback_called = []

        def mock_tcp_connect(ip, port, peer_id):
            tcp_callback_called.append((ip, port, peer_id))
            return True

        signal_msg = {
            "peer_id": "target_peer",
            "ip": "127.0.0.1",
            "udp_port": 0,       # Không có UDP
            "tcp_port": 6881,
        }

        res = mgr.handle_punch_signal(signal_msg, connect_tcp_callback=mock_tcp_connect)
        self.assertEqual(res["peer_id"], "target_peer")
        self.assertTrue(res["tcp_connected"])
        self.assertEqual(len(tcp_callback_called), 1)
        self.assertEqual(tcp_callback_called[0], ("127.0.0.1", 6881, "target_peer"))


if __name__ == "__main__":
    unittest.main(verbosity=2)

