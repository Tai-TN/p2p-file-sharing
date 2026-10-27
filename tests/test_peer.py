import unittest
import os
import shutil
import tempfile
import time
import socket
import hashlib
import threading
from unittest.mock import patch
from peer.selection import choose_next_piece
from peer.peer_node import PeerNode
from peer.transfer_engine import TransferEngine
from tracker.tracker_server import TrackerServer
from storage.metainfo import create_metainfo
from storage.piece_manager import PieceManager

class TestPeer(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.piece_length = 256 * 1024  # 256 KB
        self.sample_file_path = os.path.join(self.test_dir, "sample.dat")

        # Tạo file mẫu 768 KB (gồm 3 mảnh)
        self.sample_data = os.urandom(768 * 1024)
        with open(self.sample_file_path, "wb") as f:
            f.write(self.sample_data)

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_rarest_first_selection(self):
        """Kiểm tra thuật toán Rarest-First chọn đúng mảnh hiếm nhất."""
        missing = [0, 1, 2]
        # Peer A có [0, 1], Peer B có [0]
        # => Piece 0 xuất hiện 2 lần, Piece 1 xuất hiện 1 lần (hiếm nhất), Piece 2 không ai có
        bitfields = {
            "peer_A": [True, True, False],
            "peer_B": [True, False, False]
        }
        chosen = choose_next_piece(missing, bitfields, strategy="rarest_first")
        self.assertEqual(chosen, 1)

    def test_end_to_end_p2p_file_transfer(self):
        """
        Kiểm tra truyền file P2P trực tiếp thực tế end-to-end:
        1 Tracker Server + 1 Peer Seeder (có 100% file) -> 1 Peer Leecher (tải 0% -> 100%).
        """
        tracker_port = 8990
        seeder_port = 7001
        leecher_port = 7002

        # 1. Khởi chạy Tracker Server
        tracker = TrackerServer(host="127.0.0.1", port=tracker_port, peer_timeout=10.0)
        tracker.start()
        time.sleep(0.1)

        try:
            # 2. Tạo metainfo cho file mẫu
            meta = create_metainfo(self.sample_file_path, piece_length=self.piece_length, tracker_url=f"127.0.0.1:{tracker_port}")

            # 3. Khởi chạy Seeder (Có file mẫu 100%)
            seeder_pm = PieceManager(self.sample_file_path, meta)
            seeder_pm.verify_all_pieces()
            self.assertTrue(seeder_pm.is_complete())

            seeder_node = PeerNode("seeder_01", "127.0.0.1", seeder_port, seeder_pm)
            seeder_node.start_listening()

            seeder_engine = TransferEngine("seeder_01", "127.0.0.1", tracker_port, seeder_port, seeder_pm)
            seeder_engine._announce_to_tracker(event="started")
            self.addCleanup(seeder_engine.stop)

            # 4. Khởi chạy Leecher (Tải file mới)
            download_file_path = os.path.join(self.test_dir, "downloaded.dat")
            leecher_pm = PieceManager(download_file_path, meta)
            self.assertFalse(leecher_pm.is_complete())

            leecher_node = PeerNode("leecher_01", "127.0.0.1", leecher_port, leecher_pm)
            leecher_node.start_listening()

            leecher_engine = TransferEngine("leecher_01", "127.0.0.1", tracker_port, leecher_port, leecher_pm, max_workers=2)
            self.addCleanup(leecher_engine.stop)

            # Khởi động tải P2P
            success = leecher_engine.start_download(peer_node=leecher_node)
            self.assertTrue(success)
            self.assertTrue(leecher_pm.is_complete())

            # 5. So sánh nội dung file tải về có trùng khớp 100% với file gốc không
            with open(download_file_path, "rb") as f:
                downloaded_data = f.read()

            self.assertEqual(downloaded_data, self.sample_data)

            # Dọn dẹp nodes
            seeder_node.stop()
            leecher_node.stop()

        finally:
            tracker.stop()

    def test_parallel_requests_to_one_seeder_after_discovery(self):
        """Một luồng tìm peer, bốn socket tải các mảnh khác nhau đồng thời."""
        source = os.path.join(self.test_dir, "parallel.dat")
        source_data = os.urandom(16 * 512 * 1024)
        with open(source, "wb") as file:
            file.write(source_data)
        meta = create_metainfo(source, piece_length=512 * 1024)

        tracker = TrackerServer(host="127.0.0.1", port=0)
        tracker.start()
        tracker_port = tracker.server_socket.getsockname()[1]

        seeder_pm = PieceManager(source, meta)
        seeder_pm.verify_all_pieces()
        seeder_node = PeerNode("parallel-seeder", "127.0.0.1", 0, seeder_pm)
        seeder_node.start_listening()
        seeder_port = seeder_node.server_socket.getsockname()[1]
        seeder_engine = TransferEngine("parallel-seeder", "127.0.0.1", tracker_port,
                                       seeder_port, seeder_pm)

        download_path = os.path.join(self.test_dir, "parallel-download.dat")
        leecher_pm = PieceManager(download_path, meta)
        leecher_node = PeerNode("parallel-leecher", "127.0.0.1", 0, leecher_pm)
        leecher_node.start_listening()
        leecher_port = leecher_node.server_socket.getsockname()[1]
        leecher_engine = TransferEngine("parallel-leecher", "127.0.0.1", tracker_port,
                                        leecher_port, leecher_pm, max_workers=4)

        active = 0
        peak = 0
        requests = []
        counter_lock = threading.Lock()
        first_request = threading.Event()
        original_read_piece = seeder_pm.read_piece

        def delayed_read_piece(piece_index):
            nonlocal active, peak
            with counter_lock:
                active += 1
                peak = max(peak, active)
                requests.append(piece_index)
                first_request.set()
            try:
                time.sleep(0.04)
                return original_read_piece(piece_index)
            finally:
                with counter_lock:
                    active -= 1

        seeder_pm.read_piece = delayed_read_piece
        tracker_threads = []
        original_get_peers = leecher_engine._get_peers_from_tracker

        def traced_get_peers():
            tracker_threads.append(threading.current_thread().name)
            return original_get_peers()

        leecher_engine._get_peers_from_tracker = traced_get_peers
        result = []
        download_thread = threading.Thread(
            target=lambda: result.append(leecher_engine.start_download(peer_node=leecher_node)),
            name="download-task", daemon=True,
        )

        try:
            with patch.object(TransferEngine, "DISCOVERY_INTERVAL", 0.05):
                download_thread.start()
                time.sleep(0.15)  # Seeder tham gia sau khi leecher đã hỏi tracker.
                seeder_engine._announce_to_tracker(event="started")
                self.assertTrue(first_request.wait(1.5), "Worker chờ quá lâu sau khi tìm thấy seeder")
                download_thread.join(timeout=8)

            self.assertFalse(download_thread.is_alive(), "Tải file không hoàn tất")
            self.assertEqual(result, [True])
            self.assertGreaterEqual(peak, 2)
            self.assertEqual(sorted(requests), list(range(16)))
            self.assertTrue(tracker_threads)
            self.assertTrue(all(name in {"download-task", "peer-discovery-parallel-leecher"}
                                for name in tracker_threads))
            with open(download_path, "rb") as file:
                self.assertEqual(hashlib.sha256(file.read()).digest(),
                                 hashlib.sha256(source_data).digest())
        finally:
            leecher_engine.stop()
            seeder_engine.stop()
            download_thread.join(timeout=1)
            leecher_node.stop()
            seeder_node.stop()
            tracker.stop()

if __name__ == "__main__":
    unittest.main()
