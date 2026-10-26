import unittest
import os
import sys
import shutil
import tempfile
import time
import socket

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass
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
            # PieceManager khởi tạo bitfield rỗng; seeder phải xác minh file
            # nguồn để đánh dấu các piece hợp lệ trước khi phục vụ upload.
            seeder_pm.verify_all_pieces()
            self.assertTrue(seeder_pm.is_complete())

            seeder_node = PeerNode("seeder_01", "127.0.0.1", seeder_port, seeder_pm)
            seeder_node.start_listening()

            seeder_engine = TransferEngine("seeder_01", "127.0.0.1", tracker_port, seeder_port, seeder_pm)
            seeder_engine._announce_to_tracker(event="started")

            # 4. Khởi chạy Leecher (Tải file mới)
            download_file_path = os.path.join(self.test_dir, "downloaded.dat")
            leecher_pm = PieceManager(download_file_path, meta)
            self.assertFalse(leecher_pm.is_complete())

            leecher_node = PeerNode("leecher_01", "127.0.0.1", leecher_port, leecher_pm)
            leecher_node.start_listening()

            leecher_engine = TransferEngine("leecher_01", "127.0.0.1", tracker_port, leecher_port, leecher_pm, max_workers=2)

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

if __name__ == "__main__":
    unittest.main()
