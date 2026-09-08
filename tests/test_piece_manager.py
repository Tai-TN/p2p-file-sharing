import unittest
import os
import tempfile
import shutil
import hashlib
from storage.piece_manager import PieceMetadata, PieceManager

class TestPieceManager(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.piece_size = 100  # 100 bytes cho test nhanh
        
        # Tạo một file nguồn ngẫu nhiên 350 bytes -> 4 pieces (100, 100, 100, 50)
        self.source_file = os.path.join(self.test_dir, "test_file.bin")
        self.source_data = os.urandom(350)
        with open(self.source_file, "wb") as f:
            f.write(self.source_data)

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_metadata_creation(self):
        metadata = PieceMetadata.create_from_file(self.source_file, piece_size=self.piece_size)
        self.assertEqual(metadata.file_size, 350)
        self.assertEqual(metadata.piece_size, 100)
        self.assertEqual(metadata.total_pieces, 4)
        self.assertEqual(len(metadata.piece_hashes), 4)

        # Kiểm tra serialization
        meta_dict = metadata.to_dict()
        reconstructed = PieceMetadata.from_dict(meta_dict)
        self.assertEqual(reconstructed.info_hash, metadata.info_hash)
        self.assertEqual(reconstructed.total_pieces, 4)

    def test_out_of_order_write_and_read(self):
        metadata = PieceMetadata.create_from_file(self.source_file, piece_size=self.piece_size)
        dest_file = os.path.join(self.test_dir, "downloaded.bin")
        
        manager = PieceManager(dest_file, metadata, is_seeder=False)
        self.assertFalse(manager.is_complete())
        self.assertEqual(manager.get_progress(), 0.0)
        
        # Cắt dữ liệu nguồn thành 4 chunks
        chunks = [
            self.source_data[0:100],
            self.source_data[100:200],
            self.source_data[200:300],
            self.source_data[300:350]
        ]
        
        # Ghi không theo thứ tự: piece 2 -> piece 0 -> piece 3 -> piece 1
        self.assertTrue(manager.write_piece(2, chunks[2]))
        self.assertEqual(manager.get_progress(), 25.0)
        self.assertEqual(manager.get_missing_pieces(), [0, 1, 3])
        
        self.assertTrue(manager.write_piece(0, chunks[0]))
        self.assertTrue(manager.write_piece(3, chunks[3]))
        self.assertFalse(manager.is_complete())
        
        self.assertTrue(manager.write_piece(1, chunks[1]))
        self.assertTrue(manager.is_complete())
        self.assertEqual(manager.get_progress(), 100.0)

        # Đọc lại và kiểm tra toàn bộ file đĩa có giống file gốc 100% không
        with open(dest_file, "rb") as f:
            downloaded_data = f.read()
        self.assertEqual(downloaded_data, self.source_data)

    def test_corrupted_piece_rejection(self):
        metadata = PieceMetadata.create_from_file(self.source_file, piece_size=self.piece_size)
        dest_file = os.path.join(self.test_dir, "downloaded_corrupt.bin")
        
        manager = PieceManager(dest_file, metadata, is_seeder=False)
        
        corrupted_data = b"X" * 100
        # Ghi piece 0 với data bị hỏng
        self.assertFalse(manager.write_piece(0, corrupted_data))
        self.assertFalse(manager.get_bitfield()[0])

    def test_existing_file_resume(self):
        metadata = PieceMetadata.create_from_file(self.source_file, piece_size=self.piece_size)
        dest_file = os.path.join(self.test_dir, "resume_file.bin")
        
        # Tạo manager 1 và ghi piece 0 và piece 1
        mgr1 = PieceManager(dest_file, metadata, is_seeder=False)
        mgr1.write_piece(0, self.source_data[0:100])
        mgr1.write_piece(1, self.source_data[100:200])
        
        # Giả lập tắt app và khởi động lại manager 2 trên file đã có 1 phần
        mgr2 = PieceManager(dest_file, metadata, is_seeder=False)
        self.assertTrue(mgr2.get_bitfield()[0])
        self.assertTrue(mgr2.get_bitfield()[1])
        self.assertFalse(mgr2.get_bitfield()[2])
        self.assertEqual(mgr2.get_missing_pieces(), [2, 3])

if __name__ == '__main__':
    unittest.main()
