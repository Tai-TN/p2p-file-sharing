import os
import threading
import hashlib
from typing import Dict, Any, List, Optional

class PieceManager:
    """
    Quản lý việc đọc, ghi các mảnh (Piece) của tập tin trên đĩa.
    Đảm bảo an toàn đa luồng (Thread-safe) khi nhiều worker thread tải và ghi file đồng thời.
    """
    def __init__(self, target_file_path: str, metainfo: Dict[str, Any]):
        self.target_file_path = target_file_path
        self.metainfo = metainfo
        self.info = metainfo["info"]
        self.file_size = self.info["file_size"]
        self.piece_length = self.info["piece_length"]
        self.expected_hashes = self.info["pieces"]
        self.num_pieces = len(self.expected_hashes)

        # Thread lock ngăn ngừa ghi đè vị trí seek() khi nhiều luồng truy cập file cùng lúc
        self.file_lock = threading.Lock()

        # Bitfield: Danh sách boolean đánh dấu mảnh nào đã tải đúng, mảnh nào chưa
        self.bitfield: List[bool] = [False] * self.num_pieces

        # Chuẩn bị file đĩa và quét kiểm tra các piece đã sẵn có
        self._initialize_storage()

    def _initialize_storage(self) -> None:
        """
        Khởi tạo tập tin trên đĩa và xác minh các mảnh đã có sẵn (nếu có).
        """
        with self.file_lock:
            # Tạo file trống nếu chưa tồn tại
            if not os.path.exists(self.target_file_path):
                os.makedirs(os.path.dirname(os.path.abspath(self.target_file_path)), exist_ok=True)
                with open(self.target_file_path, "wb") as f:
                    # Tiền phân bổ dung lượng file (Pre-allocate file size)
                    f.truncate(self.file_size)

        # Đọc và xác minh hash của từng mảnh trên đĩa
        #self.verify_all_pieces()
        

    def verify_all_pieces(self) -> None:
        """Xác minh toàn bộ các piece hiện có trên đĩa so với SHA-256 mong đợi."""
        for i in range(self.num_pieces):
            piece_data = self._read_raw_piece(i)
            if piece_data and self._hash_matches(i, piece_data):
                self.bitfield[i] = True
            else:
                self.bitfield[i] = False

    def _get_piece_bounds(self, piece_index: int) -> tuple:
        """Tính offset bắt đầu và độ dài thực tế của mảnh thứ piece_index."""
        offset = piece_index * self.piece_length
        if piece_index == self.num_pieces - 1:
            # Piece cuối cùng có thể nhỏ hơn piece_length chuẩn
            length = self.file_size - offset
        else:
            length = self.piece_length
        return offset, length

    def _hash_matches(self, piece_index: int, data: bytes) -> bool:
        """Kiểm tra mã băm SHA-256 của data có trùng với expected_hashes[piece_index] không."""
        computed = hashlib.sha256(data).hexdigest()
        return computed == self.expected_hashes[piece_index]

    def _read_raw_piece(self, piece_index: int) -> Optional[bytes]:
        """Đọc raw bytes của mảnh piece_index từ đĩa tại vị trí offset chuẩn xác."""
        if piece_index < 0 or piece_index >= self.num_pieces:
            return None

        offset, length = self._get_piece_bounds(piece_index)
        
        with self.file_lock:
            try:
                with open(self.target_file_path, "rb") as f:
                    f.seek(offset)
                    data = f.read(length)
                    if len(data) == length:
                        return data
                    return None
            except Exception as e:
                print(f"[Storage Error] Lỗi đọc piece {piece_index}: {e}")
                return None

    def read_piece(self, piece_index: int) -> Optional[bytes]:
        """Đọc mảnh piece_index chỉ khi nó khả dụng và hợp lệ (bitfield == True)."""
        if not self.has_piece(piece_index):
            return None
        return self._read_raw_piece(piece_index)

    def write_piece(self, piece_index: int, data: bytes) -> bool:
        """
        Ghi dữ liệu mảnh vừa tải về vào đúng vị trí offset trên đĩa.
        Chỉ ghi khi SHA-256 hash của dữ liệu tải về hoàn toàn hợp lệ.
        """
        if piece_index < 0 or piece_index >= self.num_pieces:
            return False

        offset, expected_length = self._get_piece_bounds(piece_index)
        if len(data) != expected_length:
            print(f"[Storage Error] Piece {piece_index} sai độ dài: nhận {len(data)}, chờ {expected_length}")
            return False

        # Kiểm tra Checksum SHA-256
        if not self._hash_matches(piece_index, data):
            print(f"[Storage Error] Piece {piece_index} bị hỏng Checksum SHA-256!")
            return False

        # Ghi vào đĩa bằng seek() với Thread Lock
        with self.file_lock:
            try:
                with open(self.target_file_path, "r+b") as f:
                    f.seek(offset)
                    f.write(data)
                    f.flush()

                # Cập nhật trạng thái bitfield
                self.bitfield[piece_index] = True
                return True
            except Exception as e:
                print(f"[Storage Error] Lỗi khi ghi piece {piece_index}: {e}")
                return False

    def has_piece(self, piece_index: int) -> bool:
        if 0 <= piece_index < self.num_pieces:
            return self.bitfield[piece_index]
        return False

    def get_bitfield(self) -> List[bool]:
        return list(self.bitfield)

    def missing_pieces(self) -> List[int]:
        """Trả về danh sách chỉ số các piece còn thiếu."""
        return [i for i, owned in enumerate(self.bitfield) if not owned]

    def is_complete(self) -> bool:
        """Kiểm tra file đã tải hoàn tất 100% chưa."""
        return all(self.bitfield)

    def completed_pieces_count(self) -> int:
        """Trả về số lượng piece đã tải xong."""
        return sum(1 for b in self.bitfield if b)

    def progress_percentage(self) -> float:
        """Tính % tiến độ tải hiện tại."""
        return (self.completed_pieces_count() / self.num_pieces) * 100.0
