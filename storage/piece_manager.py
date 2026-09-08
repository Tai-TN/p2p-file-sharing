import os
import hashlib
import threading
from typing import List, Dict, Any, Optional

DEFAULT_PIECE_SIZE = 512 * 1024  # 512 KB (mặc định)

class PieceMetadata:
    """
    Quản lý thông tin metadata của file chia sẻ (tương tự file .torrent).
    """
    def __init__(self, filename: str, file_size: int, piece_size: int, piece_hashes: List[str], info_hash: str = ""):
        self.filename = filename
        self.file_size = file_size
        self.piece_size = piece_size
        self.piece_hashes = piece_hashes
        self.total_pieces = len(piece_hashes)
        self.info_hash = info_hash or self.compute_info_hash()

    def compute_info_hash(self) -> str:
        """Tính SHA-256 tổng thể của metadata."""
        concat_str = f"{self.filename}:{self.file_size}:{self.piece_size}:" + "".join(self.piece_hashes)
        return hashlib.sha256(concat_str.encode('utf-8')).hexdigest()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "filename": self.filename,
            "file_size": self.file_size,
            "piece_size": self.piece_size,
            "total_pieces": self.total_pieces,
            "piece_hashes": self.piece_hashes,
            "info_hash": self.info_hash
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'PieceMetadata':
        return cls(
            filename=data["filename"],
            file_size=data["file_size"],
            piece_size=data["piece_size"],
            piece_hashes=data["piece_hashes"],
            info_hash=data.get("info_hash", "")
        )

    @classmethod
    def create_from_file(cls, file_path: str, piece_size: int = DEFAULT_PIECE_SIZE) -> 'PieceMetadata':
        """
        Đọc một file trên đĩa, chia nhỏ thành các piece và tính chuỗi băm SHA-256 cho từng piece.
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File không tồn tại: {file_path}")

        filename = os.path.basename(file_path)
        file_size = os.path.getsize(file_path)
        piece_hashes = []

        with open(file_path, "rb") as f:
            while True:
                chunk = f.read(piece_size)
                if not chunk:
                    break
                piece_hash = hashlib.sha256(chunk).hexdigest()
                piece_hashes.append(piece_hash)

        return cls(filename, file_size, piece_size, piece_hashes)


class PieceManager:
    """
    Quản lý việc đọc/ghi file trên đĩa cứng, xác minh checksum SHA-256,
    và theo dõi mảng bitfield cho các piece đã sở hữu/còn thiếu.
    """
    def __init__(self, save_path: str, metadata: PieceMetadata, is_seeder: bool = False):
        self.save_path = save_path
        self.metadata = metadata
        self.lock = threading.Lock()
        
        # Mảng boolean theo dõi piece nào đã sở hữu
        self.bitfield: List[bool] = [is_seeder] * metadata.total_pieces
        
        # Nếu file chưa tồn tại (leecher tải mới), khởi tạo file trống đúng dung lượng
        if not os.path.exists(save_path):
            directory = os.path.dirname(save_path)
            if directory:
                os.makedirs(directory, exist_ok=True)
            with open(save_path, "wb") as f:
                f.truncate(metadata.file_size)
        elif not is_seeder:
            # Nếu file đã tồn tại một phần, kiểm tra hash từng piece để khôi phục bitfield (Resume download)
            self._verify_existing_file()

    def _verify_existing_file(self):
        """Kiểm tra các piece hiện có trong file để khôi phục lại bitfield khi khởi động lại."""
        with open(self.save_path, "rb") as f:
            for idx in range(self.metadata.total_pieces):
                offset = idx * self.metadata.piece_size
                f.seek(offset)
                piece_len = self.get_piece_length(idx)
                chunk = f.read(piece_len)
                if len(chunk) == piece_len:
                    current_hash = hashlib.sha256(chunk).hexdigest()
                    if current_hash == self.metadata.piece_hashes[idx]:
                        self.bitfield[idx] = True

    def get_piece_length(self, piece_index: int) -> int:
        """Tính độ dài chính xác của mảnh piece_index (mảnh cuối có thể nhỏ hơn piece_size)."""
        if piece_index < 0 or piece_index >= self.metadata.total_pieces:
            raise IndexError("Piece index ngoài phạm vi")
        if piece_index == self.metadata.total_pieces - 1:
            remainder = self.metadata.file_size % self.metadata.piece_size
            return remainder if remainder > 0 else self.metadata.piece_size
        return self.metadata.piece_size

    def verify_piece(self, piece_index: int, data: bytes) -> bool:
        """Xác minh tính đúng đắn của dữ liệu mảnh nhận được bằng SHA-256."""
        if piece_index < 0 or piece_index >= self.metadata.total_pieces:
            return False
        expected_hash = self.metadata.piece_hashes[piece_index]
        actual_hash = hashlib.sha256(data).hexdigest()
        return actual_hash == expected_hash

    def read_piece(self, piece_index: int) -> Optional[bytes]:
        """Đọc mảnh piece_index từ đĩa cứng (Thread-safe)."""
        if not self.bitfield[piece_index]:
            return None
        
        piece_len = self.get_piece_length(piece_index)
        offset = piece_index * self.metadata.piece_size
        
        with self.lock:
            with open(self.save_path, "rb") as f:
                f.seek(offset)
                data = f.read(piece_len)
                return data

    def write_piece(self, piece_index: int, data: bytes) -> bool:
        """Ghi mảnh piece_index xuống đĩa cứng sau khi verify thành công (Thread-safe)."""
        if not self.verify_piece(piece_index, data):
            print(f"[Storage Error] Checksum không khớp cho piece {piece_index}")
            return False
            
        offset = piece_index * self.metadata.piece_size
        
        with self.lock:
            with open(self.save_path, "r+b") as f:
                f.seek(offset)
                f.write(data)
                f.flush()
            self.bitfield[piece_index] = True
            
        return True

    def get_bitfield(self) -> List[bool]:
        """Trả về bản sao mảng bitfield hiện tại."""
        with self.lock:
            return list(self.bitfield)

    def is_complete(self) -> bool:
        """Kiểm tra file đã được tải đầy đủ 100% chưa."""
        with self.lock:
            return all(self.bitfield)

    def get_progress(self) -> float:
        """Tính phần trăm tiến độ hoàn thành (0.0% -> 100.0%)."""
        with self.lock:
            downloaded = sum(1 for b in self.bitfield if b)
            return (downloaded / self.metadata.total_pieces) * 100.0

    def get_missing_pieces(self) -> List[int]:
        """Lấy danh sách chỉ số các piece chưa có."""
        with self.lock:
            return [i for i, owned in enumerate(self.bitfield) if not owned]
