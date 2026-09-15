import os
import json
import hashlib
from typing import Dict, Any

DEFAULT_PIECE_LENGTH = 512 * 1024  # 512 KB per piece

def calculate_sha256(data: bytes) -> str:
    """Tính mã băm SHA-256 cho mảng bytes."""
    return hashlib.sha256(data).hexdigest()

def create_metainfo(file_path: str, piece_length: int = DEFAULT_PIECE_LENGTH, tracker_url: str = "127.0.0.1:8000") -> Dict[str, Any]:
    """
    Chia nhỏ tập tin thành các mảnh (Piece) và tạo Dictionary metainfo (tương tự file .torrent).
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Tập tin không tồn tại: {file_path}")

    file_size = os.path.getsize(file_path)
    file_name = os.path.basename(file_path)

    pieces = []
    with open(file_path, "rb") as f:
        while True:
            chunk = f.read(piece_length)
            if not chunk:
                break
            pieces.append(calculate_sha256(chunk))

    # Cấu trúc nội dung thông tin tập tin
    info = {
        "name": file_name,
        "file_size": file_size,
        "piece_length": piece_length,
        "pieces": pieces
    }

    # Tính info_hash: Mã băm SHA-256 đại diện duy nhất cho tập tin này trong hệ thống P2P
    canonical_info_str = json.dumps(info, sort_keys=True)
    info_hash = calculate_sha256(canonical_info_str.encode('utf-8'))

    metainfo = {
        "tracker": tracker_url,
        "info_hash": info_hash,
        "info": info
    }

    return metainfo

def save_metainfo(metainfo: Dict[str, Any], output_path: str) -> None:
    """Lưu dữ liệu metainfo ra file JSON."""
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(metainfo, f, indent=4, ensure_ascii=False)

def load_metainfo(meta_path: str) -> Dict[str, Any]:
    """Đọc file metadata từ đĩa."""
    with open(meta_path, "r", encoding="utf-8") as f:
        return json.load(f)
