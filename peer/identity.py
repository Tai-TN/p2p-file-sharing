"""
Sinh và lưu peer_id cố định cho mỗi peer.

peer_id = "peer_" + 12 ký tự hex đầu của uuid4 (48 bit ngẫu nhiên → gần như không
thể trùng trong một mạng LAN). Id được sinh MỘT lần rồi lưu ra file, nên peer
thoát ra vào lại vẫn giữ nguyên danh tính (Tracker và Tit-for-Tat đều dựa vào id).
"""
import os
import re
import uuid

PEER_ID_PATTERN = re.compile(r"^[A-Za-z0-9_\-]{1,40}$")


def new_peer_id() -> str:
    return f"peer_{uuid.uuid4().hex[:12]}"


def is_valid_peer_id(value: str) -> bool:
    return bool(value) and bool(PEER_ID_PATTERN.match(value))


def load_or_create_peer_id(path: str) -> str:
    """Đọc peer_id từ file; nếu chưa có (hoặc hỏng) thì sinh mới và lưu lại."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            value = f.read().strip()
        if is_valid_peer_id(value):
            return value
    except OSError:
        pass

    value = new_peer_id()
    try:
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            f.write(value)
    except OSError:
        pass  # Không ghi được thì vẫn chạy với id mới (chỉ mất tính cố định)
    return value
