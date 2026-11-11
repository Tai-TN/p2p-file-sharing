"""
Gom output của chương trình thành nhật ký cho màn "Nhật ký".

Code lõi dùng print(); thay vì sửa hàng trăm dòng print (dễ conflict với các nhánh
khác), app thay sys.stdout bằng một luồng "tee": vẫn in ra terminal như cũ, đồng
thời đưa từng dòng vào bộ đệm có giờ và mức độ.
"""
import io
import sys
import threading
import time
from collections import deque
from typing import Dict, List


def classify(line: str) -> str:
    """Đoán mức độ của một dòng log dựa trên nội dung."""
    low = line.lower()
    if "lỗi" in low or "error" in low or "traceback" in low or "thất bại" in low:
        return "ERROR"
    if ("sai" in low or "ngắt" in low or "chặn" in low or "warn" in low
            or "hỏng" in low or "mất kết nối" in low or "quá hạn" in low):
        return "WARN"
    if "✓" in line or "hoàn tất" in low or "thành công" in low:
        return "OK"
    return "INFO"


class LogBuffer:
    def __init__(self, max_lines: int = 1000):
        self._lines: deque = deque(maxlen=max_lines)
        self._next_id = 1
        self._lock = threading.Lock()

    def add(self, text: str, level: str = "") -> None:
        text = text.rstrip()
        if not text:
            return
        with self._lock:
            self._lines.append({
                "id": self._next_id,
                "time": time.time(),
                "level": level or classify(text),
                "text": text,
            })
            self._next_id += 1

    def since(self, last_id: int) -> List[Dict]:
        with self._lock:
            return [line for line in self._lines if line["id"] > last_id]

    def clear(self) -> None:
        with self._lock:
            self._lines.clear()


class TeeStream(io.TextIOBase):
    """Luồng ghi vừa chuyển tiếp ra stream gốc vừa đẩy từng dòng hoàn chỉnh vào LogBuffer."""

    def __init__(self, original, buffer: LogBuffer):
        self._original = original
        self._buffer = buffer
        self._pending = ""
        self._lock = threading.Lock()

    def write(self, s: str) -> int:
        if self._original is not None:
            try:
                self._original.write(s)
            except Exception:
                pass
        with self._lock:
            self._pending += s
            *lines, self._pending = self._pending.split("\n")
        for line in lines:
            self._buffer.add(line)
        return len(s)

    def flush(self) -> None:
        if self._original is not None:
            try:
                self._original.flush()
            except Exception:
                pass

    @property
    def encoding(self):
        return getattr(self._original, "encoding", "utf-8")


def install(buffer: LogBuffer) -> None:
    """Thay sys.stdout / sys.stderr bằng TeeStream (gọi một lần khi app khởi động)."""
    for name in ("stdout", "stderr"):
        original = getattr(sys, name)
        if original is not None:
            try:
                original.reconfigure(encoding="utf-8")
            except Exception:
                pass
        setattr(sys, name, TeeStream(original, buffer))
