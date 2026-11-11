"""Bộ đo tốc độ truyền dữ liệu dùng cho giao diện (byte/giây trong cửa sổ trượt)."""
import threading
import time
from collections import deque
from typing import Deque, Optional, Tuple


class RateMeter:
    """
    Đếm tổng số byte và tính tốc độ trung bình trong `window` giây gần nhất.
    An toàn đa luồng: nhiều worker có thể cùng gọi add().
    """

    def __init__(self, window: float = 5.0) -> None:
        self.window = window
        self.total = 0
        self._samples: Deque[Tuple[float, int]] = deque()
        self._lock = threading.Lock()

    def add(self, byte_count: int, now: Optional[float] = None) -> None:
        ts = time.monotonic() if now is None else now
        with self._lock:
            self.total += byte_count
            self._samples.append((ts, byte_count))
            self._trim(ts)

    def rate(self, now: Optional[float] = None) -> float:
        ts = time.monotonic() if now is None else now
        with self._lock:
            self._trim(ts)
            if not self._samples:
                return 0.0
            return sum(b for _, b in self._samples) / self.window

    def _trim(self, now: float) -> None:
        while self._samples and now - self._samples[0][0] > self.window:
            self._samples.popleft()
