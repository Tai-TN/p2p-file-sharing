"""Tit-for-tat choking and unchoking policy."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
import random
import threading
import time
from typing import Deque, Dict, Iterable, Optional, Set, Tuple


@dataclass
class PeerStats:
    """Thống kê đóng góp truyền dữ liệu của một Peer từ xa."""

    peer_id: str  # ID logic của Peer; nhiều socket của cùng Peer dùng chung thống kê này.
    bytes_uploaded_to_peer: int = 0  # Tổng số byte ta đã upload cho Peer.
    bytes_downloaded_from_peer: int = 0  # Tổng số byte ta đã download từ Peer.
    recent_upload_bytes: int = 0  # Tổng byte upload được ghi nhận trong các mẫu gần đây.
    last_activity: float = field(default_factory=time.monotonic)  # Lần cuối có truyền dữ liệu.
    last_unchoke_time: Optional[float] = None  # Thời điểm Peer gần nhất được mở khóa upload.
    # Mỗi phần tử là (thời điểm gửi thành công, số byte), dùng để tính tốc độ gần đây.
    _upload_samples: Deque[Tuple[float, int]] = field(default_factory=deque, repr=False)
    _download_samples: Deque[Tuple[float, int]] = field(default_factory=deque, repr=False)

    def record_upload(self, byte_count: int, now: Optional[float] = None) -> None:
        if byte_count < 0:
            raise ValueError("byte_count must not be negative")
        timestamp = time.monotonic() if now is None else now
        self.bytes_uploaded_to_peer += byte_count
        self.recent_upload_bytes += byte_count
        self.last_activity = timestamp
        self._upload_samples.append((timestamp, byte_count))

    def record_download(self, byte_count: int, now: Optional[float] = None) -> None:
        if byte_count < 0:
            raise ValueError("byte_count must not be negative")
        timestamp = time.monotonic() if now is None else now
        self.bytes_downloaded_from_peer += byte_count
        self.last_activity = timestamp
        self._download_samples.append((timestamp, byte_count))

    def upload_rate(self, now: float, window: float) -> float:
        cutoff = now - window
        while self._upload_samples and self._upload_samples[0][0] < cutoff:
            self._upload_samples.popleft()
        return sum(size for _, size in self._upload_samples) / window

    def download_rate(self, now: float, window: float) -> float:
        cutoff = now - window
        while self._download_samples and self._download_samples[0][0] < cutoff:
            self._download_samples.popleft()
        return sum(size for _, size in self._download_samples) / window


class ChokingManager:
    """Thread-safe regular and optimistic unchoking policy.

    A peer must be marked interested before it can consume an upload slot.
    Regular slots prefer peers that recently uploaded data to us.  One
    optimistic slot periodically gives an interested peer an opportunity.
    """

    def __init__(
        self,
        regular_slots: int = 2,
        optimistic_slots: int = 1,
        choking_interval: float = 20.0,
        measurement_window: float = 30.0,
        rng: Optional[random.Random] = None,
    ) -> None:
        if regular_slots < 0 or optimistic_slots < 0:
            raise ValueError("slot counts must not be negative")
        if choking_interval <= 0 or measurement_window <= 0:
            raise ValueError("intervals must be positive")

        self.regular_slots = regular_slots  # Số Peer đóng góp tốt được mở khóa cố định.
        self.optimistic_slots = optimistic_slots  # Số slot thử nghiệm cho Peer khác.
        self.choking_interval = choking_interval  # Chu kỳ tính lại danh sách unchoke (giây).
        self.measurement_window = measurement_window  # Khoảng thời gian đo tốc độ đóng góp.
        self._rng = rng or random.Random()  # Bộ sinh số ngẫu nhiên cho optimistic unchoking.
        self._stats: Dict[str, PeerStats] = {}  # peer_id -> thống kê truyền dữ liệu.
        self._interested: Set[str] = set()  # Các Peer đang muốn nhận piece từ ta.
        self._unchoked: Set[str] = set()  # Các Peer hiện được phép gửi REQUEST.
        self._optimistic: Set[str] = set()  # Các Peer đang chiếm optimistic slot.
        self._last_recalculation = 0.0  # Lần gần nhất chính sách được tính lại.
        self._lock = threading.RLock()  # Bảo vệ trạng thái vì nhiều connection cùng truy cập.

    def register_peer(self, peer_id: str) -> None:
        with self._lock:
            self._stats.setdefault(peer_id, PeerStats(peer_id))

    def unregister_peer(self, peer_id: str) -> None:
        with self._lock:
            self._stats.pop(peer_id, None)
            self._interested.discard(peer_id)
            self._unchoked.discard(peer_id)
            self._optimistic.discard(peer_id)

    def set_interested(self, peer_id: str, interested: bool) -> None:
        with self._lock:
            self.register_peer(peer_id)
            if interested:
                self._interested.add(peer_id)
                # Nếu còn slot unchoke khả dụng, mở khóa ngay lập tức
                if len(self._unchoked) < (self.regular_slots + self.optimistic_slots):
                    self.recalculate(force=True)
            else:
                self._interested.discard(peer_id)
                self._unchoked.discard(peer_id)
                self._optimistic.discard(peer_id)
                if self._interested:
                    self.recalculate(force=True)

    def record_upload(self, peer_id: str, byte_count: int, now: Optional[float] = None) -> None:
        with self._lock:
            self.register_peer(peer_id)
            self._stats[peer_id].record_upload(byte_count, now)

    def record_download(self, peer_id: str, byte_count: int, now: Optional[float] = None) -> None:
        with self._lock:
            self.register_peer(peer_id)
            self._stats[peer_id].record_download(byte_count, now)

    def recalculate(self, now: Optional[float] = None, force: bool = False) -> Set[str]:
        """Recalculate slots and return peer IDs whose state changed."""
        timestamp = time.monotonic() if now is None else now
        with self._lock:
            if not force and timestamp - self._last_recalculation < self.choking_interval:
                return set()

            # Chỉ xếp hạng Peer có nhu cầu nhận dữ liệu; Peer không interested
            # không được chiếm upload slot.
            interested = self._interested & self._stats.keys()

            # Xếp hạng: ưu tiên peer có download_rate cao nhất (họ upload cho ta nhiều nhất).
            # Nếu không có dữ liệu download (ví dụ ta là Seeder), xếp hạng theo upload_rate.
            has_downloads = any(
                self._stats[p].download_rate(timestamp, self.measurement_window) > 0
                for p in interested
            )

            def peer_score(p_id: str) -> tuple:
                if has_downloads:
                    rate = self._stats[p_id].download_rate(timestamp, self.measurement_window)
                else:
                    rate = self._stats[p_id].upload_rate(timestamp, self.measurement_window)
                return (rate, self._stats[p_id].last_activity)

            ranked = sorted(
                interested,
                key=peer_score,
                reverse=True,
            )
            regular = set(ranked[: self.regular_slots])
            candidates = [peer_id for peer_id in ranked if peer_id not in regular]

            # Peer không vào regular slot vẫn có cơ hội thử nghiệm qua optimistic slot.
            optimistic = set()
            if self.optimistic_slots and candidates:
                optimistic.update(
                    self._rng.sample(candidates, min(self.optimistic_slots, len(candidates)))
                )

            new_unchoked = regular | optimistic
            changed = self._unchoked ^ new_unchoked
            for peer_id in new_unchoked - self._unchoked:
                self._stats[peer_id].last_unchoke_time = timestamp
            self._unchoked = new_unchoked
            self._optimistic = optimistic
            self._last_recalculation = timestamp
            return changed

    def is_unchoked(self, peer_id: str, now: Optional[float] = None) -> bool:
        self.recalculate(now)
        with self._lock:
            return peer_id in self._unchoked

    def unchoked_peers(self, now: Optional[float] = None) -> Set[str]:
        self.recalculate(now)
        with self._lock:
            return set(self._unchoked)

    def optimistic_peers(self) -> Set[str]:
        with self._lock:
            return set(self._optimistic)

    def stats(self, peer_id: str) -> Optional[PeerStats]:
        with self._lock:
            return self._stats.get(peer_id)

    def peer_ids(self) -> Iterable[str]:
        with self._lock:
            return tuple(self._stats)
