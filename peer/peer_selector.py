"""ETA-based selection of peers that can provide a requested piece."""

from __future__ import annotations

import random
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple


PeerCandidate = Tuple[str, Dict[str, Any]]


@dataclass
class PeerState:
    """Trạng thái đo được dùng để xếp hạng một peer."""

    # None nghĩa là peer chưa từng tải thành công; không dùng 0 cho trạng thái này.
    estimated_rate: Optional[float] = None
    # Số request đã được giữ chỗ nhưng chưa kết thúc, tính chung cho mọi worker.
    pending_requests: int = 0
    # Không chọn peer trước thời điểm này nếu peer vừa trả về CHOKE.
    choked_until: float = 0.0
    # Số request lỗi liên tiếp; được đặt lại sau một request thành công.
    failure_count: int = 0
    # Không chọn peer trước thời điểm này nếu peer vừa lỗi (backoff tăng dần).
    retry_after: float = 0.0


class PeerSelector:
    """Chọn peer theo thời gian hoàn thành ước tính nhỏ nhất.

    Selector này được dùng chung bởi tất cả worker tải file. Việc chọn peer
    đồng thời reserve một request để các worker nhìn thấy tải của nhau.
    """

    def __init__(
        self,
        *,
        smoothing_factor: float = 0.3,
        explore_rate: float = 0.1,
        eta_tie_threshold: float = 0.15,
        choke_cooldown: float = 1.0,
        default_bootstrap_rate: float = 1_048_576.0,
        failure_penalty: float = 0.5,
        failure_backoff: float = 0.25,
        max_failure_backoff: float = 30.0,
        rng: Optional[random.Random] = None,
    ) -> None:
        # Các tham số đều được kiểm tra ở đây để tránh tạo ra ETA không hợp lệ.
        if not 0 < smoothing_factor <= 1:
            raise ValueError("smoothing_factor must be in (0, 1]")
        if not 0 <= explore_rate <= 1:
            raise ValueError("explore_rate must be between 0 and 1")
        if eta_tie_threshold < 0:
            raise ValueError("eta_tie_threshold must not be negative")
        if choke_cooldown < 0:
            raise ValueError("choke_cooldown must not be negative")
        if default_bootstrap_rate <= 0:
            raise ValueError("default_bootstrap_rate must be positive")
        if not 0 < failure_penalty <= 1:
            raise ValueError("failure_penalty must be in (0, 1]")
        if failure_backoff < 0 or max_failure_backoff < 0:
            raise ValueError("failure backoff must not be negative")

        self.smoothing_factor = smoothing_factor
        self.explore_rate = explore_rate
        self.eta_tie_threshold = eta_tie_threshold
        self.choke_cooldown = choke_cooldown
        self.default_bootstrap_rate = default_bootstrap_rate
        self.failure_penalty = failure_penalty
        self.failure_backoff = failure_backoff
        self.max_failure_backoff = max_failure_backoff
        self._rng = rng or random.Random()
        self._states: Dict[str, PeerState] = {}
        # Bảo vệ state vì nhiều worker có thể chọn/cập nhật cùng một peer.
        self._lock = threading.RLock()


    def select_peer(
        self,
        available_peers: Sequence[PeerCandidate],
        piece_size: int,
        now: Optional[float] = None,
    ) -> Optional[PeerCandidate]:
        """Lọc, xếp hạng, reserve và trả về peer phù hợp nhất."""
        if piece_size <= 0:
            raise ValueError("piece_size must be positive")

        timestamp = time.monotonic() if now is None else now
        with self._lock:
            candidates = self._eligible_candidates(available_peers, timestamp)
            if not candidates:
                return None

            # Exploration giúp thử lại peer mới hoặc peer đang có tốc độ thay đổi.
            if self._rng.random() < self.explore_rate:
                selected = self._rng.choice(candidates)
            else:
                # Peer chưa có số đo dùng bootstrap rate để tránh chia cho 0
                # và vẫn có cơ hội được chọn trong giai đoạn khởi động.
                known_rates = [
                    state.estimated_rate
                    for state in self._states.values()
                    if state.estimated_rate is not None
                ]
                bootstrap_rate = max(
                    known_rates,
                    default=self.default_bootstrap_rate,
                )
                ranked = [
                    (
                        # ETA đã tính cả số request đang chờ của peer.
                        self._eta(
                            self._states[peer_id],
                            piece_size,
                            bootstrap_rate,
                        ),
                        peer_id,
                        peer_info,
                    )
                    for peer_id, peer_info in candidates
                ]
                best_eta = min(item[0] for item in ranked)
                threshold = best_eta * (1 + self.eta_tie_threshold)
                # Nếu ETA gần nhau trong ngưỡng, random để tránh dồn tải vào một peer.
                tied = [item for item in ranked if item[0] <= threshold]
                _, peer_id, peer_info = self._rng.choice(tied)
                selected = (peer_id, peer_info)

            # Reserve ngay trong lock để worker khác không cùng thấy peer đang rảnh.
            self._states[selected[0]].pending_requests += 1
            return selected

    def record_success(
        self,
        peer_id: str,
        byte_count: int,
        elapsed: float,
    ) -> None:
        """Ghi nhận request thành công và cập nhật tốc độ trung bình trượt."""
        if byte_count <= 0:
            raise ValueError("byte_count must be positive")
        if elapsed <= 0:
            raise ValueError("elapsed must be positive")

        with self._lock:
            state = self._state_for(peer_id)
            current_rate = byte_count / elapsed
            if state.estimated_rate is None:
                # Lần đo đầu tiên dùng trực tiếp tốc độ thực tế.
                state.estimated_rate = current_rate
            else:
                alpha = self.smoothing_factor
                # EWMA làm tốc độ ổn định hơn, tránh bị ảnh hưởng bởi một lần đo nhiễu.
                state.estimated_rate = (
                    (1 - alpha) * state.estimated_rate
                    + alpha * current_rate
                )
            self._release_request(state)
            state.failure_count = 0
            state.retry_after = 0.0

    def record_failure(self, peer_id: str, now: Optional[float] = None) -> None:
        """Giải phóng request lỗi, hạ ưu tiên và tạm loại peer theo backoff."""
        timestamp = time.monotonic() if now is None else now
        with self._lock:
            state = self._state_for(peer_id)
            self._release_request(state)
            state.failure_count += 1
            if state.estimated_rate is not None:
                # Không xóa peer sau một lỗi; chỉ giảm ưu tiên để còn cơ hội thử lại.
                state.estimated_rate *= self.failure_penalty
            # Peer chưa đo (rate None) được phạt trong _eta qua failure_count.
            # Lỗi liên tiếp -> chờ lâu hơn (0.25s, 0.5s, 1s, ... tối đa 30s),
            # tránh việc peer chết bị chọn lại liên tục.
            delay = min(
                self.failure_backoff * (2 ** (state.failure_count - 1)),
                self.max_failure_backoff,
            )
            state.retry_after = timestamp + delay

    def record_choke(
        self,
        peer_id: str,
        now: Optional[float] = None,
    ) -> None:
        """Giải phóng request bị choke và tạm thời loại peer khỏi lựa chọn."""
        timestamp = time.monotonic() if now is None else now
        with self._lock:
            state = self._state_for(peer_id)
            self._release_request(state)
            # CHOKE là trạng thái tạm thời, không nhất thiết là lỗi kết nối.
            state.choked_until = timestamp + self.choke_cooldown

    def available_peers(
        self,
        peer_ids: Sequence[str],
        now: Optional[float] = None,
    ) -> List[str]:
        """Các peer có thể xin ngay lúc này (không trong cooldown CHOKE/backoff lỗi)."""
        timestamp = time.monotonic() if now is None else now
        with self._lock:
            return [
                peer_id for peer_id in peer_ids
                if self._state_for(peer_id).choked_until <= timestamp
                and self._state_for(peer_id).retry_after <= timestamp
            ]

    def get_peer_stats(self, peer_id: str) -> PeerState:
        """Trả về bản sao state để log, chẩn đoán và kiểm thử."""
        with self._lock:
            state = self._state_for(peer_id)
            return PeerState(
                estimated_rate=state.estimated_rate,
                pending_requests=state.pending_requests,
                choked_until=state.choked_until,
                failure_count=state.failure_count,
                retry_after=state.retry_after,
            )

    def _eligible_candidates(
        self,
        available_peers: Sequence[PeerCandidate],
        now: float,
    ) -> List[PeerCandidate]:
        candidates = []
        for peer_id, peer_info in available_peers:
            state = self._state_for(peer_id)
            # Peer đang cooldown (CHOKE hoặc backoff sau lỗi) không được gửi request mới.
            if state.choked_until <= now and state.retry_after <= now:
                candidates.append((peer_id, peer_info))
        return candidates

    def _eta(
        self,
        state: PeerState,
        piece_size: int,
        bootstrap_rate: float,
    ) -> float:
        # Rate None được thay bằng bootstrap rate; rate thực tế luôn dương.
        if state.estimated_rate is not None:
            rate = state.estimated_rate
        else:
            # Peer chưa từng thành công mà đã lỗi thì không được coi là
            # "nhanh nhất": mỗi lỗi giảm bootstrap rate theo failure_penalty.
            rate = bootstrap_rate * (self.failure_penalty ** state.failure_count)
        return (state.pending_requests + 1) * piece_size / rate

    def _state_for(self, peer_id: str) -> PeerState:
        return self._states.setdefault(peer_id, PeerState())

    @staticmethod
    def _release_request(state: PeerState) -> None:
        # max(0, ...) bảo đảm pending không bị âm nếu release lặp do lỗi ở caller.
        state.pending_requests = max(0, state.pending_requests - 1)
