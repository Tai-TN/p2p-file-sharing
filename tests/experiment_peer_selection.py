"""
Thí nghiệm CHẠY THẬT so sánh 3 cách chọn peer để xin mảnh:

  - ETA           : thuật toán của dự án (PeerSelector) — ETA = (pending + 1) × size / rate
  - Least-loaded  : chỉ nhìn số request đang chờ, chọn peer ít request nhất (hòa thì random)
  - Random        : chọn ngẫu nhiên một peer có mảnh

Cả 3 dùng CHUNG mọi thứ khác (Rarest-First, 4 worker, CHOKE cooldown, backoff khi lỗi),
chỉ khác đúng bước "chọn peer". Nhờ vậy khác biệt đo được là do tiêu chí chọn peer.

Kịch bản: 1 Tracker + NUM_SOURCES seeder đủ file + 1 leecher, trên localhost.
Tốc độ upload mỗi seeder đặt trong SCENARIOS (None = không giới hạn).

Chạy (ở thư mục gốc dự án):   python tests/experiment_peer_selection.py
Kết quả: tests/experiment_results/peer_selection_*.png + peer_selection_results.csv
Cần matplotlib:  pip install matplotlib
"""
import contextlib
import csv
import io
import os
import random
import shutil
import socket
import statistics
import sys
import tempfile
import threading
import time
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from peer.peer_node import PeerNode  # noqa: E402
from peer.peer_selector import PeerSelector  # noqa: E402
from peer.transfer_engine import TransferEngine  # noqa: E402
from storage.metainfo import create_metainfo  # noqa: E402
from storage.piece_manager import PieceManager  # noqa: E402
from tracker.tracker_server import TrackerServer  # noqa: E402

# ═══════════════════ THAM SỐ THÍ NGHIỆM (chỉnh ở đây) ═══════════════════ #
FILE_PATH = None           # None = tự tạo file ngẫu nhiên FILE_SIZE_MB MB
FILE_SIZE_MB = 64
MB = 1024 * 1024
# Tên kịch bản -> tốc độ upload của từng seeder (byte/giây, None = không giới hạn)
SCENARIOS = {
    "Không giới hạn": [None, None, None, None],
    "Đều 4 MB/s": [4 * MB, 4 * MB, 4 * MB, 4 * MB],
    "Lệch 8/4/2/1 MB/s": [8 * MB, 4 * MB, 2 * MB, 1 * MB],
    # Nhiều peer hơn số worker (4): trường hợp thường gặp trong swarm thật.
    "8 peer: 2×8 + 6×1 MB/s": [8 * MB, 8 * MB] + [1 * MB] * 6,
}
# Kịch bản dùng cho đồ thị 2 (mỗi peer gửi bao nhiêu % số mảnh)
SHARE_SCENARIO = "8 peer: 2×8 + 6×1 MB/s"
STRATEGIES = ["eta", "least_loaded", "random"]
RUNS = 3
MAX_SECONDS = 180
# ════════════════════════════════════════════════════════════════════════ #

LABELS = {"eta": "ETA (dự án)", "least_loaded": "Least-loaded", "random": "Random"}
COLORS = {"eta": "#2a78d6", "least_loaded": "#eb6834", "random": "#1baf7a"}
OUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "experiment_results")


class LeastLoadedSelector(PeerSelector):
    """Chỉ dùng số request đang chờ: chọn peer có pending_requests nhỏ nhất."""

    def select_peer(self, available_peers, piece_size, now=None):
        timestamp = time.monotonic() if now is None else now
        with self._lock:
            candidates = self._eligible_candidates(available_peers, timestamp)
            if not candidates:
                return None
            least = min(self._states[p].pending_requests for p, _ in candidates)
            tied = [c for c in candidates if self._states[c[0]].pending_requests == least]
            selected = self._rng.choice(tied)
            self._states[selected[0]].pending_requests += 1
            return selected


class RandomSelector(PeerSelector):
    """Chọn ngẫu nhiên (giống random.choice cũ), vẫn giữ chỗ để thống kê pending đúng."""

    def select_peer(self, available_peers, piece_size, now=None):
        timestamp = time.monotonic() if now is None else now
        with self._lock:
            candidates = self._eligible_candidates(available_peers, timestamp)
            if not candidates:
                return None
            selected = self._rng.choice(candidates)
            self._states[selected[0]].pending_requests += 1
            return selected


SELECTORS = {"eta": PeerSelector, "least_loaded": LeastLoadedSelector, "random": RandomSelector}


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def run_once(strategy, rates, source_file, meta, work_dir, run):
    tracker_port = free_port()
    tracker = TrackerServer(host="127.0.0.1", port=tracker_port, peer_timeout=60.0)
    tracker.start()
    time.sleep(0.1)

    sources = []
    for k, rate in enumerate(rates):
        peer_id = f"seeder_{k + 1}"
        port = free_port()
        pm = PieceManager(source_file, meta)
        pm.bitfield = [True] * len(meta["info"]["pieces"])   # đủ file (đã kiểm hash 1 lần ở main)
        node = PeerNode(peer_id, "127.0.0.1", port, pm, upload_rate_limit=rate)
        node.start_listening()
        TransferEngine(peer_id, "127.0.0.1", tracker_port, port, pm)._announce_to_tracker("started")
        sources.append((node, pm))

    out_path = os.path.join(work_dir, f"download_{strategy}_{run}.bin")
    leecher_pm = PieceManager(out_path, meta)
    engine = TransferEngine("leecher", "127.0.0.1", tracker_port, free_port(),
                            leecher_pm, max_workers=4)
    engine.peer_selector = SELECTORS[strategy]()   # chỉ thay bước chọn peer

    # Đếm mỗi peer gửi bao nhiêu mảnh + thời điểm mảnh cuối (chính xác hơn log 0,5 s).
    per_peer = Counter()
    last_piece = [0.0]
    original_success = engine.peer_selector.record_success

    def counted_success(peer_id, byte_count, elapsed):
        per_peer[peer_id] += 1
        last_piece[0] = time.perf_counter()
        return original_success(peer_id, byte_count, elapsed)

    engine.peer_selector.record_success = counted_success

    t0 = time.perf_counter()
    download = threading.Thread(target=engine.start_download, daemon=True)
    download.start()
    while not leecher_pm.is_complete() and time.perf_counter() - t0 < MAX_SECONDS:
        time.sleep(0.02)
    elapsed = last_piece[0] - t0 if leecher_pm.is_complete() else None

    engine.stop()
    for node, pm in sources:
        node.stop()
        pm.close()
    tracker.stop()
    download.join(timeout=3)
    time.sleep(0.5)
    for path in (out_path, out_path + ".progress.json"):
        for _ in range(20):
            try:
                leecher_pm.close()
                if os.path.exists(path):
                    os.remove(path)
                break
            except OSError:
                time.sleep(0.5)
    return elapsed, per_peer


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    work_dir = tempfile.mkdtemp(prefix="p2p_peer_selection_")
    try:
        source_file = FILE_PATH
        if source_file is None:
            source_file = os.path.join(work_dir, "experiment_file.bin")
            with open(source_file, "wb") as f:
                f.write(os.urandom(FILE_SIZE_MB * MB))
        meta = create_metainfo(source_file)
        size_mb = os.path.getsize(source_file) / MB
        print(f"File {size_mb:.0f} MB, {len(meta['info']['pieces'])} mảnh, "
              f"{RUNS} lần chạy mỗi cấu hình\n")

        rows, times, shares = [], {}, {}
        for scenario, rates in SCENARIOS.items():
            for strategy in STRATEGIES:
                ts, total = [], Counter()
                for run in range(RUNS):
                    with contextlib.redirect_stdout(io.StringIO()):
                        elapsed, per_peer = run_once(strategy, rates, source_file, meta,
                                                     work_dir, run)
                    ts.append(elapsed)
                    total.update(per_peer)
                    rows.append({"scenario": scenario, "strategy": LABELS[strategy], "run": run + 1,
                                 "seconds": None if elapsed is None else round(elapsed, 3),
                                 **{f"pieces_seeder_{k + 1}": per_peer.get(f"seeder_{k + 1}", 0)
                                    for k in range(len(rates))}})
                ok = [t for t in ts if t is not None]
                times[(scenario, strategy)] = statistics.mean(ok) if ok else float("nan")
                shares[(scenario, strategy)] = total
                print(f"{scenario:<20} {LABELS[strategy]:<14} "
                      f"{times[(scenario, strategy)]:6.2f} s  "
                      f"({', '.join('%.2f' % t if t else 'lỗi' for t in ts)})")
            print()

        with open(os.path.join(OUT_DIR, "peer_selection_results.csv"), "w", newline="",
                  encoding="utf-8") as f:
            keys = sorted({k for r in rows for k in r}, key=lambda k: list(rows[0]).index(k)
                          if k in rows[0] else 99)
            w = csv.DictWriter(f, fieldnames=keys)
            w.writeheader()
            w.writerows(rows)

        plot(times, shares, size_mb)
        print(f"Đã lưu đồ thị và CSV vào: {OUT_DIR}")
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)


def plot(times, shares, size_mb):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    ink, quiet, grid = "#0b0b0b", "#52514e", "#e4e3df"
    plt.rcParams.update({
        "font.family": "DejaVu Sans", "font.size": 11, "axes.edgecolor": quiet,
        "axes.labelcolor": ink, "xtick.color": quiet, "ytick.color": quiet,
        "axes.spines.top": False, "axes.spines.right": False,
    })
    scenarios = list(SCENARIOS)

    def frame(ax, title, subtitle):
        ax.set_title(title, loc="left", fontsize=14, fontweight="bold", color=ink, pad=26)
        ax.text(0, 1.02, subtitle, transform=ax.transAxes, fontsize=10.5, color=quiet)
        ax.grid(axis="y", color=grid, linewidth=0.8)
        ax.set_axisbelow(True)

    # 1. Thời gian tải theo kịch bản
    fig, ax = plt.subplots(figsize=(9, 5.2))
    width = 0.26
    for j, s in enumerate(STRATEGIES):
        xs = [i + (j - 1) * width for i in range(len(scenarios))]
        ys = [times[(sc, s)] for sc in scenarios]
        bars = ax.bar(xs, ys, width=width, color=COLORS[s], edgecolor="white", linewidth=2,
                      label=LABELS[s])
        for b, v in zip(bars, ys):
            ax.text(b.get_x() + b.get_width() / 2, v + 0.2, f"{v:.1f}s", ha="center",
                    fontsize=10, color=ink)
    ax.set_xticks(range(len(scenarios)))
    ax.set_xticklabels([sc.replace(": ", ":\n") for sc in scenarios])
    frame(ax, "Nhiều peer hơn worker: ETA nhanh gấp đôi Least-loaded",
          f"Thời gian tải file {size_mb:.0f} MB, 4 worker, trung bình {RUNS} lần (thấp hơn là tốt hơn)")
    ax.set_ylabel("Thời gian tải (giây)")
    ax.legend(frameon=False, loc="upper left")
    fig.tight_layout()
    fig.savefig(os.path.join(OUT_DIR, "peer_selection_1_time.png"), dpi=160)
    plt.close(fig)

    # 2. Mỗi seeder gửi bao nhiêu mảnh, ở kịch bản SHARE_SCENARIO
    sc = SHARE_SCENARIO
    rates = SCENARIOS[sc]
    names = [f"seeder_{k + 1}" for k in range(len(rates))]
    labels = [f"Seeder {k + 1}\n{'không giới hạn' if r is None else f'{r / MB:g} MB/s'}"
              for k, r in enumerate(rates)]
    fig, ax = plt.subplots(figsize=(9, 5.2))
    for j, s in enumerate(STRATEGIES):
        total = sum(shares[(sc, s)].values()) or 1
        xs = [i + (j - 1) * width for i in range(len(names))]
        ys = [100 * shares[(sc, s)].get(n, 0) / total for n in names]
        ax.bar(xs, ys, width=width, color=COLORS[s], edgecolor="white", linewidth=2,
               label=LABELS[s])
    ax.set_xticks(range(len(names)))
    ax.set_xticklabels(labels)
    frame(ax, "ETA dồn việc cho peer nhanh, Least-loaded chia đều cho cả peer chậm",
          f"% số mảnh mỗi seeder gửi, kịch bản \"{sc}\", gộp {RUNS} lần chạy")
    ax.set_ylabel("% số mảnh")
    ax.legend(frameon=False, loc="upper right")
    fig.tight_layout()
    fig.savefig(os.path.join(OUT_DIR, "peer_selection_2_share.png"), dpi=160)
    plt.close(fig)


if __name__ == "__main__":
    main()
