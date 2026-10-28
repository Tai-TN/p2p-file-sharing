"""
Thí nghiệm CHẠY THẬT so sánh 3 chiến lược chọn mảnh: Rarest-First, Random, Sequential.

Kịch bản "seeder gốc rời mạng sớm":
  - 1 Tracker + NUM_SOURCES peer nguồn + 1 leecher, tất cả chạy thật trên localhost.
  - Nguồn 0 (seeder gốc) có ĐỦ file, nhưng rời mạng sau LEAVE_AT giây.
  - Các nguồn còn lại chỉ có MỘT PHẦN file. RARE_FRACTION số mảnh CHỈ seeder gốc có
    (mảnh hiếm), nằm rải rác trong file.
  - Mỗi nguồn bị giới hạn upload SOURCE_RATE_MB MB/s để giống mạng thật.

Câu hỏi: khi seeder gốc rời đi, leecher đã kịp lấy đủ mảnh hiếm chưa?
  - Rarest-First xin mảnh hiếm TRƯỚC  -> kịp lấy hết -> tải xong 100%.
  - Random / Sequential chỉ lấy mảnh hiếm khi tình cờ tới lượt -> mất một phần
    -> kẹt dưới 100% vì không còn ai có các mảnh đó.

Chạy (ở thư mục gốc dự án):   python tests/experiment_piece_selection.py
Kết quả: tests/experiment_results/ (2 đồ thị PNG + 1 file CSV)
Cần matplotlib (chỉ file thí nghiệm này dùng):  pip install matplotlib

Tên file KHÔNG bắt đầu bằng "test_" để `python -m unittest discover tests`
không tự chạy thí nghiệm này (chậm, mở nhiều cổng).
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

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from peer.peer_node import PeerNode  # noqa: E402
from peer.transfer_engine import TransferEngine  # noqa: E402
from storage.metainfo import create_metainfo  # noqa: E402
from storage.piece_manager import PieceManager  # noqa: E402
from tracker.tracker_server import TrackerServer  # noqa: E402

# ═══════════════════ THAM SỐ THÍ NGHIỆM (chỉnh ở đây) ═══════════════════ #
# File dùng để thí nghiệm. None = tự tạo file ngẫu nhiên FILE_SIZE_MB MB.
FILE_PATH = None
FILE_SIZE_MB = 32
PIECE_LENGTH = 512 * 1024

NUM_SOURCES = 4            # số peer nguồn (nguồn 0 = seeder gốc, sẽ rời mạng)
RARE_FRACTION = 0.10       # tỷ lệ mảnh chỉ seeder gốc có
COMMON_SHARE = 0.6         # mỗi nguồn phụ giữ mỗi mảnh thường với xác suất này
SOURCE_RATE_MB = 2.0       # giới hạn upload mỗi nguồn (MB/s)
# Seeder gốc rời mạng sau LEAVE_AT giây kể từ lúc leecher bắt đầu tải.
# None = tự tính: đủ thời gian để gửi hết mảnh hiếm một lần + 40% dư.
LEAVE_AT = None

STRATEGIES = ["rarest_first", "random", "sequential"]
RUNS = 3                   # số lần chạy mỗi chiến lược (lấy trung bình)
STALL_TIMEOUT = 4.0        # dừng khi không nhận thêm mảnh nào trong ngần này giây
MAX_SECONDS = 120          # giới hạn thời gian mỗi lần chạy
SEED = 2026                # cố định cách chia mảnh để các chiến lược công bằng
# ════════════════════════════════════════════════════════════════════════ #

LABELS = {"rarest_first": "Rarest-First", "random": "Random", "sequential": "Sequential"}
COLORS = {"rarest_first": "#2a78d6", "random": "#eb6834", "sequential": "#1baf7a"}
OUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "experiment_results")


def free_port() -> int:
    """Xin hệ điều hành một cổng TCP trống để các lần chạy không đụng cổng nhau."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def assign_pieces(num_pieces: int, rng: random.Random):
    """Chia mảnh cho các nguồn. Trả về (danh sách tập mảnh của từng nguồn, tập mảnh hiếm)."""
    rare = set(rng.sample(range(num_pieces), max(1, int(num_pieces * RARE_FRACTION))))
    holdings = [set(range(num_pieces))]          # nguồn 0: seeder gốc có đủ file
    holdings += [set() for _ in range(NUM_SOURCES - 1)]
    for i in range(num_pieces):
        if i in rare:
            continue                              # mảnh hiếm: chỉ seeder gốc có
        owners = [k for k in range(1, NUM_SOURCES) if rng.random() < COMMON_SHARE]
        if not owners:                            # mảnh thường luôn có >= 1 nguồn phụ
            owners = [rng.randrange(1, NUM_SOURCES)]
        for k in owners:
            holdings[k].add(i)
    return holdings, rare


def run_once(strategy, source_file, meta, holdings, rare, leave_at, work_dir, run=0):
    """Chạy 1 lần tải thật. Trả về dict kết quả + danh sách (giây, % hoàn thành)."""
    num_pieces = len(meta["info"]["pieces"])
    tracker_port = free_port()
    tracker = TrackerServer(host="127.0.0.1", port=tracker_port, peer_timeout=60.0)
    tracker.start()
    time.sleep(0.1)

    # ── Khởi động các nguồn ──
    sources = []
    for k, pieces in enumerate(holdings):
        peer_id = "seeder_goc" if k == 0 else f"nguon_{k}"
        port = free_port()
        pm = PieceManager(source_file, meta)
        pm.bitfield = [i in pieces for i in range(num_pieces)]   # chỉ giữ một phần file
        node = PeerNode(peer_id, "127.0.0.1", port, pm,
                        upload_rate_limit=SOURCE_RATE_MB * 1024 * 1024)
        node.start_listening()
        announcer = TransferEngine(peer_id, "127.0.0.1", tracker_port, port, pm)
        announcer._announce_to_tracker(event="started")
        sources.append((node, announcer, pm))

    # ── Leecher ──
    # Tên file riêng cho mỗi lần chạy: nếu dùng lại, engine sẽ "resume" từ file
    # .progress.json của lần trước và cho kết quả sai.
    out_path = os.path.join(work_dir, f"download_{strategy}_{run}.bin")
    leecher_pm = PieceManager(out_path, meta)
    leecher_port = free_port()
    engine = TransferEngine("leecher", "127.0.0.1", tracker_port, leecher_port,
                            leecher_pm, max_workers=4, strategy=strategy)

    # Bọc write_piece để ghi lại thời điểm nhận từng mảnh hợp lệ.
    timeline = []
    t0 = [0.0]
    original_write = leecher_pm.write_piece

    def timed_write(index, data):
        ok = original_write(index, data)
        if ok:
            timeline.append((time.monotonic() - t0[0], index))
        return ok

    leecher_pm.write_piece = timed_write

    t0[0] = time.monotonic()
    download = threading.Thread(target=engine.start_download, daemon=True)
    download.start()

    # ── Theo dõi: cho seeder gốc rời mạng ở giây leave_at, dừng khi xong / bị kẹt ──
    left = False
    while True:
        time.sleep(0.05)
        now = time.monotonic() - t0[0]
        if not left and now >= leave_at:
            node, announcer, _ = sources[0]
            announcer._announce_to_tracker(event="stopped")
            node.stop()
            left = True
        if leecher_pm.is_complete():
            break
        last_piece_at = timeline[-1][0] if timeline else 0.0
        if left and now - max(last_piece_at, leave_at) > STALL_TIMEOUT:
            break
        if now > MAX_SECONDS:
            break

    got = set(i for _, i in timeline)
    result = {
        "strategy": strategy,
        "complete": leecher_pm.is_complete(),
        "pct_done": 100.0 * len(got) / num_pieces,
        "rare_got": len(got & rare),
        "rare_total": len(rare),
        "finish_time": timeline[-1][0] if leecher_pm.is_complete() else None,
    }

    # ── Dọn dẹp ──
    engine.stop()
    for node, _, pm in sources:
        node.stop()
        pm.close()
    tracker.stop()
    download.join(timeout=3)
    time.sleep(0.5)  # chờ các thread kết nối in nốt log lỗi đóng socket (đang bị ẩn)

    # Xóa file vừa tải ngay sau mỗi lần chạy, nếu không ổ đĩa sẽ đầy khi dùng file lớn
    # (9 lần chạy × kích thước file). Trên Windows không xóa được file còn đang mở:
    # một worker có thể vừa ghi nốt 1 mảnh và mở lại file, nên đóng rồi thử lại vài lần.
    for path in (out_path, out_path + ".progress.json"):
        for _ in range(20):
            try:
                leecher_pm.close()
                if os.path.exists(path):
                    os.remove(path)
                break
            except OSError:
                time.sleep(0.5)

    progress = [(t, 100.0 * (n + 1) / num_pieces) for n, (t, _) in enumerate(timeline)]
    return result, progress


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    work_dir = tempfile.mkdtemp(prefix="p2p_experiment_")
    try:
        # ── Chuẩn bị file + metadata ──
        source_file = FILE_PATH
        if source_file is None:
            source_file = os.path.join(work_dir, "experiment_file.bin")
            with open(source_file, "wb") as f:
                f.write(os.urandom(FILE_SIZE_MB * 1024 * 1024))
        meta = create_metainfo(source_file, piece_length=PIECE_LENGTH)
        num_pieces = len(meta["info"]["pieces"])
        holdings, rare = assign_pieces(num_pieces, random.Random(SEED))

        rare_bytes = len(rare) * PIECE_LENGTH
        leave_at = LEAVE_AT or round(1.4 * rare_bytes / (SOURCE_RATE_MB * 1024 * 1024) + 0.5, 1)
        size_mb = os.path.getsize(source_file) / 1024 / 1024

        print(f"File: {os.path.basename(source_file)} ({size_mb:.1f} MB, {num_pieces} mảnh)")
        print(f"{NUM_SOURCES} nguồn, mỗi nguồn {SOURCE_RATE_MB} MB/s · "
              f"{len(rare)} mảnh hiếm chỉ seeder gốc có · seeder gốc rời mạng ở giây {leave_at}")
        for k, h in enumerate(holdings):
            print(f"  {'seeder_goc' if k == 0 else f'nguon_{k}':<11} giữ {len(h):>3}/{num_pieces} mảnh")
        print()

        results, progresses = [], {}
        for run in range(RUNS):
            for strategy in STRATEGIES:
                # Ẩn log của các thread (rất nhiều dòng) trong lúc chạy.
                with contextlib.redirect_stdout(io.StringIO()):
                    result, progress = run_once(strategy, source_file, meta, holdings,
                                                rare, leave_at, work_dir, run)
                result["run"] = run + 1
                results.append(result)
                progresses.setdefault(strategy, progress)   # đồ thị tiến độ: lần chạy đầu
                status = (f"xong sau {result['finish_time']:.1f}s" if result["complete"]
                          else f"kẹt ở {result['pct_done']:.0f}%")
                print(f"Lần {run + 1} · {LABELS[strategy]:<12} {status:<16} "
                      f"mảnh hiếm lấy được {result['rare_got']}/{result['rare_total']}")

        write_csv(results)
        summary = summarize(results)
        print("\nTrung bình:")
        for s in STRATEGIES:
            row = summary[s]
            print(f"  {LABELS[s]:<12} hoàn thành {row['pct_done']:5.1f}% · "
                  f"mảnh hiếm {row['rare_got']:.1f}/{len(rare)} · "
                  f"tải xong {row['complete_runs']}/{RUNS} lần")
        plot(progresses, summary, leave_at, len(rare), num_pieces, size_mb)
        print(f"\nĐã lưu đồ thị và CSV vào: {OUT_DIR}")
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)


def summarize(results):
    summary = {}
    for s in STRATEGIES:
        rows = [r for r in results if r["strategy"] == s]
        summary[s] = {
            "pct_done": statistics.mean(r["pct_done"] for r in rows),
            "rare_got": statistics.mean(r["rare_got"] for r in rows),
            "complete_runs": sum(r["complete"] for r in rows),
        }
    return summary


def write_csv(results):
    path = os.path.join(OUT_DIR, "results.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["run", "strategy", "complete", "pct_done",
                                          "rare_got", "rare_total", "finish_time"])
        w.writeheader()
        for r in results:
            w.writerow({k: r[k] for k in w.fieldnames})


def plot(progresses, summary, leave_at, rare_total, num_pieces, size_mb):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    ink, quiet, grid = "#0b0b0b", "#52514e", "#e4e3df"
    plt.rcParams.update({
        "font.family": "DejaVu Sans", "font.size": 11, "axes.edgecolor": quiet,
        "axes.labelcolor": ink, "xtick.color": quiet, "ytick.color": quiet,
        "axes.spines.top": False, "axes.spines.right": False,
    })

    def frame(ax, title, subtitle):
        ax.set_title(title, loc="left", fontsize=14, fontweight="bold", color=ink, pad=26)
        ax.text(0, 1.02, subtitle, transform=ax.transAxes, fontsize=10.5, color=quiet)
        ax.grid(axis="y", color=grid, linewidth=0.8)
        ax.set_axisbelow(True)

    # 1. Tiến độ tải theo thời gian
    fig, ax = plt.subplots(figsize=(9, 5.2))
    for s in STRATEGIES:
        pts = progresses.get(s, [])
        if pts:
            xs = [0.0] + [t for t, _ in pts]
            ys = [0.0] + [p for _, p in pts]
            ax.step(xs, ys, where="post", color=COLORS[s], linewidth=2, label=LABELS[s])
    ax.axvline(leave_at, color=quiet, linewidth=1, linestyle="--")
    ax.text(leave_at + 0.15, 4, "seeder gốc rời mạng", fontsize=10, color=ink)
    frame(ax, "Chỉ Rarest-First tải xong khi seeder gốc rời mạng",
          f"% file đã tải theo thời gian · file {size_mb:.0f} MB, {num_pieces} mảnh, "
          f"{rare_total} mảnh chỉ seeder gốc có")
    ax.set_xlabel("Thời gian (giây)")
    ax.set_ylabel("% file đã tải")
    ax.set_ylim(0, 105)
    ax.set_xlim(left=0)
    ax.legend(frameon=False, loc="lower right")
    fig.tight_layout()
    fig.savefig(os.path.join(OUT_DIR, "1_progress_over_time.png"), dpi=160)
    plt.close(fig)

    # 2. Số mảnh hiếm lấy được trước khi seeder gốc rời mạng
    fig, ax = plt.subplots(figsize=(9, 5.2))
    names = [LABELS[s] for s in STRATEGIES]
    vals = [summary[s]["rare_got"] for s in STRATEGIES]
    bars = ax.bar(names, vals, width=0.5, color=[COLORS[s] for s in STRATEGIES],
                  edgecolor="white", linewidth=2)
    for b, s, v in zip(bars, STRATEGIES, vals):
        ax.text(b.get_x() + b.get_width() / 2, v + rare_total * 0.03,
                f"{v:.1f}/{rare_total}", ha="center", fontsize=12, fontweight="bold", color=ink)
        ax.text(b.get_x() + b.get_width() / 2, v + rare_total * 0.10,
                f"tải xong {summary[s]['complete_runs']}/{RUNS} lần", ha="center",
                fontsize=10, color=quiet)
    ax.axhline(rare_total, color=quiet, linewidth=1, linestyle="--")
    frame(ax, "Rarest-First lấy đủ mảnh hiếm trước khi seeder gốc rời đi",
          f"Số mảnh hiếm leecher lấy được, trung bình {RUNS} lần chạy · "
          f"nét đứt = đủ {rare_total} mảnh")
    ax.set_ylabel("Số mảnh hiếm lấy được")
    ax.set_ylim(0, rare_total * 1.25)
    fig.tight_layout()
    fig.savefig(os.path.join(OUT_DIR, "2_rare_pieces_obtained.png"), dpi=160)
    plt.close(fig)


if __name__ == "__main__":
    main()
