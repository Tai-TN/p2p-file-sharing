"""
Mô phỏng swarm để so sánh 3 chiến lược chọn mảnh: rarest_first, random, sequential.

Mô phỏng dùng ĐÚNG hàm choose_next_piece() trong peer/selection.py, nên kết quả
phản ánh thuật toán thật của dự án, chỉ thay phần mạng bằng mô hình theo "vòng".

Mô hình (rút gọn từ BitTorrent, giống cách các bài báo đánh giá rarest-first):
  - 1 vòng = thời gian gửi xong 1 mảnh qua 1 slot upload.
  - 1 seeder có đủ file, N leecher bắt đầu cùng lúc với 0 mảnh (flash crowd).
  - Seeder nối với mọi leecher; mỗi leecher nối ngẫu nhiên với vài leecher khác.
  - Mỗi vòng: mỗi leecher có D slot tải. Với mỗi slot, nó gọi choose_next_piece()
    trên bitfield của các hàng xóm, rồi xin mảnh đó từ một hàng xóm còn slot upload.
  - Mảnh nhận trong vòng t có thể được upload lại từ vòng t+1.
  - Leecher tải xong ở lại làm seeder.

Hai thí nghiệm:
  A. Seeder ở lại đến cuối  -> đo thời gian tải xong, và vòng mà MỌI mảnh đã có
     ít nhất 1 bản sao ngoài seeder (từ lúc đó seeder có thể rời mạng an toàn).
  B. Seeder rời mạng ở vòng T (quét T) -> đo % leecher vẫn tải xong được.

Chạy:  python benchmark/piece_selection_sim.py
Kết quả: benchmark/results/*.png và *.csv
"""
import os
import sys
import csv
import random
import statistics

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from peer.selection import choose_next_piece  # noqa: E402

STRATEGIES = ["rarest_first", "random", "sequential"]
LABELS = {"rarest_first": "Rarest-First", "random": "Random", "sequential": "Sequential"}

# ── Tham số mô phỏng ──
NUM_PIECES = 100          # 100 mảnh × 512 KB = 50 MB
NUM_LEECHERS = 20
NEIGHBORS = 5             # số leecher hàng xóm của mỗi leecher
SEED_UPLOAD_SLOTS = 4     # seeder gửi tối đa 4 mảnh / vòng
PEER_UPLOAD_SLOTS = 2     # mỗi leecher gửi tối đa 2 mảnh / vòng
DOWNLOAD_SLOTS = 2        # mỗi leecher tải tối đa 2 mảnh / vòng
MAX_ROUNDS = 400
RUNS = 30                 # lặp lại với 30 seed ngẫu nhiên rồi lấy trung bình
# Thí nghiệm B: seeder rời mạng ở vòng T. 25 vòng = vừa đủ gửi 1 bản file.
LEAVE_ROUNDS = list(range(25, 61, 5))
RUNS_B = 20

SEEDER = "seeder"


def build_topology(rng):
    """Seeder nối với mọi leecher; leecher nối ngẫu nhiên với nhau (đồ thị vô hướng)."""
    leechers = [f"L{i}" for i in range(NUM_LEECHERS)]
    nbrs = {p: set() for p in leechers}
    for p in leechers:
        while len(nbrs[p]) < NEIGHBORS:
            q = rng.choice(leechers)
            if q != p:
                nbrs[p].add(q)
                nbrs[q].add(p)
    for p in leechers:
        nbrs[p].add(SEEDER)
    # sorted(): thứ tự duyệt set thay đổi theo mỗi lần chạy Python -> cố định để
    # cùng seed luôn cho cùng kết quả.
    return leechers, {p: sorted(q) for p, q in nbrs.items()}


def simulate(strategy, seed, seeder_leave_round=None):
    rng = random.Random(seed)
    random.seed(seed)  # choose_next_piece dùng module random khi hòa
    leechers, nbrs = build_topology(rng)

    have = {p: [False] * NUM_PIECES for p in leechers}
    have[SEEDER] = [True] * NUM_PIECES
    online = set(leechers) | {SEEDER}
    finish_round = {}
    completed_curve = []   # số leecher đã xong sau mỗi vòng
    min_copies_curve = []  # số bản sao (trong leecher) của mảnh hiếm nhất
    lost_pieces = 0

    for rnd in range(1, MAX_ROUNDS + 1):
        if seeder_leave_round is not None and rnd == seeder_leave_round + 1:
            online.discard(SEEDER)

        upload_left = {p: (SEED_UPLOAD_SLOTS if p == SEEDER else PEER_UPLOAD_SLOTS)
                       for p in online}
        deliveries = []  # (người nhận, mảnh) — có hiệu lực cuối vòng

        order = [p for p in leechers if p not in finish_round]
        rng.shuffle(order)
        for p in order:
            requested = set()
            for _ in range(DOWNLOAD_SLOTS):
                peers_online = [q for q in nbrs[p] if q in online]
                # Rarest-first đếm độ hiếm trên MỌI hàng xóm (giống TransferEngine).
                peer_bitfields = {q: have[q] for q in peers_online}
                # Chỉ xét mảnh mà một hàng xóm còn slot upload đang có.
                free = [q for q in peers_online if upload_left[q] > 0]
                missing = [i for i in range(NUM_PIECES)
                           if not have[p][i] and i not in requested
                           and any(have[q][i] for q in free)]
                piece = choose_next_piece(missing, peer_bitfields, strategy=strategy)
                if piece is None:
                    break
                holders = [q for q in free if have[q][piece]]
                src = rng.choice(holders)
                upload_left[src] -= 1
                requested.add(piece)
                deliveries.append((p, piece))

        for p, piece in deliveries:
            have[p][piece] = True
        for p in leechers:
            if p not in finish_round and all(have[p]):
                finish_round[p] = rnd

        copies = [sum(have[p][i] for p in leechers) for i in range(NUM_PIECES)]
        completed_curve.append(len(finish_round))
        min_copies_curve.append(min(copies))

        if len(finish_round) == NUM_LEECHERS:
            break
        if SEEDER not in online and not deliveries:
            # Không còn ai có mảnh còn thiếu: swarm "chết", file không tải xong được.
            lost_pieces = sum(1 for c in copies if c == 0)
            break

    # Kéo dài đường cong tới MAX_ROUNDS để lấy trung bình theo vòng.
    while len(completed_curve) < MAX_ROUNDS:
        completed_curve.append(completed_curve[-1])
        min_copies_curve.append(min_copies_curve[-1])

    return {
        "finish": finish_round,
        "completed_curve": completed_curve,
        "min_copies_curve": min_copies_curve,
        "lost_pieces": lost_pieces,
    }


def main():
    out_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")
    os.makedirs(out_dir, exist_ok=True)

    exp_a = {s: [simulate(s, seed) for seed in range(RUNS)] for s in STRATEGIES}
    exp_b = {s: {t: statistics.mean(
                    len(simulate(s, seed, t)["finish"]) / NUM_LEECHERS * 100
                    for seed in range(RUNS_B))
                 for t in LEAVE_ROUNDS}
             for s in STRATEGIES}

    def safe_round(r):
        # Vòng đầu tiên mà mảnh hiếm nhất có >= 1 bản sao trong các leecher.
        return next(t + 1 for t, c in enumerate(r["min_copies_curve"]) if c >= 1)

    # ── Bảng tóm tắt ──
    summary = []
    for s in STRATEGIES:
        avg_finish = statistics.mean(
            statistics.mean(r["finish"].values()) for r in exp_a[s])
        last_finish = statistics.mean(max(r["finish"].values()) for r in exp_a[s])
        safe = statistics.mean(safe_round(r) for r in exp_a[s])
        row = {
            "strategy": LABELS[s],
            "avg_finish_round": round(avg_finish, 1),
            "last_finish_round": round(last_finish, 1),
            "all_pieces_out_of_seeder_round": round(safe, 1),
        }
        for t in LEAVE_ROUNDS:
            row[f"pct_done_seeder_leaves_r{t}"] = round(exp_b[s][t], 1)
        summary.append(row)

    with open(os.path.join(out_dir, "summary.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(summary[0].keys()))
        w.writeheader()
        w.writerows(summary)

    # Đường cong trung bình theo vòng (thí nghiệm A).
    horizon = max(max(r["finish"].values()) for s in STRATEGIES for r in exp_a[s]) + 5
    curves = {}
    with open(os.path.join(out_dir, "curves.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["round"] + [f"{LABELS[s]} completed" for s in STRATEGIES]
                   + [f"{LABELS[s]} min copies" for s in STRATEGIES])
        for s in STRATEGIES:
            curves[s] = (
                [statistics.mean(r["completed_curve"][t] for r in exp_a[s]) for t in range(horizon)],
                [statistics.mean(r["min_copies_curve"][t] for r in exp_a[s]) for t in range(horizon)],
            )
        for t in range(horizon):
            w.writerow([t + 1] + [round(curves[s][0][t], 2) for s in STRATEGIES]
                       + [round(curves[s][1][t], 2) for s in STRATEGIES])

    print(f"{'Chiến lược':<14}{'TB xong':>9}{'Người cuối':>12}{'Đủ mảnh ngoài seeder':>22}")
    for row in summary:
        print(f"{row['strategy']:<14}{row['avg_finish_round']:>9}{row['last_finish_round']:>12}"
              f"{row['all_pieces_out_of_seeder_round']:>22}")
    print("\n% leecher tải xong theo vòng seeder rời mạng:")
    print(f"{'Vòng rời':<10}" + "".join(f"{LABELS[s]:>14}" for s in STRATEGIES))
    for t in LEAVE_ROUNDS:
        print(f"{t:<10}" + "".join(f"{exp_b[s][t]:>13.0f}%" for s in STRATEGIES))

    plot(curves, summary, exp_b, horizon, out_dir)


def plot(curves, summary, exp_b, horizon, out_dir):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    colors = {"rarest_first": "#2a78d6", "random": "#eb6834", "sequential": "#1baf7a"}
    ink, quiet, grid = "#0b0b0b", "#52514e", "#e4e3df"
    plt.rcParams.update({
        "font.family": "DejaVu Sans", "font.size": 11,
        "axes.edgecolor": quiet, "axes.labelcolor": ink, "xtick.color": quiet,
        "ytick.color": quiet, "axes.spines.top": False, "axes.spines.right": False,
        "figure.facecolor": "white", "axes.facecolor": "white",
    })
    xs = list(range(1, horizon + 1))

    def finish(ax, title, subtitle, xlabel, ylabel):
        ax.set_title(title, loc="left", fontsize=14, fontweight="bold", color=ink, pad=26)
        ax.text(0, 1.02, subtitle, transform=ax.transAxes, fontsize=10.5, color=quiet)
        ax.set_xlabel(xlabel)
        ax.set_ylabel(ylabel)
        ax.grid(axis="y", color=grid, linewidth=0.8)
        ax.set_axisbelow(True)

    # 1. Số leecher đã tải xong theo thời gian
    fig, ax = plt.subplots(figsize=(9, 5.2))
    for s in STRATEGIES:
        ys = curves[s][0]
        ax.plot(xs, ys, color=colors[s], linewidth=2, label=LABELS[s])
    finish(ax, "Seeder ở lại: Sequential chậm gấp đôi hai cách còn lại",
           f"Số leecher đã tải xong · TB {RUNS} lần chạy, {NUM_LEECHERS} leecher, "
           f"{NUM_PIECES} mảnh, seeder gửi {SEED_UPLOAD_SLOTS} mảnh/vòng",
           "Thời gian (vòng; 1 vòng = thời gian gửi 1 mảnh)", "Số leecher đã tải xong")
    ax.set_ylim(0, NUM_LEECHERS + 0.5)
    ax.set_xlim(1, horizon)
    ax.set_yticks(range(0, NUM_LEECHERS + 1, 5))
    ax.legend(frameon=False, loc="center right")
    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, "1_completion_over_time.png"), dpi=160)
    plt.close(fig)

    # 2. Số bản sao của mảnh hiếm nhất
    fig, ax = plt.subplots(figsize=(9, 5.2))
    safe = {s: r["all_pieces_out_of_seeder_round"] for s, r in zip(STRATEGIES, summary)}
    for s in STRATEGIES:
        ys = curves[s][1]
        ax.plot(xs, ys, color=colors[s], linewidth=2, label=LABELS[s])
        if safe[s] <= horizon:
            ax.axvline(safe[s], color=colors[s], linewidth=1, linestyle="--")
            y_label = {"rarest_first": 1.0, "random": 3.0, "sequential": 1.0}[s]
            ax.text(safe[s] + 0.8, y_label, f"vòng {safe[s]:.0f}", fontsize=10, color=ink)
    finish(ax, "Rarest-First đưa đủ mọi mảnh ra khỏi seeder sớm nhất",
           "Số leecher giữ mảnh hiếm nhất · nét đứt = mọi mảnh đã có bản sao ngoài seeder",
           "Thời gian (vòng)", "Số bản sao của mảnh hiếm nhất")
    ax.set_ylim(0, NUM_LEECHERS + 0.5)
    ax.set_xlim(1, horizon)
    ax.set_yticks(range(0, NUM_LEECHERS + 1, 5))
    ax.legend(frameon=False, loc="center right")
    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, "2_rarest_piece_copies.png"), dpi=160)
    plt.close(fig)

    # 3. Seeder rời mạng ở vòng T
    fig, ax = plt.subplots(figsize=(9, 5.2))
    for s in STRATEGIES:
        ys = [exp_b[s][t] for t in LEAVE_ROUNDS]
        ax.plot(LEAVE_ROUNDS, ys, color=colors[s], linewidth=2, marker="o", markersize=6,
                markeredgecolor="white", markeredgewidth=1.5, label=LABELS[s])
    finish(ax, "Seeder có thể rời mạng sớm hơn khi dùng Rarest-First",
           f"% leecher vẫn tải xong · TB {RUNS_B} lần chạy mỗi điểm · "
           f"vòng {LEAVE_ROUNDS[0]} = seeder vừa gửi đủ 1 bản file",
           "Vòng seeder rời mạng (T)", "% leecher tải xong")
    ax.set_ylim(-3, 105)
    ax.set_xticks(LEAVE_ROUNDS)
    ax.legend(frameon=False, loc="center right")
    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, "3_seeder_leaves_early.png"), dpi=160)
    plt.close(fig)


if __name__ == "__main__":
    main()
