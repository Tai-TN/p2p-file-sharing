"""
run_app.py — Ứng dụng desktop (Tkinter) để demo hệ thống P2P.

Chạy:  python run_app.py
Không cần cài thêm thư viện (Tkinter có sẵn trong Python trên Windows).

File này chỉ là lớp giao diện bọc ngoài P2PClient / TrackerServer,
không thay đổi logic lõi. Các bảng chỉ ĐỌC trạng thái để hiển thị.
"""
import os
import queue
import subprocess
import sys
import threading
import time
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
from tkinter.scrolledtext import ScrolledText

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

from peer.client import P2PClient
from tracker.tracker_server import TrackerServer

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SHARED_DIR = os.path.join(BASE_DIR, "demo_workspace", "shared")
DOWNLOAD_DIR = os.path.join(BASE_DIR, "demo_workspace", "downloads")

# ── Màu sắc ──────────────────────────────────────────────────────────────── #
BG = "#f4f6fa"
CARD = "#ffffff"
ACCENT = "#2563eb"
TEXT = "#1f2937"
MUTED = "#6b7280"
GREEN = "#16a34a"
AMBER = "#f59e0b"
RED = "#dc2626"
GRAY = "#d1d5db"


class _StdoutTee:
    """Chép mọi dòng print() của chương trình vào ô Nhật ký (thread-safe)."""

    def __init__(self, original, sink: "queue.Queue[str]"):
        self.original = original
        self.sink = sink

    def write(self, text):
        if self.original is not None:
            try:
                self.original.write(text)
            except Exception:
                pass
        if text:
            self.sink.put(text)

    def flush(self):
        if self.original is not None:
            try:
                self.original.flush()
            except Exception:
                pass


def _fmt_bytes(n: float) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024 or unit == "GB":
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} GB"


def _fmt_rate(bps: float) -> str:
    return "—" if bps <= 0 else f"{_fmt_bytes(bps)}/s"


def _text_bar(pct: float, width: int = 12) -> str:
    filled = int(round(pct / 100 * width))
    return "█" * filled + "░" * (width - filled) + f"  {pct:.1f}%"


def _open_folder(path: str) -> None:
    os.makedirs(path, exist_ok=True)
    try:
        if sys.platform == "win32":
            os.startfile(path)  # type: ignore[attr-defined]
        elif sys.platform == "darwin":
            subprocess.Popen(["open", path])
        else:
            subprocess.Popen(["xdg-open", path])
    except Exception as exc:
        messagebox.showerror("Lỗi", f"Không mở được thư mục:\n{exc}")


class P2PApp(tk.Tk):
    REFRESH_MS = 1000

    def __init__(self):
        super().__init__()
        self.title("PBL4 — P2P File Sharing")
        self.geometry("1100x720")
        self.minsize(900, 600)
        self.configure(bg=BG)

        self.client: P2PClient | None = None
        self.tracker: TrackerServer | None = None
        self._ui_calls: "queue.Queue" = queue.Queue()     # hàm cần chạy trên luồng UI
        self._log_queue: "queue.Queue[str]" = queue.Queue()
        self._speed: dict = {}                             # info_hash -> (t, bytes, rate)
        self._grid_cache = None                            # (key, [rect_ids], [colors])

        sys.stdout = _StdoutTee(sys.stdout, self._log_queue)
        sys.stderr = _StdoutTee(sys.stderr, self._log_queue)

        self._setup_style()
        self._build_settings()
        self._build_toolbar()
        self._build_files()
        self._build_tabs()
        self._set_running(False)

        os.makedirs(SHARED_DIR, exist_ok=True)
        os.makedirs(DOWNLOAD_DIR, exist_ok=True)
        self.protocol("WM_DELETE_WINDOW", self._on_close)
        self.after(150, self._pump)
        self.after(self.REFRESH_MS, self._refresh)

    # ── Giao diện ──────────────────────────────────────────────────────── #

    def _setup_style(self):
        style = ttk.Style(self)
        try:
            style.theme_use("clam")
        except tk.TclError:
            pass
        base_font = ("Segoe UI", 10)
        style.configure(".", background=BG, foreground=TEXT, font=base_font)
        style.configure("Card.TFrame", background=CARD)
        style.configure("Card.TLabel", background=CARD, foreground=TEXT)
        style.configure("Muted.TLabel", background=CARD, foreground=MUTED)
        style.configure("Title.TLabel", background=BG, foreground=TEXT,
                        font=("Segoe UI Semibold", 15))
        style.configure("Accent.TButton", background=ACCENT, foreground="white",
                        borderwidth=0, padding=(14, 6))
        style.map("Accent.TButton",
                  background=[("disabled", GRAY), ("active", "#1d4ed8")])
        style.configure("TButton", padding=(12, 6))
        style.configure("Treeview", rowheight=26, background=CARD,
                        fieldbackground=CARD, borderwidth=0)
        style.configure("Treeview.Heading", font=("Segoe UI Semibold", 10),
                        background="#e5e7eb", relief="flat")
        style.map("Treeview", background=[("selected", "#dbeafe")],
                  foreground=[("selected", TEXT)])
        style.configure("TNotebook", background=BG, borderwidth=0)
        style.configure("TNotebook.Tab", padding=(14, 6))

    def _build_settings(self):
        header = ttk.Frame(self)
        header.pack(fill="x", padx=14, pady=(12, 4))
        ttk.Label(header, text="P2P File Sharing", style="Title.TLabel").pack(side="left")
        self.status_dot = tk.Label(header, text="●", fg=RED, bg=BG, font=("Segoe UI", 14))
        self.status_dot.pack(side="right")
        self.status_lbl = ttk.Label(header, text="Chưa khởi động", foreground=MUTED)
        self.status_lbl.pack(side="right", padx=4)

        card = ttk.Frame(self, style="Card.TFrame", padding=10)
        card.pack(fill="x", padx=14, pady=4)

        self.var_port = tk.StringVar(value="6881")
        self.var_tracker = tk.StringVar(value="127.0.0.1:8000")
        self.var_transport = tk.StringVar(value="auto")
        self.var_secret = tk.StringVar(value=os.environ.get("P2P_TRACKER_SECRET", ""))
        self.var_run_tracker = tk.BooleanVar(value=False)

        fields = [
            ("Cổng peer", ttk.Entry(card, textvariable=self.var_port, width=8)),
            ("Tracker (host:port)", ttk.Entry(card, textvariable=self.var_tracker, width=22)),
            ("Kết nối", ttk.Combobox(card, textvariable=self.var_transport, width=6,
                                     values=("auto", "tcp", "udp"), state="readonly")),
            ("Mật khẩu chung", ttk.Entry(card, textvariable=self.var_secret, width=14, show="•")),
        ]
        self._setting_widgets = []
        for col, (label, widget) in enumerate(fields):
            ttk.Label(card, text=label, style="Muted.TLabel").grid(row=0, column=col, sticky="w", padx=6)
            widget.grid(row=1, column=col, sticky="w", padx=6)
            self._setting_widgets.append(widget)

        chk = ttk.Checkbutton(card, text="Chạy tracker trên máy này", variable=self.var_run_tracker)
        chk.grid(row=1, column=len(fields), padx=10)
        self._setting_widgets.append(chk)

        self.btn_start = ttk.Button(card, text="Khởi động", style="Accent.TButton",
                                    command=self._start)
        self.btn_start.grid(row=1, column=len(fields) + 1, padx=6)

        self.info_lbl = ttk.Label(card, text="", style="Muted.TLabel")
        self.info_lbl.grid(row=2, column=0, columnspan=len(fields) + 2, sticky="w", padx=6, pady=(8, 0))

    def _build_toolbar(self):
        bar = ttk.Frame(self)
        bar.pack(fill="x", padx=14, pady=(8, 4))
        self.btn_share = ttk.Button(bar, text="⬆  Chia sẻ file", style="Accent.TButton",
                                    command=self._share)
        self.btn_share.pack(side="left")
        self.btn_download = ttk.Button(bar, text="⬇  Tải file (.meta.json)", style="Accent.TButton",
                                       command=self._download)
        self.btn_download.pack(side="left", padx=8)
        ttk.Button(bar, text="Thư mục metadata", command=lambda: _open_folder(SHARED_DIR)).pack(side="left")
        ttk.Button(bar, text="Thư mục tải về", command=lambda: _open_folder(DOWNLOAD_DIR)).pack(side="left", padx=8)

    def _build_files(self):
        card = ttk.Frame(self, style="Card.TFrame", padding=6)
        card.pack(fill="both", expand=False, padx=14, pady=4)
        cols = ("name", "type", "progress", "pieces", "speed", "path", "status")
        heads = ("Tên file", "Vai trò", "Tiến độ", "Mảnh", "Tốc độ tải", "Đường đi", "Trạng thái")
        widths = (220, 80, 200, 80, 100, 100, 220)
        self.files = ttk.Treeview(card, columns=cols, show="headings", height=6, selectmode="browse")
        for c, h, w in zip(cols, heads, widths):
            self.files.heading(c, text=h)
            self.files.column(c, width=w, anchor="w", stretch=(c in ("name", "status")))
        self.files.tag_configure("done", foreground=GREEN)
        self.files.tag_configure("error", foreground=RED)
        self.files.pack(fill="both", expand=True)
        self.files.bind("<<TreeviewSelect>>", lambda _e: self._refresh_tabs())

    def _build_tabs(self):
        nb = ttk.Notebook(self)
        nb.pack(fill="both", expand=True, padx=14, pady=(4, 12))
        self.tabs = nb

        # Tab 1: bản đồ mảnh
        pieces = ttk.Frame(nb, style="Card.TFrame", padding=8)
        nb.add(pieces, text="Bản đồ mảnh")
        legend = ttk.Frame(pieces, style="Card.TFrame")
        legend.pack(fill="x")
        for color, label in ((GREEN, "Đã có"), (AMBER, "Đang tải"), (GRAY, "Chưa có")):
            tk.Label(legend, text="■", fg=color, bg=CARD, font=("Segoe UI", 13)).pack(side="left")
            ttk.Label(legend, text=label, style="Card.TLabel").pack(side="left", padx=(0, 14))
        self.grid_lbl = ttk.Label(legend, text="Chọn một file ở bảng trên", style="Muted.TLabel")
        self.grid_lbl.pack(side="right")
        self.canvas = tk.Canvas(pieces, bg=CARD, highlightthickness=0)
        self.canvas.pack(fill="both", expand=True, pady=(6, 0))
        self.canvas.bind("<Configure>", lambda _e: self._invalidate_grid())

        # Tab 2: peer & tit-for-tat
        peers = ttk.Frame(nb, style="Card.TFrame", padding=8)
        nb.add(peers, text="Peer & Tit-for-Tat")
        self.choke_lbl = ttk.Label(peers, text="", style="Muted.TLabel")
        self.choke_lbl.pack(fill="x", pady=(0, 6))
        cols = ("peer", "state", "interested", "up_rate", "down_rate", "up_total", "down_total")
        heads = ("Peer", "Trạng thái upload", "Muốn tải", "Ta gửi", "Ta nhận", "Tổng đã gửi", "Tổng đã nhận")
        self.peers = ttk.Treeview(peers, columns=cols, show="headings", height=8)
        for c, h in zip(cols, heads):
            self.peers.heading(c, text=h)
            self.peers.column(c, width=130 if c != "peer" else 220, anchor="w")
        self.peers.tag_configure("unchoke", foreground=GREEN)
        self.peers.tag_configure("optimistic", foreground=ACCENT)
        self.peers.tag_configure("choke", foreground=MUTED)
        self.peers.pack(fill="both", expand=True)

        # Tab 3: nhật ký
        logs = ttk.Frame(nb, style="Card.TFrame", padding=4)
        nb.add(logs, text="Nhật ký")
        self.log = ScrolledText(logs, height=10, font=("Consolas", 9), bg="#111827",
                                fg="#e5e7eb", insertbackground="white", borderwidth=0)
        self.log.pack(fill="both", expand=True)
        self.log.configure(state="disabled")

    # ── Điều khiển ─────────────────────────────────────────────────────── #

    def _set_running(self, running: bool):
        state = "disabled" if running else "normal"
        for w in self._setting_widgets:
            try:
                w.configure(state="readonly" if (not running and isinstance(w, ttk.Combobox)) else state)
            except tk.TclError:
                pass
        self.btn_start.configure(state=state)
        for b in (self.btn_share, self.btn_download):
            b.configure(state="normal" if running else "disabled")

    def _call_ui(self, fn, *args):
        """Cho luồng nền yêu cầu cập nhật giao diện một cách an toàn."""
        self._ui_calls.put((fn, args))

    def _run_bg(self, fn, on_error_title: str):
        def worker():
            try:
                fn()
            except Exception as exc:
                self._call_ui(messagebox.showerror, on_error_title, str(exc))
        threading.Thread(target=worker, daemon=True).start()

    def _start(self):
        try:
            port = int(self.var_port.get())
            assert 1 <= port <= 65535
        except Exception:
            messagebox.showerror("Sai cấu hình", "Cổng peer phải là số 1–65535")
            return
        host, _, tport = self.var_tracker.get().strip().partition(":")
        try:
            tracker_port = int(tport) if tport else 8000
        except ValueError:
            messagebox.showerror("Sai cấu hình", "Tracker phải có dạng host hoặc host:port")
            return
        host = host or "127.0.0.1"
        secret = self.var_secret.get().strip()
        if secret:
            os.environ["P2P_TRACKER_SECRET"] = secret
        else:
            os.environ.pop("P2P_TRACKER_SECRET", None)
        run_tracker = self.var_run_tracker.get()
        transport = self.var_transport.get()

        self._set_running(True)
        self.status_lbl.configure(text="Đang khởi động (kiểm tra NAT qua STUN)…")
        self.status_dot.configure(fg=AMBER)

        def worker():
            try:
                if run_tracker:
                    self.tracker = TrackerServer(host="0.0.0.0", port=tracker_port, peer_timeout=120.0)
                    self.tracker.start()
                client = P2PClient(listen_port=port, tracker_host=host,
                                   tracker_port=tracker_port, transport=transport)
                client.start()
                self.client = client
                self._call_ui(self._on_started)
            except Exception as exc:
                if self.tracker:
                    self.tracker.stop()
                    self.tracker = None
                self._call_ui(self._on_start_failed, str(exc))

        threading.Thread(target=worker, daemon=True).start()

    def _on_started(self):
        c = self.client
        nat = c.nat_info
        public = f"{nat.public_ip}:{nat.public_port}" if nat else "không lấy được (STUN lỗi)"
        extra = "  |  Tracker đang chạy trên máy này" if self.tracker else ""
        self.info_lbl.configure(
            text=f"Peer ID: {c.peer_id}   |   IP LAN: {c.my_ip}:{c.listen_port}   |   "
                 f"IP công khai (UDP): {public}{extra}")
        self.title(f"PBL4 — P2P File Sharing  [{c.peer_id}]")

    def _on_start_failed(self, err: str):
        self._set_running(False)
        self.status_lbl.configure(text="Khởi động thất bại")
        self.status_dot.configure(fg=RED)
        messagebox.showerror("Không khởi động được", err)

    def _share(self):
        if not self.client:
            return
        src = filedialog.askopenfilename(title="Chọn file để chia sẻ")
        if not src:
            return
        meta = os.path.join(SHARED_DIR, os.path.basename(src) + ".meta.json")
        client = self.client

        def job():
            print(f"[App] Đang chuẩn bị chia sẻ {os.path.basename(src)} (tính SHA-256)…")
            client.share_file(src, meta)
            self._call_ui(messagebox.showinfo, "Đang chia sẻ",
                          f"Đã chia sẻ: {os.path.basename(src)}\n\n"
                          f"Gửi file metadata này cho máy muốn tải:\n{meta}")
        self._run_bg(job, "Không chia sẻ được")

    def _download(self):
        if not self.client:
            return
        meta = filedialog.askopenfilename(title="Chọn file .meta.json", initialdir=SHARED_DIR,
                                          filetypes=[("Metadata", "*.meta.json"), ("JSON", "*.json")])
        if not meta:
            return
        client = self.client
        self._run_bg(lambda: client.download_file(meta, DOWNLOAD_DIR), "Không tải được")

    def _on_close(self):
        if not self.client and not self.tracker:
            self.destroy()
            return
        self.status_lbl.configure(text="Đang dừng…")
        self.update_idletasks()

        def stop():
            try:
                if self.client:
                    self.client.stop()
                if self.tracker:
                    self.tracker.stop()
            finally:
                self._call_ui(self.destroy)
        threading.Thread(target=stop, daemon=True).start()

    # ── Cập nhật định kỳ ──────────────────────────────────────────────── #

    def _pump(self):
        """Chạy các yêu cầu từ luồng nền và đổ log vào ô Nhật ký."""
        while True:
            try:
                fn, args = self._ui_calls.get_nowait()
            except queue.Empty:
                break
            try:
                fn(*args)
            except tk.TclError:
                return
        chunks = []
        while True:
            try:
                chunks.append(self._log_queue.get_nowait())
            except queue.Empty:
                break
        if chunks:
            self.log.configure(state="normal")
            self.log.insert("end", "".join(chunks))
            lines = int(self.log.index("end-1c").split(".")[0])
            if lines > 3000:
                self.log.delete("1.0", f"{lines - 3000}.0")
            self.log.see("end")
            self.log.configure(state="disabled")
        self.after(150, self._pump)

    def _snapshot_tasks(self):
        c = self.client
        if not c:
            return []
        with c._lock:
            return list(c._tasks.items())

    def _refresh(self):
        try:
            self._refresh_header()
            self._refresh_files()
            self._refresh_tabs()
        except Exception as exc:  # không để lỗi hiển thị làm chết vòng cập nhật
            print(f"[App] Lỗi cập nhật giao diện: {exc}")
        self.after(self.REFRESH_MS, self._refresh)

    def _refresh_header(self):
        c = self.client
        if not c:
            return
        ch = getattr(c, "_signal_channel", None)
        ok = bool(ch and ch.is_connected)
        self.status_dot.configure(fg=GREEN if ok else AMBER)
        self.status_lbl.configure(text="Đã kết nối tracker" if ok
                                  else "Chưa kết nối được tracker — đang thử lại…")

    def _refresh_files(self):
        now = time.monotonic()
        seen = set()
        for info_hash, task in self._snapshot_tasks():
            pm, engine = task["pm"], task["engine"]
            seen.add(info_hash)
            done_bytes = min(pm.completed_pieces_count() * pm.piece_length, pm.file_size)
            prev = self._speed.get(info_hash)
            rate = 0.0
            if prev:
                dt = now - prev[0]
                inst = max(0.0, (done_bytes - prev[1]) / dt) if dt > 0 else 0.0
                rate = 0.6 * prev[2] + 0.4 * inst
            self._speed[info_hash] = (now, done_bytes, rate)

            pct = pm.progress_percentage()
            is_download = task["type"] == "Download"
            status = task["status"]
            path = getattr(engine, "current_path", None) or ("—" if is_download else "")
            values = (
                task["name"],
                "Tải về" if is_download else "Chia sẻ",
                _text_bar(pct),
                f"{pm.completed_pieces_count()}/{pm.num_pieces}",
                _fmt_rate(rate) if is_download and pct < 100 else "—",
                path if is_download else "—",
                status,
            )
            tag = ("done",) if pct >= 100 else ("error",) if status.startswith(("Lỗi", "Thất bại")) else ()
            if self.files.exists(info_hash):
                self.files.item(info_hash, values=values, tags=tag)
            else:
                self.files.insert("", "end", iid=info_hash, values=values, tags=tag)
                if len(self.files.get_children()) == 1:
                    self.files.selection_set(info_hash)
        for iid in self.files.get_children():
            if iid not in seen:
                self.files.delete(iid)

    def _refresh_tabs(self):
        self._refresh_grid()
        self._refresh_peers()

    # Bản đồ mảnh -------------------------------------------------------- #

    def _invalidate_grid(self):
        self._grid_cache = None
        self._refresh_grid()

    def _selected_task(self):
        sel = self.files.selection()
        if not sel:
            return None, None
        for info_hash, task in self._snapshot_tasks():
            if info_hash == sel[0]:
                return info_hash, task
        return None, None

    def _refresh_grid(self):
        info_hash, task = self._selected_task()
        if not task:
            self.canvas.delete("all")
            self._grid_cache = None
            self.grid_lbl.configure(text="Chọn một file ở bảng trên")
            return
        pm, engine = task["pm"], task["engine"]
        bitfield = list(pm.bitfield)
        in_flight = set(getattr(engine, "in_flight_pieces", set()) or set())
        n = len(bitfield)
        colors = [GREEN if have else (AMBER if i in in_flight else GRAY)
                  for i, have in enumerate(bitfield)]

        width = max(self.canvas.winfo_width(), 100)
        height = max(self.canvas.winfo_height(), 60)
        key = (info_hash, n, width, height)
        if not self._grid_cache or self._grid_cache[0] != key:
            self.canvas.delete("all")
            gap = 2
            # Chọn cỡ ô lớn nhất sao cho n ô vừa khung
            size = 28
            while size > 4:
                cols = max(1, (width + gap) // (size + gap))
                rows = -(-n // cols)
                if rows * (size + gap) <= height:
                    break
                size -= 1
            cols = max(1, (width + gap) // (size + gap))
            rects = []
            for i in range(n):
                r, c = divmod(i, cols)
                x, y = c * (size + gap), r * (size + gap)
                rects.append(self.canvas.create_rectangle(x, y, x + size, y + size,
                                                          fill=colors[i], width=0))
            self._grid_cache = (key, rects, colors)
        else:
            _, rects, old = self._grid_cache
            for i, color in enumerate(colors):
                if old[i] != color:
                    self.canvas.itemconfigure(rects[i], fill=color)
            self._grid_cache = (key, rects, colors)
        have = sum(bitfield)
        self.grid_lbl.configure(
            text=f"{task['name']}: {have}/{n} mảnh  ·  mỗi mảnh {_fmt_bytes(pm.piece_length)}")

    # Peer & Tit-for-Tat ------------------------------------------------- #

    def _refresh_peers(self):
        c = self.client
        if not c:
            return
        ch = c.node._choking
        now = time.monotonic()
        rows = []
        with ch._lock:
            unchoked = set(ch._unchoked)
            optimistic = set(ch._optimistic)
            interested = set(ch._interested)
            for pid, st in ch._stats.items():
                rows.append((pid, st.upload_rate(now, ch.measurement_window),
                             st.download_rate(now, ch.measurement_window),
                             st.bytes_uploaded_to_peer, st.bytes_downloaded_from_peer))
        self.choke_lbl.configure(
            text=f"Upload slot: {ch.regular_slots} thường (ưu tiên peer gửi cho ta nhiều nhất) + "
                 f"{ch.optimistic_slots} optimistic (chọn ngẫu nhiên)  ·  "
                 f"tính lại mỗi {ch.choking_interval:.0f}s  ·  đo tốc độ trong {ch.measurement_window:.0f}s gần nhất")
        rows.sort(key=lambda r: (r[0] not in unchoked, -r[2]))
        seen = set()
        for pid, up, down, up_total, down_total in rows:
            seen.add(pid)
            if pid in optimistic:
                state, tag = "UNCHOKE (optimistic)", "optimistic"
            elif pid in unchoked:
                state, tag = "UNCHOKE", "unchoke"
            else:
                state, tag = "CHOKE", "choke"
            values = (pid, state, "Có" if pid in interested else "Không",
                      _fmt_rate(up), _fmt_rate(down), _fmt_bytes(up_total), _fmt_bytes(down_total))
            if self.peers.exists(pid):
                self.peers.item(pid, values=values, tags=(tag,))
            else:
                self.peers.insert("", "end", iid=pid, values=values, tags=(tag,))
        for iid in self.peers.get_children():
            if iid not in seen:
                self.peers.delete(iid)
        for index, (pid, *_rest) in enumerate(rows):
            self.peers.move(pid, "", index)


if __name__ == "__main__":
    P2PApp().mainloop()