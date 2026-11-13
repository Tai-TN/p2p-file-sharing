"""
Cầu nối giữa giao diện (JavaScript) và P2PClient.

Mọi phương thức public của PeerApi được pywebview đưa sang JS dưới dạng
`window.pywebview.api.<tên>()`. Thuộc tính nội bộ phải bắt đầu bằng "_" để
pywebview không cố đưa chúng sang JS.
"""
import itertools
import json
import os
import subprocess
import sys
import threading
import time
from typing import Any, Dict, List, Optional

from app.config import AppConfig
from app.log_buffer import LogBuffer
from peer.client import P2PClient, STATE_PAUSED, check_tracker
from storage.metainfo import load_metainfo

try:  # pywebview chỉ cần khi chạy app thật; test có thể import module này mà không cần nó.
    import webview
except ImportError:  # pragma: no cover
    webview = None


def _ok(**data) -> Dict[str, Any]:
    return {"ok": True, **data}


def _err(message: str) -> Dict[str, Any]:
    return {"ok": False, "error": message}


class PeerApi:
    def __init__(self, config: AppConfig, log_buffer: LogBuffer):
        self._cfg = config
        self._log = log_buffer
        self._window = None
        self._lock = threading.Lock()
        self._jobs: Dict[str, Dict[str, Any]] = {}  # Các lượt chia sẻ đang tính SHA-256
        self._job_ids = itertools.count(1)
        self._needs_restart = False
        self._start_error: Optional[str] = None
        self._tracker_ok: Optional[bool] = None
        self._last_saved_tasks = ""
        self._closing = False

        self._client = P2PClient(
            peer_id=config["peer_name"],
            listen_port=config["listen_port"],
            tracker_host=config["tracker_host"],
            tracker_port=config["tracker_port"],
            regular_slots=config["regular_slots"],
            optimistic_slots=config["optimistic_slots"],
            choking_interval=float(config["choking_interval"]),
            max_workers=config["max_workers"],
        )

    # ── Vòng đời ──────────────────────────────────────────────────────── #

    def _attach_window(self, window) -> None:
        self._window = window

    def _start(self) -> None:
        """Mở cổng lắng nghe, rồi khôi phục nhiệm vụ cũ (nếu bật) trên luồng nền."""
        try:
            self._client.start()
        except OSError as e:
            self._start_error = (f"Không mở được cổng {self._cfg['listen_port']} ({e}). "
                                 "Đổi cổng lắng nghe trong Cài đặt rồi khởi động lại app.")
            print(f"[App] Lỗi: {self._start_error}")
            return
        threading.Thread(target=self._tracker_watch_loop, daemon=True).start()
        if self._cfg["auto_resume"]:
            threading.Thread(target=self._restore_tasks, daemon=True).start()

    def _shutdown(self) -> None:
        if self._closing:
            return  # Đã dừng rồi (ví dụ restart_app rồi cửa sổ đóng)
        self._closing = True
        self._persist_tasks()
        if self._start_error is None:
            self._client.stop()

    def _tracker_watch_loop(self) -> None:
        """Kiểm tra Tracker định kỳ để thanh trạng thái luôn đúng, kể cả khi chưa có file nào."""
        while not self._closing:
            self._tracker_ok = check_tracker(self._client.tracker_host, self._client.tracker_port)
            for _ in range(100):
                if self._closing:
                    return
                time.sleep(0.1)

    def _restore_tasks(self) -> None:
        for task in self._cfg.load_tasks():
            try:
                meta_path = task.get("meta_path", "")
                file_path = task.get("file_path", "")
                if not (os.path.exists(meta_path) and file_path):
                    continue
                if task.get("type") == "Share":
                    if os.path.exists(file_path):
                        self._start_share_job(file_path, meta_path)
                else:
                    self._client.download_file(meta_path, os.path.dirname(file_path),
                                               start_paused=bool(task.get("paused")))
            except Exception as e:
                print(f"[App] Lỗi khôi phục '{task.get('file_path')}': {e}")

    def _persist_tasks(self) -> None:
        """Ghi danh sách nhiệm vụ ra tasks.json (chỉ ghi khi có thay đổi)."""
        tasks = []
        for t in self._client.snapshot()["tasks"]:
            tasks.append({
                "type": t["type"],
                "meta_path": t["meta_path"],
                "file_path": t["file_path"],
                "paused": t["state"] == STATE_PAUSED,
            })
        with self._lock:
            for job in self._jobs.values():  # Lượt chia sẻ chưa xong cũng cần nhớ
                if job["state"] == "running":
                    tasks.append({"type": "Share", "meta_path": job["meta_path"],
                                  "file_path": job["path"], "paused": False})
        encoded = json.dumps(tasks, sort_keys=True)
        if encoded != self._last_saved_tasks:
            self._cfg.save_tasks(tasks)
            self._last_saved_tasks = encoded

    # ── Trạng thái ────────────────────────────────────────────────────── #

    def get_state(self, last_log_id: int = 0) -> Dict[str, Any]:
        snap = None if self._start_error else self._client.snapshot()
        if snap is not None:
            if snap["tracker_ok"] is None:
                snap["tracker_ok"] = self._tracker_ok
            self._persist_tasks()
        with self._lock:
            jobs = [dict(j) for j in self._jobs.values()]
            # Lượt chia sẻ đã xong quá 15 giây thì không cần hiện nữa
            now = time.time()
            for job_id in [k for k, j in self._jobs.items()
                           if j["state"] != "running" and now - j["finished_at"] > 15]:
                del self._jobs[job_id]
        return {
            "client": snap,
            "jobs": jobs,
            "logs": self._log.since(int(last_log_id or 0)),
            "start_error": self._start_error,
            "needs_restart": self._needs_restart,
            "config": dict(self._cfg.values),
        }

    # ── Hộp thoại chọn file / thư mục ─────────────────────────────────── #

    def _dialog(self, kind, file_types=()) -> Optional[str]:
        if self._window is None or webview is None:
            return None
        result = self._window.create_file_dialog(kind, allow_multiple=False, file_types=file_types)
        if not result:
            return None
        return result[0] if isinstance(result, (list, tuple)) else result

    def pick_file(self) -> Optional[str]:
        return self._dialog(webview.FileDialog.OPEN)

    def pick_meta_file(self) -> Optional[str]:
        return self._dialog(webview.FileDialog.OPEN,
                            ("File metadata (*.meta.json;*.json)", "Tất cả (*.*)"))

    def pick_folder(self) -> Optional[str]:
        return self._dialog(webview.FileDialog.FOLDER)

    # ── Chia sẻ file ──────────────────────────────────────────────────── #

    def share(self, path: str) -> Dict[str, Any]:
        if self._start_error:
            return _err(self._start_error)
        if not path or not os.path.isfile(path):
            return _err("Không tìm thấy file. Chọn lại file cần chia sẻ.")
        if os.path.getsize(path) == 0:
            return _err("File rỗng, không có gì để chia sẻ.")
        os.makedirs(self._cfg["meta_dir"], exist_ok=True)
        meta_path = os.path.join(self._cfg["meta_dir"], os.path.basename(path) + ".meta.json")
        job_id = self._start_share_job(path, meta_path)
        return _ok(job_id=job_id)

    def _start_share_job(self, path: str, meta_path: str) -> str:
        job_id = str(next(self._job_ids))
        job = {
            "id": job_id, "path": os.path.abspath(path), "name": os.path.basename(path),
            "size": os.path.getsize(path), "meta_path": meta_path,
            "stage": "hash", "done": 0, "total": 0,
            "state": "running", "error": None, "info_hash": None, "finished_at": 0.0,
        }
        with self._lock:
            self._jobs[job_id] = job
        threading.Thread(target=self._run_share_job, args=(job_id,), daemon=True).start()
        return job_id

    def _run_share_job(self, job_id: str) -> None:
        with self._lock:
            job = self._jobs[job_id]
            path, meta_path = job["path"], job["meta_path"]

        def progress(stage: str, done: int, total: int) -> None:
            with self._lock:
                job.update(stage=stage, done=done, total=total)

        try:
            # File .meta.json cũ trùng tên nhưng là file khác → tạo lại.
            if os.path.exists(meta_path):
                try:
                    old = load_metainfo(meta_path)
                    if old["info"]["file_size"] != os.path.getsize(path):
                        os.remove(meta_path)
                except Exception:
                    os.remove(meta_path)
            try:
                info_hash = self._client.share_file(path, meta_path, progress_cb=progress)
            except ValueError:
                # Metadata cũ cùng kích thước nhưng nội dung khác → băm lại một lần.
                os.remove(meta_path)
                info_hash = self._client.share_file(path, meta_path, progress_cb=progress)
            with self._lock:
                job.update(state="done", info_hash=info_hash, finished_at=time.time())
        except Exception as e:
            print(f"[App] Lỗi chia sẻ {os.path.basename(path)}: {e}")
            with self._lock:
                job.update(state="error", error=str(e), finished_at=time.time())

    # ── Tải file ──────────────────────────────────────────────────────── #

    def preview(self, meta_path: str, download_dir: str = "") -> Dict[str, Any]:
        if self._start_error:
            return _err(self._start_error)
        if not meta_path or not os.path.isfile(meta_path):
            return _err("Không tìm thấy file .meta.json.")
        try:
            info = self._client.preview_meta(meta_path, download_dir or self._cfg["download_dir"])
        except (ValueError, KeyError, TypeError):
            return _err("File này không phải .meta.json hợp lệ.")
        except OSError as e:
            return _err(f"Không đọc được file: {e}")
        return _ok(preview=info, meta_path=os.path.abspath(meta_path))

    def download(self, meta_path: str, download_dir: str = "") -> Dict[str, Any]:
        if self._start_error:
            return _err(self._start_error)
        target_dir = download_dir or self._cfg["download_dir"]
        try:
            info_hash = self._client.download_file(meta_path, target_dir)
        except Exception as e:
            return _err(f"Không bắt đầu tải được: {e}")
        return _ok(info_hash=info_hash)

    # ── Điều khiển nhiệm vụ ───────────────────────────────────────────── #

    def pause(self, info_hash: str) -> Dict[str, Any]:
        return _ok() if self._client.pause_task(info_hash) else _err("Không tạm dừng được file này.")

    def resume(self, info_hash: str) -> Dict[str, Any]:
        return _ok() if self._client.resume_task(info_hash) else _err("Không tiếp tục được file này.")

    def remove(self, info_hash: str, delete_file: bool = False) -> Dict[str, Any]:
        ok = self._client.remove_task(info_hash, delete_file=bool(delete_file))
        self._persist_tasks()
        return _ok() if ok else _err("Không tìm thấy file trong danh sách.")

    def open_folder(self, path: str) -> Dict[str, Any]:
        """Mở thư mục chứa file (và chọn sẵn file đó trên Windows)."""
        if not path:
            return _err("Không có đường dẫn.")
        try:
            if os.path.isfile(path) and sys.platform == "win32":
                subprocess.Popen(["explorer", "/select,", os.path.normpath(path)])
            else:
                folder = path if os.path.isdir(path) else os.path.dirname(path)
                if sys.platform == "win32":
                    os.startfile(folder)
                elif sys.platform == "darwin":
                    subprocess.Popen(["open", folder])
                else:
                    subprocess.Popen(["xdg-open", folder])
            return _ok()
        except OSError as e:
            return _err(f"Không mở được thư mục: {e}")

    # ── Cài đặt ───────────────────────────────────────────────────────── #

    def save_settings(self, values: Dict[str, Any]) -> Dict[str, Any]:
        try:
            needs_restart = self._cfg.update(values or {})
        except ValueError as e:
            return _err(str(e))
        if needs_restart:
            self._needs_restart = True
        return _ok(needs_restart=self._needs_restart)

    def test_tracker(self, host: str, port: Any) -> Dict[str, Any]:
        try:
            port_num = int(port)
        except (TypeError, ValueError):
            return _err("Cổng tracker phải là số.")
        if check_tracker(str(host).strip(), port_num):
            return _ok()
        return _err(f"Không kết nối được tracker {host}:{port_num}.")

    def restart_app(self) -> Dict[str, Any]:
        """Khởi động lại app để áp dụng cài đặt mới."""
        self._shutdown()
        env = os.environ.copy()
        if getattr(sys, "frozen", False):
            cmd: List[str] = [sys.executable] + sys.argv[1:]
            # Bản .exe (PyInstaller --onefile): nếu không đặt biến này, app mới dùng
            # chung thư mục giải nén _MEIxxxx của app cũ, nên app cũ thoát không xóa
            # được thư mục đó và hiện cảnh báo "Failed to remove temporary directory".
            env["PYINSTALLER_RESET_ENVIRONMENT"] = "1"
        else:
            cmd = [sys.executable] + sys.argv
        subprocess.Popen(cmd, env=env)
        if self._window is not None:
            self._window.destroy()
        return _ok()

    # ── Nhật ký ───────────────────────────────────────────────────────── #

    def clear_logs(self) -> Dict[str, Any]:
        self._log.clear()
        return _ok()
