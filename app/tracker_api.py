"""Cầu nối giữa giao diện quản trị (JavaScript) và TrackerServer."""
import json
import os
import threading
import time
from typing import Any, Dict, Optional

from app.log_buffer import LogBuffer
from tracker.tracker_server import TrackerServer


def _ok(**data) -> Dict[str, Any]:
    return {"ok": True, **data}


def _err(message: str) -> Dict[str, Any]:
    return {"ok": False, "error": message}


class TrackerApi:
    def __init__(self, data_dir: str, log_buffer: LogBuffer,
                 host: str = "0.0.0.0", port: int = 8000, peer_timeout: float = 120.0):
        self._data_dir = os.path.abspath(data_dir)
        os.makedirs(self._data_dir, exist_ok=True)
        self._blocklist_path = os.path.join(self._data_dir, "tracker_blocklist.json")
        self._log = log_buffer
        self._lock = threading.Lock()
        self._host = host
        self._port = port
        self._server = TrackerServer(host=host, port=port, peer_timeout=peer_timeout)
        self._error: Optional[str] = None
        self._load_blocklist()

    # ── Vòng đời ──────────────────────────────────────────────────────── #

    def _attach_window(self, window) -> None:
        self._window = window

    def _start(self) -> None:
        self.start_server()

    def _shutdown(self) -> None:
        if self._server.is_running:
            self._server.stop()

    def start_server(self, port: Any = None) -> Dict[str, Any]:
        with self._lock:
            if self._server.is_running:
                return _ok()
            if port not in (None, ""):
                try:
                    new_port = int(port)
                except (TypeError, ValueError):
                    return _err("Cổng phải là số.")
                if not 1 <= new_port <= 65535:
                    return _err("Cổng phải nằm trong khoảng 1–65535.")
                self._port = new_port
                self._server.port = new_port
            try:
                self._server.start()
                self._error = None
                return _ok()
            except OSError as e:
                self._error = f"Không mở được cổng {self._port}: {e}"
                print(f"[Tracker Admin] Lỗi: {self._error}")
                return _err(self._error)

    def stop_server(self) -> Dict[str, Any]:
        with self._lock:
            if self._server.is_running:
                self._server.stop()
            return _ok()

    # ── Trạng thái ────────────────────────────────────────────────────── #

    def get_state(self, last_log_id: int = 0) -> Dict[str, Any]:
        running = self._server.is_running
        start_time = self._server.start_time
        return {
            "running": running,
            "host": self._host,
            "port": self._port,
            "uptime": (time.time() - start_time) if running and start_time else 0,
            "error": self._error,
            "state": self._server.state.snapshot(),
            "logs": self._log.since(int(last_log_id or 0)),
        }

    # ── Thao tác quản trị ─────────────────────────────────────────────── #

    def remove_peer(self, info_hash: str, peer_id: str) -> Dict[str, Any]:
        if self._server.state.remove_peer(info_hash, peer_id):
            print(f"[Tracker Admin] Đã xóa {peer_id} khỏi swarm {info_hash[:8]}...")
            return _ok()
        return _err("Peer không còn trong swarm.")

    def block_peer(self, peer_id: str, ip: str = "") -> Dict[str, Any]:
        if not peer_id and not ip:
            return _err("Thiếu peer_id hoặc IP để chặn.")
        removed = self._server.state.block_peer(peer_id or "", ip or "")
        self._save_blocklist()
        print(f"[Tracker Admin] Đã chặn {peer_id or ip} (gỡ khỏi {removed} swarm)")
        return _ok(removed=removed)

    def unblock(self, peer_id: str = "", ip: str = "") -> Dict[str, Any]:
        if self._server.state.unblock_peer(peer_id or "", ip or ""):
            self._save_blocklist()
            print(f"[Tracker Admin] Đã bỏ chặn {peer_id or ip}")
            return _ok()
        return _err("Mục này không có trong danh sách chặn.")

    def clear_logs(self) -> Dict[str, Any]:
        self._log.clear()
        return _ok()

    # ── Lưu danh sách chặn để không mất khi khởi động lại ─────────────── #

    def _load_blocklist(self) -> None:
        try:
            with open(self._blocklist_path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except (OSError, ValueError):
            return
        state = self._server.state
        with state.lock:
            state.blocked_peer_ids.update(str(x) for x in data.get("peer_ids", []))
            state.blocked_ips.update(str(x) for x in data.get("ips", []))

    def _save_blocklist(self) -> None:
        state = self._server.state
        with state.lock:
            data = {"peer_ids": sorted(state.blocked_peer_ids), "ips": sorted(state.blocked_ips)}
        try:
            with open(self._blocklist_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
        except OSError:
            pass
