"""Đọc / ghi cấu hình và danh sách nhiệm vụ của app Peer (lưu dạng JSON trong thư mục dữ liệu)."""
import json
import os
import random
import sys
from typing import Any, Dict, List

DEFAULTS: Dict[str, Any] = {
    "tracker_host": "127.0.0.1",
    "tracker_port": 8000,
    "listen_port": 6881,
    "peer_name": "",          # Rỗng → tự sinh peer_XXXX ở lần chạy đầu
    "download_dir": "",       # Rỗng → <thư mục dữ liệu>/downloads
    "meta_dir": "",           # Rỗng → <thư mục dữ liệu>/meta
    # Nâng cao
    "max_workers": 4,
    "regular_slots": 2,
    "optimistic_slots": 1,
    "choking_interval": 20,
    "auto_resume": True,
}

# Các khóa cần khởi động lại app mới có hiệu lực (do PeerNode / engine đã tạo sẵn).
RESTART_KEYS = {"tracker_host", "tracker_port", "listen_port", "peer_name",
                "max_workers", "regular_slots", "optimistic_slots", "choking_interval"}

LABELS = {
    "tracker_host": "Địa chỉ tracker",
    "tracker_port": "Cổng tracker",
    "listen_port": "Cổng lắng nghe",
    "peer_name": "Tên peer",
    "download_dir": "Thư mục lưu file tải về",
    "meta_dir": "Thư mục lưu .meta.json",
    "max_workers": "Số luồng tải song song",
    "regular_slots": "Slot upload thường",
    "optimistic_slots": "Slot optimistic",
    "choking_interval": "Chu kỳ xét lại",
}

INT_RANGES = {
    "tracker_port": (1, 65535),
    "listen_port": (1, 65535),
    "max_workers": (1, 16),
    "regular_slots": (0, 10),
    "optimistic_slots": (0, 5),
    "choking_interval": (5, 300),
}


def default_data_dir() -> str:
    """Thư mục dữ liệu mặc định: %APPDATA%\\P2PShare trên Windows, ~/.p2pshare nơi khác."""
    if sys.platform == "win32" and os.environ.get("APPDATA"):
        return os.path.join(os.environ["APPDATA"], "P2PShare")
    return os.path.join(os.path.expanduser("~"), ".p2pshare")


class AppConfig:
    def __init__(self, data_dir: str):
        self.data_dir = os.path.abspath(data_dir)
        os.makedirs(self.data_dir, exist_ok=True)
        self.path = os.path.join(self.data_dir, "config.json")
        self.tasks_path = os.path.join(self.data_dir, "tasks.json")
        self.values: Dict[str, Any] = dict(DEFAULTS)
        self.load()

        changed = False
        if not self.values["peer_name"]:
            self.values["peer_name"] = f"peer_{random.randint(1000, 9999)}"
            changed = True
        if not self.values["download_dir"]:
            self.values["download_dir"] = os.path.join(self.data_dir, "downloads")
            changed = True
        if not self.values["meta_dir"]:
            self.values["meta_dir"] = os.path.join(self.data_dir, "meta")
            changed = True
        if changed or not os.path.exists(self.path):
            self.save()

    def __getitem__(self, key: str) -> Any:
        return self.values[key]

    def load(self) -> None:
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                data = json.load(f)
            for key in DEFAULTS:
                if key in data:
                    self.values[key] = data[key]
        except (OSError, ValueError):
            pass

    def save(self) -> None:
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(self.values, f, indent=2, ensure_ascii=False)

    def validate(self, new_values: Dict[str, Any]) -> Dict[str, Any]:
        """Kiểm tra và chuẩn hóa giá trị từ giao diện. Ném ValueError với thông báo tiếng Việt."""
        clean: Dict[str, Any] = {}
        for key, value in new_values.items():
            if key not in DEFAULTS:
                continue
            label = LABELS.get(key, key)
            if key in INT_RANGES:
                try:
                    num = int(value)
                except (TypeError, ValueError):
                    raise ValueError(f"{label} phải là số nguyên.")
                lo, hi = INT_RANGES[key]
                if not lo <= num <= hi:
                    raise ValueError(f"{label} phải nằm trong khoảng {lo}–{hi}.")
                clean[key] = num
            elif key == "auto_resume":
                clean[key] = bool(value)
            else:
                text = str(value or "").strip()
                if not text:
                    raise ValueError(f"{label} không được để trống.")
                if key == "peer_name" and (len(text) > 32 or " " in text):
                    raise ValueError("Tên peer tối đa 32 ký tự và không có khoảng trắng.")
                clean[key] = text
        return clean

    def update(self, new_values: Dict[str, Any]) -> bool:
        """Lưu cấu hình mới. Trả về True nếu có thay đổi cần khởi động lại app."""
        clean = self.validate(new_values)
        needs_restart = any(k in RESTART_KEYS and clean[k] != self.values.get(k) for k in clean)
        self.values.update(clean)
        self.save()
        return needs_restart

    # ── Danh sách nhiệm vụ (để tự tải tiếp / chia sẻ lại khi mở app) ──

    def load_tasks(self) -> List[Dict[str, Any]]:
        try:
            with open(self.tasks_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            return [t for t in data if isinstance(t, dict)]
        except (OSError, ValueError):
            return []

    def save_tasks(self, tasks: List[Dict[str, Any]]) -> None:
        try:
            with open(self.tasks_path, "w", encoding="utf-8") as f:
                json.dump(tasks, f, indent=2, ensure_ascii=False)
        except OSError:
            pass
