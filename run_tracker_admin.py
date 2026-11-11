"""
run_tracker_admin.py — App quản trị Tracker (chạy tracker + giao diện xem / xóa / chặn peer).

Cách chạy:
    python run_tracker_admin.py                  # lắng nghe 0.0.0.0:8000
    python run_tracker_admin.py --port 9000
"""
import argparse
import os

from app.config import default_data_dir
from app.log_buffer import LogBuffer, install


def main() -> None:
    parser = argparse.ArgumentParser(description="P2P Tracker — app quản trị")
    parser.add_argument("--host", default="0.0.0.0", help="Địa chỉ lắng nghe (mặc định 0.0.0.0 cho LAN)")
    parser.add_argument("--port", type=int, default=8000, help="Cổng tracker (mặc định 8000)")
    parser.add_argument("--peer-timeout", type=float, default=120.0,
                        help="Số giây không announce thì loại peer (mặc định 120)")
    parser.add_argument("--data-dir", default=os.path.join(default_data_dir(), "tracker"),
                        help="Thư mục lưu danh sách chặn")
    parser.add_argument("--debug", action="store_true", help="Bật DevTools")
    args = parser.parse_args()

    logs = LogBuffer()
    install(logs)

    from app.launcher import run_window
    from app.tracker_api import TrackerApi

    api = TrackerApi(args.data_dir, logs, host=args.host, port=args.port, peer_timeout=args.peer_timeout)
    run_window("P2P Tracker", "tracker.html", api, width=1180, height=760, debug=args.debug)


if __name__ == "__main__":
    main()
