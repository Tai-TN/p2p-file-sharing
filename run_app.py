"""
run_app.py — App desktop cho Peer (chia sẻ / tải file) chạy bằng pywebview.

Cách chạy:
    python run_app.py                         # dữ liệu lưu ở %APPDATA%\\P2PShare
    python run_app.py --data-dir ./peer2      # chạy peer thứ hai trên cùng máy
    python run_app.py --debug                 # mở DevTools để gỡ lỗi giao diện
"""
import argparse

from app.config import AppConfig, default_data_dir
from app.log_buffer import LogBuffer, install


def main() -> None:
    parser = argparse.ArgumentParser(description="P2P Share — app chia sẻ / tải file")
    parser.add_argument("--data-dir", default=default_data_dir(),
                        help="Thư mục lưu cài đặt, danh sách file và file tải về")
    parser.add_argument("--debug", action="store_true", help="Bật DevTools")
    args = parser.parse_args()

    logs = LogBuffer()
    install(logs)  # Gom print() của code lõi vào màn Nhật ký

    from app.launcher import run_window
    from app.peer_api import PeerApi

    config = AppConfig(args.data_dir)
    api = PeerApi(config, logs)
    print(f"[App] Thư mục dữ liệu: {config.data_dir}")
    run_window("P2P Share", "peer.html", api, debug=args.debug, enable_drop=True)


if __name__ == "__main__":
    main()
