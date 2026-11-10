"""
run_tracker.py — Khởi động Tracker Server
Chạy: python run_tracker.py
"""
import argparse
import os
import time
from tracker.tracker_server import TrackerServer

# ── Cấu hình ──
# Dùng "0.0.0.0" để lắng nghe trên tất cả network interface (cho phép LAN)
# Dùng "127.0.0.1" nếu chỉ chạy local trên 1 máy
HOST = "0.0.0.0"
PORT = 8000

def main():
    parser = argparse.ArgumentParser(description="Tracker và relay TCP")
    parser.add_argument("--host", default=HOST)
    parser.add_argument("--port", type=int, default=PORT)
    args = parser.parse_args()
    print(f"----- Tracker/Relay đang khởi động tại {args.host}:{args.port} -----")
    if not os.environ.get("P2P_TRACKER_SECRET"):
        print("Cảnh báo: chưa đặt P2P_TRACKER_SECRET; chỉ dùng trên mạng tin cậy")

    tracker = TrackerServer(host=args.host, port=args.port, peer_timeout=120.0)
    tracker.start()

    print(f"----- Tracker đang chạy-----\n")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nĐang dừng Tracker...")
        tracker.stop()

if __name__ == "__main__":
    main()
