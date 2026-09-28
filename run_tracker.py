"""
run_tracker.py — Khởi động Tracker Server
Chạy: python run_tracker.py
"""
import time
from tracker.tracker_server import TrackerServer

# ── Cấu hình ──
# Dùng "0.0.0.0" để lắng nghe trên tất cả network interface (cho phép LAN)
# Dùng "127.0.0.1" nếu chỉ chạy local trên 1 máy
HOST = "0.0.0.0"
PORT = 8000

def main():
    print(f"----- Tracker Server đang khởi động tại {HOST}:{PORT} -----")

    tracker = TrackerServer(host=HOST, port=PORT, peer_timeout=120.0)
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
