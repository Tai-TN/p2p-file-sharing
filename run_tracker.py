"""
run_tracker.py — Khởi động Tracker Server
Chạy: python run_tracker.py
"""
import time
from tracker.tracker_server import TrackerServer

# ── Cấu hình ──
# Lắng nghe IPv6 trên mọi interface; dùng "::1" nếu chỉ thử trên một máy.
HOST = "::"
PORT = 8000

def main():
    print(f"----- Tracker Server đang khởi động tại [{HOST}]:{PORT} -----")

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
