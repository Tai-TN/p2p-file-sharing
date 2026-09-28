"""
run_tracker.py — Khoi dong Tracker Server + Web Dashboard
Chay: python run_tracker.py
"""
import sys
import os
import time

# Fix Unicode output tren Windows
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

from tracker.tracker_server import TrackerServer
from web.tracker_web import start_tracker_web

# ── Cau hinh ──
HOST = "127.0.0.1"
PORT = 8000
WEB_PORT = 8080

def main():
    print(f"----- Tracker Server dang khoi dong tai {HOST}:{PORT} -----")

    tracker = TrackerServer(host=HOST, port=PORT, peer_timeout=120.0)
    tracker.start()

    # Khoi chay Web Dashboard (daemon thread)
    start_tracker_web(tracker.state, port=WEB_PORT)

    print(f"----- Tracker dang chay -----")
    print(f"----- Dashboard: http://127.0.0.1:{WEB_PORT} -----\n")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nDang dung Tracker...")
        tracker.stop()

if __name__ == "__main__":
    main()
