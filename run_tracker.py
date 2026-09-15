import argparse
import time
from tracker.tracker_server import TrackerServer

def main():
    parser = argparse.ArgumentParser(description="Chạy máy chủ Tracker P2P File Sharing")
    parser.add_argument("--host", type=str, default="127.0.0.1", help="IP lắng nghe của Tracker (mặc định 127.0.0.1)")
    parser.add_argument("--port", type=int, default=8000, help="Cổng lắng nghe của Tracker (mặc định 8000)")
    args = parser.parse_args()

    tracker = TrackerServer(host=args.host, port=args.port)
    tracker.start()

    print("Nhấn Ctrl+C để dừng Tracker Server...")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nĐang dừng Tracker...")
        tracker.stop()

if __name__ == "__main__":
    main()
