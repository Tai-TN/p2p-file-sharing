"""
run_client.py — P2P Client (Seeder + Leecher)

Cách chạy:
    python run_client.py                        # localhost, cổng 6881
    python run_client.py 6882                   # cổng tùy chọn
    python run_client.py 6881 192.168.1.10      # qua mạng LAN
"""
import os, sys, random

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass
from peer.client import P2PClient

# ── Cấu hình ──
LISTEN_PORT  = int(sys.argv[1]) if len(sys.argv) > 1 else 6881
TRACKER_HOST = sys.argv[2]     if len(sys.argv) > 2 else "127.0.0.1"
TRACKER_PORT = 8000

BASE_DIR     = os.path.dirname(os.path.abspath(__file__))
SHARED_DIR   = os.path.join(BASE_DIR, "demo_workspace", "shared")
DOWNLOAD_DIR = os.path.join(BASE_DIR, "demo_workspace", "downloads")


def print_menu(peer_id: str) -> None:
    print(f"\n{'='*50}")
    print(f"  P2P CLIENT [{peer_id}] - Cổng {LISTEN_PORT} | Tracker {TRACKER_HOST}:{TRACKER_PORT}")
    print(f"{'='*50}")
    print("  1. Chia sẻ file")
    print("  2. Tải file")
    print("  3. Xem tiến độ")
    print("  4. Thoát")
    print(f"{'='*50}")


def main() -> None:
    peer_id = f"peer_{random.randint(1000, 9999)}"

    client = P2PClient(
        peer_id=peer_id,
        listen_port=LISTEN_PORT,
        tracker_host=TRACKER_HOST,
        tracker_port=TRACKER_PORT,
    )
    client.start()

    os.makedirs(SHARED_DIR,   exist_ok=True)
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    try:
        while True:
            print_menu(peer_id)
            choice = input("Lựa chọn: ").strip()

            if choice == "1":
                # ── Chia sẻ file ──
                src = input("Đường dẫn file cần chia sẻ: ").strip().strip("'\"")
                if not os.path.exists(src):
                    print(f"  Lỗi: Không tìm thấy file.")
                    continue
                meta_path = os.path.join(SHARED_DIR, os.path.basename(src) + ".meta.json")
                try:
                    client.share_file(src, meta_path)
                    print(f"  - Đang Seed: {os.path.basename(src)}")
                    print(f"  - Metadata: {meta_path}")
                except Exception as e:
                    print(f"  Lỗi: {e}")

            elif choice == "2":
                # ── Tải file 
                meta_path = input("Đường dẫn file .meta.json: ").strip().strip("'\"")
                if not os.path.exists(meta_path):
                    print(f"  Lỗi: Không tìm thấy file metadata.")
                    continue
                try:
                    client.download_file(meta_path, DOWNLOAD_DIR)
                    print(f"  - Đã bắt đầu tải ngầm — chọn mục 3 để xem tiến độ.")
                except Exception as e:
                    print(f"  Lỗi: {e}")

            elif choice == "3":
                # ── Xem tiến độ ──
                tasks = client.get_status_list()
                if not tasks:
                    print("  Chưa có file nào.")
                else:
                    print(f"\n  {'Tên file':<30} {'Loại':<8} {'Tiến độ':<9} {'Mảnh':<16} Trạng thái")
                    print(f"  {'-'*80}")
                    for t in tasks:
                        print(f"  {t['name'][:28]:<30} {t['type']:<8} {t['progress']:<9} {t['pieces']:<16} {t['status']}")

            elif choice == "4":
                break

            else:
                print("  Lựa chọn không hợp lệ.")

    except KeyboardInterrupt:
        print("\nĐang dừng...")
    finally:
        client.stop()
        print("Đã thoát.")


if __name__ == "__main__":
    main()
