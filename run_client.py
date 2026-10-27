"""
run_client.py — P2P Client (Seeder + Leecher)

Cách chạy:
    python run_client.py                        # localhost, cổng 6881
    python run_client.py 6882                   # cổng tùy chọn
    python run_client.py 6881 192.168.1.10      # qua mạng LAN
    python run_client.py 6881 100.108.79.47 --tailscale
                                                # Fedora qua Tailscale
    python run_client.py 6882 100.108.79.47 --advertise-ip 100.82.132.18
                                                # Android/Termux qua Tailscale
"""
import argparse
import ipaddress
import json
import os
import random
import subprocess
from peer.client import P2PClient

# ── Cấu hình ──
TRACKER_PORT = 8000

BASE_DIR     = os.path.dirname(os.path.abspath(__file__))
SHARED_DIR   = os.path.join(BASE_DIR, "demo_workspace", "shared")
DOWNLOAD_DIR = os.path.join(BASE_DIR, "demo_workspace", "downloads")


def ipv4_address(value: str) -> str:
    """Chỉ chấp nhận IPv4 vì các socket của dự án đang dùng AF_INET."""
    try:
        return str(ipaddress.IPv4Address(value))
    except ipaddress.AddressValueError as exc:
        raise argparse.ArgumentTypeError(f"IPv4 không hợp lệ: {value}") from exc


def tailscale_command(*args: str) -> str:
    """Chạy CLI Tailscale trên Fedora với thời gian chờ hữu hạn."""
    try:
        result = subprocess.run(
            ["tailscale", *args], capture_output=True, text=True,
            timeout=5, check=True,
        )
    except FileNotFoundError as exc:
        raise RuntimeError("Không tìm thấy CLI tailscale trên máy này") from exc
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError("CLI tailscale không phản hồi sau 5 giây") from exc
    except subprocess.CalledProcessError as exc:
        detail = (exc.stderr or exc.stdout or "").strip()
        raise RuntimeError(detail or "CLI tailscale trả về lỗi") from exc
    return result.stdout.strip()


def get_tailscale_ip() -> str:
    """Lấy IPv4 của Fedora khi thiết bị đã kết nối vào tailnet."""
    try:
        status = json.loads(tailscale_command("status", "--json"))
    except (json.JSONDecodeError, TypeError) as exc:
        raise RuntimeError("Không đọc được trạng thái Tailscale") from exc
    if not isinstance(status, dict) or status.get("BackendState") != "Running":
        raise RuntimeError("Tailscale chưa kết nối; hãy chạy 'sudo tailscale up'")

    raw_ip = tailscale_command("ip", "-4")
    try:
        return str(ipaddress.IPv4Address(raw_ip))
    except ipaddress.AddressValueError as exc:
        raise RuntimeError(f"CLI tailscale trả IPv4 không hợp lệ: {raw_ip!r}") from exc


def show_tailscale_status() -> None:
    """Hiển thị đường direct/relay hiện tại, không ảnh hưởng tới việc tải file."""
    try:
        print("\n  Trạng thái Tailscale hiện tại:")
        print(tailscale_command("status"))
    except RuntimeError as exc:
        print(f"  Không xem được trạng thái Tailscale: {exc}")


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="P2P client chia sẻ file")
    parser.add_argument("listen_port", nargs="?", type=int, default=6881)
    parser.add_argument("tracker_host", nargs="?", default="127.0.0.1")
    ip_options = parser.add_mutually_exclusive_group()
    ip_options.add_argument(
        "--advertise-ip",
        type=ipv4_address,
        help="IPv4 báo cho tracker (ví dụ IP Tailscale); mặc định dùng IP LAN",
    )
    ip_options.add_argument(
        "--tailscale", action="store_true",
        help="Fedora: tự lấy IPv4 Tailscale để báo cho tracker",
    )
    args = parser.parse_args(argv)
    if not 1 <= args.listen_port <= 65535:
        parser.error("listen_port phải nằm trong khoảng 1–65535")
    return args


def print_menu(peer_id: str, listen_port: int, tracker_host: str) -> None:
    print(f"\n{'='*50}")
    print(f"  P2P CLIENT [{peer_id}] - Cổng {listen_port} | Tracker {tracker_host}:{TRACKER_PORT}")
    print(f"{'='*50}")
    print("  1. Chia sẻ file")
    print("  2. Tải file")
    print("  3. Xem tiến độ")
    print("  4. Thoát")
    print(f"{'='*50}")


def main() -> None:
    args = parse_args()
    peer_id = f"peer_{random.randint(1000, 9999)}"
    if args.tailscale:
        try:
            args.advertise_ip = get_tailscale_ip()
        except RuntimeError as exc:
            raise SystemExit(f"Lỗi Tailscale: {exc}") from exc

    client = P2PClient(
        peer_id=peer_id,
        listen_port=args.listen_port,
        tracker_host=args.tracker_host,
        tracker_port=TRACKER_PORT,
        advertise_ip=args.advertise_ip,
    )
    client.start()

    os.makedirs(SHARED_DIR,   exist_ok=True)
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    try:
        while True:
            print_menu(peer_id, args.listen_port, args.tracker_host)
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
                if args.tailscale:
                    show_tailscale_status()

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