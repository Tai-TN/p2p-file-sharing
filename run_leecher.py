
import os, sys, time, random, hashlib
from storage.metainfo import load_metainfo
from web.peer_web import start_peer_web
from storage.piece_manager import PieceManager
from peer.peer_node import PeerNode
from peer.transfer_engine import TransferEngine

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# ── Cấu hình ──
TRACKER_HOST = "127.0.0.1"
TRACKER_PORT = 8000
LEECHER_PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 6882
WEB_PORT = 9002 if LEECHER_PORT == 6882 else LEECHER_PORT + 2000

META_FILE    = os.path.join(os.path.dirname(os.path.abspath(__file__)), "demo_workspace", "shared", "game.meta.json")
DOWNLOAD_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "demo_workspace", "downloads")

def main():
    # Đọc metadata
    if not os.path.exists(META_FILE):
        print(f"Chưa có metadata!")
        sys.exit(1)

    meta       = load_metainfo(META_FILE)
    file_name  = meta["info"]["name"]
    file_size  = meta["info"]["file_size"]
    num_pieces = len(meta["info"]["pieces"])
    download_path = os.path.join(DOWNLOAD_DIR, file_name)

    print(f"File: {file_name} ({file_size / 1024 / 1024:.1f} MB, {num_pieces} mảnh)")

    #  Khởi tạo PieceManager
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    pm = PieceManager(download_path, meta)

    #  Khởi động leecher
    peer_id = f"leecher_{random.randint(1000, 9999)}"
    node = PeerNode(peer_id=peer_id, host="127.0.0.1", port=LEECHER_PORT, piece_manager=pm)
    node.start_listening()

    engine = TransferEngine(peer_id=peer_id, tracker_host=TRACKER_HOST,
                            tracker_port=TRACKER_PORT, listen_port=LEECHER_PORT,
                            piece_manager=pm, max_workers=4)
    
    start_peer_web(peer_id_or_pm=peer_id, piece_manager=pm, engine=engine, peer_node=node, port=WEB_PORT)
    print(f"----- Leecher [{peer_id}] dang ket noi Tracker va tim Seeder -----")
    print(f"----- Dashboard: http://127.0.0.1:{WEB_PORT} -----\n")

    #  Bắt đầu tải
    start_time = time.time()
    try:
        success = engine.start_download(peer_node=node)

        if success:
            elapsed = time.time() - start_time
            print(f"\nTẢI HOÀN TẤT trong {elapsed:.1f}s!")
            print(f"   File lưu tại: {download_path}")

            # Xác minh SHA-256
            h = hashlib.sha256()
            with open(download_path, "rb") as f:
                for chunk in iter(lambda: f.read(65536), b""):
                    h.update(chunk)
            print(f"   SHA-256: {h.hexdigest()[:32]}...")

            # Chuyển sang seeder
            print(f"\n Chuyển sang chế độ Seeder. Ctrl+C để dừng.")
            while True:
                time.sleep(30)
                engine._announce_to_tracker(event="started")
        else:
            print("\n Tải thất bại! Kiểm tra Tracker và Seeder.")

    except KeyboardInterrupt:
        print("\nĐang dừng...")
    finally:
        engine._announce_to_tracker(event="stopped")
        node.stop()
        print(" Leecher đã dừng.")

if __name__ == "__main__":
    main()
