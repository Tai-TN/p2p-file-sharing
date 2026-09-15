"""
run_leecher.py — Tải file từ mạng P2P

Chạy: python run_leecher.py
(Cần chạy run_tracker.py và run_seeder.py trước)
"""
import os, sys, time, random, hashlib
from storage.metainfo import load_metainfo
from storage.piece_manager import PieceManager
from peer.peer_node import PeerNode
from peer.transfer_engine import TransferEngine

# ── Cấu hình ──
TRACKER_HOST = "127.0.0.1"
TRACKER_PORT = 8000
LEECHER_PORT = 6882

META_FILE    = os.path.join(os.path.dirname(os.path.abspath(__file__)), "demo_workspace", "shared", "lofi_music.meta.json")
DOWNLOAD_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "demo_workspace", "downloads")

def main():
    # Bước 1: Đọc metadata
    if not os.path.exists(META_FILE):
        print(f"✘ Chưa có metadata! Hãy chạy run_seeder.py trước.")
        sys.exit(1)

    meta       = load_metainfo(META_FILE)
    file_name  = meta["info"]["name"]
    file_size  = meta["info"]["file_size"]
    num_pieces = len(meta["info"]["pieces"])
    download_path = os.path.join(DOWNLOAD_DIR, file_name)

    print(f"📥 File: {file_name} ({file_size / 1024 / 1024:.1f} MB, {num_pieces} mảnh)")

    # Bước 2: Khởi tạo PieceManager
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    pm = PieceManager(download_path, meta)

    # Bước 3: Khởi động leecher
    peer_id = f"leecher_{random.randint(1000, 9999)}"
    node = PeerNode(peer_id=peer_id, host="127.0.0.1", port=LEECHER_PORT, piece_manager=pm)
    node.start_listening()

    engine = TransferEngine(peer_id=peer_id, tracker_host=TRACKER_HOST,
                            tracker_port=TRACKER_PORT, listen_port=LEECHER_PORT,
                            piece_manager=pm, max_workers=4)

    print(f"🔍 Leecher [{peer_id}] đang kết nối Tracker và tìm Seeder...\n")

    # Bước 4: Bắt đầu tải
    start_time = time.time()
    try:
        success = engine.start_download(peer_node=node)

        if success:
            elapsed = time.time() - start_time
            print(f"\n✨ TẢI HOÀN TẤT trong {elapsed:.1f}s!")
            print(f"   File lưu tại: {download_path}")

            # Xác minh SHA-256
            h = hashlib.sha256()
            with open(download_path, "rb") as f:
                for chunk in iter(lambda: f.read(65536), b""):
                    h.update(chunk)
            print(f"   SHA-256: {h.hexdigest()[:32]}...")

            # Chuyển sang seeder
            print(f"\n📤 Chuyển sang chế độ Seeder. Ctrl+C để dừng.")
            while True:
                time.sleep(30)
                engine._announce_to_tracker(event="started")
        else:
            print("\n✘ Tải thất bại! Kiểm tra Tracker và Seeder.")

    except KeyboardInterrupt:
        print("\nĐang dừng...")
    finally:
        engine._announce_to_tracker(event="stopped")
        node.stop()
        print("✔ Leecher đã dừng.")

if __name__ == "__main__":
    main()
