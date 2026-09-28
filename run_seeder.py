"""
run_seeder.py — Chia sẻ file lên mạng P2P

Chạy 1 seeder:   python run_seeder.py
Chạy thêm:       python run_seeder.py 6883
"""
import os, sys, time, random
from storage.metainfo import create_metainfo, save_metainfo, load_metainfo
from storage.piece_manager import PieceManager
from peer.peer_node import PeerNode
from peer.transfer_engine import TransferEngine
from web.peer_web import start_peer_web   

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")


# ── Cấu hình ──
TRACKER_HOST = "127.0.0.1"
TRACKER_PORT = 8000
SEEDER_PORT  = int(sys.argv[1]) if len(sys.argv) > 1 else 6881

# Đường dẫn file muốn chia sẻ 
SOURCE_FILE = "C:/Users/Lenovo/Downloads/BvN-Online-Setup.exe"
META_FILE   = os.path.join(os.path.dirname(os.path.abspath(__file__)), "demo_workspace", "shared", "game.meta.json")

def main():
    # Kiểm tra file nguồn
    if not os.path.exists(SOURCE_FILE):
        print(f"File không tồn tại: {SOURCE_FILE}")
        sys.exit(1)
    print(f"File: {os.path.basename(SOURCE_FILE)} ({os.path.getsize(SOURCE_FILE) / 1024 / 1024:.1f} MB)")

    # Tạo metadata nếu chưa có
    os.makedirs(os.path.dirname(META_FILE), exist_ok=True)
    if not os.path.exists(META_FILE):
        print("Đang tạo metadata (tính SHA-256 từng mảnh)...")
        meta = create_metainfo(SOURCE_FILE, tracker_url=f"{TRACKER_HOST}:{TRACKER_PORT}")
        save_metainfo(meta, META_FILE)
    else:
        meta = load_metainfo(META_FILE)
    print(f"Metadata: {os.path.basename(META_FILE)}")

    # Xác minh file và khởi động seeder
    pm = PieceManager(SOURCE_FILE, meta)
    if not pm.is_complete():
        print("File nguồn bị hỏng!")
        sys.exit(1)

    peer_id = f"seeder_{random.randint(1000, 9999)}"
    node = PeerNode(peer_id=peer_id, host="127.0.0.1", port=SEEDER_PORT, piece_manager=pm)
    node.start_listening()

    engine = TransferEngine(peer_id=peer_id, tracker_host=TRACKER_HOST,
                            tracker_port=TRACKER_PORT, listen_port=SEEDER_PORT, piece_manager=pm)
    engine._announce_to_tracker(event="started")

    web_port = 9001 if SEEDER_PORT == 6881 else SEEDER_PORT + 2000
    start_peer_web(peer_id_or_pm=peer_id, piece_manager=pm, engine=engine, peer_node=node, port=web_port)

    print(f"----- Seeder [{peer_id}] dang chay tai cong {SEEDER_PORT} -----")
    print(f"----- Dashboard: http://127.0.0.1:{web_port} -----\n")

    try:
        while True:
            time.sleep(30)
            engine._announce_to_tracker(event="started")
    except KeyboardInterrupt:
        engine._announce_to_tracker(event="stopped")
        node.stop()
        print("\nSeeder da dung.")

if __name__ == "__main__":
    main()
