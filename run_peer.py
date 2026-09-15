import os
import sys
import time
import argparse
import random
from storage.metainfo import create_metainfo, save_metainfo, load_metainfo
from storage.piece_manager import PieceManager
from peer.peer_node import PeerNode
from peer.transfer_engine import TransferEngine

def main():
    parser = argparse.ArgumentParser(description="Chương trình Peer P2P File Sharing (Seeding / Downloading)")
    
    parser.add_argument("--create-meta", type=str, help="Tạo file metainfo (.json) từ tập tin gốc")
    parser.add_argument("--tracker", type=str, default="127.0.0.1:8000", help="Địa chỉ Tracker (mặc định 127.0.0.1:8000)")
    
    parser.add_argument("--seed", action="store_true", help="Chế độ Seeding (phát chia sẻ file gốc)")
    parser.add_argument("--download", action="store_true", help="Chế độ Downloading (tải file từ mạng P2P)")
    
    parser.add_argument("--meta", type=str, help="Đường dẫn file .json metadata")
    parser.add_argument("--file", type=str, help="Đường dẫn file dữ liệu gốc (dùng cho Seeding)")
    parser.add_argument("--out", type=str, help="Đường dẫn file đầu ra sẽ tải về (dùng cho Downloading)")
    parser.add_argument("--port", type=int, default=0, help="Cổng lắng nghe P2P TCP (để 0 sẽ tự chọn cổng trống ngẫu nhiên)")
    parser.add_argument("--peer-id", type=str, default="", help="Tên mã Peer (mặc định tự sinh ngẫu nhiên)")

    args = parser.parse_args()

    # Trường hợp 1: Tạo file metainfo
    if args.create_meta:
        file_path = args.create_meta
        if not os.path.exists(file_path):
            print(f"[Error] Tập tin không tồn tại: {file_path}")
            sys.exit(1)

        print(f"Đang tạo file metadata cho {file_path}...")
        meta = create_metainfo(file_path, tracker_url=args.tracker)
        out_meta_path = f"{file_path}.meta.json"
        save_metainfo(meta, out_meta_path)
        print(f"✅ Đã tạo file metadata thành công: {out_meta_path}")
        print(f"   Info Hash: {meta['info_hash']}")
        print(f"   Số lượng mảnh: {len(meta['info']['pieces'])}")
        return

    # Sinh peer_id ngẫu nhiên nếu không truyền vào
    peer_id = args.peer_id if args.peer_id else f"peer_{random.randint(1000, 9999)}"
    
    # Tự chọn cổng TCP ngẫu nhiên nếu không chỉ định
    listen_port = args.port if args.port != 0 else random.randint(6881, 6999)

    # Đọc địa chỉ Tracker từ chuỗi URL
    tracker_parts = args.tracker.split(":")
    tracker_host = tracker_parts[0]
    tracker_port = int(tracker_parts[1]) if len(tracker_parts) > 1 else 8000

    # Trường hợp 2: Chế độ Seeding (Phát file)
    if args.seed:
        if not args.meta or not args.file:
            print("[Error] Chế độ --seed yêu cầu truyền cả --meta <file.json> và --file <path_file_goc>")
            sys.exit(1)

        meta = load_metainfo(args.meta)
        pm = PieceManager(args.file, meta)

        if not pm.is_complete():
            print("[Error] Tập tin Seeding chưa đầy đủ 100% mảnh!")
            sys.exit(1)

        # Khởi chạy Peer Listener Server
        node = PeerNode(peer_id=peer_id, host="127.0.0.1", port=listen_port, piece_manager=pm)
        node.start_listening()

        # Khởi chạy Transfer Engine gửi ANNOUNCE định kỳ
        engine = TransferEngine(
            peer_id=peer_id,
            tracker_host=tracker_host,
            tracker_port=tracker_port,
            listen_port=listen_port,
            piece_manager=pm
        )
        engine._announce_to_tracker(event="started")

        print(f"\n🚀 [SEEDER {peer_id}] Đã sẵn sàng phát chia sẻ file '{pm.info['name']}' tại cổng {listen_port}")
        print("Nhấn Ctrl+C để dừng Seeding...")

        try:
            while True:
                time.sleep(10)
                engine._announce_to_tracker(event="started")
        except KeyboardInterrupt:
            print(f"\nĐang dừng Seeder {peer_id}...")
            engine._announce_to_tracker(event="stopped")
            node.stop()

    # Trường hợp 3: Chế độ Downloading (Tải file)
    elif args.download:
        if not args.meta or not args.out:
            print("[Error] Chế độ --download yêu cầu truyền cả --meta <file.json> và --out <path_dau_ra>")
            sys.exit(1)

        meta = load_metainfo(args.meta)
        pm = PieceManager(args.out, meta)

        # Bật Peer Listener để sau khi tải xong mảnh nào có thể chia sẻ lại cho Peer khác (Leecher -> Seeder)
        node = PeerNode(peer_id=peer_id, host="127.0.0.1", port=listen_port, piece_manager=pm)
        node.start_listening()

        engine = TransferEngine(
            peer_id=peer_id,
            tracker_host=tracker_host,
            tracker_port=tracker_port,
            listen_port=listen_port,
            piece_manager=pm
        )

        print(f"\n📥 [LEECHER {peer_id}] Bắt đầu tiến trình tải P2P tại cổng {listen_port}...")
        success = engine.start_download(peer_node=node)

        if success:
            print(f"\n✨ Tải thành công! Bắt đầu chuyển sang chế độ Seeder duy trì chia sẻ file...")
            try:
                while True:
                    time.sleep(10)
                    engine._announce_to_tracker(event="started")
            except KeyboardInterrupt:
                engine._announce_to_tracker(event="stopped")
                node.stop()
        else:
            node.stop()

    else:
        parser.print_help()

if __name__ == "__main__":
    main()
