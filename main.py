import argparse
import os

from storage.metainfo import DEFAULT_PIECE_LENGTH, create_metainfo, save_metainfo


def main():
    parser = argparse.ArgumentParser(
        description="Tạo file metadata .meta.json từ file nguồn."
    )
    parser.add_argument("source", help="Đường dẫn đến file nguồn")
    parser.add_argument(
        "output",
        nargs="?",
        help="Đường dẫn file metadata đầu ra (mặc định: <source-stem>.meta.json)",
    )
    parser.add_argument(
        "--piece-length",
        type=int,
        default=DEFAULT_PIECE_LENGTH,
        help=f"Kích thước mỗi piece tính bằng byte (mặc định: {DEFAULT_PIECE_LENGTH})",
    )
    parser.add_argument(
        "--tracker",
        default="127.0.0.1:8000",
        help="Địa chỉ tracker được ghi vào metadata (mặc định: 127.0.0.1:8000)",
    )
    args = parser.parse_args()

    if args.piece_length <= 0:
        parser.error("--piece-length phải lớn hơn 0")

    source_path = os.path.abspath(args.source)
    if not os.path.isfile(source_path):
        parser.error(f"File nguồn không tồn tại: {args.source}")

    output_path = args.output or os.path.splitext(source_path)[0] + ".meta.json"
    output_path = os.path.abspath(output_path)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    metainfo = create_metainfo(
        source_path,
        piece_length=args.piece_length,
        tracker_url=args.tracker,
    )
    save_metainfo(metainfo, output_path)
import argparse
import os
import random
import time

from peer.peer_node import PeerNode
from peer.transfer_engine import TransferEngine
from storage.metainfo import (
    DEFAULT_PIECE_LENGTH,
    create_metainfo,
    load_metainfo,
    save_metainfo,
)
from storage.piece_manager import PieceManager


def _default_meta_path(source_path: str) -> str:
    return os.path.splitext(os.path.abspath(source_path))[0] + ".meta.json"


def create_metadata(args: argparse.Namespace) -> str:
    source_path = os.path.abspath(args.source)
    if not os.path.isfile(source_path):
        raise FileNotFoundError(f"File nguồn không tồn tại: {args.source}")

    output_path = os.path.abspath(args.output or _default_meta_path(source_path))
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    metainfo = create_metainfo(
        source_path,
        piece_length=args.piece_length,
        tracker_url=args.tracker,
    )
    save_metainfo(metainfo, output_path)
    print(f"Đã tạo metadata: {output_path}")
    print(f"Số piece: {len(metainfo['info']['pieces'])}")
    print(f"Info hash: {metainfo['info_hash']}")
    return output_path


def upload_file(args: argparse.Namespace) -> None:
    source_path = os.path.abspath(args.source)
    if not os.path.isfile(source_path):
        raise FileNotFoundError(f"File nguồn không tồn tại: {args.source}")

    meta_path = os.path.abspath(args.meta or _default_meta_path(source_path))
    if os.path.exists(meta_path):
        metainfo = load_metainfo(meta_path)
    else:
        metadata_args = argparse.Namespace(
            source=source_path,
            output=meta_path,
            piece_length=args.piece_length,
            tracker=f"{args.tracker_host}:{args.tracker_port}",
        )
        create_metadata(metadata_args)
        metainfo = load_metainfo(meta_path)

    piece_manager = PieceManager(source_path, metainfo)
    if not piece_manager.is_complete():
        raise ValueError("File nguồn không khớp với metadata hoặc bị hỏng")

    peer_id = f"seeder_{random.randint(1000, 9999)}"
    node = PeerNode(peer_id, args.host, args.listen_port, piece_manager)
    engine = TransferEngine(
        peer_id,
        args.tracker_host,
        args.tracker_port,
        args.listen_port,
        piece_manager,
    )
    node.start_listening()
    engine._announce_to_tracker(event="started")
    print(f"Đang upload: {source_path}")
    print(f"Seeder [{peer_id}] lắng nghe tại {args.host}:{args.listen_port}")

    try:
        while True:
            time.sleep(30)
            engine._announce_to_tracker(event="started")
    except KeyboardInterrupt:
        print("\nĐang dừng upload...")
    finally:
        engine._announce_to_tracker(event="stopped")
        node.stop()


def download_file(args: argparse.Namespace) -> None:
    meta_path = os.path.abspath(args.meta)
    if not os.path.isfile(meta_path):
        raise FileNotFoundError(f"Không tìm thấy metadata: {args.meta}")

    metainfo = load_metainfo(meta_path)
    file_name = metainfo["info"]["name"]
    output_path = os.path.abspath(args.output or os.path.join(args.output_dir, file_name))
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    piece_manager = PieceManager(output_path, metainfo)
    peer_id = f"leecher_{random.randint(1000, 9999)}"
    node = PeerNode(peer_id, args.host, args.listen_port, piece_manager)
    engine = TransferEngine(
        peer_id,
        args.tracker_host,
        args.tracker_port,
        args.listen_port,
        piece_manager,
        max_workers=args.workers,
    )
    node.start_listening()
    print(f"Đang download: {file_name}")
    print(f"Lưu tại: {output_path}")

    try:
        if engine.start_download(peer_node=node):
            print(f"Tải hoàn tất: {output_path}")
        else:
            raise RuntimeError("Tải file thất bại: không tìm thấy seeder hoặc kết nối lỗi")
    finally:
        engine._announce_to_tracker(event="stopped")
        node.stop()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="P2P file sharing: tạo metadata, upload, download")
    subparsers = parser.add_subparsers(dest="command", required=True)

    meta_parser = subparsers.add_parser("meta", help="Tạo file .meta.json từ file nguồn")
    meta_parser.add_argument("source")
    meta_parser.add_argument("output", nargs="?")
    meta_parser.add_argument("--piece-length", type=int, default=DEFAULT_PIECE_LENGTH)
    meta_parser.add_argument("--tracker", default="127.0.0.1:8000")
    meta_parser.set_defaults(handler=create_metadata)

    upload_parser = subparsers.add_parser("upload", help="Upload/chia sẻ một file")
    upload_parser.add_argument("source", help="File cần chia sẻ")
    upload_parser.add_argument("--meta", help="File metadata, mặc định nằm cạnh source")
    upload_parser.add_argument("--host", default="127.0.0.1")
    upload_parser.add_argument("--listen-port", type=int, default=6881)
    upload_parser.add_argument("--tracker-host", default="127.0.0.1")
    upload_parser.add_argument("--tracker-port", type=int, default=8000)
    upload_parser.add_argument("--piece-length", type=int, default=DEFAULT_PIECE_LENGTH)
    upload_parser.set_defaults(handler=upload_file)

    download_parser = subparsers.add_parser("download", help="Download một file theo metadata")
    download_parser.add_argument("meta", help="File .meta.json")
    download_parser.add_argument("--output", help="Đường dẫn file đích cụ thể")
    download_parser.add_argument("--output-dir", default="downloads")
    download_parser.add_argument("--host", default="127.0.0.1")
    download_parser.add_argument("--listen-port", type=int, default=6882)
    download_parser.add_argument("--tracker-host", default="127.0.0.1")
    download_parser.add_argument("--tracker-port", type=int, default=8000)
    download_parser.add_argument("--workers", type=int, default=4)
    download_parser.set_defaults(handler=download_file)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    if getattr(args, "piece_length", 1) <= 0:
        parser.error("--piece-length phải lớn hơn 0")
    if getattr(args, "workers", 1) <= 0:
        parser.error("--workers phải lớn hơn 0")
    try:
        args.handler(args)
    except (FileNotFoundError, ValueError, RuntimeError, OSError) as exc:
        parser.error(str(exc))


if __name__ == "__main__":
    main()

    


if __name__ == "__main__":
    main()

