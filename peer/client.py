import os
import socket
import random
import threading
from typing import Dict, List, Optional

from storage.metainfo import create_metainfo, save_metainfo, load_metainfo
from storage.piece_manager import PieceManager
from peer.peer_node import PeerNode
from peer.transfer_engine import TransferEngine


def get_local_ip() -> str:
    """Lấy IP thực của máy trong mạng LAN."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"


class P2PClient:
    """
    Kiến trúc bên trong:
    - 1 PeerNode duy nhất lắng nghe trên listen_port (đóng vai Server upload).
    - Nhiều TransferEngine chạy ngầm, mỗi engine phụ trách 1 file đang download.

    Quy tắc P2P thực sự:
    - Khi bắt đầu DOWNLOAD một file, PieceManager của file đó được đăng ký
      vào PeerNode NGAY LẬP TỨC → vừa tải vừa sẵn sàng chia sẻ những mảnh
      đã có cho các Peer khác trong swarm.
    - Khi tải xong 100%, engine chỉ cần announce "completed" lên Tracker.
      PeerNode tiếp tục phục vụ upload mà không cần làm thêm gì.
    """

    def __init__(self, peer_id: Optional[str] = None, listen_port: int = 6881,
                 tracker_host: str = "127.0.0.1", tracker_port: int = 8000):
        self.peer_id = peer_id or f"peer_{random.randint(1000, 9999)}"
        self.listen_port = listen_port
        self.tracker_host = tracker_host
        self.tracker_port = tracker_port

        # IP thực của máy — báo cho Tracker để peer khác biết kết nối vào đâu
        self.my_ip = get_local_ip()

        # PeerNode bind 0.0.0.0 → chấp nhận kết nối từ mọi máy trong LAN
        self.node = PeerNode(peer_id=self.peer_id, host="0.0.0.0", port=listen_port)

        # Bảng nhiệm vụ: { info_hash: task_dict }
        self._tasks: Dict[str, Dict] = {}
        self._lock = threading.Lock()


    def start(self) -> None:
        """Mở cổng TCP và sẵn sàng nhận kết nối từ Peer khác."""
        self.node.start_listening()
        print(f"[P2PClient {self.peer_id}] Đã khởi động")
        print(f"  IP máy này : {self.my_ip}:{self.listen_port}")
        print(f"  Tracker    : {self.tracker_host}:{self.tracker_port}")

    def stop(self) -> None:
        """Dừng tất cả engine, hủy đăng ký với Tracker, đóng kết nối."""
        with self._lock:
            snapshot = list(self._tasks.values())

        for task in snapshot:
            engine: TransferEngine = task["engine"]
            try:
                engine._announce_to_tracker(event="stopped")
            except Exception:
                pass
            try:
                engine.stop()
            except Exception:
                pass

        self.node.stop()
        print("[P2PClient] Đã dừng hoàn toàn.")

    # ── Chia sẻ file (Seeder)

    def share_file(self, source_file: str, meta_output_path: str) -> str:
        """
        Chia sẻ một file đã có sẵn trên máy lên mạng P2P. Tự tạo file .meta.json nếu chưa có (quá trình này tính SHA-256 từng mảnh — có thể mất vài phút với file lớn).
        Trả về info_hash của file để theo dõi.
        """
        if not os.path.exists(source_file):
            raise FileNotFoundError(f"File không tồn tại: {source_file}")

        # Tạo hoặc load metadata
        os.makedirs(os.path.dirname(os.path.abspath(meta_output_path)), exist_ok=True)
        if not os.path.exists(meta_output_path):
            print(f"[P2PClient] Đang tạo metadata (tính SHA-256)...")
            meta = create_metainfo(
                source_file,
                tracker_url=f"{self.tracker_host}:{self.tracker_port}"
            )
            save_metainfo(meta, meta_output_path)
            print(f"[P2PClient] Đã tạo metadata: {os.path.basename(meta_output_path)}")
        else:
            meta = load_metainfo(meta_output_path)

        info_hash = meta["info_hash"]

        # Kiểm tra đã share file này chưa
        with self._lock:
            if info_hash in self._tasks:
                print(f"[P2PClient] '{meta['info']['name']}' đã được đăng ký rồi.")
                return info_hash

        pm = PieceManager(source_file, meta)

        # Kiểm tra tính toàn vẹn file nguồn
        print(f"  Đang kiểm tra tính toàn vẹn file (SHA-256)...")
        pm.verify_all_pieces()
        if not pm.is_complete():
            raise ValueError("File nguồn không đầy đủ hoặc bị hỏng SHA-256!")

        # Đăng ký vào PeerNode → các Peer khác có thể xin mảnh
        self.node.add_file(info_hash, pm)

        # Tạo engine chỉ để Announce lên Tracker (không tải gì)
        engine = TransferEngine(
            peer_id=self.peer_id,
            tracker_host=self.tracker_host,
            tracker_port=self.tracker_port,
            listen_port=self.listen_port,
            piece_manager=pm,
            my_ip=self.my_ip,
        )
        engine._announce_to_tracker(event="started")

        with self._lock:
            self._tasks[info_hash] = {
                "name": meta["info"]["name"],
                "pm": pm,
                "engine": engine,
                "type": "Share",
                "status": "Seeding",
            }

        print(f"[P2PClient] Đang Seed: {meta['info']['name']}")
        return info_hash

    # ── Tải file (Leecher)
    def download_file(self, meta_file_path: str, download_dir: str) -> str:
        """
        Tải một file từ mạng P2P dựa vào file .meta.json.Tiến trình tải chạy ngầm trên background thread.Trả về info_hash của file.
        """
        if not os.path.exists(meta_file_path):
            raise FileNotFoundError(f"File metadata không tồn tại: {meta_file_path}")

        meta = load_metainfo(meta_file_path)
        info_hash = meta["info_hash"]
        file_name = meta["info"]["name"]

        # Kiểm tra đã có nhiệm vụ này chưa
        with self._lock:
            if info_hash in self._tasks:
                print(f"[P2PClient] Nhiệm vụ '{file_name}' đã tồn tại!")
                return info_hash

        os.makedirs(download_dir, exist_ok=True)
        download_path = os.path.join(download_dir, file_name)

        pm = PieceManager(download_path, meta)

        # Đăng ký vào PeerNode ngay — vừa tải vừa upload những mảnh đã có
        self.node.add_file(info_hash, pm)

        engine = TransferEngine(
            peer_id=self.peer_id,
            tracker_host=self.tracker_host,
            tracker_port=self.tracker_port,
            listen_port=self.listen_port,
            piece_manager=pm,
            max_workers=4,
            my_ip=self.my_ip,
        )

        with self._lock:
            self._tasks[info_hash] = {
                "name": file_name,
                "pm": pm,
                "engine": engine,
                "type": "Download",
                "status": "Downloading...",
            }

        # Chạy tải ngầm
        t = threading.Thread(
            target=self._download_worker,
            args=(info_hash, engine),
            daemon=True,
        )
        t.start()

        print(f"[P2PClient] Đã khởi động tải ngầm: {file_name}")
        return info_hash

    def _download_worker(self, info_hash: str, engine: TransferEngine) -> None:
        """Luồng ngầm chạy TransferEngine cho đến khi tải xong, rồi chuyển sang Seeding."""
        try:
            success = engine.start_download(peer_node=self.node)
            with self._lock:
                if info_hash in self._tasks:
                    self._tasks[info_hash]["status"] = (
                        "Seeding (Hoàn tất)" if success else "Thất bại / Đã dừng"
                    )
        except Exception as e:
            with self._lock:
                if info_hash in self._tasks:
                    self._tasks[info_hash]["status"] = f"Lỗi: {e}"

    #Xem trạng thái
    def get_status_list(self) -> List[Dict]:
        """Trả về danh sách trạng thái của tất cả file đang quản lý."""
        result = []
        with self._lock:
            for info_hash, task in self._tasks.items():
                pm: PieceManager = task["pm"]
                result.append({
                    "name": task["name"],
                    "type": task["type"],
                    "status": task["status"],
                    "progress": f"{pm.progress_percentage():.1f}%",
                    "pieces": f"{pm.completed_pieces_count()}/{pm.num_pieces}",
                    "info_hash": info_hash[:12] + "...",
                })
        return result
