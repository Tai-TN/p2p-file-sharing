import json
import os
import socket
import random
import threading
from typing import Callable, Dict, List, Optional

from protocol.messages import create_get_peers_msg, recv_msg, send_msg
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


def check_tracker(host: str, port: int, timeout: float = 3.0) -> bool:
    """Thử mở kết nối TCP tới Tracker; True nếu Tracker đang nhận kết nối."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


# Trạng thái nhiệm vụ dùng cho giao diện (khác với chuỗi "status" hiển thị ở CLI).
STATE_DOWNLOADING = "downloading"
STATE_PAUSED = "paused"
STATE_SEEDING = "seeding"
STATE_ERROR = "error"


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
                 tracker_host: str = "127.0.0.1", tracker_port: int = 8000,
                 regular_slots: int = 2, optimistic_slots: int = 1,
                 choking_interval: float = 20.0,
                 measurement_window: float = 30.0,
                 max_workers: int = 4):
        self.peer_id = peer_id or f"peer_{random.randint(1000, 9999)}"
        self.listen_port = listen_port
        self.tracker_host = tracker_host
        self.tracker_port = tracker_port
        # Các tham số này được truyền xuống PeerNode để cấu hình Tit-for-Tat.
        self.regular_slots = regular_slots  # Số upload slot dành cho Peer đóng góp tốt.
        self.optimistic_slots = optimistic_slots  # Số slot thử nghiệm ngẫu nhiên.
        self.choking_interval = choking_interval  # Chu kỳ đổi danh sách unchoke (giây).
        self.measurement_window = measurement_window  # Cửa sổ đo tốc độ đóng góp (giây).
        self.max_workers = max_workers  # Số luồng tải song song cho mỗi file.

        # IP thực của máy — báo cho Tracker để peer khác biết kết nối vào đâu
        self.my_ip = get_local_ip()

        # PeerNode bind 0.0.0.0 → chấp nhận kết nối từ mọi máy trong LAN
        self.node = PeerNode(
            peer_id=self.peer_id,
            host="0.0.0.0",
            port=listen_port,
            regular_slots=regular_slots,
            optimistic_slots=optimistic_slots,
            choking_interval=choking_interval,
            measurement_window=measurement_window,
        )

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
            engine: Optional[TransferEngine] = task.get("engine")
            if engine is None:
                continue
            if task.get("state") != STATE_PAUSED:
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

    def share_file(self, source_file: str, meta_output_path: str,
                   progress_cb: Optional[Callable[[str, int, int], None]] = None) -> str:
        """
        Chia sẻ một file đã có sẵn trên máy lên mạng P2P. Tự tạo file .meta.json nếu chưa có (quá trình này tính SHA-256 từng mảnh — có thể mất vài phút với file lớn).
        Trả về info_hash của file để theo dõi.

        progress_cb(stage, done, total): stage là "hash" (tạo metadata) hoặc "verify"
        (kiểm tra file nguồn); dùng để vẽ thanh tiến độ trên giao diện.
        """
        if not os.path.exists(source_file):
            raise FileNotFoundError(f"File không tồn tại: {source_file}")

        def stage_cb(stage: str):
            if progress_cb is None:
                return None
            return lambda done, total: progress_cb(stage, done, total)

        # Tạo hoặc load metadata
        os.makedirs(os.path.dirname(os.path.abspath(meta_output_path)), exist_ok=True)
        if not os.path.exists(meta_output_path):
            print(f"[P2PClient] Đang tạo metadata (tính SHA-256)...")
            meta = create_metainfo(
                source_file,
                tracker_url=f"{self.tracker_host}:{self.tracker_port}",
                on_progress=stage_cb("hash"),
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
        pm.verify_all_pieces(on_progress=stage_cb("verify"))
        if not pm.is_complete():
            raise ValueError("File nguồn không đầy đủ hoặc bị hỏng SHA-256!")

        # Đăng ký vào PeerNode → các Peer khác có thể xin mảnh
        self.node.add_file(info_hash, pm)

        # Tạo engine để Announce lên Tracker và duy trì Seeding
        engine = self._new_engine(pm)
        engine.start_seeding()

        with self._lock:
            self._tasks[info_hash] = {
                "name": meta["info"]["name"],
                "pm": pm,
                "engine": engine,
                "type": "Share",
                "status": "Seeding",
                "state": STATE_SEEDING,
                "meta_path": os.path.abspath(meta_output_path),
                "file_path": os.path.abspath(source_file),
            }

        print(f"[P2PClient] Đang Seed: {meta['info']['name']}")
        return info_hash

    # ── Tải file (Leecher)
    def download_file(self, meta_file_path: str, download_dir: str, start_paused: bool = False) -> str:
        """
        Tải một file từ mạng P2P dựa vào file .meta.json.Tiến trình tải chạy ngầm trên background thread.Trả về info_hash của file.
        start_paused=True: chỉ thêm vào danh sách ở trạng thái Tạm dừng (dùng khi mở lại app).
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

        with self._lock:
            self._tasks[info_hash] = {
                "name": file_name,
                "pm": pm,
                "engine": None,
                "type": "Download",
                "status": "Downloading...",
                "state": STATE_DOWNLOADING,
                "meta_path": os.path.abspath(meta_file_path),
                "file_path": os.path.abspath(download_path),
            }
            if start_paused:
                self._tasks[info_hash]["state"] = STATE_PAUSED
                self._tasks[info_hash]["status"] = "Tạm dừng"

        if start_paused:
            return info_hash
        self._start_download_engine(info_hash)
        print(f"[P2PClient] Đã khởi động tải ngầm: {file_name}")
        return info_hash

    def _new_engine(self, pm: PieceManager) -> TransferEngine:
        return TransferEngine(
            peer_id=self.peer_id,
            tracker_host=self.tracker_host,
            tracker_port=self.tracker_port,
            listen_port=self.listen_port,
            piece_manager=pm,
            max_workers=self.max_workers,
            my_ip=self.my_ip,
        )

    def _start_download_engine(self, info_hash: str) -> None:
        """Tạo engine mới cho nhiệm vụ tải và chạy nó trên luồng ngầm."""
        with self._lock:
            task = self._tasks[info_hash]
            pm: PieceManager = task["pm"]
            engine = self._new_engine(pm)
            task["engine"] = engine
            task["state"] = STATE_DOWNLOADING
            task["status"] = "Downloading..."

        # Đăng ký vào PeerNode ngay — vừa tải vừa upload những mảnh đã có
        self.node.add_file(info_hash, pm)

        t = threading.Thread(
            target=self._download_worker,
            args=(info_hash, engine),
            daemon=True,
        )
        t.start()

    def _download_worker(self, info_hash: str, engine: TransferEngine) -> None:
        """Luồng ngầm chạy TransferEngine cho đến khi tải xong, rồi chuyển sang Seeding."""
        try:
            success = engine.start_download(peer_node=self.node)
            with self._lock:
                task = self._tasks.get(info_hash)
                # Engine cũ (đã bị tạm dừng / thay thế) không được ghi đè trạng thái.
                if task is None or task["engine"] is not engine:
                    return
                if success:
                    task["status"] = "Seeding (Hoàn tất)"
                    task["state"] = STATE_SEEDING
                elif task["state"] != STATE_PAUSED:
                    task["status"] = "Thất bại / Đã dừng"
                    task["state"] = STATE_ERROR
        except Exception as e:
            with self._lock:
                task = self._tasks.get(info_hash)
                if task is not None and task["engine"] is engine:
                    task["status"] = f"Lỗi: {e}"
                    task["state"] = STATE_ERROR
                    task["error"] = str(e)

    # ── Điều khiển nhiệm vụ (dùng cho giao diện)

    def pause_task(self, info_hash: str) -> bool:
        """Tạm dừng một file đang tải: dừng engine, rời swarm, giữ tiến độ trong .progress.json."""
        with self._lock:
            task = self._tasks.get(info_hash)
            if task is None or task["type"] != "Download" or task["state"] != STATE_DOWNLOADING:
                return False
            task["state"] = STATE_PAUSED
            task["status"] = "Tạm dừng"
            engine: TransferEngine = task["engine"]

        try:
            engine._announce_to_tracker(event="stopped")
        except Exception:
            pass
        engine.stop()  # stop() tự lưu bitfield ra file progress
        self.node.remove_file(info_hash)
        print(f"[P2PClient] Đã tạm dừng: {task['name']}")
        return True

    def resume_task(self, info_hash: str) -> bool:
        """Tiếp tục tải một file đã tạm dừng (hoặc bị lỗi)."""
        with self._lock:
            task = self._tasks.get(info_hash)
            if task is None or task["type"] != "Download" or task["state"] not in (STATE_PAUSED, STATE_ERROR):
                return False
        self._start_download_engine(info_hash)
        print(f"[P2PClient] Tiếp tục tải: {task['name']}")
        return True

    def remove_task(self, info_hash: str, delete_file: bool = False) -> bool:
        """
        Gỡ một file khỏi danh sách: rời swarm và ngừng chia sẻ.
        delete_file=True chỉ áp dụng cho file đang tải dở: xóa luôn dữ liệu và file progress.
        """
        with self._lock:
            task = self._tasks.pop(info_hash, None)
        if task is None:
            return False

        engine: Optional[TransferEngine] = task.get("engine")
        if engine is not None:
            if task["state"] != STATE_PAUSED:
                try:
                    engine._announce_to_tracker(event="stopped")
                except Exception:
                    pass
            engine.stop()
        self.node.remove_file(info_hash)
        try:
            task["pm"].close()
        except Exception:
            pass

        if delete_file and task["type"] == "Download" and not task["pm"].is_complete():
            for path in (task["file_path"], task["file_path"] + ".progress.json"):
                try:
                    if os.path.exists(path):
                        os.remove(path)
                except OSError:
                    pass
        print(f"[P2PClient] Đã gỡ: {task['name']}")
        return True

    # ── Xem trước trước khi tải

    def query_peers(self, info_hash: str) -> Optional[List[Dict]]:
        """Hỏi Tracker danh sách peer của một file. None nếu không liên lạc được Tracker."""
        try:
            with socket.create_connection((self.tracker_host, self.tracker_port), timeout=4.0) as sock:
                send_msg(sock, create_get_peers_msg(info_hash, self.peer_id))
                resp = recv_msg(sock)
            return list(resp.get("peers") or [])
        except Exception:
            return None

    def preview_meta(self, meta_file_path: str, download_dir: str) -> Dict:
        """Đọc file .meta.json và cho biết thông tin file, số peer đang có, tiến độ tải dở (nếu có)."""
        meta = load_metainfo(meta_file_path)
        info = meta["info"]
        info_hash = meta["info_hash"]

        peers = self.query_peers(info_hash)
        target = os.path.join(download_dir, info["name"])

        resume_progress = None
        progress_file = target + ".progress.json"
        if os.path.exists(progress_file):
            try:
                with open(progress_file, "r") as f:
                    data = json.load(f)
                if data.get("info_hash") == info_hash:
                    bf = data.get("bitfield") or []
                    if bf:
                        resume_progress = round(sum(1 for b in bf if b) * 100.0 / len(bf), 1)
            except Exception:
                pass

        with self._lock:
            existing = self._tasks.get(info_hash)

        return {
            "info_hash": info_hash,
            "name": info["name"],
            "file_size": info["file_size"],
            "piece_length": info["piece_length"],
            "num_pieces": len(info["pieces"]),
            "tracker": meta.get("tracker", ""),
            "tracker_ok": peers is not None,
            "peer_count": len(peers) if peers is not None else 0,
            "seeder_count": sum(1 for p in (peers or []) if p.get("left", 1) == 0),
            "target_path": target,
            "target_exists": os.path.exists(target),
            "resume_progress": resume_progress,
            "already_added": existing is not None,
        }

    # ── Xem trạng thái

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

    def snapshot(self) -> Dict:
        """Ảnh chụp đầy đủ trạng thái client cho giao diện (chỉ đọc)."""
        with self._lock:
            tasks = [(h, dict(t)) for h, t in self._tasks.items()]

        items = []
        total_down = 0.0
        tracker_flags = []
        for info_hash, task in tasks:
            engine: Optional[TransferEngine] = task.get("engine")
            pm: PieceManager = task["pm"]
            if engine is not None:
                snap = engine.snapshot()
                tracker_flags.append(snap["tracker_ok"])
            else:
                snap = {"download_rate": 0.0, "peers": [], "eta_seconds": None,
                        "blocked": False, "downloaded_bytes": 0}
            downloading = task["state"] == STATE_DOWNLOADING
            down_rate = snap["download_rate"] if downloading else 0.0
            total_down += down_rate
            items.append({
                "info_hash": info_hash,
                "name": task["name"],
                "type": task["type"],
                "state": task["state"],
                "status": task["status"],
                "error": task.get("error"),
                "file_path": task.get("file_path"),
                "meta_path": task.get("meta_path"),
                "file_size": pm.file_size,
                "num_pieces": pm.num_pieces,
                "have_pieces": pm.completed_pieces_count(),
                "progress": round(pm.progress_percentage(), 1),
                "downloaded_bytes": snap.get("downloaded_bytes", 0),
                "download_rate": down_rate,
                "upload_rate": self.node.file_upload_rate(info_hash),
                "uploaded_bytes": self.node.file_uploaded_bytes(info_hash),
                "eta_seconds": snap.get("eta_seconds") if downloading else None,
                "download_peers": snap.get("peers", []) if downloading else [],
                "upload_peers": self.node.snapshot_uploads(info_hash),
                "blocked": snap.get("blocked", False),
            })

        # Tracker được coi là "ok" nếu lần liên lạc gần nhất của bất kỳ engine nào thành công.
        tracker_ok = None
        if any(flag is True for flag in tracker_flags):
            tracker_ok = True
        elif any(flag is False for flag in tracker_flags):
            tracker_ok = False

        return {
            "peer_id": self.peer_id,
            "ip": self.my_ip,
            "port": self.listen_port,
            "tracker": f"{self.tracker_host}:{self.tracker_port}",
            "tracker_ok": tracker_ok,
            "download_rate": total_down,
            "upload_rate": self.node.upload_meter.rate(),
            "tasks": items,
        }
