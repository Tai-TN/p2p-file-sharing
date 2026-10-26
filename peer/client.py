import os
import socket
import random
import threading
from typing import Dict, List, Optional

from storage.metainfo import create_metainfo, save_metainfo, load_metainfo
from storage.piece_manager import PieceManager
from peer.peer_node import PeerNode
from peer.transfer_engine import TransferEngine
from nat.stun_client import STUNClient, NATInfo, NATType
from nat.signaling import TrackerSignalChannel
from nat.hole_punch import HolePunchManager


def get_local_ip() -> str:
    """Lấy IP thực của máy trong mạng LAN."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect(("8.8.8.8", 80))
            return s.getsockname()[0]
    except Exception:
        return "127.0.0.1"


class P2PClient:
    """
    Kiến trúc bên trong:
    - 1 PeerNode duy nhất lắng nghe trên listen_port (đóng vai Server upload).
    - Nhiều TransferEngine chạy ngầm, mỗi engine phụ trách 1 file đang download.
    - 1 STUNClient chạy lúc start() để lấy public endpoint và NAT type;
      kết quả được chia sẻ cho mọi TransferEngine qua self.nat_info.
    """

    def __init__(self, peer_id: Optional[str] = None, listen_port: int = 6881,
                 tracker_host: str = "127.0.0.1", tracker_port: int = 8000,
                 advertise_ip: Optional[str] = None):
        self.peer_id = peer_id or f"peer_{random.randint(1000, 9999)}"
        self.listen_port = listen_port
        self.tracker_host = tracker_host
        self.tracker_port = tracker_port

        # Địa chỉ LAN mà peer khác dùng để kết nối tới máy này (fallback khi STUN thất bại).
        self.my_ip = advertise_ip if advertise_ip is not None else get_local_ip()

        # PeerNode bind 0.0.0.0 → chấp nhận kết nối từ mọi interface
        self.node = PeerNode(peer_id=self.peer_id, host="0.0.0.0", port=listen_port)

        # Kết quả STUN — None trước khi start() được gọi
        self.nat_info: Optional[NATInfo] = None

        # Signal channel persistent với Tracker — None cho đến khi start() thành công
        self._signal_channel: Optional[TrackerSignalChannel] = None

        # Trình quản lý đục lỗ NAT
        self._hole_punch_mgr = HolePunchManager(my_peer_id=self.peer_id, local_port=listen_port)

        # Bảng nhiệm vụ: { info_hash: task_dict }
        self._tasks: Dict[str, Dict] = {}
        self._lock = threading.Lock()


    def start(self) -> None:
        """Mở cổng TCP, chạy STUN để lấy public endpoint, rồi bắt đầu lắng nghe."""
        self.node.start_listening()

        # Chạy STUN nhanh (quick mode) để lấy public IP:UDP_port
        # Không block lâu: dùng get_public_endpoint() thay vì get_nat_info() đầy đủ
        print(f"[P2PClient {self.peer_id}] Đang kiểm tra NAT qua STUN...")
        try:
            stun = STUNClient(local_port=self.listen_port, timeout=3.0)
            self.nat_info = stun.get_public_endpoint()
        except Exception as e:
            print(f"[P2PClient {self.peer_id}] STUN thất bại ({e}) — tiếp tục với IP LAN")
            self.nat_info = None

        print(f"[P2PClient {self.peer_id}] Đã khởi động")
        print(f"  IP LAN     : {self.my_ip}:{self.listen_port}")
        if self.nat_info:
            print(f"  Public IP  : {self.nat_info.public_ip}:{self.nat_info.public_port} (UDP/STUN)")
            print(f"  NAT type   : {self.nat_info.nat_type}")
        else:
            print(f"  Public IP  : (không lấy được — STUN thất bại)")
        print(f"  Tracker    : {self.tracker_host}:{self.tracker_port}")

        # Mở signal channel với Tracker (chỉ khi có STUN info)
        if self.nat_info:
            self._signal_channel = TrackerSignalChannel(
                tracker_host=self.tracker_host,
                tracker_port=self.tracker_port,
                peer_id=self.peer_id,
                ip=self.nat_info.public_ip,
                udp_port=self.nat_info.public_port,
                tcp_port=self.listen_port,
            )
            self._signal_channel.on_punch_signal = self._on_punch_signal
            self._signal_channel.on_punch_failed  = self._on_punch_failed
            if self._signal_channel.start():
                print(f"[P2PClient {self.peer_id}] Signal channel: sẵn sàng")
            else:
                print(f"[P2PClient {self.peer_id}] Signal channel: không kết nối được "
                      f"(Tracker chưa hỗ trợ hoặc chưa online)")
                self._signal_channel = None

    def stop(self) -> None:
        """Dừng tất cả engine, hủy đăng ký với Tracker, đóng kết nối."""
        with self._lock:
            snapshot = list(self._tasks.values())

        for task in snapshot:
            engine: TransferEngine = task["engine"]
            try:
                engine.stop()
            except Exception:
                pass
            try:
                engine._announce_to_tracker(event="stopped")
            except Exception:
                pass

        # Đóng signal channel trước khi dừng node
        if self._signal_channel:
            try:
                self._signal_channel.stop()
            except Exception:
                pass
            self._signal_channel = None

        self.node.stop()
        print("[P2PClient] Đã dừng hoàn toàn.")

    # ── NAT traversal callbacks ───────────────────────────────────────────── #

    def _on_punch_signal(self, msg: dict) -> None:
        """
        Callback khi Tracker gửi PUNCH_SIGNAL — có peer muốn hole punch tới mình.
        Kích hoạt UDP hole punching và kết nối hai chiều trong background thread.
        """
        peer_id  = msg.get("peer_id", "?")
        ip       = msg.get("ip", "?")
        udp_port = msg.get("udp_port", 0)
        tcp_port = msg.get("tcp_port", 0)
        print(f"[P2PClient {self.peer_id}] PUNCH_SIGNAL: peer={peer_id} "
              f"UDP={ip}:{udp_port} TCP={tcp_port} → Bắt đầu đục lỗ...")

        def _punch_worker():
            # 1. Đục lỗ UDP
            res = self._hole_punch_mgr.handle_punch_signal(msg)
            if res.get("udp_success"):
                print(f"[P2PClient {self.peer_id}] Đục lỗ UDP tới {peer_id} thành công!")

            # 2. Thử kết nối TCP từ các engine đang tải
            with self._lock:
                engines = [t["engine"] for t in self._tasks.values() if "engine" in t]
            for eng in engines:
                if not eng.piece_manager.is_complete():
                    eng._update_peer_connections([{
                        "peer_id": peer_id,
                        "ip": ip,
                        "port": tcp_port,
                        "udp_port": udp_port,
                    }])

        t = threading.Thread(target=_punch_worker, daemon=True, name=f"punch-{peer_id}")
        t.start()

    def _on_punch_failed(self, reason: str) -> None:
        """Callback khi Tracker báo hole punch thất bại."""
        print(f"[P2PClient {self.peer_id}] PUNCH_FAILED: {reason}")

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

        # Seeder cũng cần heartbeat để không hết hạn trên tracker.
        engine = TransferEngine(
            peer_id=self.peer_id,
            tracker_host=self.tracker_host,
            tracker_port=self.tracker_port,
            listen_port=self.listen_port,
            piece_manager=pm,
            my_ip=self.my_ip,
            nat_info=self.nat_info,   # public endpoint từ STUN
            signal_channel=self._signal_channel,
        )
        try:
            engine.start_announcing()
        except Exception:
            self.node.remove_file(info_hash)
            raise

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
            nat_info=self.nat_info,   # public endpoint từ STUN
            signal_channel=self._signal_channel,
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
            print(f"[P2PClient {self.peer_id}] Lỗi tải file: {e}")
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
