import json
import os
import secrets
import socket
import threading
import time
from typing import Callable, Dict, List, Optional

from protocol.auth import connect_tracker
from protocol.messages import MessageType, create_get_peers_msg, recv_msg, send_msg
from storage.metainfo import create_metainfo, save_metainfo, load_metainfo
from storage.piece_manager import PieceManager
from peer.peer_node import PeerNode
from peer.transfer_engine import TransferEngine
from nat.stun_client import STUNClient, NATInfo, NATType
from nat.signaling import TrackerSignalChannel
from nat.hole_punch import HolePunchManager
from nat.udp_transfer import MAX_WINDOW, MIN_WINDOW, UDPTransfer
from nat.relay import join_relay


def get_local_ip() -> str:
    """Lấy IP thực của máy trong mạng LAN."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect(("8.8.8.8", 80))
            return s.getsockname()[0]
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
    - 1 STUNClient chạy lúc start() để lấy public endpoint và NAT type;
      kết quả được chia sẻ cho mọi TransferEngine qua self.nat_info.
    """

    def __init__(self, peer_id: Optional[str] = None, listen_port: int = 6881,
                 tracker_host: str = "127.0.0.1", tracker_port: int = 8000,
                 advertise_ip: Optional[str] = None, transport: str = "auto",
                 udp_parallel: int = 4, udp_window: int = 48,
                 regular_slots: int = 2, optimistic_slots: int = 1,
                 choking_interval: float = 20.0,
                 measurement_window: float = 30.0,
                 max_workers: int = 4):
        if transport not in ("auto", "tcp", "udp"):
            raise ValueError("transport phải là auto, tcp hoặc udp")
        if not isinstance(udp_parallel, int) or not 1 <= udp_parallel <= 4:
            raise ValueError("udp_parallel phải trong khoảng 1–4")
        if not isinstance(udp_window, int) or not MIN_WINDOW <= udp_window <= MAX_WINDOW:
            raise ValueError(f"udp_window phải trong khoảng {MIN_WINDOW}–{MAX_WINDOW}")
        self.transport = transport
        self.udp_parallel = udp_parallel
        self.peer_id = peer_id or f"peer_{secrets.token_hex(8)}"
        self.listen_port = listen_port
        self.tracker_host = tracker_host
        self.tracker_port = tracker_port
        self.regular_slots = regular_slots
        self.optimistic_slots = optimistic_slots
        self.choking_interval = choking_interval
        self.measurement_window = measurement_window
        self.max_workers = max_workers  # Số luồng tải song song cho mỗi file (chế độ tcp).

        # Địa chỉ LAN mà peer khác dùng để kết nối tới máy này (fallback khi STUN thất bại).
        self.my_ip = advertise_ip if advertise_ip is not None else get_local_ip()

        # PeerNode bind 0.0.0.0 → chấp nhận kết nối từ mọi interface
        self.node = PeerNode(peer_id=self.peer_id, host="0.0.0.0", port=listen_port,
                             regular_slots=regular_slots,
                             optimistic_slots=optimistic_slots,
                             choking_interval=choking_interval,
                             measurement_window=measurement_window)

        # Kết quả STUN — None trước khi start() được gọi
        self.nat_info: Optional[NATInfo] = None

        # Signal channel persistent với Tracker — None cho đến khi start() thành công
        self._signal_channel: Optional[TrackerSignalChannel] = None

        # Trình quản lý đục lỗ NAT
        self._hole_punch_mgr = HolePunchManager(my_peer_id=self.peer_id, local_port=listen_port)
        self._udp_transfer = (UDPTransfer(self._hole_punch_mgr,
                                          self.node.get_piece_manager, window=udp_window)
                              if transport in ("auto", "udp") else None)
        self.udp_candidates: List[Dict] = []

        # Bảng nhiệm vụ: { info_hash: task_dict }
        self._tasks: Dict[str, Dict] = {}
        self._lock = threading.Lock()
        self._punch_active: set[str] = set()
        self._next_punch_signal: Dict[str, float] = {}
        self._relay_active: set[str] = set()
        self._relay_workers: set[threading.Thread] = set()
        self._relay_pending_sockets: set[socket.socket] = set()
        self._stopping = threading.Event()


    def start(self) -> None:
        """Mở cổng TCP, chạy STUN để lấy public endpoint, rồi bắt đầu lắng nghe."""
        self.node.start_listening()
        # Cổng 0 trong test chọn cổng TCP tự động; dùng cổng thực cho ANNOUNCE/STUN.
        if self.listen_port == 0 and self.node.server_socket is not None:
            self.listen_port = self.node.server_socket.getsockname()[1]
            self._hole_punch_mgr.local_port = self.listen_port
            self._hole_punch_mgr.puncher.local_port = self.listen_port

        # Chạy STUN nhanh (quick mode) để lấy public IP:UDP_port
        # Không block lâu: dùng get_public_endpoint() thay vì get_nat_info() đầy đủ
        print(f"[P2PClient {self.peer_id}] Đang kiểm tra NAT qua STUN...")
        udp_sock = None
        try:
            stun = STUNClient(local_port=self.listen_port, timeout=3.0)
            udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            udp_sock.bind(("0.0.0.0", self.listen_port))
            self.nat_info = stun.get_public_endpoint(udp_sock)
            if self.nat_info:
                if self.transport in ("auto", "udp"):
                    self.udp_candidates = stun.gather_candidates(udp_sock, self.nat_info)
                    print(f"[P2PClient {self.peer_id}] Ứng viên UDP: {self.udp_candidates}")
                    mappings = {(c["ip"], c["port"]) for c in self.udp_candidates
                                if c["type"] == "srflx"}
                    if len(mappings) > 1:
                        print(f"[P2PClient {self.peer_id}] Cảnh báo: NAT ánh xạ UDP "
                              "khác nhau theo máy STUN; đường trực tiếp có thể thất bại")
                self._hole_punch_mgr.set_socket(udp_sock, stun.last_server)
                udp_sock = None  # manager sở hữu socket đến khi client dừng
        except Exception as e:
            print(f"[P2PClient {self.peer_id}] STUN thất bại ({e}) — tiếp tục với IP LAN")
            self.nat_info = None
        finally:
            if udp_sock is not None:
                udp_sock.close()

        if self.transport == "udp" and self.nat_info is None:
            self.node.stop()
            raise RuntimeError("Chế độ UDP cần STUN hoạt động để lấy địa chỉ ứng viên")

        print(f"[P2PClient {self.peer_id}] Đã khởi động")
        print(f"  IP LAN     : {self.my_ip}:{self.listen_port}")
        if self.nat_info:
            print(f"  Public IP  : {self.nat_info.public_ip}:{self.nat_info.public_port} (UDP/STUN)")
            print(f"  NAT type   : {self.nat_info.nat_type}")
        else:
            print(f"  Public IP  : (không lấy được — STUN thất bại)")
        print(f"  Tracker    : {self.tracker_host}:{self.tracker_port}")

        # Relay cần signaling ngay cả khi UDP/STUN bị chặn.
        self._signal_channel = TrackerSignalChannel(
            tracker_host=self.tracker_host,
            tracker_port=self.tracker_port,
            peer_id=self.peer_id,
            ip=self.nat_info.public_ip if self.nat_info else self.my_ip,
            udp_port=self.nat_info.public_port if self.nat_info else 0,
            tcp_port=self.listen_port,
            candidates=self.udp_candidates,
        )
        self._signal_channel.on_punch_signal = self._on_punch_signal
        self._signal_channel.on_punch_failed = self._on_punch_failed
        self._signal_channel.on_relay_invite = self._on_relay_invite
        if self._signal_channel.start():
            print(f"[P2PClient {self.peer_id}] Signal channel: sẵn sàng")
        else:
            print(f"[P2PClient {self.peer_id}] Signal channel: đang thử kết nối lại")

    def stop(self) -> None:
        """Dừng tất cả engine, hủy đăng ký với Tracker, đóng kết nối."""
        self._stopping.set()
        with self._lock:
            snapshot = list(self._tasks.values())

        running = [t for t in snapshot if t.get("engine") is not None]
        for task in running:
            try:
                task["engine"].stop()
            except Exception:
                pass
        for task in running:
            thread = task.get("thread")
            if thread and thread is not threading.current_thread():
                thread.join(timeout=2.0)
        for task in running:
            if task.get("state") == STATE_PAUSED:
                continue  # Đã rời swarm lúc tạm dừng
            try:
                task["engine"]._announce_to_tracker(event="stopped")
            except Exception:
                pass

        # Đóng signal channel trước khi dừng node
        if self._signal_channel:
            try:
                self._signal_channel.stop()
            except Exception:
                pass
            self._signal_channel = None

        if self._udp_transfer:
            self._udp_transfer.close()
        self._hole_punch_mgr.close()
        with self._lock:
            pending = list(self._relay_pending_sockets)
        for sock in pending:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            sock.close()
        self.node.stop()
        with self._lock:
            workers = list(self._relay_workers)
        for worker in workers:
            if worker is not threading.current_thread():
                worker.join(timeout=1.0)
        for task in snapshot:
            try:
                task["pm"].close()
            except Exception:
                pass
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
        now = time.monotonic()
        with self._lock:
            if peer_id in self._punch_active or now < self._next_punch_signal.get(peer_id, 0.0):
                return
            self._punch_active.add(peer_id)
            self._next_punch_signal[peer_id] = now + 30.0
        print(f"[P2PClient {self.peer_id}] PUNCH_SIGNAL: peer={peer_id} "
              f"UDP={ip}:{udp_port} TCP={tcp_port} → Bắt đầu đục lỗ...")

        def _punch_worker():
            try:
                # 1. Đục lỗ UDP
                res = self._hole_punch_mgr.handle_punch_signal(msg)
                if res.get("udp_success"):
                    print(f"[P2PClient {self.peer_id}] Đường UDP trực tiếp tới {peer_id} "
                          f"đã xác nhận: {self._hole_punch_mgr.get_endpoint(peer_id)}")

                # UDP hole punching không mở cổng TCP tương ứng.
            finally:
                with self._lock:
                    self._punch_active.discard(peer_id)

        t = threading.Thread(target=_punch_worker, daemon=True, name=f"punch-{peer_id}")
        t.start()

    def _on_punch_failed(self, reason: str) -> None:
        """Callback khi Tracker báo hole punch thất bại."""
        print(f"[P2PClient {self.peer_id}] PUNCH_FAILED: {reason}")

    def _on_relay_invite(self, msg: dict) -> None:
        session_id = msg.get("session_id")
        token = msg.get("join_token")
        info_hash = msg.get("info_hash")
        if (self._stopping.is_set() or not isinstance(session_id, str)
                or not isinstance(token, str) or not isinstance(info_hash, str)
                or self.node.get_piece_manager(info_hash) is None):
            return
        with self._lock:
            if session_id in self._relay_active:
                return
            self._relay_active.add(session_id)

        def worker():
            pending_sock = None
            def remember(sock):
                nonlocal pending_sock
                pending_sock = sock
                with self._lock:
                    self._relay_pending_sockets.add(sock)
            try:
                sock = join_relay(self.tracker_host, self.tracker_port,
                                  self.peer_id, session_id, token, on_socket=remember)
                with self._lock:
                    self._relay_pending_sockets.discard(sock)
                if self._stopping.is_set():
                    sock.close()
                    return
                print(f"[P2PClient {self.peer_id}] RELAY tới {msg.get('peer_id')} sẵn sàng")
                self.node.serve_socket(sock, (self.tracker_host, self.tracker_port))
            except Exception as exc:
                if not self._stopping.is_set():
                    print(f"[P2PClient {self.peer_id}] Không vào được relay: {exc}")
            finally:
                with self._lock:
                    if pending_sock is not None:
                        self._relay_pending_sockets.discard(pending_sock)
                    self._relay_active.discard(session_id)
                    self._relay_workers.discard(threading.current_thread())

        thread = threading.Thread(target=worker, daemon=True, name=f"relay-{session_id[:8]}")
        with self._lock:
            self._relay_workers.add(thread)
        thread.start()

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

        # Seeder cũng cần heartbeat để không hết hạn trên tracker.
        engine = self._new_engine(pm)
        try:
            engine.start_announcing(peer_node=self.node)
        except Exception:
            self.node.remove_file(info_hash)
            pm.close()
            raise

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
            nat_info=self.nat_info,   # public endpoint từ STUN
            signal_channel=self._signal_channel,
            transport=self.transport,
            udp_transfer=self._udp_transfer,
            udp_parallel=self.udp_parallel,
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
            task.pop("error", None)

        # Đăng ký vào PeerNode ngay — vừa tải vừa upload những mảnh đã có
        self.node.add_file(info_hash, pm)

        t = threading.Thread(
            target=self._download_worker,
            args=(info_hash, engine),
            daemon=True,
        )
        with self._lock:
            task["thread"] = t
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
            print(f"[P2PClient {self.peer_id}] Lỗi tải file: {e}")
            with self._lock:
                task = self._tasks.get(info_hash)
                if task is not None and task["engine"] is engine and task["state"] != STATE_PAUSED:
                    task["status"] = f"Lỗi: {e}"
                    task["state"] = STATE_ERROR
                    task["error"] = str(e)

    # ── Điều khiển nhiệm vụ (dùng cho giao diện)

    @staticmethod
    def _stop_engine(engine: TransferEngine, thread: Optional[threading.Thread]) -> None:
        """Dừng engine trước (để heartbeat không đăng ký lại), rồi mới báo rời swarm."""
        engine.stop()
        if thread and thread is not threading.current_thread():
            thread.join(timeout=2.0)
        try:
            engine._announce_to_tracker(event="stopped")
        except Exception:
            pass

    def pause_task(self, info_hash: str) -> bool:
        """Tạm dừng một file đang tải: dừng engine, rời swarm, giữ tiến độ trong .progress.json."""
        with self._lock:
            task = self._tasks.get(info_hash)
            if task is None or task["type"] != "Download" or task["state"] != STATE_DOWNLOADING:
                return False
            task["state"] = STATE_PAUSED
            task["status"] = "Tạm dừng"
            engine: TransferEngine = task["engine"]
            thread = task.get("thread")

        self._stop_engine(engine, thread)  # stop() tự lưu bitfield ra file progress
        self.node.remove_file(info_hash)
        print(f"[P2PClient] Đã tạm dừng: {task['name']}")
        return True

    def resume_task(self, info_hash: str) -> bool:
        """Tiếp tục tải một file đã tạm dừng (hoặc bị lỗi)."""
        with self._lock:
            task = self._tasks.get(info_hash)
            if task is None or task["type"] != "Download" or task["state"] not in (STATE_PAUSED, STATE_ERROR):
                return False
            old_engine = task.get("engine")
            old_thread = task.get("thread")
        if old_engine is not None and task["state"] == STATE_ERROR:
            # Engine lỗi có thể vẫn còn heartbeat chạy; dọn trước khi tạo engine mới.
            self._stop_engine(old_engine, old_thread)
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
            if task["state"] == STATE_PAUSED:
                engine.stop()
            else:
                self._stop_engine(engine, task.get("thread"))
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
            with connect_tracker(self.tracker_host, self.tracker_port, timeout=4.0) as sock:
                if not send_msg(sock, create_get_peers_msg(info_hash, self.peer_id)):
                    return None
                resp = recv_msg(sock)
            if not isinstance(resp, dict) or resp.get("type") != MessageType.GET_PEERS_RESPONSE.value:
                return None
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
                    "status": (f"{task['status']} [{task['engine'].current_path}]"
                               if task["type"] == "Download" and task.get("engine") is not None
                               and task["engine"].current_path
                               else task["status"]),
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
                        "blocked": False, "downloaded_bytes": 0,
                        "path": None}
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
                "path": snap.get("path") if downloading else None,
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
            "nat": (f"{self.nat_info.public_ip}:{self.nat_info.public_port} ({self.nat_info.nat_type})"
                    if self.nat_info else None),
            "tasks": items,
        }
