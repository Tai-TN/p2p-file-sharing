import time
import threading
from collections import deque
from typing import Dict, Any, List, Optional

class TrackerState:
    """
    Quản lý bộ nhớ trạng thái trung tâm của Tracker.
    Lưu trữ cấu trúc Swarms: info_hash -> { peer_id -> peer_info }
    Đảm bảo an toàn đa luồng (Thread-safe).
    """
    # 1file -> 1 swarm -> 1 mảng peer có file
    def __init__(self, peer_timeout: float = 60.0, max_events: int = 200):
        self.peer_timeout = peer_timeout
        self.lock = threading.Lock()

        # Cấu trúc: { info_hash: { peer_id: { "ip": str, "port": int, "last_seen": float, "downloaded": int, "left": int } } }
        self.swarms: Dict[str, Dict[str, Dict[str, Any]]] = {}

        # Theo dõi số lần bị tố cáo: { info_hash: { bad_peer_id: set(reporter_ids) } }
        # Nếu >= 2 peer khác nhau tố cáo cùng 1 peer -> xóa ngay
        self._bad_reports: Dict[str, Dict[str, set]] = {}

        # Tên file theo info_hash (peer gửi kèm trong ANNOUNCE) để hiển thị cho admin.
        self.file_names: Dict[str, str] = {}

        # Danh sách chặn do admin đặt: peer_id và IP bị chặn không được vào swarm nào.
        self.blocked_peer_ids: set = set()
        self.blocked_ips: set = set()

        # Nhật ký hoạt động gần đây cho giao diện quản trị (mới nhất ở cuối).
        self.events: deque = deque(maxlen=max_events)

    def _log_event(self, kind: str, text: str) -> None:
        """Ghi một sự kiện vào nhật ký hoạt động (gọi khi đang giữ self.lock)."""
        self.events.append({"time": time.time(), "kind": kind, "text": text})

    def _name_of(self, info_hash: str) -> str:
        return self.file_names.get(info_hash) or (info_hash[:8] + "...")

    def is_blocked(self, peer_id: str, ip: str = "") -> bool:
        """Kiểm tra peer có nằm trong danh sách chặn (theo peer_id hoặc IP) không."""
        with self.lock:
            return peer_id in self.blocked_peer_ids or (bool(ip) and ip in self.blocked_ips)

    def register_peer(self, info_hash: str, peer_id: str, ip: str, port: int, downloaded: int = 0, left: int = 0, event: str = "started", name: str = "") -> bool:
        """Đăng ký hoặc cập nhật trạng thái hoạt động của 1 Peer trong Swarm.

        Trả về False nếu peer bị admin chặn (không được đăng ký).
        """
        with self.lock:
            if peer_id in self.blocked_peer_ids or ip in self.blocked_ips:
                return False

            if name and info_hash not in self.file_names:
                self.file_names[info_hash] = name

            if info_hash not in self.swarms:
                self.swarms[info_hash] = {}

            swarm = self.swarms[info_hash]

            if event == "stopped":
                if peer_id in swarm:
                    del swarm[peer_id]
                    self._log_event("STOPPED", f"{peer_id} rời {self._name_of(info_hash)}")
                    print(f"[Tracker State] Peer {peer_id} đã thoát khỏi swarm {info_hash[:8]}...")
                return True

            is_new = peer_id not in swarm
            was_complete = (not is_new) and swarm[peer_id].get("left", 1) == 0
            swarm[peer_id] = {
                "peer_id": peer_id,
                "ip": ip,
                "port": port,
                "downloaded": downloaded,
                "left": left,
                "last_seen": time.time()
            }
            if is_new:
                self._log_event("ANNOUNCE", f"{peer_id} ({ip}:{port}) tham gia {self._name_of(info_hash)}")
            elif event == "completed" and not was_complete:
                self._log_event("COMPLETED", f"{peer_id} tải xong {self._name_of(info_hash)}")
            print(f"[Tracker State] Cập nhật Peer {peer_id} ({ip}:{port}) cho file {info_hash[:8]}... (event={event})")
            return True

    def get_peers(self, info_hash: str, exclude_peer_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Lấy danh sách các Peer đang online chia sẻ tập tin info_hash (loại trừ chính peer đang hỏi)."""
        with self.lock:
            if info_hash not in self.swarms:
                return []

            active_peers = []
            now = time.time()

            for p_id, p_info in list(self.swarms[info_hash].items()):
                # Loại bỏ peer quá hạn (stale)
                if now - p_info["last_seen"] > self.peer_timeout:
                    continue

                if exclude_peer_id and p_id == exclude_peer_id:
                    continue

                # Peer bị chặn sau khi đã đăng ký thì không được giới thiệu cho ai nữa.
                if p_id in self.blocked_peer_ids or p_info["ip"] in self.blocked_ips:
                    continue

                active_peers.append({
                    "peer_id": p_info["peer_id"],
                    "ip": p_info["ip"],
                    "port": p_info["port"],
                    "left": p_info.get("left", 0),
                })

            return active_peers

    def report_bad_peers(self, info_hash: str, reporter_id: str, bad_peer_ids: List[str]) -> List[str]:
        """
        Nhận báo cáo từ reporter_id rằng các peer trong bad_peer_ids đã chết.
        Nếu >= 2 peer khác nhau tố cáo cùng 1 peer -> xóa ngay khỏi Swarm.
        Trả về danh sách peer đã bị xóa.
        """
        removed = []
        with self.lock:
            if info_hash not in self._bad_reports:
                self._bad_reports[info_hash] = {}

            reports = self._bad_reports[info_hash]
            swarm = self.swarms.get(info_hash, {})

            for bad_id in bad_peer_ids:
                if bad_id == reporter_id:
                    continue  # Không tự tố cáo chính mình

                if bad_id not in reports:
                    reports[bad_id] = set()
                reports[bad_id].add(reporter_id)
                self._log_event(
                    "REPORT",
                    f"{reporter_id} báo {bad_id} mất kết nối ({len(reports[bad_id])} báo cáo)",
                )

                # Đủ ngưỡng tố cáo (>= 2 peer khác nhau) -> xóa ngay
                if len(reports[bad_id]) >= 2 and bad_id in swarm:
                    del swarm[bad_id]
                    del reports[bad_id]  # Reset bộ đếm
                    removed.append(bad_id)
                    self._log_event("REMOVED", f"{bad_id} bị xóa khỏi {self._name_of(info_hash)} do bị báo lỗi")
                    print(f"[Tracker State] Peer {bad_id} bị xóa khỏi swarm {info_hash[:8]}... do bị tố cáo chết bởi >= 2 peer.")

        return removed

    def cleanup_stale_peers(self) -> int:
        """Dọn dẹp các Peer đã dừng hoạt động hoặc mất kết nối quá thời hạn timeout."""
        with self.lock:
            now = time.time()
            removed_count = 0

            for info_hash in list(self.swarms.keys()):
                swarm = self.swarms[info_hash]
                for peer_id in list(swarm.keys()):
                    if now - swarm[peer_id]["last_seen"] > self.peer_timeout:
                        print(f"[Tracker Cleanup] Loại bỏ peer chết: {peer_id} khỏi swarm {info_hash[:8]}...")
                        self._log_event("TIMEOUT", f"{peer_id} quá hạn, bị loại khỏi {self._name_of(info_hash)}")
                        del swarm[peer_id]
                        removed_count += 1

                # Nếu swarm không còn peer nào thì dọn dẹp swarm
                if not swarm:
                    del self.swarms[info_hash]

            return removed_count

    def get_all_swarms_info(self) -> Dict[str, Dict]:
        """Trả về thống kê tổng quan tất cả các Swarm đang hoạt động (dùng để hiển thị trạng thái Tracker)."""
        result = {}
        now = time.time()
        with self.lock:
            for info_hash, swarm in self.swarms.items():
                seeders  = sum(1 for p in swarm.values() if p.get("left", 1) == 0)
                leechers = sum(1 for p in swarm.values() if p.get("left", 1) > 0)
                active   = sum(1 for p in swarm.values() if now - p["last_seen"] <= self.peer_timeout)
                if active > 0:
                    result[info_hash[:8] + "..."] = {
                        "peer_count": active,
                        "seeders":    seeders,
                        "leechers":   leechers
                    }
        return result

    # ── Thao tác quản trị (dùng cho app admin của Tracker) ─────────────── #

    def remove_peer(self, info_hash: str, peer_id: str) -> bool:
        """Admin xóa một peer khỏi một swarm. Peer có thể quay lại ở lần ANNOUNCE sau."""
        with self.lock:
            swarm = self.swarms.get(info_hash)
            if not swarm or peer_id not in swarm:
                return False
            del swarm[peer_id]
            self._bad_reports.get(info_hash, {}).pop(peer_id, None)
            self._log_event("ADMIN", f"Admin xóa {peer_id} khỏi {self._name_of(info_hash)}")
            if not swarm:
                del self.swarms[info_hash]
            return True

    def block_peer(self, peer_id: str, ip: str = "") -> int:
        """
        Admin chặn một peer (theo peer_id, và theo IP nếu truyền vào).
        Xóa peer khỏi mọi swarm; trả về số swarm đã xóa peer.
        """
        with self.lock:
            if peer_id:
                self.blocked_peer_ids.add(peer_id)
            if ip:
                self.blocked_ips.add(ip)
            removed = 0
            for info_hash in list(self.swarms.keys()):
                swarm = self.swarms[info_hash]
                for p_id in list(swarm.keys()):
                    if p_id == peer_id or (ip and swarm[p_id]["ip"] == ip):
                        del swarm[p_id]
                        removed += 1
                if not swarm:
                    del self.swarms[info_hash]
            target = peer_id + (f" ({ip})" if ip else "")
            self._log_event("ADMIN", f"Admin chặn {target}")
            return removed

    def unblock_peer(self, peer_id: str = "", ip: str = "") -> bool:
        """Admin bỏ chặn peer_id và/hoặc IP. Trả về True nếu có mục nào được bỏ chặn."""
        with self.lock:
            changed = False
            if peer_id and peer_id in self.blocked_peer_ids:
                self.blocked_peer_ids.discard(peer_id)
                changed = True
            if ip and ip in self.blocked_ips:
                self.blocked_ips.discard(ip)
                changed = True
            if changed:
                self._log_event("ADMIN", f"Admin bỏ chặn {peer_id or ip}")
            return changed

    def snapshot(self) -> Dict[str, Any]:
        """Ảnh chụp toàn bộ trạng thái Tracker cho giao diện quản trị (chỉ đọc)."""
        now = time.time()
        with self.lock:
            swarms = []
            for info_hash, swarm in self.swarms.items():
                reports = self._bad_reports.get(info_hash, {})
                peers = []
                for p in swarm.values():
                    age = now - p["last_seen"]
                    if age > self.peer_timeout:
                        continue
                    downloaded = p.get("downloaded", 0)
                    left = p.get("left", 0)
                    total = downloaded + left
                    peers.append({
                        "peer_id": p["peer_id"],
                        "ip": p["ip"],
                        "port": p["port"],
                        "left": left,
                        "progress": 100.0 if total == 0 else round(downloaded * 100.0 / total, 1),
                        "is_seeder": left == 0,
                        "last_seen_ago": round(age, 1),
                        "reports": len(reports.get(p["peer_id"], ())),
                    })
                if not peers:
                    continue
                peers.sort(key=lambda x: (not x["is_seeder"], x["peer_id"]))
                swarms.append({
                    "info_hash": info_hash,
                    "name": self._name_of(info_hash),
                    "peers": peers,
                    "seeders": sum(1 for x in peers if x["is_seeder"]),
                    "leechers": sum(1 for x in peers if not x["is_seeder"]),
                })
            swarms.sort(key=lambda s: s["name"].lower())
            return {
                "swarms": swarms,
                "blocked_peer_ids": sorted(self.blocked_peer_ids),
                "blocked_ips": sorted(self.blocked_ips),
                "events": list(self.events)[-50:][::-1],
                "peer_timeout": self.peer_timeout,
            }
