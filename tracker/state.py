import time
import threading
from typing import Dict, Any, List, Optional

class TrackerState:
    """
    Quản lý bộ nhớ trạng thái trung tâm của Tracker.
    Lưu trữ cấu trúc Swarms: info_hash -> { peer_id -> peer_info }
    Đảm bảo an toàn đa luồng (Thread-safe).
    """
    # 1file -> 1 swarm -> 1 mảng peer có file
    def __init__(self, peer_timeout: float = 60.0):
        self.peer_timeout = peer_timeout
        self.lock = threading.Lock()
        
        # Cấu trúc: { info_hash: { peer_id: { "ip": str, "port": int, "last_seen": float, "downloaded": int, "left": int } } }
        self.swarms: Dict[str, Dict[str, Dict[str, Any]]] = {}

        # Theo dõi số lần bị tố cáo: { info_hash: { bad_peer_id: set(reporter_ids) } }
        # Nếu >= 2 peer khác nhau tố cáo cùng 1 peer -> xóa ngay
        self._bad_reports: Dict[str, Dict[str, set]] = {}

    def register_peer(self, info_hash: str, peer_id: str, ip: str, port: int, downloaded: int = 0, left: int = 0, event: str = "started") -> bool:
        """Đăng ký hoặc cập nhật trạng thái hoạt động của 1 Peer trong Swarm."""
        with self.lock:
            if info_hash not in self.swarms:
                self.swarms[info_hash] = {}

            swarm = self.swarms[info_hash]

            if event == "stopped":
                if peer_id in swarm:
                    del swarm[peer_id]
                    print(f"[Tracker State] Peer {peer_id} đã thoát khỏi swarm {info_hash[:8]}...")
                return True

            swarm[peer_id] = {
                "peer_id": peer_id,
                "ip": ip,
                "port": port,
                "downloaded": downloaded,
                "left": left,
                "last_seen": time.time()
            }
            print(f"[Tracker State] Cập nhật Peer {peer_id} ([{ip}]:{port}) cho file {info_hash[:8]}... (event={event})")
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

                active_peers.append({
                    "peer_id": p_info["peer_id"],
                    "ip": p_info["ip"],
                    "port": p_info["port"]
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
            swarm = self.swarms.get(info_hash, {})
            if reporter_id not in swarm:
                return removed
            if info_hash not in self._bad_reports:
                self._bad_reports[info_hash] = {}

            reports = self._bad_reports[info_hash]
            for bad_id in bad_peer_ids:
                if bad_id == reporter_id or bad_id not in swarm:
                    continue  # Không tự tố cáo chính mình

                if bad_id not in reports:
                    reports[bad_id] = set()
                reports[bad_id].add(reporter_id)

                # Đủ ngưỡng tố cáo (>= 2 peer khác nhau) -> xóa ngay
                if len(reports[bad_id]) >= 2 and bad_id in swarm:
                    del swarm[bad_id]
                    del reports[bad_id]  # Reset bộ đếm
                    removed.append(bad_id)
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
                        del swarm[peer_id]
                        self._bad_reports.get(info_hash, {}).pop(peer_id, None)
                        removed_count += 1

                # Nếu swarm không còn peer nào thì dọn dẹp swarm
                if not swarm:
                    del self.swarms[info_hash]
                    self._bad_reports.pop(info_hash, None)

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
