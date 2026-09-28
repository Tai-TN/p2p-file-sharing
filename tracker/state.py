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
    def __init__(self, peer_timeout: float = 60.0):
        self.peer_timeout = peer_timeout
        self.lock = threading.Lock()
        
        # Cấu trúc: { info_hash: { peer_id: { "ip": str, "port": int, "last_seen": float, "downloaded": int, "left": int } } }
        self.swarms: Dict[str, Dict[str, Dict[str, Any]]] = {}

        # Event log cho Web UI (lưu tối đa 200 sự kiện gần nhất)
        self.event_log: deque = deque(maxlen=200)
        self.start_time: float = time.time()

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
                    self._add_event("peer_left", peer_id, info_hash, ip, port)
                return True

            is_new = peer_id not in swarm

            swarm[peer_id] = {
                "peer_id": peer_id,
                "ip": ip,
                "port": port,
                "downloaded": downloaded,
                "left": left,
                "last_seen": time.time()
            }
            print(f"[Tracker State] Cập nhật Peer {peer_id} ({ip}:{port}) cho file {info_hash[:8]}... (event={event})")

            if event == "completed":
                self._add_event("peer_completed", peer_id, info_hash, ip, port)
            elif is_new:
                role = "Seeder" if left == 0 else "Leecher"
                self._add_event("peer_joined", peer_id, info_hash, ip, port, role)

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

    def cleanup_stale_peers(self) -> int:
        """Dọn dẹp các Peer đã dừng hoạt động hoặc mất kết nối quá thời hạn timeout."""
        with self.lock:
            now = time.time()
            removed_count = 0
            
            for info_hash in list(self.swarms.keys()):
                swarm = self.swarms[info_hash]
                for peer_id in list(swarm.keys()):
                    if now - swarm[peer_id]["last_seen"] > self.peer_timeout:
                        p = swarm[peer_id]
                        print(f"[Tracker Cleanup] Loại bỏ peer chết: {peer_id} khỏi swarm {info_hash[:8]}...")
                        self._add_event("peer_timeout", peer_id, info_hash, p.get("ip", ""), p.get("port", 0))
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

    # ── Các method mới phục vụ Web UI ──

    def _add_event(self, event_type: str, peer_id: str, info_hash: str, ip: str = "", port: int = 0, extra: str = "") -> None:
        """Ghi lại 1 sự kiện vào event log (được gọi bên trong lock)."""
        self.event_log.append({
            "time": time.strftime("%H:%M:%S"),
            "timestamp": time.time(),
            "type": event_type,
            "peer_id": peer_id,
            "info_hash": info_hash[:8] + "...",
            "ip": ip,
            "port": port,
            "extra": extra,
        })

    def get_events(self, limit: int = 50) -> List[Dict]:
        """Trả về danh sách sự kiện gần nhất (mới nhất ở đầu)."""
        with self.lock:
            return list(reversed(list(self.event_log)))[:limit]

    def get_detailed_swarms_info(self) -> Dict[str, Dict]:
        """Trả về thông tin chi tiết từng peer trong mỗi swarm (dùng cho Web UI)."""
        result = {}
        now = time.time()
        with self.lock:
            for info_hash, swarm in self.swarms.items():
                peers = []
                for p_id, p_info in swarm.items():
                    if now - p_info["last_seen"] > self.peer_timeout:
                        continue
                    peers.append({
                        "peer_id": p_info["peer_id"],
                        "ip": p_info["ip"],
                        "port": p_info["port"],
                        "downloaded": p_info.get("downloaded", 0),
                        "left": p_info.get("left", 0),
                        "last_seen_ago": int(now - p_info["last_seen"]),
                        "role": "Seeder" if p_info.get("left", 1) == 0 else "Leecher",
                    })
                if peers:
                    seeders = sum(1 for p in peers if p["role"] == "Seeder")
                    result[info_hash] = {
                        "info_hash_short": info_hash[:8] + "...",
                        "total_peers": len(peers),
                        "seeders": seeders,
                        "leechers": len(peers) - seeders,
                        "peers": peers,
                    }
        return result
