import time
import threading
from typing import Dict, Any, List, Optional

class TrackerState:
    """
    Quản lý bộ nhớ trạng thái trung tâm của Tracker.
    Lưu trữ cấu trúc Swarms: info_hash -> { peer_id -> peer_info }
    Đảm bảo an toàn đa luồng (Thread-safe).
    """
    def __init__(self, peer_timeout: float = 60.0):
        self.peer_timeout = peer_timeout
        self.lock = threading.Lock()
        
        # Cấu trúc: { info_hash: { peer_id: { "ip": str, "port": int, "last_seen": float, "downloaded": int, "left": int } } }
        self.swarms: Dict[str, Dict[str, Dict[str, Any]]] = {}

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
                        print(f"[Tracker Cleanup] Loại bỏ peer chết: {peer_id} khỏi swarm {info_hash[:8]}...")
                        del swarm[peer_id]
                        removed_count += 1

                # Nếu swarm không còn peer nào thì dọn dẹp swarm
                if not swarm:
                    del self.swarms[info_hash]

            return removed_count
