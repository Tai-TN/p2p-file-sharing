import time
import socket
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

        # Signal channels: { peer_id -> socket } — kênh TCP persistent để push PUNCH_SIGNAL
        # Dùng lock riêng để không hold lock swarm trong khi gọi mạng
        self._signal_channels: Dict[str, Any] = {}   # Any = socket.socket
        self._sc_lock = threading.Lock()

    def register_peer(self, info_hash: str, peer_id: str, ip: str, port: int,
                      downloaded: int = 0, left: int = 0, event: str = "started",
                      udp_port: int = 0, public_ip: str = "") -> bool:
        """
        Đăng ký hoặc cập nhật trạng thái hoạt động của 1 Peer trong Swarm.

        Trường mới:
          udp_port:  UDP port đã NAT-map (từ STUN), dùng cho hole punch.
          public_ip: Public IP từ STUN. Nếu có, Tracker ưu tiên dùng IP này
                     thay vì ip (LAN) khi báo cho peer khác.
        """
        with self.lock:
            if info_hash not in self.swarms:
                self.swarms[info_hash] = {}

            swarm = self.swarms[info_hash]

            if event == "stopped":
                if peer_id in swarm:
                    del swarm[peer_id]
                    print(f"[Tracker State] Peer {peer_id} đã thoát khỏi swarm {info_hash[:8]}...")
                return True

            # Chọn IP quảng bá tốt nhất:
            # Ưu tiên public_ip từ STUN (chính xác nhất cho kết nối qua Internet).
            # Nếu không có, dùng ip (LAN) mà peer tự báo.
            advertised_ip = public_ip if public_ip else ip

            swarm[peer_id] = {
                "peer_id":   peer_id,
                "ip":        advertised_ip,  # IP tốt nhất để peer khác kết nối
                "lan_ip":    ip,             # IP LAN gốc — giữ lại để debug
                "port":      port,
                "udp_port":  udp_port,       # UDP port cho hole punch (0 = chưa có)
                "downloaded": downloaded,
                "left":      left,
                "last_seen": time.time(),
            }
            nat_info = f" UDP={udp_port}" if udp_port else ""
            print(f"[Tracker State] Cập nhật Peer {peer_id} ({advertised_ip}:{port}{nat_info}) "
                  f"cho file {info_hash[:8]}... (event={event})")
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
                    "peer_id":  p_info["peer_id"],
                    "ip":       p_info["ip"],
                    "port":     p_info["port"],
                    "udp_port": p_info.get("udp_port", 0),   # 0 = peer này chưa có STUN
                    "left":     p_info.get("left", 0),
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

    # ── Signal channel management ─────────────────────────────────────────── #

    def register_signal_channel(self, peer_id: str, sock: Any,
                                 ip: str, udp_port: int, tcp_port: int,
                                 candidates: Optional[list] = None) -> None:
        """
        Lưu socket signal channel persistent của một peer.
        Cũng lưu thêm endpoint vào một bảng riêng để PUNCH_REQUEST tra cứu.
        """
        with self._sc_lock:
            # Đóng channel cũ nếu peer kết nối lại
            old = self._signal_channels.get(peer_id)
            if old:
                try:
                    old["sock"].close()
                except Exception:
                    pass
            self._signal_channels[peer_id] = {
                "sock":     sock,
                "ip":       ip,
                "udp_port": udp_port,
                "tcp_port": tcp_port,
                "candidates": candidates or [],
            }
        print(f"[Tracker State] Signal channel đã đăng ký: {peer_id} ({ip}:{udp_port} UDP)")

    def unregister_signal_channel(self, peer_id: str, sock: Any = None) -> None:
        """Xóa signal channel khi peer ngắt kết nối."""
        with self._sc_lock:
            current = self._signal_channels.get(peer_id)
            if not current or (sock is not None and current["sock"] is not sock):
                return
            self._signal_channels.pop(peer_id)
        print(f"[Tracker State] Signal channel đã ngắt: {peer_id}")

    def get_signal_info(self, peer_id: str) -> Optional[Dict[str, Any]]:
        """
        Tra cứu thông tin signal channel của peer.
        Trả về dict { sock, ip, udp_port, tcp_port } hoặc None nếu peer không có channel.
        """
        with self._sc_lock:
            info = self._signal_channels.get(peer_id)
            if info:
                return dict(info)   # trả bản sao để tránh race condition
            return None

    def deliver_punch_signal(self, target_peer_id: str, msg: Dict[str, Any]) -> bool:
        """
        Gửi PUNCH_SIGNAL tới target_peer_id qua signal channel của họ.
        Không giữ lock trong khi gửi mạng.

        Returns:
            True  nếu gửi thành công.
            False nếu peer không có signal channel hoặc gửi lỗi.
        """
        # Lấy socket tham chiếu (ngoài lock)
        info = self.get_signal_info(target_peer_id)
        if not info:
            return False

        sock = info["sock"]
        try:
            from protocol.messages import send_msg
            if send_msg(sock, msg):
                return True
            self.unregister_signal_channel(target_peer_id, sock)
            return False
        except Exception as e:
            print(f"[Tracker State] Lỗi gửi PUNCH_SIGNAL tới {target_peer_id}: {e}")
            self.unregister_signal_channel(target_peer_id, sock)
            return False
