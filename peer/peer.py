import socket
import threading
import hashlib
from cryptography.hazmat.primitives.asymmetric import ed25519
from cryptography.hazmat.primitives import serialization
from storage.piece_manager import PieceManager
from peer.peer_node import PeerNode
from peer.transfer_engine import TransferEngine

class Peer:
    def __init__(self, host: str, port: int, tracker_host: str, tracker_port: int, piece_manager: PieceManager):
        # 1. Tạo cặp khóa & Peer ID duy nhất
        self.private_key = ed25519.Ed25519PrivateKey.generate()
        self.public_key = self.private_key.public_key()
        self.id = self.generate_peer_id()

        self.host = host
        self.port = port
        self.piece_manager = piece_manager
        
        # 2. Thành phần Upload (Server tiếp nhận kết nối và gửi mảnh đi)
        self.node = PeerNode(
            peer_id=self.id, 
            host=self.host, 
            port=self.port, 
            piece_manager=self.piece_manager
        )

        # 3. Thành phần Download (Client kết nối Tracker, kéo mảnh từ peer khác)
        self.engine = TransferEngine(
            peer_id=self.id,
            tracker_host=tracker_host,
            tracker_port=tracker_port,
            listen_port=self.port,
            piece_manager=self.piece_manager
        )

    def generate_peer_id(self) -> str:
        pub_bytes = self.public_key.public_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PublicFormat.Raw
        )
        return hashlib.sha256(pub_bytes).hexdigest()[:40]

    def start(self):
        """Khởi động đồng thời cả 2 chiều: Lắng nghe phục vụ upload và bắt đầu tải file."""
        print(f"[{self.id}] Khởi động Peer tại cổng {self.port}...")
        
        # Bật server upload (chạy ngầm daemon thread)
        self.node.start_listening()

        # Bật engine download (kết nối tracker và tải)
        download_success = self.engine.start_download(peer_node=self.node)
        return download_success

    def stop(self):
        """Dừng an toàn cả 2 chiều."""
        self.engine.is_running = False
        self.node.stop()