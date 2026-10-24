import json
import struct
import base64
from enum import Enum
from typing import Dict, Any, Optional

class MessageType(str, Enum):
    # Tracker Messages
    ANNOUNCE = "ANNOUNCE"
    ANNOUNCE_RESPONSE = "ANNOUNCE_RESPONSE"
    GET_PEERS = "GET_PEERS"
    GET_PEERS_RESPONSE = "GET_PEERS_RESPONSE"

    # Peer-to-Peer Messages
    HANDSHAKE = "HANDSHAKE"
    HANDSHAKE_RESPONSE = "HANDSHAKE_RESPONSE"
    BITFIELD = "BITFIELD"
    HAVE = "HAVE"
    REQUEST = "REQUEST"
    PIECE = "PIECE"
    KEEP_ALIVE = "KEEP_ALIVE"
    ERROR = "ERROR"

def recv_exact(sock, num_bytes: int) -> bytes:
    """
    Hàm bổ trợ đọc chính xác N bytes từ TCP Socket.
    """
    buf = bytearray()
    while len(buf) < num_bytes:
        chunk = sock.recv(num_bytes - len(buf)) # Số byte còn thiếu vẫn cần nhận
        if not chunk:
            # Socket bị đóng hoặc hỏng kết nối khi chưa đọc đủ data
            raise EOFError
        buf.extend(chunk)
    return bytes(buf)

def send_msg(sock, payload: Dict[str, Any]) -> bool:
    raw_json = json.dumps(payload).encode('utf-8') # dump : Chuyển Dict -> Json
    length_prefix = struct.pack(">I", len(raw_json)) # Đóng gói thành 1 chuỗi nhị phân
    sock.sendall(length_prefix + raw_json)
    return True


def recv_msg(sock) -> Dict[str, Any]:
    """
    Nhận 1 thông điệp JSON hoàn chỉnh từ socket bằng quy tắc Length-prefix framing.
    """
    length_prefix = recv_exact(sock, 4)
        
    msg_len = struct.unpack(">I", length_prefix)[0]
        
    raw_json = recv_exact(sock, msg_len)
    try:
        result = json.loads(raw_json.decode('utf-8'))
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        raise Exception("JSON khong hop le") from e
    
    if not isinstance(result, dict):
        raise Exception("Thong diep JSON sai kieu du lieu")
        
    return result
    

# --- CAC HAM TAO MESSAGE MAU (BUILDERS) ---

def create_announce_msg(info_hash: str, peer_id: str, port: int, ip: str = "",
                        event: str = "started", downloaded: int = 0, left: int = 0) -> Dict[str, Any]:
    return {
        "type": MessageType.ANNOUNCE.value,
        "info_hash": info_hash,
        "peer_id": peer_id,
        "ip": ip,           # IP thực của máy — để Tracker báo đúng cho peer khác kết nối vào
        "port": port,
        "event": event,
        "downloaded": downloaded,
        "left": left
    }

def create_get_peers_msg(info_hash: str, peer_id: str) -> Dict[str, Any]:
    return {
        "type": MessageType.GET_PEERS.value,
        "info_hash": info_hash,
        "peer_id": peer_id
    }

def create_handshake_msg(info_hash: str, peer_id: str) -> Dict[str, Any]:
    return {
        "type": MessageType.HANDSHAKE.value,
        "info_hash": info_hash,
        "peer_id": peer_id
    }

def create_bitfield_msg(bitfield: list) -> Dict[str, Any]:
    """
    bitfield: Danh sách các giá trị boolean hoặc 0/1 đại diện cho mảnh đã sở hữu.
    VD: [1, 1, 0, 1] nghĩa là có piece 0, 1, 3 và thiếu piece 2.
    """
    return {
        "type": MessageType.BITFIELD.value,
        "bitfield": bitfield
    }

def create_have_msg(piece_index: int) -> Dict[str, Any]:
    return {
        "type": MessageType.HAVE.value,
        "piece_index": piece_index
    }

def create_request_msg(piece_index: int) -> Dict[str, Any]:
    return {
        "type": MessageType.REQUEST.value,
        "piece_index": piece_index
    }

def create_piece_msg(piece_index: int, data_bytes: bytes) -> Dict[str, Any]:
    """
    Mã hóa binary chunk data thành Base64 string để truyền an toàn qua JSON protocol.
    """
    encoded_data = base64.b64encode(data_bytes).decode('utf-8')
    return {
        "type": MessageType.PIECE.value,
        "piece_index": piece_index,
        "data": encoded_data
    }

def create_keepalive_msg() -> Dict[str, Any]:
    return {
        "type": MessageType.KEEP_ALIVE.value
    }
