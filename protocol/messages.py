import json
import struct
import base64
import socket as _socket
from enum import Enum
from typing import Dict, Any, List, Optional, Tuple

# Limit framed input before allocating/reading its body (default pieces are 512 KiB).
MAX_MESSAGE_SIZE = 64 * 1024 * 1024
# Giới hạn kích thước binary payload (piece data) — mặc định 512 KiB * 4 an toàn
MAX_BINARY_PAYLOAD = 4 * 1024 * 1024


class ProtocolError(ValueError):
    """A complete frame has invalid length, UTF-8, JSON or message shape."""


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
    Dùng memoryview để tránh copy bytes nhiều lần khi nhận dữ liệu lớn.
    """
    buf = bytearray(num_bytes)
    view = memoryview(buf)
    received = 0
    while received < num_bytes:
        n = sock.recv_into(view[received:], num_bytes - received)
        if not n:
            raise EOFError("Socket đóng trước khi nhận đủ thông điệp")
        received += n
    return bytes(buf)

def send_msg(sock, payload: Dict[str, Any]) -> bool:
    raw_json = json.dumps(payload).encode('utf-8') # dump : Chuyển Dict -> Json
    if not 0 < len(raw_json) <= MAX_MESSAGE_SIZE:
        raise ProtocolError("Kích thước thông điệp vượt giới hạn")
    length_prefix = struct.pack(">I", len(raw_json)) # Đóng gói thành 1 chuỗi nhị phân
    sock.sendall(length_prefix + raw_json)
    return True


def recv_msg(sock) -> Dict[str, Any]:
    """
    Nhận 1 thông điệp JSON hoàn chỉnh từ socket bằng quy tắc Length-prefix framing.
    """
    length_prefix = recv_exact(sock, 4)
    msg_len = struct.unpack(">I", length_prefix)[0]
    if not 0 < msg_len <= MAX_MESSAGE_SIZE:
        raise ProtocolError("Độ dài thông điệp không hợp lệ")
    raw_json = recv_exact(sock, msg_len)
    try:
        result = json.loads(raw_json.decode('utf-8'))
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        raise ProtocolError("JSON không hợp lệ") from e

    if not isinstance(result, dict):
        raise ProtocolError("Thông điệp JSON phải là object")

    return result

# --- CAC HAM TAO MESSAGE MAU (BUILDERS) ---

def create_announce_msg(info_hash: str, peer_id: str, port: int, ip: str = "",
                        event: str = "started", downloaded: int = 0, left: int = 0,
                        bad_peers: Optional[List[str]] = None) -> Dict[str, Any]:
    return {
        "type": MessageType.ANNOUNCE.value,
        "info_hash": info_hash,
        "peer_id": peer_id,
        "ip": ip,           # IP thực của máy — để Tracker báo đúng cho peer khác kết nối vào
        "port": port,
        "event": event,
        "downloaded": downloaded,
        "left": left,
        "bad_peers": bad_peers or []  # Danh sách peer_id bị xác nhận là chết
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
    Tạo PIECE message header. Dùng binary transfer (size field) thay vì Base64.
    Giao thức binary: [4B JSON len][JSON header với size=N][N raw bytes data]
    Hàm này chỉ tạo header JSON; gọi send_piece() để gửi đồng thời header + data.
    """
    return {
        "type": MessageType.PIECE.value,
        "piece_index": piece_index,
        "size": len(data_bytes),   # Báo hiệu binary transfer — không dùng base64
    }


def send_piece(sock, piece_index: int, data_bytes: bytes) -> None:
    """
    Gửi PIECE message theo binary framing:
      [4B JSON len][JSON header {type, piece_index, size}][raw bytes]
    Không dùng Base64 → tiết kiệm 33% bandwidth và CPU.
    """
    header = create_piece_msg(piece_index, data_bytes)
    raw_json = json.dumps(header).encode('utf-8')
    if not 0 < len(raw_json) <= MAX_MESSAGE_SIZE:
        raise ProtocolError("Kích thước header PIECE vượt giới hạn")
    data_len = len(data_bytes)
    if not 0 < data_len <= MAX_BINARY_PAYLOAD:
        raise ProtocolError("Kích thước binary payload PIECE vượt giới hạn")
    # Gửi 1 lần: length_prefix + JSON + raw bytes (giảm số syscall)
    sock.sendall(struct.pack(">I", len(raw_json)) + raw_json + data_bytes)


def recv_piece_data(sock, header: Dict[str, Any]) -> Tuple[int, bytes]:
    """
    Đọc binary payload của PIECE message sau khi đã đọc header JSON bằng recv_msg().
    Trả về (piece_index, raw_bytes).
    Hỗ trợ backward-compat: nếu header có 'data' (base64) thì decode, không đọc thêm socket.
    """
    piece_index = header.get("piece_index")
    if not isinstance(piece_index, int) or isinstance(piece_index, bool) or piece_index < 0:
        raise ProtocolError("piece_index không hợp lệ trong PIECE header")

    # Binary path (mới): header có 'size'
    if "size" in header:
        data_len = header["size"]
        if not isinstance(data_len, int) or isinstance(data_len, bool) or not 0 < data_len <= MAX_BINARY_PAYLOAD:
            raise ProtocolError("Kích thước binary payload không hợp lệ")
        data = recv_exact(sock, data_len)
        return piece_index, data

    # Base64 path (cũ — backward compat):
    encoded = header.get("data")
    if not isinstance(encoded, str):
        raise ProtocolError("PIECE header thiếu 'size' và 'data'")
    try:
        data = base64.b64decode(encoded, validate=True)
    except Exception as exc:
        raise ProtocolError("Base64 decode lỗi") from exc
    return piece_index, data

def create_keepalive_msg() -> Dict[str, Any]:
    return {
        "type": MessageType.KEEP_ALIVE.value
    }
