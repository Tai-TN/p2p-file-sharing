"""
Module Protocol cho hệ thống P2P File Sharing.
Chứa các hàm đóng gói, giải mã thông điệp và xử lý framing trên TCP Socket.
"""

from .messages import (
    MessageType,
    send_msg,
    recv_msg,
    create_announce_msg,
    create_get_peers_msg,
    create_handshake_msg,
    create_bitfield_msg,
    create_have_msg,
    create_request_msg,
    create_piece_msg,
    create_keepalive_msg
)
# xác định những tên được xem là API public của package.
"""
from protocol import *
thì Python sẽ lấy các tên nằm trong __all__:
"""

__all__ = [
    'MessageType',
    'send_msg',
    'recv_msg',
    'create_announce_msg',
    'create_get_peers_msg',
    'create_handshake_msg',
    'create_bitfield_msg',
    'create_have_msg',
    'create_request_msg',
    'create_piece_msg',
    'create_keepalive_msg'
]
