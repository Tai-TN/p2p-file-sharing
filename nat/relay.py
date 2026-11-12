"""Kết nối outbound tới relay trên cùng cổng TCP của tracker."""

import socket
from typing import Callable, Optional

from protocol.auth import connect_tracker
from protocol.messages import MessageType, recv_msg, send_msg


def _connect(host: str, port: int, message: dict, timeout: float,
             on_socket: Optional[Callable[[socket.socket], None]] = None) -> socket.socket:
    sock = connect_tracker(host, port, timeout=timeout)
    try:
        if on_socket:
            on_socket(sock)
        sock.settimeout(timeout)
        if not send_msg(sock, message):
            raise ConnectionError("Không gửi được yêu cầu relay")
        response = recv_msg(sock)
        if not isinstance(response, dict) or response.get("type") != MessageType.RELAY_READY.value:
            reason = response.get("reason", "không có phản hồi") if isinstance(response, dict) else "không có phản hồi"
            raise ConnectionError(f"Relay không sẵn sàng: {reason}")
        sock.settimeout(180.0)
        return sock
    except Exception:
        sock.close()
        raise


def open_relay(host: str, port: int, peer_id: str, target_peer_id: str,
               info_hash: str, timeout: float = 20.0,
               on_socket: Optional[Callable[[socket.socket], None]] = None) -> socket.socket:
    return _connect(host, port, {
        "type": MessageType.RELAY_OPEN.value,
        "peer_id": peer_id,
        "target_peer_id": target_peer_id,
        "info_hash": info_hash,
    }, timeout, on_socket)


def join_relay(host: str, port: int, peer_id: str, session_id: str,
               join_token: str, timeout: float = 20.0,
               on_socket: Optional[Callable[[socket.socket], None]] = None) -> socket.socket:
    return _connect(host, port, {
        "type": MessageType.RELAY_JOIN.value,
        "peer_id": peer_id,
        "session_id": session_id,
        "join_token": join_token,
    }, timeout, on_socket)
