"""Xác thực bí mật chung cho kết nối tới tracker công khai.

Đây chỉ là kiểm soát truy cập tracker/relay; dữ liệu file không được mã hóa.
"""

import hashlib
import hmac
import os
import secrets
import socket

from .messages import MessageType, recv_msg, send_msg


def configured_secret() -> str:
    return os.environ.get("P2P_TRACKER_SECRET", "")


def connect_tracker(host: str, port: int, timeout: float = 5.0) -> socket.socket:
    sock = socket.create_connection((host, port), timeout=timeout)
    try:
        secret = configured_secret()
        if secret:
            challenge = recv_msg(sock)
            if not isinstance(challenge, dict) or challenge.get("type") != MessageType.AUTH_CHALLENGE.value:
                raise ConnectionError("Tracker không hỗ trợ xác thực hoặc không phản hồi")
            nonce = challenge.get("nonce")
            if not isinstance(nonce, str) or len(nonce) != 32:
                raise ConnectionError("Thử thách xác thực không hợp lệ")
            digest = hmac.new(secret.encode(), nonce.encode("ascii"), hashlib.sha256).hexdigest()
            if not send_msg(sock, {"type": MessageType.AUTH_RESPONSE.value, "digest": digest}):
                raise ConnectionError("Không gửi được xác thực tới tracker")
            try:
                result = recv_msg(sock)
            except EOFError as exc:
                raise PermissionError("Tracker từ chối bí mật chung") from exc
            if not isinstance(result, dict) or result.get("type") != MessageType.AUTH_OK.value:
                raise PermissionError("Tracker từ chối bí mật chung")
        return sock
    except Exception:
        sock.close()
        raise


def authenticate_client(sock: socket.socket, secret: str) -> bool:
    if not secret:
        return True
    nonce = secrets.token_hex(16)
    if not send_msg(sock, {"type": MessageType.AUTH_CHALLENGE.value, "nonce": nonce}):
        return False
    response = recv_msg(sock)
    digest = response.get("digest") if isinstance(response, dict) else None
    if (not isinstance(response, dict) or response.get("type") != MessageType.AUTH_RESPONSE.value
            or not isinstance(digest, str) or len(digest) != 64):
        return False
    expected = hmac.new(secret.encode(), nonce.encode("ascii"), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(digest, expected):
        return False
    return send_msg(sock, {"type": MessageType.AUTH_OK.value})
