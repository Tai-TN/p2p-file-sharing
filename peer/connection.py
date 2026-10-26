"""Quản lý một kết nối TCP lâu dài giữa hai Peer."""

from __future__ import annotations

import socket
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

from protocol.messages import (
    MessageType,
    create_handshake_msg,
    create_interested_msg,
    create_request_msg,
    recv_msg,
    send_msg,
)


class PeerConnection:
    """Đóng gói socket, handshake và trạng thái choking của một Peer."""

    def __init__(
        self,
        local_peer_id: str,
        remote_peer_id: str,
        info_hash: str,
        ip: str,
        port: int,
        on_have: Optional[Any] = None,
    ) -> None:
        self.local_peer_id = local_peer_id
        self.remote_peer_id = remote_peer_id
        self.info_hash = info_hash
        self.ip = ip
        self.port = port
        self.on_have = on_have

        self.socket: Optional[socket.socket] = None
        self.bitfield: List[bool] = []
        self.is_choked = True
        self.is_interested = False
        # Message điều khiển nhận trước REQUEST kế tiếp được giữ lại tại đây.
        self.pending_message: Optional[Dict[str, Any]] = None
        # Dùng để biết connection còn hoạt động gần đây hay đã bỏ lâu.
        self.last_activity = time.monotonic()
        self._send_lock = threading.Lock()

    def connect(self, timeout: float = 5.0) -> bool:
        """Mở socket, thực hiện handshake và nhận bitfield ban đầu."""
        if self.socket is not None:
            return True

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.settimeout(timeout)
            sock.connect((self.ip, self.port))

            if not self.send(create_handshake_msg(self.info_hash, self.local_peer_id), sock):
                raise ConnectionError("Không gửi được HANDSHAKE")

            response = recv_msg(sock)
            if (
                not response
                or response.get("type") != MessageType.HANDSHAKE_RESPONSE.value
                or response.get("info_hash") != self.info_hash
            ):
                raise ConnectionError("HANDSHAKE_RESPONSE không hợp lệ")

            bitfield_msg = recv_msg(sock)
            if not bitfield_msg or bitfield_msg.get("type") != MessageType.BITFIELD.value:
                raise ConnectionError("Không nhận được BITFIELD")

            self.bitfield = bitfield_msg.get("bitfield", [])
            self.socket = sock
            self.last_activity = time.monotonic()
            return True
        except Exception:
            sock.close()
            return False

    def send(
        self,
        message: Dict[str, Any],
        sock: Optional[socket.socket] = None,
    ) -> bool:
        """Gửi một frame có lock để các message không bị trộn trên TCP."""
        target = sock or self.socket
        if target is None:
            return False
        with self._send_lock:
            sent = send_msg(target, message)
        if sent:
            self.last_activity = time.monotonic()
        return sent

    def send_interested(self) -> bool:
        """Báo cho Peer từ xa biết rằng ta đang cần nhận piece."""
        if self.is_interested:
            return True
        if not self.send(create_interested_msg()):
            return False
        self.is_interested = True
        return True

    def receive(self) -> Optional[Dict[str, Any]]:
        """Nhận message và cập nhật trạng thái CHOKE/UNCHOKE tương ứng."""
        if self.socket is None:
            return None

        # Ưu tiên message đã giữ lại từ lần đọc trước, tránh làm mất frame.
        if self.pending_message is not None:
            message = self.pending_message
            self.pending_message = None
        else:
            message = recv_msg(self.socket)
        if not message:
            return None

        self.last_activity = time.monotonic()
        message_type = message.get("type")
        if message_type == MessageType.CHOKE.value:
            self.is_choked = True
        elif message_type == MessageType.UNCHOKE.value:
            self.is_choked = False
        elif message_type == MessageType.HAVE.value:
            have_idx = message.get("piece_index")
            if have_idx is not None:
                if 0 <= have_idx < len(self.bitfield):
                    self.bitfield[have_idx] = True
                if self.on_have is not None:
                    try:
                        self.on_have(self.remote_peer_id, have_idx)
                    except Exception:
                        pass
        return message

    def request_piece(self, piece_index: int) -> Optional[Dict[str, Any]]:
        """Gửi REQUEST và đọc đến khi nhận PIECE hoặc CHOKE."""
        if self.socket is None:
            return None

        if not self.send(create_request_msg(piece_index)):
            return None

        # HAVE/KEEP_ALIVE/UNCHOKE có thể đến trước PIECE nên phải bỏ qua
        # các message trung gian, không được giả định frame tiếp theo là PIECE.
        while True:
            message = self.receive()
            if not message:
                return None
            message_type = message.get("type")
            if message_type in (
                MessageType.KEEP_ALIVE.value,
                MessageType.HAVE.value,
                MessageType.UNCHOKE.value,
            ):
                continue
            if message_type in (MessageType.PIECE.value, MessageType.ERROR.value):
                # Câu trả lời của một REQUEST cũ còn sót trong socket: bỏ qua, đọc tiếp.
                if message.get("piece_index") != piece_index:
                    continue
                return message
            if message_type == MessageType.CHOKE.value:
                return message
            # Tin lạ: bỏ qua, không giữ lại để không làm lệch REQUEST sau.

    def set_timeout(self, timeout: float) -> None:
        """Đặt timeout đọc/ghi cho socket hiện tại."""
        if self.socket is not None:
            self.socket.settimeout(timeout)

    def close(self) -> None:
        """Đóng socket an toàn và cho phép đối tượng được dọn dẹp nhiều lần."""
        if self.socket is not None:
            try:
                self.socket.close()
            except OSError:
                pass
            finally:
                self.socket = None
                self.is_choked = True
                self.is_interested = False
                self.pending_message = None
