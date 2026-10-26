"""
nat/signaling.py — Client-side kênh signaling persistent với Tracker.

Chức năng:
  - Mở 1 TCP connection persistent tới Tracker, gửi SIGNAL_REGISTER.
  - Background thread liên tục đọc PUNCH_SIGNAL từ Tracker và gọi callback.
  - Phương thức request_punch() gửi PUNCH_REQUEST để yêu cầu Tracker
    thông báo cho peer target cùng thực hiện hole punch.

Cách dùng (trong P2PClient.start()):
    channel = TrackerSignalChannel(
        tracker_host, tracker_port,
        peer_id=self.peer_id,
        ip=nat_info.public_ip,
        udp_port=nat_info.public_port,
        tcp_port=self.listen_port,
    )
    channel.on_punch_signal = self._on_punch_signal
    if channel.start():
        self._signal_channel = channel
"""

import socket
import threading
import logging
from typing import Optional, Callable, Dict, Any

from protocol.messages import (
    MessageType,
    send_msg,
    recv_msg,
    create_signal_register_msg,
    create_punch_request_msg,
)

logger = logging.getLogger(__name__)

# Thời gian giữa các lần thử kết nối lại signal channel (giây)
_RECONNECT_DELAY = 15.0
# Gửi KEEP_ALIVE định kỳ để tránh bị NAT đóng kết nối idle
_KEEPALIVE_INTERVAL = 30.0


class TrackerSignalChannel:
    """
    Kênh signaling persistent giữa peer và Tracker.

    Vòng đời:
      start()  → kết nối + đăng ký + khởi chạy listener thread
      request_punch(target)  → gửi PUNCH_REQUEST, nhận PUNCH_SIGNAL qua callback
      stop()   → đóng kết nối + dừng listener thread
    """

    def __init__(self,
                 tracker_host: str,
                 tracker_port: int,
                 peer_id: str,
                 ip: str,
                 udp_port: int,
                 tcp_port: int,
                 connect_timeout: float = 5.0,
                 candidates: Optional[list] = None):
        """
        Args:
            tracker_host/port: Địa chỉ Tracker.
            peer_id:    ID của peer này.
            ip:         Public IP (từ STUN) để báo cho Tracker.
            udp_port:   Public UDP port (từ STUN) để hole punch.
            tcp_port:   TCP listen port để báo cho peer khác sau hole punch.
            connect_timeout: Timeout khi kết nối TCP tới Tracker (giây).
        """
        self.tracker_host = tracker_host
        self.tracker_port = tracker_port
        self.peer_id  = peer_id
        self.ip       = ip
        self.udp_port = udp_port
        self.tcp_port = tcp_port
        self.candidates = candidates or []
        self.connect_timeout = connect_timeout

        self._sock: Optional[socket.socket] = None
        self._sock_lock = threading.Lock()
        self._send_lock = threading.Lock()
        self._stop_event = threading.Event()
        self._is_running = False
        self._listener_thread: Optional[threading.Thread] = None
        self._keepalive_thread: Optional[threading.Thread] = None

        # Callback khi nhận PUNCH_SIGNAL từ Tracker.
        # Được gọi từ listener thread với dict { peer_id, ip, udp_port, tcp_port }.
        self.on_punch_signal: Optional[Callable[[Dict[str, Any]], None]] = None

        # Callback khi nhận PUNCH_FAILED.
        self.on_punch_failed: Optional[Callable[[str], None]] = None

    # ── Public API ────────────────────────────────────────────────────────── #

    def start(self) -> bool:
        """
        Kết nối tới Tracker và đăng ký signal channel.

        Returns:
            True  nếu kết nối và đăng ký thành công.
            False nếu thất bại (Tracker không hỗ trợ signaling hoặc lỗi mạng).
        """
        if self._is_running:
            return True

        sock = self._connect_and_register()
        if not sock:
            return False

        with self._sock_lock:
            self._sock = sock

        self._stop_event.clear()
        self._is_running = True

        # Listener thread: đọc PUNCH_SIGNAL từ Tracker
        self._listener_thread = threading.Thread(
            target=self._listen_loop, daemon=True, name=f"signal-listen-{self.peer_id}"
        )
        self._listener_thread.start()

        # Keepalive thread: gửi KEEP_ALIVE định kỳ
        self._keepalive_thread = threading.Thread(
            target=self._keepalive_loop, daemon=True, name=f"signal-ka-{self.peer_id}"
        )
        self._keepalive_thread.start()

        logger.info(f"[SignalChannel {self.peer_id}] Đã kết nối tới Tracker "
                    f"{self.tracker_host}:{self.tracker_port}")
        return True

    def stop(self) -> None:
        """Đóng kênh signaling và dừng các thread ngầm."""
        self._is_running = False
        self._stop_event.set()
        self._close_socket()
        for thread in (self._listener_thread, self._keepalive_thread):
            if thread and thread is not threading.current_thread():
                thread.join(timeout=self.connect_timeout + 1.0)
        logger.info(f"[SignalChannel {self.peer_id}] Đã dừng.")

    def request_punch(self, target_peer_id: str) -> bool:
        """
        Gửi PUNCH_REQUEST tới Tracker để yêu cầu Tracker báo cho target_peer_id
        cùng thực hiện hole punch với mình.

        Returns:
            True  nếu gửi request thành công (không đảm bảo target nhận được).
            False nếu chưa kết nối hoặc lỗi khi gửi.
        """
        msg = create_punch_request_msg(target_peer_id)
        with self._sock_lock:
            sock = self._sock
        if not sock:
            return False
        with self._send_lock:
            sent = send_msg(sock, msg)
        if not sent:
            self._close_socket(sock)
        return sent

    @property
    def is_connected(self) -> bool:
        with self._sock_lock:
            return self._sock is not None and self._is_running

    # ── Internal ──────────────────────────────────────────────────────────── #

    def _connect_and_register(self) -> Optional[socket.socket]:
        """
        Mở TCP connection tới Tracker, gửi SIGNAL_REGISTER, chờ SIGNAL_READY.

        Returns: socket nếu thành công, None nếu thất bại.
        """
        sock = None
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(self.connect_timeout)
            sock.connect((self.tracker_host, self.tracker_port))

            reg_msg = create_signal_register_msg(
                peer_id=self.peer_id,
                ip=self.ip,
                udp_port=self.udp_port,
                tcp_port=self.tcp_port,
                candidates=self.candidates,
            )
            if not send_msg(sock, reg_msg):
                sock.close()
                return None

            # Chờ SIGNAL_READY xác nhận từ Tracker
            sock.settimeout(self.connect_timeout)
            resp = recv_msg(sock)
            if not resp or resp.get("type") != MessageType.SIGNAL_READY.value:
                logger.warning(f"[SignalChannel] Tracker không gửi SIGNAL_READY: {resp!r}")
                sock.close()
                return None

            # Sau khi đăng ký xong, tắt timeout — connection sẽ tồn tại dài hạn
            sock.settimeout(None)
            return sock

        except Exception as e:
            if sock:
                sock.close()
            logger.warning(f"[SignalChannel {self.peer_id}] Kết nối tới Tracker thất bại: {e}")
            return None

    def _listen_loop(self) -> None:
        """
        Background thread: liên tục đọc message từ Tracker.
        Xử lý PUNCH_SIGNAL và PUNCH_FAILED.
        Tự thoát khi channel bị đóng hoặc peer stop().
        """
        while not self._stop_event.is_set():
            with self._sock_lock:
                sock = self._sock
            if not sock:
                if self._stop_event.wait(_RECONNECT_DELAY):
                    break
                new_sock = self._connect_and_register()
                if new_sock:
                    if self._stop_event.is_set():
                        new_sock.close()
                        break
                    with self._sock_lock:
                        self._sock = new_sock
                    logger.warning(f"[SignalChannel {self.peer_id}] Đã kết nối lại Tracker")
                continue

            try:
                msg = recv_msg(sock)
            except Exception:
                msg = None

            if not msg:
                # Kết nối bị đóng
                if not self._stop_event.is_set():
                    logger.warning(f"[SignalChannel {self.peer_id}] Mất kết nối Tracker")
                    self._close_socket(sock)
                continue

            msg_type = msg.get("type")

            if msg_type == MessageType.PUNCH_SIGNAL.value:
                logger.info(f"[SignalChannel {self.peer_id}] PUNCH_SIGNAL nhận được: "
                            f"peer={msg.get('peer_id')} "
                            f"ep={msg.get('ip')}:{msg.get('udp_port')}")
                if self.on_punch_signal:
                    try:
                        self.on_punch_signal(msg)
                    except Exception as e:
                        logger.error(f"[SignalChannel] Lỗi trong on_punch_signal callback: {e}")

            elif msg_type == MessageType.PUNCH_FAILED.value:
                reason = msg.get("reason", "")
                logger.warning(f"[SignalChannel {self.peer_id}] PUNCH_FAILED: {reason}")
                if self.on_punch_failed:
                    try:
                        self.on_punch_failed(reason)
                    except Exception as e:
                        logger.error(f"[SignalChannel] Lỗi trong on_punch_failed callback: {e}")

            else:
                logger.debug(f"[SignalChannel] Message không xử lý: {msg_type}")

    def _keepalive_loop(self) -> None:
        """
        Gửi KEEP_ALIVE định kỳ để NAT không đóng kết nối idle
        và Tracker biết peer vẫn còn kết nối.
        """
        keepalive_msg = {"type": MessageType.KEEP_ALIVE.value}
        while not self._stop_event.wait(_KEEPALIVE_INTERVAL):
            with self._sock_lock:
                sock = self._sock
            if not sock:
                continue
            with self._send_lock:
                sent = send_msg(sock, keepalive_msg)
            if not sent:
                self._close_socket(sock)

    def _close_socket(self, expected: Optional[socket.socket] = None) -> None:
        with self._sock_lock:
            if expected is not None and self._sock is not expected:
                return
            if self._sock:
                try:
                    self._sock.shutdown(socket.SHUT_RDWR)
                except Exception:
                    pass
                try:
                    self._sock.close()
                except Exception:
                    pass
                self._sock = None

