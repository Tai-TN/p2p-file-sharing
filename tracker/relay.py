"""Relay TCP có giới hạn tài nguyên cho hai peer sau NAT.

Sau RELAY_READY, hai socket dùng giao thức peer hiện có; tracker chỉ copy bytes.
"""

import hmac
import secrets
import socket
import threading
from dataclasses import dataclass, field
from typing import Optional

from protocol.messages import MessageType, send_msg


@dataclass
class _Session:
    requester: str
    target: str
    info_hash: str
    session_id: str
    join_token: str
    requester_sock: socket.socket
    target_sock: Optional[socket.socket] = None
    joined: threading.Event = field(default_factory=threading.Event)
    done: threading.Event = field(default_factory=threading.Event)
    finish_lock: threading.Lock = field(default_factory=threading.Lock)


class RelayManager:
    JOIN_TIMEOUT = 15.0
    IDLE_TIMEOUT = 180.0
    MAX_SESSIONS = 32
    MAX_PER_PEER = 8

    def __init__(self, state):
        self.state = state
        self._sessions = {}
        self._lock = threading.Lock()

    @staticmethod
    def _error(sock: socket.socket, reason: str) -> None:
        send_msg(sock, {"type": MessageType.RELAY_ERROR.value, "reason": reason})

    def open(self, sock: socket.socket, msg: dict) -> None:
        requester = msg.get("peer_id")
        target = msg.get("target_peer_id")
        info_hash = msg.get("info_hash")
        if (not isinstance(requester, str) or not isinstance(target, str)
                or not isinstance(info_hash, str) or len(info_hash) != 64
                or requester == target or not self.state.has_peer(info_hash, requester)
                or not self.state.has_peer(info_hash, target)
                or not self.state.get_signal_info(target)):
            self._error(sock, "Peer hoặc swarm không hợp lệ / không trực tuyến")
            return
        session = _Session(requester, target, info_hash,
                           secrets.token_hex(16), secrets.token_hex(16), sock)
        rejected = False
        with self._lock:
            requester_count = sum(requester in (s.requester, s.target)
                                  for s in self._sessions.values())
            target_count = sum(target in (s.requester, s.target)
                               for s in self._sessions.values())
            if (len(self._sessions) >= self.MAX_SESSIONS
                    or requester_count >= self.MAX_PER_PEER
                    or target_count >= self.MAX_PER_PEER):
                rejected = True
            else:
                self._sessions[session.session_id] = session
        if rejected:
            self._error(sock, "Relay đã đạt giới hạn phiên")
            return
        try:
            invite = {
                "type": MessageType.RELAY_INVITE.value,
                "session_id": session.session_id,
                "join_token": session.join_token,
                "info_hash": info_hash,
                "peer_id": requester,
            }
            if not self.state.deliver_signal(target, invite):
                self._error(sock, "Không gửi được lời mời tới peer")
                return
            if not session.joined.wait(self.JOIN_TIMEOUT):
                self._error(sock, "Hết hạn chờ peer vào relay")
                return
            target_sock = session.target_sock
            if target_sock is None:
                return
            ready = {"type": MessageType.RELAY_READY.value}
            if not send_msg(sock, ready) or not send_msg(target_sock, ready):
                return
            sock.settimeout(self.IDLE_TIMEOUT)
            target_sock.settimeout(self.IDLE_TIMEOUT)
            print(f"[Relay] {requester} ↔ {target} ({info_hash[:12]})")
            reverse = threading.Thread(target=self._pipe,
                                       args=(target_sock, sock, session), daemon=True)
            reverse.start()
            self._pipe(sock, target_sock, session)
            reverse.join(timeout=1.0)
        finally:
            self._finish(session)

    def join(self, sock: socket.socket, msg: dict) -> None:
        peer_id = msg.get("peer_id")
        session_id = msg.get("session_id")
        token = msg.get("join_token")
        if not all(isinstance(value, str) and len(value) == 32
                   for value in (session_id, token)) or not isinstance(peer_id, str):
            self._error(sock, "Mã phiên không hợp lệ")
            return
        with self._lock:
            session = self._sessions.get(session_id)
            if (not session or session.done.is_set() or peer_id != session.target or session.target_sock is not None
                    or not hmac.compare_digest(token, session.join_token)):
                session = None
            else:
                session.target_sock = sock
                session.joined.set()
        if session is None:
            self._error(sock, "Mã phiên sai, đã dùng hoặc hết hạn")
            return
        session.done.wait()

    def _pipe(self, source: socket.socket, target: socket.socket, session: _Session) -> None:
        try:
            while not session.done.is_set():
                data = source.recv(64 * 1024)
                if not data:
                    break
                target.sendall(data)
        except OSError:
            pass
        finally:
            self._finish(session)

    def _finish(self, session: _Session) -> None:
        with session.finish_lock:
            if session.done.is_set():
                return
            session.done.set()
            session.joined.set()
        with self._lock:
            self._sessions.pop(session.session_id, None)
        for sock in (session.requester_sock, session.target_sock):
            if sock is not None:
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                sock.close()

    def close(self) -> None:
        with self._lock:
            sessions = list(self._sessions.values())
        for session in sessions:
            self._finish(session)
