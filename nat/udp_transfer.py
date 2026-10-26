"""Truyền mảnh qua đường UDP đã xác nhận bằng probe/ACK.

Giao thức demo: một yêu cầu cho mỗi mảnh, gói dữ liệu 1024 byte, cửa sổ 32
gói và ACK riêng từng gói. Mỗi mảnh chỉ được công nhận sau SHA-256 ở PieceManager.
"""

import math
import os
import queue
import struct
import threading
import time
from typing import Callable, Optional


MAGIC = b"P2PF"
REQUEST, DATA, ACK, NAK = 1, 2, 3, 4
HEADER = struct.Struct("!4sB8sII")
CHUNK_SIZE = 1024
WINDOW = 32
PIECE_TIMEOUT = 30.0


class UDPTransfer:
    def __init__(self, manager, piece_provider: Callable[[str], object]):
        self.manager = manager
        self.piece_provider = piece_provider
        self._downloads = {}
        self._uploads = {}
        self._completed = {}
        self._lock = threading.Lock()
        self._closed = threading.Event()
        manager.on_data_packet = self.handle_packet

    def close(self) -> None:
        self._closed.set()
        self.manager.on_data_packet = None
        with self._lock:
            self._completed.clear()

    def _send(self, kind: int, nonce: bytes, piece: int, seq: int,
              payload: bytes, addr: tuple) -> bool:
        sock = self.manager._shared_socket
        if sock is None or self._closed.is_set():
            return False
        try:
            sock.sendto(HEADER.pack(MAGIC, kind, nonce, piece, seq) + payload, addr)
            return True
        except OSError:
            return False

    def request_piece(self, peer_id: str, info_hash: str, piece_index: int,
                      expected_length: int, timeout: float = PIECE_TIMEOUT) -> Optional[bytes]:
        addr = self.manager.get_endpoint(peer_id)
        if (addr is None or not isinstance(piece_index, int) or piece_index < 0
                or not 0 < expected_length <= 4 * 1024 * 1024):
            return None
        try:
            file_id = bytes.fromhex(info_hash)
        except ValueError:
            return None
        if len(file_id) != 32:
            return None

        nonce = os.urandom(8)
        inbox = queue.Queue(maxsize=WINDOW * 4)
        with self._lock:
            self._downloads[nonce] = (addr, piece_index, inbox)
        chunks = math.ceil(expected_length / CHUNK_SIZE)
        buffer = bytearray(expected_length)
        received = set()
        deadline = time.monotonic() + timeout
        next_request = 0.0
        try:
            while not self._closed.is_set() and time.monotonic() < deadline:
                now = time.monotonic()
                if not received and now >= next_request:
                    self._send(REQUEST, nonce, piece_index, 0, file_id, addr)
                    next_request = now + 1.0
                try:
                    kind, seq, payload = inbox.get(timeout=0.2)
                except queue.Empty:
                    continue
                if kind == NAK:
                    return None
                if kind != DATA or seq >= chunks:
                    continue
                offset = seq * CHUNK_SIZE
                size = min(CHUNK_SIZE, expected_length - offset)
                if len(payload) != size:
                    continue
                if seq not in received:
                    buffer[offset:offset + size] = payload
                    received.add(seq)
                self._send(ACK, nonce, piece_index, seq, b"", addr)
                if len(received) == chunks:
                    with self._lock:
                        if len(self._completed) >= 128:
                            self._completed.pop(next(iter(self._completed)))
                        self._completed[nonce] = (addr, piece_index,
                                                   time.monotonic() + PIECE_TIMEOUT)
                    return bytes(buffer)
            return None
        finally:
            with self._lock:
                self._downloads.pop(nonce, None)

    def handle_packet(self, data: bytes, addr: tuple) -> None:
        if len(data) < HEADER.size or len(data) > HEADER.size + CHUNK_SIZE:
            return
        magic, kind, nonce, piece, seq = HEADER.unpack_from(data)
        if magic != MAGIC or self._closed.is_set():
            return
        payload = data[HEADER.size:]
        if kind == REQUEST:
            self._handle_request(nonce, piece, payload, addr)
            return
        with self._lock:
            download = self._downloads.get(nonce)
            upload = self._uploads.get(nonce)
            completed = self._completed.get(nonce)
            if completed and completed[2] < time.monotonic():
                self._completed.pop(nonce, None)
                completed = None
        if kind in (DATA, NAK) and download and addr == download[0] and piece == download[1]:
            try:
                download[2].put_nowait((kind, seq, payload))
            except queue.Full:
                pass  # sender sẽ gửi lại gói chưa có ACK
        elif kind == ACK and not payload and upload and addr == upload[0] and piece == upload[1]:
            try:
                upload[2].put_nowait(seq)
            except queue.Full:
                pass
        elif kind == DATA and completed and addr == completed[0] and piece == completed[1]:
            self._send(ACK, nonce, piece, seq, b"", addr)

    def _handle_request(self, nonce: bytes, piece: int, payload: bytes,
                        addr: tuple) -> None:
        if len(payload) != 32 or not self.manager.has_endpoint(addr):
            return
        pm = self.piece_provider(payload.hex())
        if pm is None or not isinstance(piece, int) or piece >= pm.num_pieces:
            self._send(NAK, nonce, piece, 0, b"", addr)
            return
        if not pm.has_piece(piece):
            self._send(NAK, nonce, piece, 0, b"", addr)
            return
        acks = queue.Queue(maxsize=WINDOW * 4)
        with self._lock:
            if nonce in self._uploads or len(self._uploads) >= 4:
                return
            self._uploads[nonce] = (addr, piece, acks)
        threading.Thread(target=self._send_piece,
                         args=(nonce, piece, pm, addr, acks), daemon=True).start()

    def _send_piece(self, nonce: bytes, piece: int, pm,
                    addr: tuple, acks: queue.Queue) -> None:
        try:
            data = pm.read_piece(piece)
            if data is None or not 0 < len(data) <= 4 * 1024 * 1024:
                self._send(NAK, nonce, piece, 0, b"", addr)
                return
            chunks = [data[i:i + CHUNK_SIZE] for i in range(0, len(data), CHUNK_SIZE)]
            pending = {}
            confirmed = set()
            next_seq = 0
            deadline = time.monotonic() + PIECE_TIMEOUT
            while (not self._closed.is_set() and len(confirmed) < len(chunks)
                   and time.monotonic() < deadline):
                while next_seq < len(chunks) and len(pending) < WINDOW:
                    if not self._send(DATA, nonce, piece, next_seq, chunks[next_seq], addr):
                        return
                    pending[next_seq] = time.monotonic()
                    next_seq += 1
                try:
                    seq = acks.get(timeout=0.04)
                    if seq in pending:
                        pending.pop(seq)
                        confirmed.add(seq)
                except queue.Empty:
                    pass
                now = time.monotonic()
                for seq, sent_at in list(pending.items()):
                    if now - sent_at >= 0.5:
                        self._send(DATA, nonce, piece, seq, chunks[seq], addr)
                        pending[seq] = now
        finally:
            with self._lock:
                self._uploads.pop(nonce, None)
