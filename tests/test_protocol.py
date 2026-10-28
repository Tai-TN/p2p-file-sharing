import socket
import struct
import unittest
from unittest.mock import Mock

from protocol.messages import MAX_MESSAGE_SIZE, ProtocolError, recv_msg, send_msg


class TestProtocolErrors(unittest.TestCase):
    def receive_frame(self, body, declared_length=None):
        sender, receiver = socket.socketpair()
        self.addCleanup(sender.close)
        self.addCleanup(receiver.close)
        receiver.settimeout(1)
        sender.sendall(struct.pack(">I", len(body) if declared_length is None else declared_length) + body)
        sender.shutdown(socket.SHUT_WR)
        return recv_msg(receiver)

    def test_eof_in_header_or_body_is_raised(self):
        sender, receiver = socket.socketpair()
        with sender, receiver:
            sender.sendall(b"\x00\x00")
            sender.shutdown(socket.SHUT_WR)
            with self.assertRaises(EOFError):
                recv_msg(receiver)
        with self.assertRaises(EOFError):
            self.receive_frame(b'{"type":', declared_length=100)

    def test_invalid_json_and_shape_raise_protocol_error(self):
        for body in (b'{"type":', b'\xff', b'[]'):
            with self.subTest(body=body), self.assertRaises(ProtocolError):
                self.receive_frame(body)

    def test_frame_size_is_checked_before_reading_body(self):
        for size in (0, MAX_MESSAGE_SIZE + 1):
            with self.subTest(size=size), self.assertRaises(ProtocolError):
                self.receive_frame(b"", declared_length=size)

    def test_socket_failures_are_not_swallowed(self):
        sock = Mock()
        sock.sendall.side_effect = BrokenPipeError("closed")
        with self.assertRaises(BrokenPipeError):
            send_msg(sock, {"type": "REQUEST"})
        sock.recv.side_effect = socket.timeout("no response")
        with self.assertRaises(socket.timeout):
            recv_msg(sock)
