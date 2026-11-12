import unittest
import struct
from unittest.mock import Mock

from protocol.messages import (
    MessageType,
    create_choke_msg,
    create_interested_msg,
    create_not_interested_msg,
    create_unchoke_msg,
    create_announce_msg,
    recv_msg,
    send_msg,
)


class TestChokingMessages(unittest.TestCase):
    def test_control_messages_have_expected_types(self):
        self.assertEqual(create_choke_msg(), {"type": MessageType.CHOKE.value})
        self.assertEqual(create_unchoke_msg(), {"type": MessageType.UNCHOKE.value})
        self.assertEqual(create_interested_msg(), {"type": MessageType.INTERESTED.value})
        self.assertEqual(
            create_not_interested_msg(),
            {"type": MessageType.NOT_INTERESTED.value},
        )

    def test_nat_and_bad_peer_fields_share_announce(self):
        msg = create_announce_msg("a" * 64, "peer", 6881,
                                  udp_port=9000, public_ip="203.0.113.2",
                                  bad_peers=["dead"])
        self.assertEqual(msg["udp_port"], 9000)
        self.assertEqual(msg["bad_peers"], ["dead"])

    def test_receive_rejects_eof_invalid_json_and_oversize(self):
        for payload, error in [(b"", EOFError),
                               (struct.pack(">I", 0), ValueError),
                               (struct.pack(">I", 2 * 1024 * 1024 + 1), ValueError),
                               (struct.pack(">I", 2) + b"[]", ValueError),
                               (struct.pack(">I", 1) + b"{", ValueError)]:
            sock = Mock()
            chunks = [payload[:4], payload[4:]] if len(payload) > 4 else [payload]
            sock.recv.side_effect = chunks
            with self.assertRaises(error):
                recv_msg(sock)

    def test_send_raises_on_socket_failure_and_oversize(self):
        sock = Mock()
        sock.sendall.side_effect = OSError("closed")
        with self.assertRaises(OSError):
            send_msg(sock, {"type": "X"})
        with self.assertRaises(ValueError):
            send_msg(sock, {"data": "x" * (2 * 1024 * 1024)})


if __name__ == "__main__":
    unittest.main()
