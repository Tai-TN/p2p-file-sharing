import unittest
from unittest.mock import patch

from peer.connection import PeerConnection
from protocol.messages import MessageType


class FakeSocket:
    """Socket giả lập để kiểm tra PeerConnection mà không cần mở cổng TCP."""

    def __init__(self, received_messages):
        self.received_messages = list(received_messages)
        self.sent_messages = []
        self.timeout = None
        self.closed = False

    def settimeout(self, timeout):
        self.timeout = timeout

    def connect(self, address):
        self.address = address

    def close(self):
        self.closed = True


class TestPeerConnection(unittest.TestCase):
    @patch("peer.connection.recv_msg")
    @patch("peer.connection.send_msg", return_value=True)
    @patch("peer.connection.socket.socket")
    def test_connect_performs_handshake_and_reads_bitfield(
        self, socket_factory, send_message, receive_message
    ):
        fake_socket = FakeSocket(
            [
                {
                    "type": MessageType.HANDSHAKE_RESPONSE.value,
                    "info_hash": "hash",
                },
                {
                    "type": MessageType.BITFIELD.value,
                    "bitfield": [True, False],
                },
            ]
        )
        socket_factory.return_value = fake_socket
        receive_message.side_effect = fake_socket.received_messages

        connection = PeerConnection("local", "remote", "hash", "127.0.0.1", 7001)

        self.assertTrue(connection.connect())
        self.assertEqual(connection.bitfield, [True, False])
        self.assertIs(connection.socket, fake_socket)
        self.assertEqual(send_message.call_count, 1)

        connection.close()
        self.assertTrue(fake_socket.closed)

    @patch("peer.connection.recv_msg")
    def test_receive_updates_choke_state(self, receive_message):
        connection = PeerConnection("local", "remote", "hash", "127.0.0.1", 7001)
        connection.socket = FakeSocket([])

        receive_message.return_value = {"type": MessageType.UNCHOKE.value}
        connection.receive()
        self.assertFalse(connection.is_choked)

        receive_message.return_value = {"type": MessageType.CHOKE.value}
        connection.receive()
        self.assertTrue(connection.is_choked)

    def test_send_interested_is_sent_only_once(self):
        connection = PeerConnection("local", "remote", "hash", "127.0.0.1", 7001)
        connection.socket = FakeSocket([])

        with patch("peer.connection.send_msg", return_value=True) as send_message:
            self.assertTrue(connection.send_interested())
            self.assertTrue(connection.send_interested())

        self.assertEqual(send_message.call_count, 1)
        self.assertTrue(connection.is_interested)

    @patch("peer.connection.recv_msg")
    def test_request_piece_skips_control_messages(self, receive_message):
        connection = PeerConnection("local", "remote", "hash", "127.0.0.1", 7001)
        connection.socket = FakeSocket([])
        receive_message.side_effect = [
            {"type": MessageType.KEEP_ALIVE.value},
            {"type": MessageType.UNCHOKE.value},
            {"type": MessageType.PIECE.value, "piece_index": 3},
        ]

        with patch("peer.connection.send_msg", return_value=True):
            response = connection.request_piece(3)

        self.assertEqual(response["type"], MessageType.PIECE.value)
        self.assertFalse(connection.is_choked)

    @patch("peer.connection.recv_msg")
    def test_receive_updates_bitfield_on_have_message(self, receive_message):
        have_events = []
        def on_have_cb(peer_id, idx):
            have_events.append((peer_id, idx))

        connection = PeerConnection("local", "remote_01", "hash", "127.0.0.1", 7001, on_have=on_have_cb)
        connection.socket = FakeSocket([])
        connection.bitfield = [False, False, False]

        receive_message.return_value = {"type": MessageType.HAVE.value, "piece_index": 1}
        msg = connection.receive()

        self.assertEqual(msg.get("type"), MessageType.HAVE.value)
        self.assertTrue(connection.bitfield[1])
        self.assertEqual(have_events, [("remote_01", 1)])


if __name__ == "__main__":
    unittest.main()
