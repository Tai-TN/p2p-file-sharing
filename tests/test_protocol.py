import unittest
import socket
import base64
from protocol.messages import (
    send_msg,
    recv_msg,
    create_announce_msg,
    create_piece_msg,
    create_bitfield_msg,
    MessageType
)

class TestProtocol(unittest.TestCase):
    def test_send_and_recv_msg(self):
        """Kiểm tra việc gửi và nhận thông điệp qua socketpair bằng 4-byte length prefix."""
        parent_sock, child_sock = socket.socketpair()
        
        try:
            sample_payload = create_announce_msg("hash123", "peer_01", 6881, event="started")
            success = send_msg(parent_sock, sample_payload)
            self.assertTrue(success)

            received_payload = recv_msg(child_sock)
            self.assertIsNotNone(received_payload)
            self.assertEqual(received_payload["type"], MessageType.ANNOUNCE.value)
            self.assertEqual(received_payload["info_hash"], "hash123")
            self.assertEqual(received_payload["peer_id"], "peer_01")
            self.assertEqual(received_payload["port"], 6881)
        finally:
            parent_sock.close()
            child_sock.close()

    def test_binary_piece_encoding(self):
        """Kiểm tra việc mã hóa dữ liệu nhị phân (piece data) thành Base64 và truyền nhận."""
        parent_sock, child_sock = socket.socketpair()
        
        try:
            original_data = b"Hello P2P File Sharing! Raw Binary Data \x00\xff\xfe"
            msg = create_piece_msg(piece_index=5, data_bytes=original_data)
            
            send_msg(parent_sock, msg)
            received = recv_msg(child_sock)
            
            self.assertEqual(received["piece_index"], 5)
            decoded_data = base64.b64decode(received["data"])
            self.assertEqual(decoded_data, original_data)
        finally:
            parent_sock.close()
            child_sock.close()

    def test_bitfield_creation(self):
        bitfield = [True, True, False, True]
        msg = create_bitfield_msg(bitfield)
        self.assertEqual(msg["type"], MessageType.BITFIELD.value)
        self.assertEqual(msg["bitfield"], [True, True, False, True])

if __name__ == '__main__':
    unittest.main()
