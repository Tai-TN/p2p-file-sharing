import unittest

from protocol.messages import (
    MessageType,
    create_choke_msg,
    create_interested_msg,
    create_not_interested_msg,
    create_unchoke_msg,
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


if __name__ == "__main__":
    unittest.main()
