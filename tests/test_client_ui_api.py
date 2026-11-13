"""Kiểm thử các API của P2PClient phục vụ giao diện: snapshot, preview, tạm dừng / tiếp tục / gỡ."""
import os
import shutil
import socket
import tempfile
import time
import unittest

from peer.client import P2PClient, STATE_DOWNLOADING, STATE_PAUSED, STATE_SEEDING, check_tracker
from storage.metainfo import create_metainfo, save_metainfo
from tracker.tracker_server import TrackerServer


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_until(predicate, timeout=15.0, interval=0.1):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


class ClientUiApiTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="p2p_ui_")
        self.tracker_port = free_port()
        self.tracker = TrackerServer(host="127.0.0.1", port=self.tracker_port)
        self.tracker.start()
        self.clients = []

        self.source = os.path.join(self.tmp, "src", "data.bin")
        os.makedirs(os.path.dirname(self.source))
        with open(self.source, "wb") as f:
            f.write(os.urandom(512 * 1024 * 2 + 1234))  # 3 mảnh, mảnh cuối nhỏ hơn

    def tearDown(self):
        for c in self.clients:
            c.stop()
        self.tracker.stop()
        time.sleep(0.2)
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _client(self, name):
        c = P2PClient(peer_id=name, listen_port=free_port(),
                      tracker_host="127.0.0.1", tracker_port=self.tracker_port)
        c.my_ip = "127.0.0.1"
        c.start()
        self.clients.append(c)
        return c

    def test_check_tracker(self):
        self.assertTrue(check_tracker("127.0.0.1", self.tracker_port))
        self.assertFalse(check_tracker("127.0.0.1", free_port(), timeout=0.5))

    def test_check_tracker_rejects_port_that_is_not_tracker(self):
        # Cổng TCP mở nhưng không trả lời theo giao thức Tracker → không được báo là OK.
        with socket.socket() as dummy:
            dummy.bind(("127.0.0.1", 0))
            dummy.listen()
            self.assertFalse(check_tracker("127.0.0.1", dummy.getsockname()[1], timeout=0.5))

    def test_share_reports_progress_and_download_completes(self):
        seeder = self._client("peer_seed")
        stages = []
        meta_path = os.path.join(self.tmp, "meta", "data.bin.meta.json")
        info_hash = seeder.share_file(self.source, meta_path,
                                      progress_cb=lambda s, d, t: stages.append((s, d, t)))
        self.assertIn(("hash", 3, 3), stages)
        self.assertIn(("verify", 3, 3), stages)

        snap = seeder.snapshot()
        self.assertEqual(snap["tasks"][0]["state"], STATE_SEEDING)
        self.assertEqual(snap["tasks"][0]["progress"], 100.0)

        leecher = self._client("peer_leech")
        dl_dir = os.path.join(self.tmp, "downloads")
        preview = leecher.preview_meta(meta_path, dl_dir)
        self.assertTrue(preview["tracker_ok"])
        self.assertEqual(preview["num_pieces"], 3)
        self.assertEqual(preview["peer_count"], 1)
        self.assertEqual(preview["seeder_count"], 1)
        self.assertIsNone(preview["resume_progress"])

        leecher.download_file(meta_path, dl_dir)
        self.assertTrue(wait_until(lambda: leecher.snapshot()["tasks"][0]["state"] == STATE_SEEDING))
        task = leecher.snapshot()["tasks"][0]
        self.assertEqual(task["have_pieces"], 3)
        self.assertEqual(task["downloaded_bytes"], os.path.getsize(self.source))

        # Seeder thấy leecher đã tải từ mình.
        self.assertTrue(wait_until(lambda: seeder.snapshot()["tasks"][0]["uploaded_bytes"] > 0))
        self.assertEqual(seeder.snapshot()["tracker_ok"], True)
        self.assertEqual(seeder.snapshot()["tasks"][0]["info_hash"], info_hash)

    def test_pause_resume_and_remove_without_peers(self):
        meta = create_metainfo(self.source, tracker_url=f"127.0.0.1:{self.tracker_port}")
        meta_path = os.path.join(self.tmp, "data.bin.meta.json")
        save_metainfo(meta, meta_path)

        leecher = self._client("peer_alone")
        dl_dir = os.path.join(self.tmp, "downloads")
        info_hash = leecher.download_file(meta_path, dl_dir)
        target = os.path.join(dl_dir, "data.bin")

        self.assertEqual(leecher.snapshot()["tasks"][0]["state"], STATE_DOWNLOADING)
        self.assertTrue(leecher.pause_task(info_hash))
        self.assertEqual(leecher.snapshot()["tasks"][0]["state"], STATE_PAUSED)
        self.assertTrue(os.path.exists(target + ".progress.json"))
        self.assertFalse(leecher.pause_task(info_hash))

        preview = leecher.preview_meta(meta_path, dl_dir)
        self.assertEqual(preview["resume_progress"], 0.0)
        self.assertTrue(preview["already_added"])

        self.assertTrue(leecher.resume_task(info_hash))
        self.assertEqual(leecher.snapshot()["tasks"][0]["state"], STATE_DOWNLOADING)

        self.assertTrue(leecher.remove_task(info_hash, delete_file=True))
        self.assertEqual(leecher.snapshot()["tasks"], [])
        self.assertTrue(wait_until(lambda: not os.path.exists(target), timeout=3))


if __name__ == "__main__":
    unittest.main()
