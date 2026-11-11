"""Kiểm thử phần cầu nối của app: cấu hình, bộ gom nhật ký, danh sách chặn của Tracker."""
import json
import os
import shutil
import socket
import tempfile
import unittest

from app.config import AppConfig
from app.log_buffer import LogBuffer, TeeStream, classify
from app.tracker_api import TrackerApi


class AppConfigTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="p2p_cfg_")

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_first_run_fills_defaults_and_persists(self):
        cfg = AppConfig(self.tmp)
        self.assertRegex(cfg["peer_name"], r"^peer_[0-9a-f]{12}$")
        self.assertEqual(cfg["download_dir"], os.path.join(self.tmp, "downloads"))
        again = AppConfig(self.tmp)
        self.assertEqual(again["peer_name"], cfg["peer_name"])

    def test_update_validates_and_reports_restart(self):
        cfg = AppConfig(self.tmp)
        with self.assertRaises(ValueError) as ctx:
            cfg.update({"max_workers": 99})
        self.assertIn("Số luồng tải song song", str(ctx.exception))
        with self.assertRaises(ValueError):
            cfg.update({"peer_name": "co khoang trang"})
        self.assertFalse(cfg.update({"auto_resume": False}))
        self.assertTrue(cfg.update({"listen_port": "6999"}))
        self.assertEqual(AppConfig(self.tmp)["listen_port"], 6999)

    def test_tasks_roundtrip(self):
        cfg = AppConfig(self.tmp)
        tasks = [{"type": "Share", "meta_path": "a.meta.json", "file_path": "a", "paused": False}]
        cfg.save_tasks(tasks)
        self.assertEqual(cfg.load_tasks(), tasks)


class LogBufferTest(unittest.TestCase):
    def test_classify(self):
        self.assertEqual(classify("[Storage Error] Lỗi đọc piece 3"), "ERROR")
        self.assertEqual(classify("Peer x gửi sai dữ liệu (lần 1/4)"), "WARN")
        self.assertEqual(classify("Piece 5 ✓ ← peer_a"), "OK")
        self.assertEqual(classify("Đang Seed: a.bin"), "INFO")

    def test_tee_splits_lines_and_since(self):
        buf = LogBuffer()
        tee = TeeStream(None, buf)
        tee.write("dòng 1\ndòng ")
        tee.write("2\n")
        lines = buf.since(0)
        self.assertEqual([l["text"] for l in lines], ["dòng 1", "dòng 2"])
        self.assertEqual(buf.since(lines[0]["id"])[0]["text"], "dòng 2")


class TrackerApiTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="p2p_tapi_")
        with socket.socket() as s:
            s.bind(("127.0.0.1", 0))
            self.port = s.getsockname()[1]

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_blocklist_survives_restart(self):
        api = TrackerApi(self.tmp, LogBuffer(), host="127.0.0.1", port=self.port)
        self.assertTrue(api.start_server()["ok"])
        api._server.state.register_peer("h", "peer_bad", "10.0.0.9", 6881)
        self.assertTrue(api.block_peer("peer_bad", "10.0.0.9")["ok"])
        self.assertEqual(api.get_state()["state"]["swarms"], [])
        api._shutdown()

        with open(os.path.join(self.tmp, "tracker_blocklist.json"), encoding="utf-8") as f:
            self.assertEqual(json.load(f), {"peer_ids": ["peer_bad"], "ips": ["10.0.0.9"]})

        api2 = TrackerApi(self.tmp, LogBuffer(), host="127.0.0.1", port=self.port)
        self.assertTrue(api2._server.state.is_blocked("peer_bad"))
        self.assertTrue(api2.unblock("peer_bad", "")["ok"])
        self.assertFalse(api2.unblock("peer_bad", "")["ok"])

    def test_start_server_rejects_bad_port(self):
        api = TrackerApi(self.tmp, LogBuffer(), host="127.0.0.1", port=self.port)
        self.assertFalse(api.start_server("abc")["ok"])
        self.assertFalse(api.start_server(70000)["ok"])


if __name__ == "__main__":
    unittest.main()
