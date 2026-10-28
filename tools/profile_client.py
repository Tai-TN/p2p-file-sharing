"""Run the normal client menu with per-peer transfer measurements.

Example: python3 tools/profile_client.py 6882 ::1 --profile-output /tmp/transfer.jsonl
The probes observe existing calls; they do not change scheduling or framing.
"""

import argparse
from collections import defaultdict
from functools import wraps
import json
from pathlib import Path
import sys
import threading
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from peer import transfer_engine
from storage.piece_manager import PieceManager


_context = threading.local()
_output_lock = threading.Lock()
_installed = False


class TransferProbe:
    def __init__(self, engine):
        self.engine = engine
        self.started = time.perf_counter()
        self.process_started = time.process_time()
        self.finished = None
        self.process_finished = None
        self.lock = threading.Lock()
        self.peers = {}

    def begin(self, peer_id):
        with self.lock:
            peer = self.peers.setdefault(peer_id, {
                "pieces": 0, "errors": 0, "bytes": 0,
                "request_s": 0.0, "thread_cpu_s": 0.0,
                "active": 0, "peak_active": 0, "sockets": set(),
                "phases": defaultdict(lambda: [0.0, 0.0]),
            })
            peer["active"] += 1
            peer["peak_active"] = max(peer["peak_active"], peer["active"])

    def record(self, peer_id, piece_index, success, wall, cpu, phases, session):
        with self.lock:
            peer = self.peers[peer_id]
            peer["active"] -= 1
            peer["pieces" if success else "errors"] += 1
            if success:
                pm = self.engine.piece_manager
                peer["bytes"] += min(pm.piece_length,
                                     pm.file_size - piece_index * pm.piece_length)
            peer["request_s"] += wall
            peer["thread_cpu_s"] += cpu
            if session is not None:
                peer["sockets"].add(session)
            for name, values in phases.items():
                peer["phases"][name][0] += values[0]
                peer["phases"][name][1] += values[1]
            self.finished = time.perf_counter()
            self.process_finished = time.process_time()

    def report(self, success):
        with self.lock:
            end = self.finished if success and self.finished else time.perf_counter()
            cpu_end = (self.process_finished if success and self.process_finished is not None
                       else time.process_time())
            elapsed = end - self.started
            peers = {}
            for peer_id, peer in self.peers.items():
                count = peer["pieces"] + peer["errors"]
                peers[peer_id] = {
                    "pieces": peer["pieces"], "errors": peer["errors"],
                    "payload_bytes": peer["bytes"], "tcp_sessions": len(peer["sockets"]),
                    "peak_requests": peer["peak_active"],
                    "mean_request_ms": 1000 * peer["request_s"] / max(count, 1),
                    "mean_thread_cpu_ms": 1000 * peer["thread_cpu_s"] / max(count, 1),
                    "mean_phase_ms": {
                        name: {"wall": 1000 * values[0] / max(count, 1),
                               "cpu": 1000 * values[1] / max(count, 1)}
                        for name, values in peer["phases"].items()},
                }
            total_bytes = sum(peer["bytes"] for peer in self.peers.values())
            return {
                "file": self.engine.piece_manager.info["name"],
                "success": success, "workers": self.engine.max_workers,
                "payload_bytes": total_bytes, "elapsed_s": elapsed,
                "payload_MiB_s": total_bytes / 1048576 / elapsed if elapsed else 0,
                "process_cpu_s": cpu_end - self.process_started,
                "outstanding_requests": sum(peer["active"] for peer in self.peers.values()),
                "peers": peers,
            }


def _phase_wrapper(original, name):
    @wraps(original)
    def measured(*args, **kwargs):
        current = getattr(_context, "request", None)
        if current is None or (name == "receive_json" and current["setup"]):
            return original(*args, **kwargs)
        started, cpu_started = time.perf_counter(), time.thread_time()
        try:
            result = original(*args, **kwargs)
            if name == "connection" and result is not None:
                try:
                    current["session"] = (result.getsockname(), result.getpeername())
                except OSError:
                    pass  # A concurrent stop may already have closed the socket.
            return result
        finally:
            phase = current["phases"][name]
            phase[0] += time.perf_counter() - started
            phase[1] += time.thread_time() - cpu_started
    return measured


def install_probes(callback):
    """Install once in this diagnostic process; callback receives one report/file."""
    global _installed
    if _installed:
        raise RuntimeError("Transfer probes are already installed")
    _installed = True
    engine_class = transfer_engine.TransferEngine
    start_download = engine_class.start_download
    download_piece = engine_class._download_piece_from_peer
    worker_socket = _phase_wrapper(engine_class._worker_socket, "connection")

    @wraps(start_download)
    def measured_download(engine, *args, **kwargs):
        engine._transfer_probe = TransferProbe(engine)
        success = False
        try:
            success = start_download(engine, *args, **kwargs)
            return success
        finally:
            if success:
                deadline = time.monotonic() + 1.0
                for worker in engine._worker_threads:
                    worker.join(timeout=max(0.0, deadline - time.monotonic()))
            callback(engine._transfer_probe.report(success))

    @wraps(download_piece)
    def measured_piece(engine, index, peer_id, *args, **kwargs):
        probe = getattr(engine, "_transfer_probe", None)
        if probe is None:
            return download_piece(engine, index, peer_id, *args, **kwargs)
        current = {"phases": defaultdict(lambda: [0.0, 0.0]),
                   "setup": False, "session": None}
        _context.request = current
        probe.begin(peer_id)
        started, cpu_started = time.perf_counter(), time.thread_time()
        success = False
        try:
            success = download_piece(engine, index, peer_id, *args, **kwargs)
            return success
        finally:
            probe.record(peer_id, index, success, time.perf_counter() - started,
                         time.thread_time() - cpu_started, current["phases"],
                         current["session"])
            _context.request = None

    def measured_socket(*args, **kwargs):
        current = getattr(_context, "request", None)
        if current is None:
            return worker_socket(*args, **kwargs)
        current["setup"] = True
        try:
            return worker_socket(*args, **kwargs)
        finally:
            current["setup"] = False

    engine_class.start_download = measured_download
    engine_class._download_piece_from_peer = measured_piece
    engine_class._worker_socket = measured_socket
    transfer_engine.recv_msg = _phase_wrapper(transfer_engine.recv_msg, "receive_json")
    PieceManager.write_piece = _phase_wrapper(PieceManager.write_piece, "verify_write")


def main():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--profile-output", default="demo_workspace/transfer-profile.jsonl")
    args, client_args = parser.parse_known_args()
    output = Path(args.profile_output)

    def save_report(report):
        with _output_lock:
            try:
                output.parent.mkdir(parents=True, exist_ok=True)
                with output.open("a", encoding="utf-8") as stream:
                    stream.write(json.dumps(report, ensure_ascii=False) + "\n")
            except OSError as exc:
                print(f"[Transfer profile] Không lưu được báo cáo: {exc}")
            else:
                print(f"[Transfer profile] Đã lưu: {output.resolve()}")
            print("\n[Transfer profile] " + json.dumps(report, ensure_ascii=False, indent=2))

    install_probes(save_report)
    sys.argv = [sys.argv[0], *client_args]
    from run_client import main as run_client
    run_client()


if __name__ == "__main__":
    main()
