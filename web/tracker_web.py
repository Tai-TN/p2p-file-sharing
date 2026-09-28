"""
web/tracker_web.py — Flask Web Server cho Tracker Dashboard
Chạy song song với TrackerServer trong daemon thread.
"""
import os
import time
import threading
from flask import Flask, render_template, jsonify

# Đường dẫn thư mục web/ hiện tại
_WEB_DIR = os.path.dirname(os.path.abspath(__file__))

app = Flask(
    __name__,
    template_folder=os.path.join(_WEB_DIR, "templates", "tracker"),
    static_folder=os.path.join(_WEB_DIR, "static"),
)

# Biến toàn cục — sẽ được gán khi gọi start_tracker_web()
_tracker_state = None
_start_time = None


# ── Routes ──

@app.route("/")
def dashboard():
    """Trang dashboard chính."""
    return render_template("dashboard.html")


@app.route("/api/status")
def api_status():
    """Trả về trạng thái tổng quan Tracker."""
    swarms = _tracker_state.get_all_swarms_info() if _tracker_state else {}
    total_peers = sum(s["peer_count"] for s in swarms.values())
    total_seeders = sum(s["seeders"] for s in swarms.values())
    total_leechers = sum(s["leechers"] for s in swarms.values())

    uptime_seconds = int(time.time() - _start_time) if _start_time else 0
    hours, remainder = divmod(uptime_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)

    return jsonify({
        "total_swarms": len(swarms),
        "total_peers": total_peers,
        "total_seeders": total_seeders,
        "total_leechers": total_leechers,
        "uptime": f"{hours}h {minutes}m {seconds}s",
        "uptime_seconds": uptime_seconds,
    })


@app.route("/api/swarms")
def api_swarms():
    """Trả về thông tin chi tiết tất cả swarm + danh sách peer."""
    if _tracker_state:
        return jsonify(_tracker_state.get_detailed_swarms_info())
    return jsonify({})


@app.route("/api/events")
def api_events():
    """Trả về danh sách sự kiện gần nhất."""
    if _tracker_state:
        return jsonify(_tracker_state.get_events(limit=50))
    return jsonify([])


# ── Khởi chạy ──

def start_tracker_web(tracker_state, port=8080):
    """Khởi chạy Flask web server trong daemon thread."""
    global _tracker_state, _start_time
    _tracker_state = tracker_state
    _start_time = tracker_state.start_time

    t = threading.Thread(
        target=lambda: app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False),
        daemon=True,
    )
    t.start()
    print(f"[Tracker Web UI] Dashboard đang chạy tại http://127.0.0.1:{port}")