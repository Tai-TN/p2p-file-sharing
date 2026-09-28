"""
web/peer_web.py — Flask Web Server cho Peer Dashboard (Seeder / Leecher)
Chạy song song với PeerNode & TransferEngine trong daemon thread.
"""
import os
import time
import threading
from flask import Flask, render_template, jsonify

# Đường dẫn thư mục web/ hiện tại
_WEB_DIR = os.path.dirname(os.path.abspath(__file__))

app = Flask(
    __name__,
    template_folder=os.path.join(_WEB_DIR, "templates", "peer"),
    static_folder=os.path.join(_WEB_DIR, "static"),
)

# Biến toàn cục — được gán khi gọi start_peer_web()
_peer_id = None
_piece_manager = None
_engine = None
_peer_node = None
_start_time = None
_listen_port = None


# ── Routes ──

@app.route("/")
def dashboard():
    """Trang dashboard của Peer."""
    return render_template("dashboard.html")


@app.route("/api/status")
def api_status():
    """Trả về trạng thái tổng quan của Peer."""
    if not _piece_manager:
        return jsonify({"error": "Chưa khởi tạo PieceManager"}), 500

    is_complete = _piece_manager.is_complete()
    owned_count = sum(1 for b in _piece_manager.bitfield if b)
    total_pieces = _piece_manager.num_pieces
    progress = _piece_manager.progress_percentage()

    # Xác định role
    if is_complete:
        role = "Seeder"
    else:
        role = "Leecher"

    # Tốc độ download
    dl_speed = _engine.get_download_speed() if _engine else 0.0

    # Số lượng kết nối
    connected_peers_count = len(_engine.connected_peers) if _engine else 0
    active_upload_sockets = len(_peer_node.active_sockets) if _peer_node else 0

    uptime_seconds = int(time.time() - _start_time) if _start_time else 0
    hours, remainder = divmod(uptime_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)

    return jsonify({
        "peer_id": _peer_id,
        "role": role,
        "listen_port": _listen_port,
        "file_name": _piece_manager.info.get("name", "Unknown"),
        "file_size": _piece_manager.file_size,
        "piece_length": _piece_manager.piece_length,
        "total_pieces": total_pieces,
        "owned_pieces": owned_count,
        "progress": round(progress, 2),
        "is_complete": is_complete,
        "download_speed": dl_speed,
        "connected_peers_count": connected_peers_count,
        "active_upload_sockets": active_upload_sockets,
        "in_flight_count": len(_engine.in_flight_pieces) if _engine else 0,
        "uptime": f"{hours}h {minutes}m {seconds}s",
        "uptime_seconds": uptime_seconds,
    })


@app.route("/api/pieces")
def api_pieces():
    """Trả về mảng bitfield và các piece đang được tải dở."""
    if not _piece_manager:
        return jsonify({"error": "Chưa khởi tạo"}), 500

    bitfield = _piece_manager.get_bitfield()
    in_flight = list(_engine.in_flight_pieces) if _engine else []

    return jsonify({
        "bitfield": bitfield,
        "in_flight": in_flight,
        "total": len(bitfield),
        "owned": sum(bitfield),
    })


@app.route("/api/peers")
def api_peers():
    """Trả về danh sách các peer đang kết nối và tình trạng của họ."""
    peers_list = []
    if _engine:
        with _engine.lock:
            for p_id, info in _engine.connected_peers.items():
                p_bitfield = info.get("bitfield", [])
                p_owned = sum(p_bitfield) if p_bitfield else 0
                peers_list.append({
                    "peer_id": p_id,
                    "ip": info.get("ip", ""),
                    "port": info.get("port", 0),
                    "owned_pieces": p_owned,
                    "total_pieces": len(p_bitfield),
                    "percentage": round((p_owned / len(p_bitfield) * 100), 1) if p_bitfield else 0,
                    "role": "Seeder" if len(p_bitfield) > 0 and p_owned == len(p_bitfield) else "Leecher",
                })

    # Incoming upload connections
    upload_peers = []
    if _peer_node:
        with _peer_node.sockets_lock:
            for addr in _peer_node.active_sockets.keys():
                upload_peers.append({
                    "ip": addr[0],
                    "port": addr[1],
                })

    return jsonify({
        "download_peers": peers_list,
        "upload_peers": upload_peers,
    })


@app.route("/api/logs")
def api_logs():
    """Trả về danh sách sự kiện upload/download gần nhất của Peer."""
    combined_logs = []

    if _engine:
        combined_logs.extend(list(_engine.transfer_log))
    if _peer_node:
        combined_logs.extend(list(_peer_node.serve_log))

    # Đảo ngược để sự kiện mới nhất lên trên
    combined_logs.reverse()
    return jsonify(combined_logs[:60])


# ── Khởi chạy ──

def start_peer_web(peer_id_or_pm=None, piece_manager=None, engine=None, peer_node=None, port: int = 9001):
    """Khởi chạy Flask web server cho Peer trong daemon thread (hỗ trợ nhiều kiểu gọi tham số)."""
    global _peer_id, _piece_manager, _engine, _peer_node, _start_time, _listen_port

    # Hỗ trợ nếu truyền (piece_manager, engine, port)
    if hasattr(peer_id_or_pm, "get_bitfield"):
        _piece_manager = peer_id_or_pm
        _engine = piece_manager
        if isinstance(engine, int):
            port = engine
            _peer_node = None
        else:
            _peer_node = engine
        _peer_id = _engine.peer_id if _engine else (_peer_node.peer_id if _peer_node else "peer")
    else:
        _peer_id = str(peer_id_or_pm) if peer_id_or_pm else (_engine.peer_id if _engine else "peer")
        _piece_manager = piece_manager
        _engine = engine
        _peer_node = peer_node

    # Nếu chưa có peer_id thì lấy từ engine hoặc node
    if not _peer_id and _engine:
        _peer_id = _engine.peer_id
    elif not _peer_id and _peer_node:
        _peer_id = _peer_node.peer_id

    _start_time = time.time()
    _listen_port = _peer_node.port if _peer_node else (_engine.listen_port if _engine else port)

    t = threading.Thread(
        target=lambda: app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False),
        daemon=True,
    )
    t.start()
    print(f"[Peer Web UI] Dashboard cho {_peer_id} dang chay tai http://127.0.0.1:{port}")