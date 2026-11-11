"""Hàm dùng chung để mở cửa sổ pywebview cho app Peer và app Tracker."""
import json
import os
import sys

import webview


def ui_path(name: str) -> str:
    """Đường dẫn tới file giao diện, hoạt động cả khi chạy từ mã nguồn lẫn khi đóng gói .exe."""
    base = getattr(sys, "_MEIPASS", os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    return os.path.join(base, "app", "ui", name)


def bind_native_drop(window) -> None:
    """
    Trình duyệt không cho JS biết đường dẫn thật của file được kéo thả.
    pywebview thêm trường pywebviewFullPath khi bắt sự kiện drop từ Python,
    nên ta bắt ở đây rồi gọi window.onNativeDrop(path) bên JS.
    """
    try:
        from webview.dom import DOMEventHandler
    except ImportError:  # pragma: no cover - pywebview quá cũ
        return

    def on_drop(event):
        files = (event or {}).get("dataTransfer", {}).get("files", [])
        for f in files:
            path = f.get("pywebviewFullPath")
            if path:
                window.evaluate_js(f"window.onNativeDrop && window.onNativeDrop({json.dumps(path)})")
                break

    window.dom.document.events.drop += DOMEventHandler(on_drop, True, True)


def run_window(title: str, page: str, api, *, width: int = 1100, height: int = 720,
               debug: bool = False, enable_drop: bool = False) -> None:
    window = webview.create_window(
        title, url=ui_path(page), js_api=api,
        width=width, height=height, min_size=(760, 520),
        background_color="#FFFFFF", text_select=False,
    )
    api._attach_window(window)
    window.events.closed += api._shutdown
    if enable_drop:
        window.events.loaded += lambda: bind_native_drop(window)
    webview.start(api._start, http_server=True, debug=debug, private_mode=False)
