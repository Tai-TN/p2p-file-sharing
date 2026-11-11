# Đóng gói app Peer và app Tracker thành 2 file .exe (Windows).
# Chạy:  powershell -ExecutionPolicy Bypass -File build_exe.ps1
# Kết quả: dist\P2PShare.exe và dist\P2PTracker.exe

$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

python -m pip install -r requirements.txt

$common = @("--noconfirm", "--clean", "--onefile", "--windowed", "--add-data", "app/ui;app/ui")

# Loại các gói có trong môi trường Python nhưng app không dùng, để file .exe nhẹ hơn.
$exclude = @("twisted", "tornado", "cryptography", "setuptools", "wheel", "pkg_resources", "pyasn1",
             "jinja2", "numpy", "matplotlib", "pandas", "PIL", "scipy", "tkinter", "unittest", "pytest",
             # Trên Windows pywebview dùng WebView2 (Edge), không cần backend Qt (>100 MB).
             "PyQt5", "PyQt6", "PySide2", "PySide6", "qtpy", "webview.platforms.qt")
foreach ($m in $exclude) { $common += @("--exclude-module", $m) }

python -m PyInstaller @common --name P2PShare run_app.py
if ($LASTEXITCODE -ne 0) { throw "Đóng gói P2PShare thất bại" }

python -m PyInstaller @common --name P2PTracker run_tracker_admin.py
if ($LASTEXITCODE -ne 0) { throw "Đóng gói P2PTracker thất bại" }

Write-Host ""
Write-Host "Xong: dist\P2PShare.exe và dist\P2PTracker.exe"
