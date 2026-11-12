# Chạy tracker và relay trên VPS

Ứng dụng chia sẻ file qua hai mạng Internet mà không cần Tailscale. Tracker và relay
dùng **cùng một cổng TCP**. Hai peer sau NAT chỉ cần mở kết nối TCP outbound tới VPS;
UDP outbound được dùng khi thử đường trực tiếp. Relay sẽ thấy nội dung file vì bản
này chưa có mã hóa đầu cuối.

## 1. Chuẩn bị VPS

- VPS Linux có IPv4 công khai, Python 3 và một cổng TCP inbound, ví dụ `8000`.
- Cho phép `8000/tcp` trong firewall của VPS **và** firewall/security group của nhà
  cung cấp. Peer không cần mở cổng inbound khi dùng relay.
- Chép hoặc clone source vào `/opt/p2p-file-sharing`. Không cần cài Python dependency.

Tạo bí mật đủ dài bằng `python3 -c 'import secrets; print(secrets.token_urlsafe(32))'`.
Lưu bí mật trong `/etc/p2p-file-sharing/tracker.env` với quyền chỉ root đọc:

```ini
P2P_TRACKER_SECRET=thay-bang-bi-mat-ngau-nhien
```

Nếu chạy lâu dài bằng systemd, tạo `/etc/systemd/system/p2p-tracker.service`:

```ini
[Unit]
Description=P2P tracker and TCP relay
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=p2p
WorkingDirectory=/opt/p2p-file-sharing
EnvironmentFile=/etc/p2p-file-sharing/tracker.env
ExecStart=/usr/bin/python3 /opt/p2p-file-sharing/run_tracker.py --host 0.0.0.0 --port 8000
Restart=on-failure
RestartSec=3

[Install]
WantedBy=multi-user.target
```

Tạo user hệ thống `p2p`, cấp quyền đọc source cho user đó và đặt quyền `600` cho
file bí mật. Sau đó chạy `sudo systemctl daemon-reload`,
`sudo systemctl enable --now p2p-tracker` và xem log bằng
`journalctl -u p2p-tracker -f`. Có thể chạy thử trực tiếp bằng
`P2P_TRACKER_SECRET='...' python3 run_tracker.py --host 0.0.0.0 --port 8000`.
Không đưa bí mật vào repository hoặc log.

## 2. Chạy hai peer

Đặt **cùng** `P2P_TRACKER_SECRET` trên VPS và cả hai máy. Ví dụ trên Linux/Termux:

```sh
export P2P_TRACKER_SECRET='thay-bang-bi-mat-giong-VPS'
python3 run_client.py 6881 VPS_IPV4 --tracker-port 8000 --transport auto
```

Trên máy seeder, chọn `1` để chia sẻ file. Chuyển file `.meta.json` sang leecher,
chạy client cùng lệnh (có thể dùng cổng lắng nghe `6882`), rồi chọn `2` để tải.
`auto` thử TCP trực tiếp, thử UDP hole punching tối đa 100 giây khi STUN hoạt động,
sau đó tự chuyển `RELAY`. `--transport tcp` và `--transport udp` dùng để chỉ thử
đường trực tiếp. `--advertise-ip` chỉ dùng khi có địa chỉ **TCP** thực sự truy cập
được; không nhập public IP lấy từ STUN vào tham số này.

Đường UDP mặc định tải tối đa bốn mảnh song song và mỗi mảnh có tối đa 48 gói
1 KiB chờ ACK. Dùng `--udp-parallel 1..4` trên leecher và `--udp-window 16..128`
trên seeder để so cấu hình; ví dụ thử mốc cũ là `--udp-parallel 1` ở leecher
và `--udp-window 32` ở seeder. Seeder in `ACK`, `gửi_lại`, RTT trung bình và
RTT cao theo từng mảnh. Leecher in tốc độ `DIRECT_UDP` tính từ byte đã kiểm tra
SHA-256; tốc độ tổng còn bao gồm thời gian tìm đường. Để so công bằng, thử cùng
một file từ 0% trên hai mạng Internet, lặp lại vài lần và so trung vị tốc độ.

Xem log hoặc mục tiến độ để biết `DIRECT_TCP`, `DIRECT_UDP` hay `RELAY`. Dùng
`sha256sum` trên file nguồn và file nhận để xác nhận nội dung trùng nhau. Khi thử
Internet, tắt Tailscale trên cả hai máy và dùng hai mạng khác nhau. Để kiểm tra
fallback, chặn đường peer trực tiếp nhưng vẫn cho hai máy kết nối outbound tới
VPS: log phải hiện `RELAY` và file nhận có SHA-256 trùng nguồn.

## Giới hạn

- Nếu một peer không kết nối TCP outbound được tới VPS, relay cũng không hoạt động.
- Bí mật chung và mã phiên giới hạn người dùng relay; chúng không mã hóa nội dung
  file. Chỉ chia sẻ file với các máy tin cậy trong bản này.
- Đường `auto` hiện chọn từng peer một; khi dùng UDP, nó tải nhiều mảnh từ peer
  đã chọn. `tcp` giữ worker tải song song cũ.
- Kiểm thử localhost xác nhận logic relay, không chứng minh kết nối qua hai NAT thật.
