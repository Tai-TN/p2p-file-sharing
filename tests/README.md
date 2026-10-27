# Kiểm thử dự án P2P

Chạy từ thư mục gốc:

```sh
python3 -m unittest discover tests
```

`test_client_network.py` kiểm tra CLI IPv6, từ chối IPv4, địa chỉ tracker suy ra từ kết nối `ANNOUNCE`, ghi đè địa chỉ, kiểm tra cổng và heartbeat seeder.

`test_peer.py` kiểm tra chọn mảnh hiếm, tải seeder → leecher qua `::1`, so dữ liệu nhận với file gốc, và nhiều yêu cầu song song tới một seeder bằng các socket riêng.

Các bài test dùng IPv6 loopback `::1` và thư mục tạm. Chúng xác nhận logic ứng dụng cục bộ; để chứng minh kết nối Internet trực tiếp, cần chạy theo [hướng dẫn hai mạng](../docs/KE_HOACH_P2P_INTERNET.md) và so SHA-256 file nhận.
