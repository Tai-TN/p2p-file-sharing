# Thử truyền file trực tiếp qua IPv6

## Trạng thái

Nhánh `feature/direct-ipv6` dùng TCP IPv6 cho tracker và dữ liệu file, không dùng Tailscale, NAT traversal hay relay. Seeder và leecher công bố địa chỉ IPv6 với tracker; tracker chỉ giúp tìm peer. Bộ test `::1` đã kiểm tra truyền nhiều mảnh và SHA-256. **Chưa xác nhận truyền qua hai mạng Internet thật**: khi kiểm tra môi trường phát triển, Fedora chưa có IPv6 global.

Nhánh `demo/tailscale` giữ bản demo cũ. Hai nhánh dùng cùng cấu trúc `.meta.json`, nhưng chương trình trên nhánh IPv6 chỉ nhận địa chỉ IPv6.

## Điều kiện mạng

- Fedora chạy tracker và seeder phải có IPv6 công khai (global unicast), ví dụ một địa chỉ thuộc dải `2000::/3`. `fe80::/10` chỉ dùng trên liên kết cục bộ và `fd00::/8` không phải địa chỉ Internet công khai.
- Android leecher dùng **mạng di động riêng** có IPv6. Không kiểm chứng Internet bằng cách nối Fedora vào hotspot của chính điện thoại leecher.
- Cổng TCP `8000` (tracker) và `6881` (seeder) trên Fedora phải nhận được kết nối từ Android. Kiểm tra firewall Fedora và firewall IPv6 của router/hotspot. Leecher chỉ cần kết nối đi để tải từ seeder; để làm seeder cho máy khác, Android cũng cần cho kết nối đi vào cổng của nó.
- Tắt Tailscale trên cả hai thiết bị khi thử. Nếu mạng không cấp IPv6 công khai hoặc chặn kết nối đi vào, dừng kết luận ở mức “đã chạy trên `::1`”; code không thể tự mở đường qua firewall.

## Chạy trên Fedora

1. Xem IPv6 của interface đang dùng:

   ```sh
   ip -6 -brief address show scope global
   ```

   Chọn địa chỉ IPv6 công khai của Fedora làm `FEDORA_IPV6` trong các lệnh dưới. Địa chỉ có thể thay đổi khi đổi mạng; kiểm tra lại trước mỗi lần thử.

2. Nếu dùng `firewalld`, mở hai cổng cho phiên thử và kiểm tra thêm firewall IPv6 của router:

   ```sh
   sudo firewall-cmd --add-port=8000/tcp --add-port=6881/tcp
   ```

3. Tại thư mục gốc dự án, chạy tracker ở terminal thứ nhất:

   ```sh
   python3 run_tracker.py
   ```

   Tracker lắng nghe IPv6 tại `[::]:8000`.

4. Tạo file mẫu mới để ứng dụng tạo `.meta.json` chứa địa chỉ tracker IPv6; giữ file gốc trên Fedora:

   ```sh
   dd if=/dev/urandom of=/tmp/ipv6-demo.bin bs=1M count=4
   sha256sum /tmp/ipv6-demo.bin
   python3 run_client.py 6881 FEDORA_IPV6
   ```

   Trong menu chọn `1`, nhập `/tmp/ipv6-demo.bin`. Metadata sẽ được tạo tại `demo_workspace/shared/ipv6-demo.bin.meta.json`. Giữ client chạy và đợi hơn 120 giây trước khi thử tải nếu muốn kiểm tra heartbeat của seeder.

Mặc định tracker công bố IPv6 nguồn thấy trên kết nối `ANNOUNCE`; dòng `Tracker đã công bố IPv6 [...]` cho biết địa chỉ thực tế. Nếu máy có nhiều IPv6 và tracker chọn địa chỉ không nhận được kết nối từ ngoài, chạy lại client với `--advertise-ip FEDORA_IPV6`.

## Chạy trên Android/Termux

1. Bật dữ liệu di động, tắt Wi-Fi và Tailscale. Chuẩn bị Python trong Termux (`pkg install python`) và cho phép truy cập thư mục Download nếu cần (`termux-setup-storage`).
2. Chép **source cùng file `.meta.json`**, không chép `/tmp/ipv6-demo.bin`, sang điện thoại. Ví dụ tạo gói trên Fedora:

   ```sh
   tar --exclude='__pycache__' --exclude='*.pyc' -czf /tmp/p2p-ipv6-src.tar.gz run_client.py peer protocol storage demo_workspace/shared/ipv6-demo.bin.meta.json
   ```

   Chép gói sang thư mục Download qua USB, sau đó giải nén trong Termux:

   ```sh
   mkdir -p ~/p2p-file-sharing
   tar -xzf ~/storage/downloads/p2p-ipv6-src.tar.gz -C ~/p2p-file-sharing
   cd ~/p2p-file-sharing
   ```

3. Trước khi tải, kiểm tra Android thực sự mở được kết nối TCP IPv6 tới Fedora. Thay `FEDORA_IPV6` bằng địa chỉ thật trong mỗi lệnh:

   ```sh
   python -c 'import socket; s=socket.socket(socket.AF_INET6); s.settimeout(5); s.connect(("FEDORA_IPV6",8000)); print("Tracker OK"); s.close()'
   python -c 'import socket; s=socket.socket(socket.AF_INET6); s.settimeout(5); s.connect(("FEDORA_IPV6",6881)); print("Seeder OK"); s.close()'
   ```

4. Chạy `python run_client.py 6882 FEDORA_IPV6`, chọn `2` và nhập `demo_workspace/shared/ipv6-demo.bin.meta.json`. File nhận ở `demo_workspace/downloads/ipv6-demo.bin`.

5. Chạy `sha256sum demo_workspace/downloads/ipv6-demo.bin` trên Android và so với kết quả trên Fedora. Ghi lại log `Tracker đã công bố IPv6`, `Kết nối P2P thành công`, tỷ lệ mảnh và SHA-256.

Nếu Android kết nối tracker được nhưng không kết nối seeder, kiểm tra lại địa chỉ IPv6 seeder được công bố, cổng 6881 và firewall/router. Nếu cả hai cổng không kết nối được, kiểm tra IPv6 công khai và cổng 8000 trước. `ping -6` thành công không thay thế kiểm tra cổng TCP.

## Giới hạn hiện tại

- Chưa có đường dự phòng khi một trong hai mạng không hỗ trợ IPv6 hoặc chặn kết nối TCP đi vào.
- Tracker và peer chưa có xác thực riêng; chỉ dùng file thử không nhạy cảm trên mạng có kiểm soát.
- Kết quả test `::1` chỉ xác nhận code chạy cục bộ, không chứng minh truyền được giữa hai mạng Internet.
