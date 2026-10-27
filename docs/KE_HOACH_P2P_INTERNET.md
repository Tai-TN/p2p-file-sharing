# Demo truyền file Linux → Android qua Tailscale

## Mục tiêu và trạng thái

Mục tiêu của lần thử này là chạy tracker và seeder trên Linux, chạy leecher Python trong Termux trên Android, rồi tải file qua hai mạng Internet khác nhau. Hai thiết bị dùng cùng mạng Tailscale; không cần public IPv4 hay port forwarding. Đây là demo qua VPN, chưa phải tính năng xuyên NAT độc lập của ứng dụng.

| Hạng mục | Trạng thái |
| --- | --- |
| Linux `taitn` trong Tailscale | Đã thấy IP `100.108.79.47` khi lập kế hoạch; kiểm tra lại bằng `tailscale ip -4` trước khi chạy. |
| Android `v2352ga` trong Tailscale | Đã thấy IP `100.82.132.18`; người dùng đã quan sát `tailscale status` báo `direct` trong lúc truyền file. |
| Code và kiểm thử trên Linux | Có `--advertise-ip`, heartbeat cho seeder và `--tailscale` để tự lấy IP trên Fedora; `python3 -m unittest discover tests` đạt 8/8 bài test, gồm truyền file qua socket localhost. |
| Gói source và file mẫu | Commit `92ba2fc` có gói `demo_workspace/p2p-file-sharing-src.tar.gz` và file mẫu; gói đó được tạo trước tùy chọn `--tailscale`. Tạo lại gói nếu muốn Android dùng cùng source mới. |
| Termux và Python trên Android | Người dùng đã gửi log Termux cho thấy kết nối tới seeder và nhận các mảnh file. |
| Truyền file Linux → Android và đối chiếu SHA-256 | Người dùng báo đã tải thành công và Fedora hiển thị đường `direct`; chưa lưu kết quả SHA-256 hai file trong tài liệu này. |

Tailscale có thể dùng relay khi NAT chặn đường kết nối trực tiếp: <https://tailscale.com/docs/reference/device-connectivity>.

## Các bước thực hiện

### 1. Chuẩn bị Android

1. Cài Termux từ F-Droid hoặc bản phát hành GitHub theo hướng dẫn của dự án: <https://github.com/termux/termux-app#installation>. Không trộn ứng dụng Termux và plugin lấy từ các nguồn khác nhau.
2. Giữ Tailscale trên điện thoại ở trạng thái Connected. Tắt Wi-Fi và dùng dữ liệu di động để thử hai mạng khác nhau.
3. Trong Termux, chạy lần lượt:

   ```sh
   pkg update
   pkg install python
   python --version
   termux-setup-storage
   ```

   Chấp nhận quyền truy cập bộ nhớ khi Android hỏi. Nếu Termux không truy cập được IP Tailscale, kiểm tra Termux có bị loại khỏi VPN trong cài đặt split tunneling của Tailscale không: <https://tailscale.com/docs/features/client/android-app-split-tunneling>.

### 2. Chuyển source đang dùng sang Android

Source trên Linux hiện có thể chứa thay đổi chưa commit; vì vậy dùng bản source thực tế đang thử. Gói ở commit `92ba2fc` chứa code cũ. Tạo gói mới từ thư mục gốc dự án bằng:

```sh
tar --exclude='__pycache__' --exclude='*.pyc' -czf /tmp/p2p-file-sharing-tailscale-src.tar.gz run_client.py run_tracker.py peer protocol storage demo_workspace/shared/internet-demo.bin.meta.json
```

Chép gói `/tmp/p2p-file-sharing-tailscale-src.tar.gz` bằng USB vào thư mục `Download` trên điện thoại. Gói chỉ chứa code và metadata, không chứa file gốc cần tải. Trong Termux:

```sh
mkdir -p ~/p2p-file-sharing
tar -xzf ~/storage/downloads/p2p-file-sharing-tailscale-src.tar.gz -C ~/p2p-file-sharing
cd ~/p2p-file-sharing
```

Nếu tên thư mục trên điện thoại khác `Download`, tìm gói bằng ứng dụng Files rồi chép vào `Download` trước khi giải nén.

### 3. Chạy tracker và seeder trên Linux

Kiểm tra Tailscale đang kết nối bằng `tailscale status`; lấy IP Fedora bằng `tailscale ip -4` để điền vào lệnh trên cả hai máy. `--tailscale` trên Fedora tự đọc IP này và báo cho tracker, không tự bật Tailscale. Nếu daemon chưa chạy, dùng `sudo systemctl start tailscaled`, sau đó `sudo tailscale up`. Các lệnh dưới dùng IP đã thấy lúc lập kế hoạch; thay IP tracker nếu thiết bị được cấp địa chỉ khác.

Terminal thứ nhất:

```sh
python3 run_tracker.py
```

Terminal thứ hai:

```sh
python3 run_client.py 6881 100.108.79.47 --tailscale
```

Trong menu, chọn `1` và nhập đường dẫn tuyệt đối của file mẫu `demo_workspace/internet-demo.bin` (1,5 MiB, gồm 3 mảnh). Có thể lấy đường dẫn bằng `realpath demo_workspace/internet-demo.bin` từ terminal Linux. Metadata đã được chuẩn bị tại `demo_workspace/shared/internet-demo.bin.meta.json`; client sẽ kiểm tra file nguồn và bắt đầu chia sẻ. Giữ tracker và seeder đang chạy. Đợi hơn 120 giây trước khi bắt đầu tải để kiểm tra heartbeat có giữ seeder trên tracker hay không.

Khi Android bắt đầu tải, chọn `3` trên Fedora để xem tiến độ và kết quả `tailscale status`. Dòng `direct` hoặc `relay` là đường Tailscale đang dùng tại thời điểm kiểm tra; ứng dụng không chờ `direct` mới truyền file. Nếu CLI thiếu, chưa kết nối hoặc không trả IPv4 hợp lệ, `--tailscale` báo lỗi ngay khi khởi động. Không dùng `--tailscale` đồng thời với `--advertise-ip`.

### 4. Chạy leecher trên Android

File metadata đã có trong gói source vừa giải nén. **Không chép `internet-demo.bin`** sang điện thoại; file nguồn phải được tải qua ứng dụng P2P.

Trong Termux, tại `~/p2p-file-sharing`, chạy:

```sh
python run_client.py 6882 100.108.79.47 --advertise-ip 100.82.132.18
```

Ứng dụng Tailscale Android không cung cấp CLI chính thức cho Termux, nên xem IPv4 Android trong ứng dụng Tailscale rồi nhập bằng `--advertise-ip` như trên.

Trong menu, chọn `2` và nhập đường dẫn tuyệt đối in bởi `realpath ~/p2p-file-sharing/demo_workspace/shared/internet-demo.bin.meta.json`. Chọn `3` để theo dõi tiến độ; file nhận ở `demo_workspace/downloads/internet-demo.bin` trong thư mục dự án trên Termux.

### 5. Xác minh

Trên Linux, chạy `sha256sum demo_workspace/internet-demo.bin`. Trong Termux, chạy `sha256sum ~/p2p-file-sharing/demo_workspace/downloads/internet-demo.bin`. Hai chuỗi SHA-256 phải trùng nhau; với file mẫu đã tạo, giá trị mong đợi là `e6193b4df0ded214f49ca1b46e1a13b7564293cc9efe8525c17814da96a1b6b5`. Ghi lại thời điểm bắt đầu, kết quả và log lỗi nếu có.

Nếu không tải được, kiểm tra theo thứ tự: Tailscale Connected ở cả hai máy; IP Fedora dùng làm địa chỉ tracker có đổi không; Termux có đi qua VPN không; tracker đang nghe cổng TCP 8000; seeder đang nghe cổng TCP 6881; dòng `IP máy này` trên seeder có đúng IP Tailscale không. `tailscale ping` chỉ kiểm tra đường mạng ảo, chưa chứng minh cổng ứng dụng đang mở.

## Giới hạn và bước sau demo

- Relay Tailscale giúp thử chức năng trên mạng không mở được cổng, nhưng tốc độ có thể thấp hơn đường trực tiếp.
- Tracker và giao thức P2P của dự án chưa có xác thực riêng. Chỉ chạy demo trong mạng Tailscale của các thiết bị tin cậy.
- Nếu muốn người dùng không cần cài Tailscale, cần thiết kế và triển khai relay hoặc cơ chế xuyên NAT trong ứng dụng ở giai đoạn sau.
