# Demo truyền file Linux → Android qua Tailscale

## Mục tiêu và trạng thái

Mục tiêu của lần thử này là chạy tracker và seeder trên Linux, chạy leecher Python trong Termux trên Android, rồi tải file qua hai mạng Internet khác nhau. Hai thiết bị dùng cùng mạng Tailscale; không cần public IPv4 hay port forwarding. Đây là demo qua VPN, chưa phải tính năng xuyên NAT độc lập của ứng dụng.

| Hạng mục | Trạng thái |
| --- | --- |
| Linux `taitn` trong Tailscale | Đã thấy IP `100.108.79.47` khi lập kế hoạch; kiểm tra lại bằng `tailscale ip -4` trước khi chạy. |
| Android `v2352ga` trong Tailscale | Đã thấy IP `100.82.132.18`; `tailscale ping` nhận phản hồi qua `DERP(sin)`. |
| Code và kiểm thử trên Linux | Đã thêm `--advertise-ip`, heartbeat và báo lỗi khi đăng ký tracker thất bại; `python3 -m unittest discover tests -v` đạt 5/5 bài test qua socket localhost. |
| Gói source và file mẫu | Đã tạo `demo_workspace/p2p-file-sharing-src.tar.gz` (gồm source và metadata), `demo_workspace/internet-demo.bin`; file mẫu không nằm trong gói để Android phải tải nó qua P2P. |
| Termux và Python trên Android | Theo phản hồi của người dùng, Android đã gửi được log tới tracker; chưa kiểm tra trực tiếp trên thiết bị. |
| Truyền file Linux → Android và đối chiếu SHA-256 | Chưa được xác nhận trên hai thiết bị. |

Kiểm tra trên Fedora ngày 25/10/2026: client đang lắng nghe TCP 6881 với lệnh `python3 run_client.py 6881 100.108.79.47 --advertise-ip 100.108.79.47`; Tailscale vẫn thấy hai thiết bị. Tuy nhiên tại thời điểm kiểm tra, không có tiến trình lắng nghe TCP 8000 trên Fedora. Cần chạy tracker trước khi seeder đăng ký. Kết quả này là ảnh chụp tại thời điểm kiểm tra, không khẳng định tracker chưa từng chạy khi Android gửi log.

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

Source trên Linux hiện có thể chứa thay đổi chưa commit; vì vậy dùng bản source thực tế đang thử, thay vì mặc định clone từ GitHub. Gói đã được chuẩn bị tại `demo_workspace/p2p-file-sharing-src.tar.gz`; gói chứa source và `.meta.json`, không chứa dữ liệu file cần tải. Nếu sửa code sau này, tạo lại gói từ thư mục gốc dự án bằng:

```sh
tar --exclude='__pycache__' --exclude='*.pyc' -czf demo_workspace/p2p-file-sharing-src.tar.gz run_client.py run_tracker.py peer protocol storage demo_workspace/shared/internet-demo.bin.meta.json
```

Chép gói này bằng USB vào thư mục `Download` trên điện thoại. Trong Termux:

```sh
mkdir -p ~/p2p-file-sharing
tar -xzf ~/storage/downloads/p2p-file-sharing-src.tar.gz -C ~/p2p-file-sharing
cd ~/p2p-file-sharing
```

Nếu tên thư mục trên điện thoại khác `Download`, tìm gói bằng ứng dụng Files rồi chép vào `Download` trước khi giải nén.

### 3. Chạy tracker và seeder trên Linux

Kiểm tra IP Tailscale hiện tại bằng `tailscale ip -4`. Các lệnh dưới dùng giá trị đã thấy lúc lập kế hoạch; thay IP nếu thiết bị được cấp địa chỉ khác.

Terminal thứ nhất:

```sh
python3 run_tracker.py
```

Terminal thứ hai:

```sh
python3 run_client.py 6881 100.108.79.47 --advertise-ip 100.108.79.47
```

Trong menu, chọn `1` và nhập đường dẫn tuyệt đối của file mẫu `demo_workspace/internet-demo.bin` (1,5 MiB, gồm 3 mảnh). Có thể lấy đường dẫn bằng `realpath demo_workspace/internet-demo.bin` từ terminal Linux. Metadata đã được chuẩn bị tại `demo_workspace/shared/internet-demo.bin.meta.json`; client sẽ kiểm tra file nguồn và bắt đầu chia sẻ. Giữ tracker và seeder đang chạy. Đợi hơn 120 giây trước khi bắt đầu tải để kiểm tra heartbeat có giữ seeder trên tracker hay không.

Sau khi chọn `1`, terminal seeder phải in `Đã đăng ký với tracker 100.108.79.47:8000`, còn terminal tracker phải in `Cập nhật Peer ...`. Chỉ khởi động client chưa đăng ký file; phải chọn `1` và nhập đúng file nguồn. Nếu đã mở client trước khi sửa code, thoát bằng menu `4`, chạy lại lệnh trên để dùng bản mới. Có thể kiểm tra tracker đang nghe bằng `ss -ltnp | rg ':8000'` trên Fedora. Nếu cổng 8000 trống hoặc client báo `Connection refused`, chạy lại `python3 run_tracker.py` và chọn `1` lại. Nếu tracker đang nghe nhưng vẫn không có log, kiểm tra dòng `Tracker` in lúc client khởi động và lỗi ANNOUNCE trong terminal seeder.

### 4. Chạy leecher trên Android

File metadata đã có trong gói source vừa giải nén. **Không chép `internet-demo.bin`** sang điện thoại; file nguồn phải được tải qua ứng dụng P2P.

Trong Termux, tại `~/p2p-file-sharing`, chạy:

```sh
python run_client.py 6882 100.108.79.47 --advertise-ip 100.82.132.18
```

Trong menu, chọn `2` và nhập đường dẫn tuyệt đối in bởi `realpath ~/p2p-file-sharing/demo_workspace/shared/internet-demo.bin.meta.json`. Chọn `3` để theo dõi tiến độ; file nhận ở `demo_workspace/downloads/internet-demo.bin` trong thư mục dự án trên Termux.

### 5. Xác minh

Trên Linux, chạy `sha256sum demo_workspace/internet-demo.bin`. Trong Termux, chạy `sha256sum ~/p2p-file-sharing/demo_workspace/downloads/internet-demo.bin`. Hai chuỗi SHA-256 phải trùng nhau; với file mẫu đã tạo, giá trị mong đợi là `e6193b4df0ded214f49ca1b46e1a13b7564293cc9efe8525c17814da96a1b6b5`. Ghi lại thời điểm bắt đầu, kết quả và log lỗi nếu có.

Nếu không tải được, kiểm tra theo thứ tự: Tailscale Connected ở cả hai máy; IP Tailscale có đổi không; Termux có đi qua VPN không; tracker đang nghe cổng TCP 8000; seeder đang nghe cổng TCP 6881; `--advertise-ip` của seeder có đúng IP Tailscale không. `tailscale ping` chỉ kiểm tra đường mạng ảo, chưa chứng minh cổng ứng dụng đang mở.

## Giới hạn và bước sau demo

- Relay Tailscale giúp thử chức năng trên mạng không mở được cổng, nhưng tốc độ có thể thấp hơn đường trực tiếp.
- Tracker và giao thức P2P của dự án chưa có xác thực riêng. Chỉ chạy demo trong mạng Tailscale của các thiết bị tin cậy.
- Nếu muốn người dùng không cần cài Tailscale, cần thiết kế và triển khai relay hoặc cơ chế xuyên NAT trong ứng dụng ở giai đoạn sau.

## Thử chế độ UDP trực tiếp (26/10/2026)

Source hiện có chế độ `--transport udp` viết bằng thư viện chuẩn Python, không dùng WebRTC.
Hai peer dùng cùng socket UDP cho STUN, thử nhiều địa chỉ ứng viên (host và địa chỉ
quan sát được từ các máy STUN), xác nhận probe/ACK rồi truyền mảnh qua UDP với
ACK, gửi lại gói mất và kiểm tra SHA-256. `--transport tcp` vẫn là mặc định.
Tracker chỉ chuyển thông tin endpoint và tín hiệu; ngrok TCP chỉ cần cho tracker.

Chạy cùng bản source mới ở cả Fedora và Android. Tắt Tailscale trên cả hai máy,
giữ Android trên dữ liệu di động:

```sh
# Máy chạy tracker, nếu tracker đặt trên Fedora:
python3 run_tracker.py

# Fedora seeder (thay HOST:PORT bằng địa chỉ ngrok TCP hiện tại của tracker):
python3 run_client.py 6881 HOST:PORT --transport udp

# Android Termux leecher:
python run_client.py 6882 HOST:PORT --transport udp
```

Seeder chọn `1` và chia sẻ file nhỏ nhiều mảnh; chuyển **chỉ file `.meta.json`** sang
Android, chọn `2` để tải. Khi thành công, hai máy phải hiện `Đường UDP trực tiếp ...
đã xác nhận`, leecher hiện `Piece ... ✓ UDP` và SHA-256 file nhận trùng file gốc.
Nếu sau 100 giây hiện `Không tạo được đường UDP trực tiếp`, đó là thất bại của
đường trực tiếp trên cặp mạng thử này. Những địa chỉ do STUN trả về chỉ là ứng viên,
không bảo đảm đi vào được qua NAT hoặc firewall. Chế độ này chưa có relay, mã hóa,
xác thực peer và chưa hỗ trợ nhiều seeder đồng thời trong một lượt tải.

Kiểm thử localhost đã truyền đủ file nhiều mảnh và so sánh SHA-256. Chưa kiểm thử
được Fedora ↔ Android trên hai mạng thực tế; không coi kiểm thử localhost là bằng
chứng xuyên NAT thành công.
