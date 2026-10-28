# Đo bottleneck khi tải từ nhiều seeder

Hai seeder ở hai mạng riêng, mỗi seeder tải riêng đạt khoảng 5 MiB/s, nhưng kết hợp còn 3 MiB/s chưa đủ để kết luận nghẽn ở đâu. Bộ điều phối hiện chỉ có 4 worker tổng cộng và chọn ngẫu nhiên peer cho mỗi mảnh. Hai seeder có thể làm một worker luân phiên dùng nhiều kết nối TCP; ứng dụng chưa đo tốc độ từng peer để phân công công việc.

Phép thử localhost với 48 MiB, 96 mảnh, 4 worker và độ trễ phản hồi giả lập đã cho thấy:

| Trường hợp | Tốc độ payload đo ngoài hàm tải |
| --- | ---: |
| A (thêm 40 ms chờ/mảnh) | 23,26 MiB/s |
| B (thêm 200 ms chờ/mảnh) | 8,62 MiB/s |
| A + B | 11,80 MiB/s |
| Hai peer cùng thêm 40 ms | 23,18 MiB/s |

Mọi file nhận trong phép thử đều trùng SHA-256. Đây là dữ liệu giả lập cục bộ: nó chứng minh việc thêm peer chậm có thể kéo tốc độ xuống, nhưng không giải thích được ngay trường hợp hai seeder đều đạt 5 MiB/s khi tải riêng.

## Thu số đo ở leecher

Chép thêm `tools/profile_client.py` sang source trên Android, rồi chạy tại thư mục gốc dự án:

```sh
python3 tools/profile_client.py 6882 FEDORA_IPV6 --profile-output demo_workspace/profile-a.jsonl
```

Thay `FEDORA_IPV6` bằng IPv6 thực của tracker. Menu và các tùy chọn khác giống `run_client.py`; seeder chạy như bình thường. Công cụ chỉ bọc các lời gọi để đo, không đổi cách chọn peer, định dạng giao thức hay dữ liệu file.

Thử cùng một file lần lượt với A, B, rồi A+B. Đặt tên báo cáo tương ứng `profile-a.jsonl`, `profile-b.jsonl`, `profile-ab.jsonl`. Trước mỗi lần, thoát leecher và chuyển file nhận cùng `.progress.json` (nếu có) ra khỏi thư mục downloads để lần sau tải mới từ 0%. Giữ nguyên file nguồn và metadata. Sau mỗi lần, đối chiếu SHA-256.

Sau khi tải hoàn tất, chương trình in `[Transfer profile]` và thêm một dòng JSON vào file báo cáo. Nếu cần dừng giữa chừng, thoát bằng menu để có báo cáo phần đã tải. File trong `demo_workspace/` không được commit.

## Đọc kết quả

- `payload_MiB_s`: số byte thực nhận thành công chia cho thời gian từ khi bắt đầu tác vụ tới yêu cầu mảnh cuối cùng kết thúc. Bao gồm khôi phục/tìm peer/kết nối ban đầu; loại thời gian chờ vòng kiểm tra hoàn tất sau mảnh cuối. Không dùng toàn bộ kích thước file làm tử số khi resume.
- `process_cpu_s`: tổng giây CPU của cả tiến trình trong khoảng đo. Chia cho `elapsed_s` để ước lượng số lõi CPU được dùng trung bình; các tác vụ khác cùng tiến trình cũng được tính.
- `peers.*.pieces`, `payload_bytes`, `errors`: mỗi peer thực sự cung cấp bao nhiêu dữ liệu và có bao nhiêu yêu cầu lỗi.
- `tcp_sessions`: số kết nối TCP mà các worker đã dùng tới peer. `peak_requests`: số yêu cầu đồng thời cao nhất tới peer, kể cả lúc mở kết nối.
- `mean_request_ms` / `mean_thread_cpu_ms`: thời gian thực và CPU trung bình của một yêu cầu, bao gồm lấy socket, nhận dữ liệu, xác minh/ghi và thông báo HAVE.
- `mean_phase_ms.connection`: thời gian lấy socket hoặc mở kết nối/handshake mới. Nhận dữ liệu handshake được tính ở đây.
- `mean_phase_ms.receive_json`: thời gian nhận thông điệp PIECE và giải mã JSON. Chênh lệch lớn giữa `wall` và `cpu` biểu thị thời gian thread chờ hoặc không được chạy CPU; cần kết hợp số đo mạng/CPU để phân biệt.
- `mean_phase_ms.verify_write`: xác minh SHA-256, chờ khóa và ghi file. `wall` cao nhưng `cpu` thấp gợi ý chờ I/O hoặc khóa; chưa đủ để kết luận ổ đĩa chậm.
- `outstanding_requests` phải bằng 0 khi đọc báo cáo tải hoàn tất. Nếu khác 0, vẫn còn yêu cầu chưa được ghi nhận xong trong thời gian chờ báo cáo.

Các worker chạy đồng thời, nên tổng thời gian của chúng có thể lớn hơn thời gian tải. Không cộng các thời gian này rồi coi là thời gian thực.

Nếu A+B có nhiều socket mới hoặc thời gian kết nối tăng, thử file lớn hơn để xem chi phí thiết lập và TCP tăng tốc ban đầu có chi phối lượt tải ngắn không. Nếu chủ yếu tăng ở `receive_json.wall` và CPU thấp, tiếp tục đo đường mạng và thời gian phản hồi từng seeder. Nếu CPU hoặc `verify_write` cao, đo sâu xử lý JSON/Base64, SHA và ghi đĩa trước khi đổi scheduler.
