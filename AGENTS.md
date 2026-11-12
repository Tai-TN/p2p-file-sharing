# Hướng dẫn làm việc với coding agent

File này áp dụng cho toàn bộ repository. Đây là quy ước làm việc chung để agent hiểu dự án và tạo thay đổi dễ kiểm tra; yêu cầu cụ thể của người dùng trong từng tác vụ được ưu tiên.

## Dự án

- Đây là ứng dụng chia sẻ file P2P bằng Python và TCP. Tracker giúp các peer tìm nhau; dữ liệu file truyền trực tiếp giữa các peer.
- `run_tracker.py` khởi động tracker; `run_client.py` khởi động client dạng menu.
- `tracker/` quản lý swarm và danh sách peer; `peer/` quản lý client, kết nối, tải và phục vụ mảnh; `protocol/` định nghĩa thông điệp và framing; `storage/` quản lý metadata, mảnh file và kiểm tra SHA-256; `tests/` chứa kiểm thử.
- `docs/HUONG_DAN_CHI_TIET.md` giải thích kiến trúc. Đối chiếu tài liệu với code hiện tại trước khi dựa vào một mô tả chi tiết.

## Nguyên tắc thay đổi

1. Đọc luồng liên quan và `git status` trước khi sửa. Giữ nguyên các thay đổi sẵn có của người dùng; không tự ý reset, xóa hoặc ghi đè chúng.
2. Chỉ sửa những phần cần cho yêu cầu. Ưu tiên thay đổi nhỏ, rõ nguyên nhân; không trộn refactor lớn vào một bản sửa lỗi.
3. Giữ tương thích với cách chạy và định dạng `.meta.json` hiện có, trừ khi tác vụ yêu cầu thay đổi. Nếu sửa giao thức, cập nhật đồng thời bên gửi, bên nhận và tài liệu liên quan.
4. Dữ liệu nhận qua mạng và file metadata là đầu vào không đáng tin cậy. Kiểm tra loại, độ dài, chỉ số mảnh và kích thước thông điệp trước khi cấp phát bộ nhớ hoặc ghi file. Không dùng đường dẫn từ metadata để ghi ra ngoài thư mục tải được chỉ định.
5. Giữ kiểm tra SHA-256 trước khi đánh dấu một mảnh là đã có. Khi khôi phục tiến độ, trạng thái lưu trong JSON không thay thế việc xác minh dữ liệu trên đĩa.
6. Với code đa luồng, bảo vệ trạng thái dùng chung bằng lock phù hợp. Không giữ lock chung trong lúc gọi mạng hoặc thao tác có thể chờ lâu. Đảm bảo socket và luồng được dừng gọn khi client thoát.
7. Lỗi mạng phải có timeout, cách thử lại hữu hạn hoặc khoảng nghỉ hợp lý, và thông tin đủ để chẩn đoán. Tránh nuốt lỗi âm thầm ở luồng tải chính.
8. Không thêm dependency chỉ để giải quyết việc thư viện chuẩn Python đã làm được với code logic đơn giản , không phức tạp.Nếu việc dùng dependency khiến code dễ hiểu và đỡ phức tạp hơn thì cần nêu rõ lý do và cách cài đặt.

## Kiểm tra trước khi bàn giao

- Chạy kiểm thử phù hợp với phần đã sửa bằng `python3 -m unittest discover tests`; nếu một test hiện có lỗi do giả định cũ, ghi rõ lỗi và nguyên nhân, không báo là đã đạt.
- Với thay đổi về truyền file, kiểm tra ít nhất luồng seeder → leecher và so sánh nội dung hoặc SHA-256 của file nhận. Với thay đổi Internet/NAT, kiểm thử trên hai mạng thực sự khác nhau; thử trong cùng LAN không chứng minh được khả năng truy cập từ Internet.
- Xem lại `git diff` để bảo đảm chỉ có thay đổi dự kiến. Báo ngắn gọn đã sửa gì, đã kiểm tra gì và còn giới hạn nào.

## Dữ liệu và môi trường

- Dùng thư mục tạm cho file thử nghiệm. Không commit file tải, file mẫu lớn, log, cache hoặc bí mật; `demo_workspace/` là dữ liệu chạy thử và đã được ignore.
- Không coi `docs/KE_HOACH_P2P_INTERNET.md` là tính năng đã triển khai. Chỉ cập nhật tài liệu kế hoạch khi tác vụ yêu cầu hoặc khi trạng thái triển khai thực tế thay đổi.
