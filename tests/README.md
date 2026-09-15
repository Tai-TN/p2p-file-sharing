# 🧪 Tài liệu Hướng dẫn và Giải thích Test (Unit Tests Documentation)

Tài liệu này giải thích chi tiết cấu trúc, mục đích và cách hoạt động của các bài thử nghiệm (Unit Tests) trong thư mục `tests/` thuộc dự án **P2P File Sharing**.

---

## 📌 1. Tổng quan cấu trúc thư mục `tests/`

```text
tests/
├── README.md               # Tài liệu hướng dẫn chi tiết về test (File này)
├── __init__.py             # Đánh dấu tests/ là một Python package
├── test_piece_manager.py   # Test module quản lý lưu trữ, chia mảnh & kiểm tra tính toàn vẹn (Checksum)
└── test_protocol.py        # Test module đóng gói, mã hóa Base64 & truyền nhận thông điệp qua Socket
```

---

## 🚀 2. Cách chạy bộ Test

Bạn có thể chạy các bài test từ thư mục gốc của dự án (`p2p-file-sharing/`):

### Cách 1: Chạy toàn bộ các test trong thư mục (Khuyên dùng)
```bash
python3 -m unittest discover tests
```

### Cách 2: Chạy riêng từng file test
```bash
python3 tests/test_piece_manager.py
python3 tests/test_protocol.py
```

---

## 🔍 3. Chi tiết từng File Test

### 📂 File 1: `test_piece_manager.py`
Module này kiểm tra toàn bộ logic lưu trữ file dưới đĩa cứng, chia mảnh (piece), tính toán hash SHA-256, ghi file không theo thứ tự và khả năng khôi phục tiến độ (Resume download).

| Tên Test Case | Mục đích & Chi tiết kiểm tra |
| :--- | :--- |
| `test_metadata_creation` | **Tạo và chuyển đổi Metadata:**<br>• Kiểm tra hàm `PieceMetadata.create_from_file()` chia file 350 bytes thành 4 pieces (100, 100, 100, 50 bytes).<br>• Kiểm tra tính chính xác của `info_hash` và mảng danh sách các chuỗi hash SHA-256.<br>• Kiểm tra tính năng Seri/Deseri (`to_dict()` và `from_dict()`). |
| `test_out_of_order_write_and_read` | **Ghi mảnh không theo thứ tự (Out-of-order execution):**<br>• Giả lập P2P tải các piece từ nhiều peer khác nhau theo thứ tự ngẫu nhiên: `Piece 2` ➔ `Piece 0` ➔ `Piece 3` ➔ `Piece 1`.<br>• Kiểm tra tính toán % tiến độ (`get_progress()`) và danh sách mảnh còn thiếu (`get_missing_pieces()`).<br>• Đọc lại toàn bộ file sau khi hoàn thành 100% và so sánh chính xác từng byte dữ liệu với file gốc. |
| `test_corrupted_piece_rejection` | **Kiểm tra từ chối dữ liệu bị hỏng (Checksum Integrity):**<br>• Gửi dữ liệu giả bị biến dạng (`b"X" * 100`) vào piece 0.<br>• Kiểm tra hàm `write_piece()` phát hiện sai checksum SHA-256, in cảnh báo `[Storage Error]` và trả về `False`.<br>• Xác nhận `bitfield[0]` vẫn là `False` (không ghi nhận piece hỏng). |
| `test_existing_file_resume` | **Khôi phục tiến độ tải dở (Resume download):**<br>• Giả lập ghi dở mảnh 0 và mảnh 1 rồi tắt ứng dụng.<br>• Tạo mới `PieceManager` trỏ vào file dở đó và kiểm tra hàm `_verify_existing_file()` có tự động quét đĩa, xác minh hash và khôi phục mảng `bitfield` đúng trạng thái trước đó hay không. |

---

### 📂 File 2: `test_protocol.py`
Module này kiểm tra giao thức truyền thông mạng P2P, đóng gói JSON framing qua giao thức Length-Prefix (4 bytes độ dài) và mã hóa dữ liệu nhị phân (Binary chunks).

| Tên Test Case | Mục đích & Chi tiết kiểm tra |
| :--- | :--- |
| `test_send_and_recv_msg` | **Truyền nhận thông điệp qua Socket (Length-prefix Framing):**<br>• Sử dụng `socket.socketpair()` tạo cặp socket ảo giao tiếp trực tiếp trong RAM.<br>• Kiểm tra `send_msg()` gắn 4-byte độ dài ở đầu gói tin JSON và `recv_msg()` bóc tách chính xác loại thông điệp `ANNOUNCE`. |
| `test_binary_piece_encoding` | **Mã hóa nhị phân dữ liệu mảnh (Base64 Encoding):**<br>• Kiểm tra hàm `create_piece_msg()` chuyển đổi mảng bytes nhị phân bất kỳ (kể cả chứa ký tự đặc biệt `\x00\xff\xfe`) thành chuỗi mã hóa Base64 an toàn cho JSON.<br>• Kiểm tra phía nhận `base64.b64decode()` và khôi phục lại nguyên vẹn dữ liệu nhị phân ban đầu. |
| `test_bitfield_creation` | **Tạo thông điệp trạng thái mảnh (`BITFIELD` message):**<br>• Kiểm tra hàm `create_bitfield_msg()` đóng gói mảng boolean đại diện cho trạng thái sở hữu các piece thành định dạng JSON chuẩn. |

---

## 🛠️ 4. Các kỹ thuật nổi bật được dùng trong Test

1. **`tempfile.mkdtemp()`**: Tạo thư mục tạm thời cách ly tuyệt đối với đĩa cứng máy thật. Sau khi test xong, hàm `tearDown()` sẽ tự động xóa sạch thư mục này, không làm bẩn đĩa cứng.
2. **`socket.socketpair()`**: Tạo cặp Socket ảo kết nối trực tiếp với nhau mà không cần mở Port thật trên Card mạng (IP/Port), giúp test socket cực nhanh và ổn định.
3. **`os.urandom(350)`**: Sinh chuỗi dữ liệu nhị phân ngẫu nhiên để kiểm tra chuẩn xác tính toàn vẹn dữ liệu khi ghi đĩa hoặc truyền qua mạng.
