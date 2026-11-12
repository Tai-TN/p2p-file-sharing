# 🧪 Tài liệu Hướng dẫn và Giải thích Test (Unit Tests Documentation)

Tài liệu này giải thích chi tiết cấu trúc, mục đích và cách hoạt động của các bài thử nghiệm (Unit Tests) trong thư mục `tests/` thuộc dự án **P2P File Sharing**.

---

## 📌 1. Tổng quan cấu trúc thư mục `tests/`

```text
tests/
├── README.md                   # Tài liệu hướng dẫn chi tiết về test (File này)
├── __init__.py                 # Đánh dấu tests/ là một Python package
├── test_peer.py                # Test thuật toán Rarest-First & Test End-to-End P2P File Transfer
├── test_connection.py          # Test lớp PeerConnection (Handshake, Bitfield, Choking, HAVE message)
├── test_choking.py             # Test thuật toán Choking Tit-for-Tat & Optimistic unchoking
└── test_protocol_messages.py   # Test đóng gói & định dạng các loại MessageType
```

---

## 🚀 2. Cách chạy bộ Test

Bạn có thể chạy các bài test từ thư mục gốc của dự án (`p2p-file-sharing/`):

### Cách 1: Chạy toàn bộ các test trong thư mục (Khuyên dùng)
```bash
python -m unittest discover tests
```

### Cách 2: Chạy riêng từng file test
```bash
python -m unittest tests/test_peer.py
python -m unittest tests/test_connection.py
python -m unittest tests/test_choking.py
python -m unittest tests/test_protocol_messages.py
```

---

## 🔍 3. Chi tiết từng File Test

### 📂 File 1: `test_peer.py`
Module này kiểm tra toàn bộ luồng truyền tải thực tế (Integration / End-to-End) và thuật toán chọn mảnh:
- `test_rarest_first_selection`: Kiểm tra thuật toán Rarest-First ưu tiên chọn mảnh có độ hiếm cao nhất trong swarm.
- `test_end_to_end_p2p_file_transfer`: Khởi chạy 1 Tracker Server, 1 Seeder Node và 1 Leecher Node; Leecher kết nối tải toàn bộ các piece đa luồng, ghi đĩa, verify SHA-256 và so khớp 100% dữ liệu gốc.

### 📂 File 2: `test_connection.py`
Kiểm tra quản lý kết nối TCP giữa 2 peer:
- Handshake & tiếp nhận bitfield ban đầu.
- Nhận diện và cập nhật trạng thái CHOKE / UNCHOKE.
- Kiểm tra chỉ gửi INTERESTED đúng một lần.
- Kiểm tra tự động bỏ qua control messages khi đang đợi PIECE.
- Kiểm tra cập nhật bitfield và gọi callback khi nhận bản tin `HAVE`.

### 📂 File 3: `test_choking.py`
Kiểm tra giải thuật Choking (Tit-for-Tat):
- Regular slots ưu tiên các peer có đóng góp tốt.
- Optimistic unchoke chọn ngẫu nhiên các peer đang interested để thử nghiệm.
- Peer không còn interested bị thu hồi upload slot ngay lập tức.
- Ngăn chặn free-rider chiếm regular slot.

### 📂 File 4: `test_protocol_messages.py`
Kiểm tra việc tạo và định dạng chuẩn xác của các thông điệp điều khiển (`CHOKE`, `UNCHOKE`, `INTERESTED`, `NOT_INTERESTED`).

---

## 🛠️ 4. Các kỹ thuật nổi bật được dùng trong Test

1. **`tempfile.mkdtemp()`**: Tạo thư mục tạm thời cách ly tuyệt đối với đĩa cứng máy thật. Sau khi test xong, hàm `tearDown()` sẽ tự động xóa sạch thư mục này, không làm bẩn đĩa cứng.
2. **`socket.socketpair()`**: Tạo cặp Socket ảo kết nối trực tiếp với nhau mà không cần mở Port thật trên Card mạng (IP/Port), giúp test socket cực nhanh và ổn định.
3. **`os.urandom(350)`**: Sinh chuỗi dữ liệu nhị phân ngẫu nhiên để kiểm tra chuẩn xác tính toàn vẹn dữ liệu khi ghi đĩa hoặc truyền qua mạng.
