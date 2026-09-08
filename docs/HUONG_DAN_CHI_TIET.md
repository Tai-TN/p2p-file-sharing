# TÀI LIỆU HƯỚNG DẪN CHI TIẾT DỰ ÁN P2P FILE SHARING

Tài liệu này tổng hợp chi tiết kiến thức lý thuyết, thiết kế kiến trúc và giải thích từng dòng code của dự án chia sẻ file ngang hàng (P2P File Sharing).

---

## TỔNG QUAN KIẾN TRÚC MẠNG P2P

Trong mô hình Client-Server truyền thống, máy chủ (Server) phải gánh toàn bộ băng thông phục vụ cho tất cả các Client tải file. Nếu số lượng Client tăng đột biến, Server sẽ bị nghẽn mạng (bottleneck).

Mô hình **Peer-to-Peer (P2P)** giải quyết bài toán này bằng cách:
1. **Tracker Server**: Đóng vai trò làm "sổ bạ" (Directory). Tracker **không chứa file data**, mà chỉ ghi nhận danh sách các máy (Peer) đang tham gia mạng và sở hữu file.
2. **Peer**: Mỗi máy vừa đóng vai trò **Client** (tải các mảnh file còn thiếu từ các Peer khác) vừa đóng vai trò **Server** (lên lịch cho phép các Peer khác tải mảnh file mình đã có).
3. **Piece Manager**: File gốc được chia thành nhiều mảnh nhỏ (Piece) có kích thước cố định (ví dụ: 512 KB). Mỗi Piece được kiểm tra tính đúng đắn bằng chuỗi băm **SHA-256**.

```
 +--------------------------------------------------------------------+
 |                           TRACKER SERVER                           |
 |              (Quản lý danh sách Peer & thông tin file)              |
 +--------------------------------------------------------------------+
                   ^                               ^
      1. Announce  |                               | 1. Announce
      & Get Peers  v                               v & Get Peers
       +-----------------------+              +-----------------------+
       |   PEER A (Seeder)     |<============>|   PEER B (Leecher)    |
       |   (Đã có 100% file)   | 2. P2P Direct|   (Đang tải từng     |
       +-----------------------+    Transfer  |    mảnh Piece)        |
                                              +-----------------------+
```

---

## PHẦN 1: GIAO THỨC MẠNG & SOCKET TCP (`protocol/`)

### 1.1. Bài toán "Dính gói TCP" (TCP Packet Fragmentation & Message Gluing)

Giao thức TCP (Transmission Control Protocol) là giao thức **dòng byte (byte-stream)** chứ không phải giao thức **dòng thông điệp (message-stream)**. Khi bạn gọi `socket.sendall()`, TCP có thể hợp nhất nhiều lệnh `send` thành một gói IP lớn, hoặc xé nhỏ một thông điệp JSON thành nhiều gói IP nhỏ.

- **Dính gói (Gluing)**: Gửi `{"type": "A"}` và `{"type": "B"}` -> Bên nhận đọc được `{"type": "A"}{"type": "B"}` trong một lệnh `recv()` => Lỗi `json.loads()`.
- **Xé gói (Fragmentation)**: Gửi một thông điệp JSON dài 2000 byte -> Bên nhận chỉ `recv()` được 1024 byte đầu tiên => Lỗi `json.loads()` do thiếu dữ liệu.

### 1.2. Giải pháp: Framing với 4-Byte Length Prefix

Để xử lý triệt để vấn đề này, dự án sử dụng quy tắc **Length-Prefix Framing**:
Mỗi thông điệp truyền qua mạng được đóng gói theo dạng:

```
+------------------------------------+----------------------------------+
|  4-Byte Tiêu đề chỉ độ dài (Length) |  Nội dung thông điệp dạng JSON   |
|     (Số nguyên Big-Endian 32-bit)  |          (UTF-8 Bytes)           |
+------------------------------------+----------------------------------+
```

#### Quy trình gửi (`send_msg`):
1. Chuyển Python Dictionary thành chuỗi JSON dạng bytes UTF-8 (`raw_json`).
2. Tính độ dài `L = len(raw_json)`.
3. Đóng gói `L` thành 4 byte số nguyên sử dụng `struct.pack(">I", L)`.
4. Gửi `4 byte độ dài + raw_json` qua socket (`sock.sendall()`).

#### Quy trình nhận (`recv_msg`):
1. Đọc đúng **4 byte đầu tiên** bằng hàm `recv_exact(sock, 4)`. Giải mã số nguyên `L = struct.unpack(">I", length_prefix)[0]`.
2. Đọc tiếp đúng **L bytes** nội dung bằng hàm `recv_exact(sock, L)`.
3. Giải mã UTF-8 và chuyển thành Python Dictionary với `json.loads()`.

### 1.3. Mã hóa dữ liệu nhị phân (Binary Data in JSON)

Vì thông điệp truyền tải dưới dạng JSON, các mảng byte nhị phân của mảnh file (Piece data) không thể chèn trực tiếp vào chuỗi JSON. 
Dự án sử dụng chuẩn mã hóa **Base64** (`base64.b64encode()` và `base64.b64decode()`) để biến mảng byte nhị phân thành chuỗi văn bản an toàn khi gửi qua JSON.

### 1.4. Các loại thông điệp (Message Types)

| Tên thông điệp | Hướng truyền | Mục đích |
|---|---|---|
| `ANNOUNCE` | Peer -> Tracker | Báo danh Peer đang online, báo tiến độ tải file |
| `ANNOUNCE_RESPONSE` | Tracker -> Peer | Phản hồi xác nhận từ Tracker |
| `GET_PEERS` | Peer -> Tracker | Xin danh sách các Peer đang sở hữu file |
| `GET_PEERS_RESPONSE` | Tracker -> Peer | Trả về danh sách IP:Port các Peer đang sở hữu file |
| `HANDSHAKE` | Peer <-> Peer | Bắt tay ban đầu giữa 2 Peer, kiểm tra `info_hash` trùng khớp |
| `BITFIELD` | Peer -> Peer | Gửi mảng boolean/bit đánh dấu mảnh nào đã có / mảnh nào thiếu |
| `HAVE` | Peer -> Peer | Phát tin (broadcast) khi vừa tải xong 1 mảnh mới |
| `REQUEST` | Peer -> Peer | Yêu cầu Peer khác gửi dữ liệu mảnh số `piece_index` |
| `PIECE` | Peer -> Peer | Trả về dữ liệu mảnh `piece_index` (mã hóa Base64) |
| `KEEP_ALIVE` | Peer <-> Peer | Giữ kết nối socket không bị timeout |

---
---

## PHẦN 2: STORAGE & PIECE MANAGER (`storage/`)

### 2.1. Kiến trúc phân mảnh File & Metadata (.torrent style)

Trong mô hình chia sẻ file P2P, tập tin không được gửi nguyên khối mà được chia nhỏ thành hàng trăm hoặc hàng ngàn **mảnh (Piece)** có kích thước cố định (mặc định 512 KB).

- **Tại sao phải chia mảnh?**
  1. **Tải song song (Parallel Downloads)**: Một Peer (Leecher) có thể tải mảnh 0 từ Peer A, mảnh 1 từ Peer B, mảnh 2 từ Peer C cùng một lúc, khai thác tối đa băng thông đường truyền.
  2. **Giảm thiểu rủi ro gián đoạn**: Nếu rớt mạng giữa chừng, Peer chỉ cần tải lại mảnh 512 KB bị gián đoạn chứ không phải tải lại toàn bộ file từ đầu.
  3. **Chia sẻ tức thì (Fast Seeding)**: Ngay khi Peer vừa tải xong 1 mảnh bất kỳ, nó có thể chia sẻ (seed) ngay mảnh đó cho các Peer khác trong mạng mà không cần chờ tải xong 100% file.

- **Cấu trúc Metadata (`PieceMetadata`)**:
  - `filename`: Tên file gốc.
  - `file_size`: Tổng dung lượng file tính theo bytes.
  - `piece_size`: Kích thước mỗi mảnh (bytes, mặc định 512 KB).
  - `total_pieces`: Tổng số mảnh $= \lceil \text{file\_size} / \text{piece\_size} \rceil$.
  - `piece_hashes`: Danh sách mảng chuỗi hash SHA-256 của từng mảnh `[hash_0, hash_1, ..., hash_N-1]`.
  - `info_hash`: Chuỗi băm SHA-256 tổng thể đại diện cho toàn bộ thông tin metadata (dùng làm ID duy nhất để định danh file trong mạng P2P).

```
File gốc (VD: 1.5 MB)
+------------------------+------------------------+------------------------+
| Piece 0 (512 KB)       | Piece 1 (512 KB)       | Piece 2 (476 KB)       |
| SHA-256: 4a8b...       | SHA-256: 9e3f...       | SHA-256: 1c7d...       |
+------------------------+------------------------+------------------------+
```

### 2.2. Kỹ thuật ghi đĩa không theo thứ tự (Out-of-Order Disk I/O với `seek`)

Vì các mảnh piece được tải về ngẫu nhiên từ nhiều Peer khác nhau (ví dụ: nhận mảnh 2 trước mảnh 0), hệ thống không thể sử dụng cơ chế ghi nối tiếp thông thường (`open("file", "ab")`).

#### Giải pháp 2 bước:
1. **Khởi tạo File rỗng (Sparse File Pre-allocation)**:
   Khi Leecher bắt đầu tải file mới, `PieceManager` tạo một file rỗng trên đĩa với dung lượng đúng bằng `file_size` sử dụng phương thức `f.truncate(file_size)`.
2. **Ghi ngẫu nhiên với `f.seek(offset)`**:
   Vị trí byte bắt đầu của mảnh thứ `i` trên đĩa được tính theo công thức:
   $$\text{Offset}_i = i \times \text{piece\_size}$$
   Khi tải thành công mảnh `i`, tiến trình mở file ở chế độ đọc/ghi nhị phân (`r+b`), di chuyển con trỏ đĩa tới `Offset_i` và ghi dữ liệu của mảnh đó vào đúng vị trí.

### 2.3. Kiểm tra tính toàn vẹn dữ liệu (Integrity Verification với SHA-256)

Môi trường P2P có rủi ro nhiễu đường truyền làm hỏng dữ liệu hoặc gặp Peer độc hại cố tình gửi dữ liệu rác (Poisoning Attack).
Do đó, trước khi ghi bất kỳ mảnh dữ liệu nào xuống đĩa cứng, `PieceManager` **bắt buộc** xác minh checksum SHA-256:

1. Tính hash SHA-256 của khối bytes vừa nhận: `actual_hash = hashlib.sha256(data).hexdigest()`.
2. So sánh `actual_hash` với `expected_hash` lưu trong metadata của mảnh đó.
3. Nếu **khớp**: Tiến hành ghi mảnh vào đĩa và đánh dấu mảnh đó đã sở hữu (`bitfield[i] = True`).
4. Nếu **không khớp**: Hủy bỏ khối dữ liệu rác, báo lỗi và yêu cầu hàng đợi tải lại mảnh đó từ Peer khác.

### 2.4. Quản lý trạng thái Bitfield & Đồng bộ đa luồng (Thread Synchronization)

- **Mảng Bitfield**: Mảng boolean `[True, False, True, ...]` đại diện cho từng mảnh file (mảnh đã có = `True`, mảnh còn thiếu = `False`).
- **Đồng bộ đa luồng (`threading.Lock`)**:
  Hệ thống P2P sử dụng nhiều luồng đồng thời: các luồng **Download Manager** ghi mảnh mới nhận xuống đĩa, đồng thời các luồng **Upload Peer Connection** đọc các mảnh từ đĩa để gửi cho Peer khác. Để tránh hiện tượng **Race Condition** hay hỏng con trỏ đĩa, toàn bộ thao tác `read_piece` và `write_piece` đều được bảo vệ bởi khóa `threading.Lock()`.
- **Khôi phục tiến độ tải (Resume Download)**:
  Khi khởi động lại ứng dụng với một file đang tải dở, `PieceManager` đọc từng khối đĩa và kiểm tra SHA-256 với metadata. Mảnh nào có sẵn và hợp lệ sẽ tự động được gán `bitfield[i] = True`, cho phép tiếp tục tải các mảnh thiếu mà không cần làm lại từ đầu.

### 2.5. Giải thích mã nguồn chi tiết (`storage/piece_manager.py`)

#### Class `PieceMetadata`
- `create_from_file(file_path, piece_size)`: Đọc file từng chunk `piece_size`, tạo danh sách `piece_hashes` và `info_hash`.
- `to_dict()` & `from_dict()`: Hỗ trợ serialization/deserialization metadata để truyền nhận JSON qua mạng.

#### Class `PieceManager`
- `__init__(save_path, metadata, is_seeder)`: Khởi tạo file đĩa rỗng (nếu Leecher) hoặc kiểm tra file có sẵn để khôi phục `bitfield`.
- `get_piece_length(piece_index)`: Trả về độ dài của piece (mảnh cuối có thể nhỏ hơn `piece_size`).
- `verify_piece(piece_index, data)`: So sánh SHA-256 của `data` với băm lưu trong metadata.
- `read_piece(piece_index)`: Đọc dữ liệu mảnh từ đĩa với `f.seek(offset)` trong khối `with self.lock:`.
- `write_piece(piece_index, data)`: Ghi mảnh xuống đĩa tại `offset` đúng vị trí và cập nhật `bitfield[piece_index] = True`.
- `get_progress()`: Trả về phần trăm hoàn thành $= (\text{Số mảnh đã có} / \text{Tổng số mảnh}) \times 100\%$.

---
---

## PHẦN 3: KIỂM THỬ TỰ ĐỘNG (UNIT TESTING - `tests/`)

### 3.1. Triết lý & Cấu trúc Kiểm thử Unit Test

Trong xây dựng phần mềm mạng và P2P, **Unit Test (Kiểm thử đơn vị)** đóng vai trò sống còn để đảm bảo:
1. Các thông điệp JSON và khung Length-Prefix Framing truyền qua Socket luôn đúng chuẩn, không bị dính gói hay thiếu byte.
2. Các phép toán trên đĩa (ghi ngẫu nhiên offset với `seek()`, chia chunk, tính băm SHA-256) không bị rò rỉ hay làm sai lệch dữ liệu file gốc.
3. Chống lỗi lặp lại (Regression Bugs): Khi nâng cấp tính năng mới không làm gãy các tính năng đã chạy ổn định trước đó.

Dự án sử dụng framework **`unittest`** tiêu chuẩn của Python.
Lệnh thực thi toàn bộ bộ test tự động:
```bash
python3 -m unittest discover -s tests
```

---

### 3.2. Giải thích chi tiết `tests/test_protocol.py` (Kiểm thử Giao thức Mạng)

File [test_protocol.py](file:///home/taitn/Study/Năm%203/PBL4/p2p-file-sharing/tests/test_protocol.py) có nhiệm vụ giả lập kết nối mạng giữa 2 đầu socket và xác minh quy trình đóng gói/giải mã thông điệp JSON.

#### 1. Kỹ thuật `socket.socketpair()`
- Thay vì phải bind địa chỉ IP thực tế (dễ bị xung đột port hoặc bị chặn bởi Firewall khi test), `socket.socketpair()` tạo ra 2 socket nối trực tiếp với nhau ngay trong bộ nhớ RAM (`parent_sock` và `child_sock`).
- Mọi dữ liệu gửi qua `parent_sock.sendall()` sẽ đi thẳng tới `child_sock.recv()`.

#### 2. Chi tiết các Test Cases:
- **`test_send_and_recv_msg`**:
  - Gửi một thông điệp `ANNOUNCE` mẫu qua `parent_sock` với `send_msg()`.
  - Nhận thông điệp ở `child_sock` bằng `recv_msg()`.
  - Sử dụng `self.assertEqual(...)` để kiểm tra độ chính xác của các trường metadata (`type`, `info_hash`, `peer_id`, `port`).
- **`test_binary_piece_encoding`**:
  - Tạo dữ liệu nhị phân chứa các byte đặc biệt: `b"Hello P2P... \x00\xff\xfe"`.
  - Mã hóa thành Base64 string thông qua `create_piece_msg()`.
  - Sau khi nhận qua socket, giải mã Base64 bằng `base64.b64decode()` và dùng `self.assertEqual()` đối chiếu với `original_data` ban đầu.
- **`test_bitfield_creation`**:
  - Kiểm tra hàm khởi tạo thông điệp `create_bitfield_msg([True, True, False, True])` trả về đúng cấu trúc Dictionary chuẩn.

---

### 3.3. Giải thích chi tiết `tests/test_piece_manager.py` (Kiểm thử Quản lý File & Băm SHA-256)

File [test_piece_manager.py](file:///home/taitn/Study/Năm%203/PBL4/p2p-file-sharing/tests/test_piece_manager.py) sử dụng thư mục tạm (`tempfile.mkdtemp()`) và dữ liệu nhị phân ngẫu nhiên để kiểm thử thực tế việc chia mảnh và đọc/ghi đĩa.

#### 1. Setup & Teardown Môi trường Test:
- `setUp()`: Sinh ngẫu nhiên một file đĩa 350 bytes (`os.urandom(350)`) với `piece_size = 100` (tạo ra 4 mảnh: 100, 100, 100, 50 bytes).
- `tearDown()`: Xóa sạch thư mục tạm sau khi test xong bằng `shutil.rmtree()` để không để lại dữ liệu rác.

#### 2. Chi tiết các Test Cases:
- **`test_metadata_creation`**:
  - Kiểm tra `PieceMetadata.create_from_file()` chia đúng 4 mảnh piece.
  - Kiểm tra tính năng chuyển đổi Dictionary (`to_dict` / `from_dict`) tái tạo chính xác `info_hash`.
- **`test_out_of_order_write_and_read`**:
  - Giả lập tải file ngẫu nhiên không theo thứ tự: Ghi mảnh 2 $\rightarrow$ mảnh 0 $\rightarrow$ mảnh 3 $\rightarrow$ mảnh 1.
  - Kiểm tra tiến độ phần trăm `get_progress()` tăng dần tương ứng (25% $\rightarrow$ 50% $\rightarrow$ 75% $\rightarrow$ 100%).
  - Đọc lại toàn bộ file trên đĩa sau khi hoàn thành 100% và so sánh bằng `self.assertEqual(downloaded_data, source_data)` để đảm bảo khớp chính xác 100% từng byte với file gốc.
- **`test_corrupted_piece_rejection`**:
  - Cố tình truyền dữ liệu bị hỏng (`b"X" * 100`) vào `write_piece(0, corrupted_data)`.
  - Kiểm tra hàm trả về `False` và `bitfield[0]` vẫn giữ giá trị `False` (không cho phép ghi dữ liệu rác xuống đĩa).
- **`test_existing_file_resume`**:
  - Tạo `PieceManager` 1 và ghi 2 mảnh (piece 0, piece 1).
  - Tắt manager 1 và mở `PieceManager` 2 trên cùng file đó (giả lập khởi động lại ứng dụng).
  - Kiểm tra manager 2 tự động quét đĩa, khôi phục mảnh 0 và 1 đã có (`True`) và xác định chính xác danh sách mảnh còn thiếu `[2, 3]`.


