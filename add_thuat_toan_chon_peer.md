# Kế hoạch thuật toán chọn peer xin mảnh

## 1. Mục tiêu

Thay cách chọn peer ngẫu nhiên bằng thuật toán **thời gian hoàn thành dự kiến nhỏ nhất** (Minimum Estimated Completion Time).

Thuật toán kết hợp:

- Tốc độ truyền ước lượng của peer.
- Số request đang chờ tại peer.
- Trạng thái `CHOKE`.
- Cơ chế thăm dò ngẫu nhiên để cập nhật lại đánh giá peer.

Việc chọn piece và chọn peer là hai bước độc lập:

```text
Rarest-First chọn piece
        ↓
PeerSelector chọn peer có piece đó
        ↓
TransferEngine gửi request
```

## 2. Phạm vi trách nhiệm

### `peer/selection.py`

Giữ nguyên nhiệm vụ chọn piece còn thiếu bằng Rarest-First.

### `peer/peer_selector.py`

Module mới, chỉ quản lý:

- Trạng thái và thống kê của từng peer.
- Tính tốc độ truyền.
- Đếm request đang chờ.
- Lọc peer đang bị choke.
- Tính ETA.
- Chọn peer.
- Cập nhật sau khi request thành công hoặc thất bại.

Module này không phụ thuộc vào socket, `PeerConnection`, `PieceManager` hoặc `PeerNode`.

### `peer/transfer_engine.py`

Chịu trách nhiệm:

- Lọc các peer có piece cần tải từ bitfield.
- Gọi `PeerSelector`.
- Đo thời gian request.
- Thực hiện request và ghi piece.
- Báo kết quả cho `PeerSelector`.

## 3. Trạng thái của mỗi peer

Mỗi peer có một state riêng:

```text
estimated_rate: Optional[float]
pending_requests: int
choked_until: float
failure_count: int
```

Ý nghĩa:

- `estimated_rate = None`: chưa từng có request thành công; không dùng `0` để biểu diễn trạng thái này.
- `estimated_rate > 0`: tốc độ ước lượng theo byte/giây.
- `pending_requests`: số request đã được reserve nhưng chưa kết thúc.
- `choked_until`: thời điểm peer được phép chọn lại.
- `failure_count`: số lỗi liên tiếp hoặc số lỗi tích lũy chưa được reset.

`PeerSelector` dùng một lock riêng để nhiều worker có thể cập nhật an toàn.

## 4. Công thức ETA

Với mỗi peer hợp lệ:

```text
ETA = (pending_requests + 1) × piece_size / rate
```

Trong đó:

- `piece_size` là kích thước thực tế của piece.
- `rate` là `estimated_rate` nếu peer đã được đo.
- Nếu peer chưa được đo, dùng tốc độ bootstrap tạm thời.

Piece cuối phải dùng kích thước thực tế:

```text
piece_size = file_size - piece_index × piece_length
```

## 5. Tốc độ bootstrap cho peer chưa đo

Không để `estimated_rate = 0` vì sẽ gây chia cho 0 hoặc ETA vô hạn.

Quy ước:

```text
None = chưa có dữ liệu đo
0    = không dùng làm tốc độ hợp lệ
```

Nếu đã có tốc độ của các peer khác:

```text
bootstrap_rate = max(known_rates)
```

Nếu chưa có peer nào được đo:

```text
bootstrap_rate = DEFAULT_BOOTSTRAP_RATE
```

Giá trị mặc định ban đầu đề xuất:

```text
DEFAULT_BOOTSTRAP_RATE = 1 MB/s
```

Bootstrap rate chỉ dùng tạm để peer mới có cơ hội được thử. Sau request đầu tiên, tốc độ phải được thay bằng tốc độ thực tế.

## 6. Cập nhật tốc độ bằng EWMA

Đo thời gian từ ngay trước khi gọi `request_piece()` đến khi nhận được response `PIECE`.

Tốc độ tức thời:

```text
current_rate = byte_count / elapsed
```

Sau request thành công:

```text
new_rate = 0.7 × old_rate + 0.3 × current_rate
```

Tương đương:

```text
new_rate = (1 - alpha) × old_rate + alpha × current_rate
alpha = 0.3
```

Nếu peer chưa từng được đo, `estimated_rate` được gán bằng `current_rate`.

Không tính thời gian handshake vào tốc độ truyền piece trong phiên bản đầu tiên, vì connection có thể được tái sử dụng.

## 7. Quy trình chọn peer

`select_peer(available_peers, piece_size)` thực hiện:

1. Nhận danh sách peer đang sở hữu piece.
2. Loại peer còn trong thời gian cooldown sau `CHOKE`.
3. Nếu không còn peer hợp lệ, trả về `None`.
4. Tính tốc độ dùng cho từng peer.
5. Tính ETA cho từng peer.
6. Có thể thực hiện exploration ngẫu nhiên theo `explore_rate`.
7. Nếu không exploration, chọn peer có ETA nhỏ nhất.
8. Nếu nhiều peer có ETA gần nhau, có thể random trong nhóm đó.
9. Reserve `pending_requests` ngay trong cùng lock trước khi trả kết quả.

Việc reserve phải diễn ra trong `PeerSelector`, không để `TransferEngine` tăng pending sau khi chọn. Điều này tránh nhiều worker cùng thấy một peer đang rảnh.

## 8. Cơ chế ngẫu nhiên

### Exploration

Tham số:

```text
explore_rate = 0.1
```

Sau khi đã lọc peer bị choke:

```text
nếu random() < explore_rate:
    chọn ngẫu nhiên một peer hợp lệ
```

Exploration giúp:

- Thử peer chưa từng được đo.
- Phát hiện peer đang nhanh lên.
- Tránh phụ thuộc quá lâu vào một peer.
- Cập nhật lại tốc độ của các peer khác.

`explore_rate = 0` dùng để tắt exploration khi benchmark.

### ETA tie threshold

Tham số:

```text
eta_tie_threshold = 0.0
```

Nếu bật, sau khi tính ETA:

```text
candidate nếu:
ETA_peer <= best_eta × (1 + eta_tie_threshold)
```

Ví dụ `eta_tie_threshold = 0.15` cho phép random giữa các peer có ETA không lớn hơn ETA tốt nhất quá 15%.

Đề xuất triển khai ban đầu:

```text
explore_rate = 0.1
eta_tie_threshold = 0.0
```

Như vậy 90% request vẫn chọn ETA nhỏ nhất tuyệt đối, còn 10% request dùng để thăm dò. Sau khi benchmark có thể thử ngưỡng `0.10` hoặc `0.15`.

## 9. Xử lý kết quả request

### Thành công

Gọi:

```text
record_success(peer_id, byte_count, elapsed)
```

Thực hiện:

- Tính `current_rate`.
- Cập nhật EWMA.
- Giảm `pending_requests` đúng một lần.
- Reset `failure_count`.

### `CHOKE`

Gọi:

```text
record_choke(peer_id)
```

Thực hiện:

- Giảm `pending_requests` đúng một lần.
- Đặt `choked_until = now + choke_cooldown`.
- Không nhất thiết đóng connection.
- Không xóa peer khỏi danh sách đã biết.

### Timeout hoặc lỗi connection

Gọi:

```text
record_failure(peer_id)
```

Thực hiện:

- Giảm `pending_requests` đúng một lần.
- Tăng `failure_count`.
- Nếu đã có tốc độ, giảm một nửa:

```text
estimated_rate = estimated_rate × 0.5
```

- Nếu chưa có tốc độ, giữ `estimated_rate = None`.
- Đóng connection lỗi tại `TransferEngine`.
- Giữ peer để có thể thử lại sau.

## 10. Vòng đời pending request

Mỗi request phải tuân theo:

```text
select_peer()
    pending_requests += 1

request thành công
    pending_requests -= 1
    record_success()

request bị CHOKE
    pending_requests -= 1
    record_choke()

request lỗi hoặc timeout
    pending_requests -= 1
    record_failure()
```

Các nhánh tạo connection thất bại hoặc gửi `INTERESTED` thất bại cũng phải release pending.

Invariant cần giữ:

```text
pending_requests >= 0
```

Mỗi request đã reserve phải được release đúng một lần.

## 11. API dự kiến

`PeerSelector` nên có các phương thức:

```text
select_peer(available_peers, piece_size)
record_success(peer_id, byte_count, elapsed)
record_failure(peer_id)
record_choke(peer_id)
get_peer_stats(peer_id)
```

Các tham số cấu hình:

```text
smoothing_factor = 0.3
explore_rate = 0.1
eta_tie_threshold = 0.0
choke_cooldown = choking_interval
default_bootstrap_rate = 1 MB/s
failure_penalty = 0.5
```

Các giá trị này nên được truyền qua `__init__` để dễ thử nghiệm và benchmark.

## 12. Tích hợp vào `TransferEngine`

Thay đoạn chọn ngẫu nhiên:

```text
p_id, p_info = random.choice(available_peers)
```

bằng:

```text
selected = peer_selector.select_peer(
    available_peers,
    actual_piece_size
)
```

Luồng xử lý:

```text
available_peers = các peer có piece
        ↓
select_peer()
        ↓
reserve pending
        ↓
tạo hoặc tái sử dụng connection
        ↓
đo thời gian request
        ↓
nhận PIECE / CHOKE / lỗi
        ↓
record_success / record_choke / record_failure
```

`PeerSelector` phải là một instance dùng chung cho toàn bộ `TransferEngine`, không tạo riêng trong từng worker.

## 13. Kiểm thử

### Unit test cho `peer_selector.py`

1. Chọn peer có ETA nhỏ nhất.
2. Peer có nhiều pending bị giảm ưu tiên.
3. `estimated_rate = None` không gây chia cho 0.
4. Tất cả peer chưa đo vẫn được chọn.
5. EWMA cập nhật đúng theo `0.7 / 0.3`.
6. Lỗi giảm tốc độ còn một nửa.
7. Lỗi khi chưa có tốc độ vẫn giữ `None`.
8. Peer bị choke bị loại trong cooldown.
9. Peer được chọn lại sau cooldown.
10. `explore_rate = 0` tắt exploration.
11. `explore_rate = 1` luôn chọn random.
12. `eta_tie_threshold = 0` chỉ chọn peer tốt nhất.
13. ETA gần nhau được random trong đúng nhóm.
14. Pending tăng và giảm đúng một lần.
15. Nhiều thread reserve đồng thời vẫn giữ trạng thái đúng.

### Integration test

Chạy lại test end-to-end hiện có trong `tests/test_peer.py`, bảo đảm:

- File vẫn tải hoàn chỉnh.
- Nội dung tải về vẫn khớp file gốc.
- Các worker không làm hỏng trạng thái pending.
- Peer bị lỗi không làm download bị kẹt vĩnh viễn.

## 14. Thứ tự triển khai

1. Tạo `peer/peer_selector.py` độc lập.
2. Viết unit test cho selector.
3. Kiểm tra các invariant về pending, choke và tốc độ.
4. Tích hợp selector vào `TransferEngine`.
5. Viết hoặc cập nhật test cho các nhánh success, choke và failure.
6. Chạy unit test selector.
7. Chạy integration test P2P.
8. Benchmark với:
   - `explore_rate = 0`.
   - `explore_rate = 0.1`.
   - `eta_tie_threshold = 0.0`.
   - `eta_tie_threshold = 0.15`.

