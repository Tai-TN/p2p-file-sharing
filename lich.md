# Lịch trình phát triển 16 tuần: Hệ thống chia sẻ tập tin P2P

**Nhóm:** 3 người | **Thời gian:** 16 tuần | **Ngân sách:** ~6-7 giờ/người/tuần (~320 giờ tổng)

**Quy ước tên**: Người 1 = Tracker & Network layer | Người 2 = Piece Manager & OS layer | Người 3 = Transfer Engine & tích hợp

**Nguyên tắc chung**: Giai đoạn 1 (tuần 1-8) làm chậm và kỹ hơn bản rút gọn — có test, có review, không vội. Giai đoạn 2 (tuần 9-16) thêm tính năng nâng cao thật sự, không phải "làm cho đủ giờ".

---

## GIAI ĐOẠN 1 — Xây core system vững chắc (Tuần 1-8)

### Tuần 1 — Nền tảng socket & thiết kế giao thức

| Ai | Việc |
|---|---|
| Cả nhóm | Setup repo Git, môi trường, họp thiết kế giao thức (đặc tả JSON schema đầy đủ cho mọi loại message dự kiến dùng, kể cả các message nâng cao sẽ thêm sau) |
| Cả nhóm | Tự chạy socket "hello world", sau đó nâng lên gửi/nhận JSON |

**Checkpoint**: 3 người đều chạy được ví dụ socket JSON cơ bản, có 1 tài liệu đặc tả giao thức được cả nhóm thống nhất (sẽ dùng xuyên suốt dự án).

### Tuần 2 — Xây từng module độc lập + viết test song song

| Ai | Việc |
|---|---|
| Người 1 | Tracker: nhận `announce`, lưu peer list, xử lý `get_peers`. Viết unit test cho từng hàm xử lý message |
| Người 2 | Piece Manager: chia file thành chunk, tính SHA-256, sinh metadata. Viết unit test cho hàm chia/verify checksum |
| Người 3 | Khung class `Peer`: listen + connect. Viết test giả lập 2 instance Peer nói chuyện |

**Checkpoint**: 3 module chạy độc lập, mỗi module có ít nhất 3-5 unit test pass.

### Tuần 3 — Ghép nối: Tracker + 1 cặp Peer

| Ai | Việc |
|---|---|
| Người 1 | Tracker xử lý peer rời mạng (timeout) |
| Người 2 | Tích hợp Piece Manager vào Peer: trả lời bitfield khi được hỏi |
| Người 3 | Ghép luồng end-to-end: announce → get_peers → connect → xin 1 piece → nhận đúng |

**Checkpoint (cột mốc quan trọng)**: demo tải thành công 1 piece giữa 2 peer qua tracker.

### Tuần 4 — Đa luồng & đồng bộ hoá

| Ai | Việc |
|---|---|
| Người 1 | Tracker xử lý nhiều peer đồng thời bằng thread pool |
| Người 2 | Lock cho ghi file, ghi đúng offset bằng `seek()` |
| Người 3 | Transfer Engine: hàng đợi piece, N thread tải song song |

**Checkpoint**: tải file hoàn chỉnh từ ≥ 2 peer đồng thời, có log chứng minh tính song song.

### Tuần 5 — Xử lý lỗi & độ bền

| Ai | Việc |
|---|---|
| Người 1 | Phát hiện peer chết qua heartbeat |
| Người 2 | Verify checksum sau nhận, tải lại piece hỏng |
| Người 3 | Timeout khi peer không phản hồi, chuyển sang peer khác |

**Checkpoint**: 3 kịch bản lỗi đều có test case chạy được thật, không chỉ code lý thuyết.

### Tuần 6 — Code review chéo & refactor

Vì có nhiều thời gian hơn bản rút gọn, đây là tuần dành riêng cho chất lượng code, việc mà bản 8 tuần phải bỏ qua vì thiếu thời gian:

| Ai | Việc |
|---|---|
| Cả nhóm | Đọc chéo code của nhau (Người 1 đọc code Người 2, và xoay vòng), ghi lại vấn đề: đặt tên biến, xử lý edge case thiếu, code trùng lặp |
| Cả nhóm | Refactor lại theo góp ý, thống nhất coding convention chung |
| Cả nhóm | Viết thêm test còn thiếu cho các trường hợp phát hiện qua review |

**Checkpoint**: code base sạch hơn, mỗi người hiểu được toàn bộ hệ thống chứ không chỉ phần mình làm — quan trọng cho việc bảo vệ vì hội đồng có thể hỏi bất kỳ ai về bất kỳ phần nào.

### Tuần 7 — Testing có hệ thống + giả lập điều kiện mạng thật

| Ai | Việc |
|---|---|
| Người 1 | Viết integration test: nhiều peer join/leave liên tục, tracker phải ổn định |
| Người 2 | Test với file lớn hơn (vài trăm MB), đo hiệu năng ghi đĩa |
| Người 3 | Dùng công cụ `tc` (traffic control, Linux) giả lập độ trễ mạng và mất gói, kiểm tra hệ thống vẫn hoạt động đúng |

**Checkpoint**: hệ thống chịu được điều kiện mạng không lý tưởng (delay, packet loss giả lập), không chỉ chạy tốt trên localhost lý tưởng.

### Tuần 8 — Ổn định Giai đoạn 1, demo sơ bộ nội bộ

| Ai | Việc |
|---|---|
| Cả nhóm | Chạy demo đầy đủ nội bộ (chưa phải bảo vệ chính thức), ghi lại mọi lỗi phát sinh |
| Cả nhóm | Fix lỗi phát hiện, đảm bảo core system ổn định tuyệt đối trước khi thêm tính năng mới |

**Checkpoint quan trọng nhất giữa kỳ**: nếu core system chưa ổn định ở đây, KHÔNG chuyển sang Giai đoạn 2 — dùng thêm 1-2 tuần đệm để ổn định trước, thà chậm còn hơn xây tính năng nâng cao trên nền chưa vững.

---

## GIAI ĐOẠN 2 — Tính năng nâng cao (Tuần 9-16)

### Tuần 9 — Choking algorithm (tit-for-tat)

| Ai | Việc |
|---|---|
| Người 1 | Thiết kế cơ chế đánh giá peer nào "hợp tác tốt" (upload nhiều cho mình thì mình ưu tiên lại) |
| Người 3 | Implement giới hạn số peer được phục vụ cùng lúc, định kỳ đổi 1 peer để thử ngẫu nhiên (optimistic unchoking) |
| Người 2 | Viết test đo tác động: peer hợp tác tốt có được ưu tiên tải nhanh hơn không |

**Checkpoint**: demo được sự khác biệt giữa peer "đóng góp nhiều" và "chỉ tải không chia sẻ" (free-rider).

### Tuần 10 — Piece selection nâng cao (kết hợp rarest-first + bandwidth-aware)

| Ai | Việc |
|---|---|
| Người 2 | Tính độ hiếm từng piece dựa trên bitfield của tất cả peer đã biết |
| Người 3 | Đo tốc độ tải thực tế từ từng peer, kết hợp với độ hiếm thành 1 điểm số ưu tiên |
| Người 1 | Viết benchmark so sánh: random selection vs rarest-first vs kết hợp bandwidth-aware |

**Checkpoint**: có số liệu benchmark rõ ràng cho thấy chiến lược mới nhanh hơn random bao nhiêu %.

### Tuần 11 — Resume download bị gián đoạn

| Ai | Việc |
|---|---|
| Người 2 | Persist trạng thái tải (piece nào đã có) xuống file `.json` định kỳ |
| Người 3 | Khi khởi động lại app, đọc trạng thái cũ, tiếp tục tải từ piece còn thiếu, không tải lại từ đầu |
| Người 1 | Test: tắt app giữa chừng, mở lại, kiểm tra không mất tiến độ |

**Checkpoint**: demo tắt app khi đang tải 50%, mở lại, tiếp tục đúng từ 50% chứ không về 0%.

### Tuần 12 — Mã hoá dữ liệu truyền

| Ai | Việc |
|---|---|
| Người 1 | Nghiên cứu tích hợp AES cho payload piece khi gửi qua mạng |
| Người 3 | Implement mã hoá/giải mã trong Transfer Engine, đảm bảo không phá vỡ checksum verify |
| Người 2 | Test hiệu năng: mã hoá làm chậm tốc độ tải bao nhiêu so với không mã hoá |

**Checkpoint**: dữ liệu truyền giữa peer được mã hoá, có số liệu so sánh overhead của việc mã hoá.

### Tuần 13 — Giao diện quản lý trực quan (web dashboard)

| Ai | Việc |
|---|---|
| Người 3 | Dựng server nhỏ (Flask/FastAPI) expose trạng thái hệ thống qua API |
| Người 1 | Dùng WebSocket đẩy update real-time (peer nào đang kết nối, tốc độ từng piece) |
| Người 2 | Làm giao diện đơn giản hiển thị: danh sách peer, biểu đồ tiến độ tải, tốc độ theo thời gian |

**Checkpoint**: có dashboard trực quan chạy song song với hệ thống P2P, hiển thị đúng dữ liệu real-time — đây là phần "ăn điểm" mạnh khi demo vì trực quan, dễ gây ấn tượng.

### Tuần 14 — Hỗ trợ multi-file / thư mục (nếu còn thời gian) hoặc DHT thử nghiệm (nếu nhóm mạnh)

Chọn 1 trong 2 theo năng lực thực tế của nhóm tại thời điểm này:

**Phương án A (an toàn hơn)** — Multi-file torrent:

| Ai | Việc |
|---|---|
| Người 2 | Mở rộng Piece Manager quản lý mapping piece → file, hỗ trợ chia sẻ cả thư mục |
| Người 3 | Cập nhật Transfer Engine xử lý ghi nhiều file đích khác nhau |

**Phương án B (khó hơn, chỉ làm nếu Giai đoạn 1 rất suôn sẻ)** — DHT cơ bản:

| Ai | Việc |
|---|---|
| Người 1 | Tìm hiểu Kademlia DHT, implement bảng định tuyến đơn giản |
| Người 3 | Thay thế 1 phần vai trò tracker bằng DHT lookup |

**Checkpoint**: 1 trong 2 tính năng trên chạy được, không cố làm cả hai cùng lúc.

### Tuần 15 — Benchmark toàn diện & viết báo cáo

| Ai | Việc |
|---|---|
| Người 1 | Benchmark tổng hợp: throughput theo số peer, tác động của choking/piece selection/mã hoá |
| Người 2 | Viết báo cáo kỹ thuật đầy đủ: kiến trúc, thuật toán, số liệu |
| Người 3 | Chuẩn bị kịch bản demo đầy đủ cho buổi bảo vệ (bao gồm cả tính năng nâng cao) |

**Checkpoint**: có bộ số liệu benchmark đầy đủ cho mọi tính năng đã làm, báo cáo bản nháp hoàn chỉnh.

### Tuần 16 — Buffer, hoàn thiện, bảo vệ thử

| Ai | Việc |
|---|---|
| Cả nhóm | Chạy thử toàn bộ demo (từ core system tới tính năng nâng cao) ít nhất 2-3 lần |
| Cả nhóm | Hoàn thiện slide, phân chia phần trình bày, chuẩn bị trả lời câu hỏi dự kiến |
| Cả nhóm | Rà báo cáo lần cuối, đảm bảo khớp với demo thực tế |

---

## Nguyên tắc xuyên suốt

- **Không chuyển giai đoạn khi checkpoint chưa đạt**: đặc biệt là ranh giới tuần 8 → 9 — core system phải ổn định tuyệt đối trước khi thêm tính năng, vì mọi tính năng Giai đoạn 2 đều xây trên nền đó.
- **Mỗi tính năng nâng cao ở Giai đoạn 2 đều phải có benchmark riêng** — đừng chỉ implement rồi để đó, số liệu chứng minh hiệu quả mới là thứ thuyết phục hội đồng.
- **Nếu tiến độ trễ ở Giai đoạn 2**: cắt theo thứ tự ưu tiên ngược — bỏ DHT trước (khó nhất, ít ảnh hưởng tới demo chính), rồi tới multi-file, giữ lại choking + piece selection + resume + dashboard vì đây là nhóm tính năng có tỷ lệ "ấn tượng khi demo / công sức bỏ ra" cao nhất.
- **Họp đầu tuần 15-20 phút** duy trì xuyên suốt 16 tuần, không chỉ riêng giai đoạn nào.
