Mình đề xuất thuật toán “thời gian hoàn thành dự kiến nhỏ nhất”. Đây là bản nâng cấp của fastest peer: gộp cả tốc độ, độ bận và trạng thái choke vào một con số. Nó thay cho dòng random.choice(available_peers) trong _download_piece.

Ý tưởng: với mỗi peer có mảnh cần tải, ước lượng “nếu xin peer này thì bao lâu mới nhận được mảnh?”, rồi chọn peer có thời gian nhỏ nhất.

Công thức:
thời gian dự kiến = (số request đang chờ ở peer đó + 1) × kích thước mảnh ÷ tốc độ của peer đó

Tốc độ: đo mỗi lần nhận xong một mảnh (kích thước ÷ thời gian từ lúc gửi REQUEST tới lúc nhận PIECE). Lưu dạng trung bình trượt, ví dụ mới = 0,7 × cũ + 0,3 × lần đo vừa rồi, để không nhảy lung tung.
Số request đang chờ: các worker của bạn đang xin peer đó bao nhiêu mảnh. Đếm chung cho cả 4 worker, tăng khi gửi REQUEST, giảm khi nhận xong hoặc lỗi. Nhờ vậy peer nhanh nhất không bị cả 4 worker dồn vào cùng lúc, và đây chính là phần “least-loaded” mà mình có thể biết được.
Peer đang choke mình: bỏ ra khỏi danh sách trong một khoảng, ví dụ bằng choking_interval. Nhờ vậy không lãng phí request vào peer đang từ chối.
Peer chưa đo lần nào: cho tốc độ mặc định bằng tốc độ cao nhất đang biết, để peer mới được thử ít nhất một lần.
Peer vừa lỗi (timeout, mất kết nối): giảm tốc độ ước lượng, ví dụ chia đôi, để nó tụt hạng thay vì bị xóa hẳn.

Thêm chút ngẫu nhiên: nếu vài peer có thời gian dự kiến gần bằng nhau (chênh dưới khoảng 10–20%) thì bốc ngẫu nhiên một trong số đó. Như vậy tốc độ của các peer khác vẫn được đo lại thỉnh thoảng, vì tốc độ thật có thể thay đổi.