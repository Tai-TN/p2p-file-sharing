import random
from typing import List, Dict, Optional

def choose_next_piece(missing_pieces: List[int], peer_bitfields: Dict[str, List[bool]], strategy: str = "rarest_first") -> Optional[int]:
    """
    Thuật toán lựa chọn mảnh piece tiếp theo cần tải.
    
    Args:
        missing_pieces: Danh sách các chỉ số piece mà local peer còn thiếu.
        peer_bitfields: Dictionary { peer_id: bitfield_list } của tất cả các peer đang kết nối.
        strategy: "rarest_first", "random" hoặc "sequential".
        
    Returns:
        Chỉ số piece_index tốt nhất nên tải tiếp theo, hoặc None nếu không peer nào sở hữu mảnh thiếu.
    """
    if not missing_pieces or not peer_bitfields:
        return None

    # Tìm các piece khả dụng (ít nhất 1 peer kết nối sở hữu)
    candidate_counts: Dict[int, int] = {}
    for piece_idx in missing_pieces:
        count = 0
        for p_id, bitfield in peer_bitfields.items():
            if piece_idx < len(bitfield) and bitfield[piece_idx]:
                count += 1
        if count > 0:
            candidate_counts[piece_idx] = count

    if not candidate_counts:
        return None

    if strategy == "sequential":
        # Tải theo thứ tự 0, 1, 2, ... (giống tải HTTP thông thường) để so sánh.
        return min(candidate_counts.keys())

    if strategy == "random":
        return random.choice(list(candidate_counts.keys()))

    # Thuật toán Rarest-First (Mảnh hiếm nhất trong Swarm)
    min_freq = min(candidate_counts.values())
    rarest_pieces = [p_idx for p_idx, count in candidate_counts.items() if count == min_freq]
    
    # Nếu có nhiều piece cùng độ hiếm thấp nhất, chọn ngẫu nhiên 1 trong số đó
    return random.choice(rarest_pieces)
