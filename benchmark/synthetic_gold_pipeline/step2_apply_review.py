# -*- coding: utf-8 -*-
"""Apply human review edits to draft_queries_v1.jsonl"""
import json
import os

INPUT = os.path.join(os.path.dirname(__file__), "output", "draft_queries_v1.jsonl")
OUTPUT = os.path.join(os.path.dirname(__file__), "output", "reviewed_queries_v1.jsonl")

# ======================================================================
# 8 queries viết lại hoàn toàn
# ======================================================================
REWRITES = {
    "q_025": {
        "question": "Trong nghiên cứu này, phẫu thuật nội soi sau phúc mạc được lựa chọn với vai trò gì trong điều trị sỏi niệu quản?",
        "query_type": "simple",
        "expected_behavior": "summary",
    },
    "q_031": {
        "question": "Theo nghiên cứu này, những yếu tố nào có liên quan đến kiến thức, thái độ hoặc thực hành về sức khỏe sinh sản ở học sinh trung học?",
        "query_type": "reasoning",
        "expected_behavior": "summary",
    },
    "q_035": {
        "question": "Trong nghiên cứu về đường cong học tập kỹ thuật Snodgrass, những biến chứng nào được dùng để đánh giá kết quả điều trị?",
        "query_type": "simple",
        "expected_behavior": "exact",
    },
    "q_038": {
        "question": "Theo nghiên cứu này, kết quả điều trị ở bệnh nhân suy tim mạn tính được đánh giá qua những chỉ số hoặc tiêu chí nào?",
        "query_type": "simple",
        "expected_behavior": "summary",
    },
    "q_039": {
        "question": "Trong nghiên cứu này, bệnh nhân suy tim mạn tính được can thiệp và theo dõi nhằm cải thiện những khía cạnh điều trị nào?",
        "query_type": "reasoning",
        "expected_behavior": "summary",
    },
    "q_071": {
        "question": "Theo nghiên cứu này, những hạn chế nào trong kiến thức, thái độ hoặc thực hành dựa trên bằng chứng của điều dưỡng cho thấy cần tăng cường đào tạo tại bệnh viện?",
        "query_type": "reasoning",
        "expected_behavior": "summary",
    },
    "q_081": {
        "question": "Nghiên cứu này ghi nhận sự khác biệt như thế nào giữa nam và nữ về tỷ lệ mắc rối loạn khớp thái dương hàm?",
        "query_type": "simple",
        "expected_behavior": "exact",
    },
    "q_088": {
        "question": "Theo nghiên cứu này, những yếu tố nào có liên quan đến kiến thức hoặc kỹ năng giáo dục sức khỏe của điều dưỡng?",
        "query_type": "reasoning",
        "expected_behavior": "summary",
    },
}

# ======================================================================
# ~25 queries sửa nhẹ wording
# ======================================================================
MINOR_FIXES = {
    "q_006": {
        "question": "Nghiên cứu này tập trung đánh giá những đặc điểm và kết quả nào khi cắt đốt polyp có cuống ở đại trực tràng?",
    },
    "q_009": {
        "question": "Trong nghiên cứu này, dấu hiệu lâm sàng phổ biến nhất ở trẻ sơ sinh sinh từ mẹ nhiễm liên cầu khuẩn nhóm B là gì?",
    },
    "q_010": {
        "question": "Những khó khăn chính trong chẩn đoán bệnh lao ở bệnh nhân ghép thận là gì?",
    },
    "q_011": {
        "question": "Nghiên cứu tại Bệnh viện Ung bướu Đà Nẵng tập trung đánh giá những đặc điểm và kết quả nào ở bệnh nhân ung thư dạ dày?",
        "query_type": "simple",
        "expected_behavior": "summary",
    },
    "q_014": {
        "question": "Hiến tạng sau chết tuần hoàn không kiểm soát được mô tả trong bài theo những tình huống nào?",
    },
    "q_018": {
        "question": "Nghiên cứu tại quận Phú Nhuận khảo sát những nhóm đối tượng nào trong hoạt động mua bán thuốc tại nhà thuốc tư nhân?",
    },
    "q_019": {
        "question": "Nghiên cứu về thực hành trên xác tươi nhằm đánh giá vai trò của phương pháp này đối với những khía cạnh nào của đào tạo thủ thuật/phẫu thuật?",
    },
    "q_021": {
        "question": "Nghiên cứu về viêm mạch máu IgA tại Bệnh viện Nhi Đồng 1 được thực hiện trên bao nhiêu trẻ và trong giai đoạn nào?",
    },
    "q_023": {
        "question": "Nghiên cứu này sử dụng thiết kế nào để đánh giá hiệu quả và tác dụng phụ của phác đồ 4 thuốc có Bismuth PTMB?",
    },
    "q_024": {
        "question": "Nghiên cứu tại Bệnh viện Thanh Nhàn nhằm đánh giá hiệu quả của biện pháp nào trong điều trị viêm tụy cấp nặng?",
    },
    "q_026": {
        "question": "Theo phần đặt vấn đề của nghiên cứu, vì sao phẫu thuật nội soi sau phúc mạc vẫn giữ vai trò quan trọng trong điều trị sỏi niệu quản?",
    },
    "q_028": {
        "question": "Nghiên cứu tại Bệnh viện Nhi Đồng 1 tập trung khảo sát kết quả tăng trưởng và những yếu tố liên quan nào ở trẻ sơ sinh sau phẫu thuật đường tiêu hóa?",
    },
    "q_030": {
        "question": "Theo phần tổng quan trong bài, vì sao gây tê ngoài màng cứng ngắt quãng tự động (PIEB) được xem là có lợi thế hơn truyền liên tục (CEI) trong giảm đau chuyển dạ?",
    },
    "q_047": {
        "question": "Trong các trường hợp trượt đốt sống thắt lưng nặng được báo cáo, những thách thức nào được đặt ra khi lựa chọn phương pháp phẫu thuật?",
    },
    "q_048": {
        "question": "Nghiên cứu về điều trị rối loạn khớp thái dương hàm dưới bằng máng nhai ổn định có sử dụng T-scan được thực hiện với thiết kế nào và tại cơ sở nào?",
    },
    "q_061": {
        "question": "Theo nghiên cứu này, tổng liều xạ trị nào có liên quan đến thời gian sống thêm toàn bộ và thời gian sống không bệnh tiến triển tốt hơn ở bệnh nhân ung thư phổi biểu mô tuyến giai đoạn IIIB, IIIC?",
    },
    "q_063": {
        "question": "Trong nghiên cứu về exosome từ máu dây rốn người, phương pháp tách exosome kết hợp tủa polymer và MagCapture™ được ghi nhận có những ưu điểm nào?",
    },
    "q_066": {
        "question": "Nghiên cứu này được thực hiện nhằm xác định tỷ lệ suy giáp và những yếu tố dự đoán nào ở bệnh nhân đái tháo đường típ 2 có bệnh thận mạn?",
    },
    "q_068": {
        "question": "Thiết bị theo dõi đường huyết liên tục (CGM) có vai trò gì trong phát hiện và quản lý hạ đường huyết về đêm ở bệnh nhân đái tháo đường típ 2?",
        "expected_behavior": "summary",
    },
    "q_069": {
        "question": "Nghiên cứu so sánh đường hầm nhỏ và đường hầm tiêu chuẩn trong điều trị sỏi san hô được thực hiện trên nhóm bệnh nhân nào và trong giai đoạn nào?",
    },
    "q_072": {
        "question": "Nghiên cứu này tập trung đánh giá kết quả của những phương pháp phẫu thuật nào ở người lớn tuổi bị hẹp ống sống thắt lưng có mất vững do thoái hóa?",
    },
    "q_073": {
        "question": "Những phương pháp phẫu thuật nào được áp dụng trong nghiên cứu này để điều trị hẹp ống sống thắt lưng có mất vững do thoái hóa ở người lớn tuổi?",
        "query_type": "simple",
    },
    "q_077": {
        "question": "Trong nghiên cứu này, việc sử dụng thông khí không xâm lấn sau rút nội khí quản ở trẻ sinh non nhằm mục tiêu gì?",
    },
    "q_078": {
        "question": "Nghiên cứu tại Bệnh viện Nhi Đồng 1 tập trung vào vấn đề gì ở trẻ sơ sinh non tháng sau rút nội khí quản?",
    },
    "q_082": {
        "question": "Nghiên cứu này nhằm mô tả những đặc điểm siêu cấu trúc nào của tổn thương thận ghép mạn tính trên kính hiển vi điện tử truyền qua?",
    },
    "q_086": {
        "question": "Theo bài viết này, những đặc điểm nào của bệnh nhân cao tuổi khiến phác đồ pemetrexed đơn trị thường được lựa chọn trong điều trị ung thư phổi không tế bào nhỏ giai đoạn IIIB–IV?",
    },
}


def main():
    # Load
    queries = []
    with open(INPUT, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                queries.append(json.loads(line))

    print(f"Loaded {len(queries)} queries")

    rewrite_count = 0
    minor_count = 0

    for q in queries:
        qid = q["query_id"]

        # Major rewrites
        if qid in REWRITES:
            for key, val in REWRITES[qid].items():
                q[key] = val
            q["review_status"] = "rewritten"
            rewrite_count += 1

        # Minor fixes
        elif qid in MINOR_FIXES:
            for key, val in MINOR_FIXES[qid].items():
                q[key] = val
            q["review_status"] = "edited"
            minor_count += 1

        else:
            q["review_status"] = "accepted"

    # Save
    with open(OUTPUT, "w", encoding="utf-8") as f:
        for q in queries:
            f.write(json.dumps(q, ensure_ascii=False) + "\n")

    print(f"Rewrites: {rewrite_count}")
    print(f"Minor edits: {minor_count}")
    print(f"Accepted as-is: {len(queries) - rewrite_count - minor_count}")
    print(f"Saved to: {OUTPUT}")


if __name__ == "__main__":
    main()
