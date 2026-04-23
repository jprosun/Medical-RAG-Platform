# -*- coding: utf-8 -*-
"""Build a full 90-record gold dataset with merged metadata."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any


BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "output"
DATASET_DIR = BASE_DIR.parent / "datasets"

REVIEWED_QUERIES_PATH = OUTPUT_DIR / "reviewed_queries_v1.jsonl"
ANNOTATION_TEMPLATE_PATH = OUTPUT_DIR / "step3_annotation_template.jsonl"
ANSWER_PACK_PATH = OUTPUT_DIR / "chatgpt_batches" / "final_merged_first_pass.json"

JSONL_OUTPUT_PATH = DATASET_DIR / "vmj_synthetic_gold_v1_90.jsonl"
JSON_OUTPUT_PATH = DATASET_DIR / "vmj_synthetic_gold_v1_90.json"

SHORT_ANSWER_OVERRIDES = {
    "q_004": "Cần sinh thiết đầy đủ các vị trí khả thi và phối hợp hóa mô miễn dịch để phân biệt chính xác các tân sinh ở nhiều cơ quan, từ đó định hướng điều trị và bảo đảm chẩn đoán mô bệnh học chính xác.",
    "q_005": "Các marker chẩn đoán có giá trị gồm CD33, MPO, CD34, CD117, HLA-DR cùng CD56 và CD7. MPO dương tính gợi tiên lượng tốt hơn, còn CD34(+)/HLA-DR(-) liên quan đáp ứng kém và PFS ngắn hơn.",
    "q_012": "Định vị thần kinh giúp tiếp cận khối u chính xác hơn, lấy u hiệu quả hơn, giảm tổn thương mô lành và biến chứng, đồng thời bảo tồn hoặc cải thiện chức năng thần kinh và chất lượng sống sau mổ.",
    "q_013": "Hồi phục chức năng vận động đạt 96,18% và điểm Karnofsky trên 80 được duy trì trong 3-6 tháng theo dõi.",
    "q_015": "CD58 và CD81 hữu ích vì biểu hiện ở 100% quần thể ác tính lúc chẩn đoán, tương đối ổn định sau điều trị và khác biệt rõ với tế bào B non bình thường hoặc hoạt hóa; CD58 mạnh hơn khoảng 2 lần còn CD81 yếu hơn khoảng 5 lần.",
    "q_021": "Nghiên cứu được thực hiện trên 234 trẻ viêm mạch máu IgA tại Bệnh viện Nhi Đồng 1 trong giai đoạn 01/2021-08/2024.",
    "q_023": "Đây là nghiên cứu mô tả loạt ca bệnh trên 34 bệnh nhân, thực hiện từ 04/2023 đến 04/2024.",
    "q_027": "Không. Context chỉ cho thấy điều trị nội khoa tối ưu, kiểm soát yếu tố nguy cơ tim mạch và theo dõi hình ảnh định kỳ giúp giảm nguy cơ tái hẹp và đột quỵ, chứ không chứng minh loại bỏ hoàn toàn nguy cơ.",
    "q_029": "CWA được xem là hữu ích vì đánh giá toàn diện hơn quá trình đông máu và cung cấp thêm các thông số Min1, Min2, Max2 từ APTT, trong khi các xét nghiệm hiện nay chỉ phản ánh một phần ngoài nồng độ yếu tố VIII.",
    "q_031": "Nghiên cứu khảo sát các yếu tố liên quan đến kiến thức, thái độ và thực hành, nhưng context không nêu cụ thể các yếu tố này; chỉ cho biết tỷ lệ đúng lần lượt là 75,4%, 80,5% và 68,6%.",
    "q_037": "Thuyên tắc mạch trước mổ nhằm giảm tưới máu khối u, hạn chế chảy máu, tạo thuận lợi cho phẫu thuật và tăng tính an toàn, khả năng cắt bỏ.",
    "q_040": "Troponin tăng ở bệnh nhân rung nhĩ là yếu tố tiên lượng mạnh, liên quan nguy cơ tử vong mọi nguyên nhân và MACE cao hơn, đặc biệt tại phòng cấp cứu, với HR gộp lần lượt là 2,7 và 2,17.",
    "q_043": "Chưa thể kết luận ngưỡng PLT ≤ 20 × 10³/µL áp dụng cho tất cả bệnh nhân vì context chỉ nêu mục tiêu khảo sát và phân tích ROC, chưa có kết quả cụ thể.",
    "q_045": "Khác biệt cận lâm sàng có ý nghĩa thống kê là tiểu cầu giảm thấp hơn và procalcitonin cao hơn ở nhóm nhiễm khuẩn huyết gram âm.",
    "q_047": "Thách thức chính là phải vừa giải ép thỏa đáng cấu trúc thần kinh vừa nắn chỉnh trượt đốt sống, nên lựa chọn phương pháp phẫu thuật vẫn còn bàn cãi.",
    "q_049": "Không. Đây là nghiên cứu can thiệp lâm sàng không đối chứng và không có nhóm so sánh, nên context không đủ để kết luận T-scan vượt trội hơn các phương pháp khác.",
    "q_051": "Siêu âm tại giường có ưu điểm nhanh, không cần vận chuyển bệnh nhân, chi phí thấp, an toàn, cho kết quả tức thì, lặp lại được nhiều lần và không phơi nhiễm tia xạ.",
    "q_058": "Mô hình này khả thi vì tận dụng thiết bị sẵn có, chi phí thấp, tích hợp tốt với HIS/LIS/PACS và hỗ trợ hội chẩn từ xa, phù hợp với môi trường nguồn lực hạn chế.",
    "q_060": "TOETVA mất thời gian lâu hơn rõ rệt so với mổ mở, khoảng 103,71 phút so với 50,86 phút, nhưng khác biệt về an toàn không có ý nghĩa thống kê.",
    "q_061": "Tổng liều xạ trị ≥ 60 Gy liên quan với sống thêm toàn bộ và sống không bệnh tiến triển tốt hơn so với nhóm < 60 Gy.",
    "q_070": "Do còn lo ngại về an toàn khi dùng dài hạn và vì chỉ định cần được cá thể hóa.",
    "q_075": "AS-OCT giúp đánh giá chính xác các thông số hình thái bán phần trước ở glaucoma góc đóng, nhưng context không đủ để kết luận nó đã đủ cho chẩn đoán và quản lý toàn diện hay cần phối hợp cụ thể với phương pháp nào khác.",
    "q_080": "Không. Đau khớp giảm rõ sau 6 tháng điều trị bằng máng ổn định, từ 90,32% xuống 16,12%, nhưng không được loại bỏ hoàn toàn.",
    "q_084": "Không thể kết luận ảnh hưởng lâu dài. Context chỉ cho thấy liên quan ở mốc 1 tháng (p < 0,05), trong khi nhạy cảm giảm dần và trở lại bình thường ở 3 tháng, còn tỷ lệ thành công ở 6 tháng là 100%.",
    "q_085": "Không. Nghiên cứu chỉ đánh giá các mốc 1 tháng, 3 tháng và 6 tháng nên không đủ thông tin để kết luận dài hạn hơn.",
    "q_087": "Không. Context chỉ cho thấy lạm dụng glucocorticoid, nhất là dùng toàn thân kéo dài và liên tục, liên quan rõ với biểu hiện Cushing và suy thượng thận mạn; không chứng minh mọi trường hợp đều mắc, và hội chứng Cushing ghi nhận ở 87,2% bệnh nhân nghiên cứu.",
    "q_088": "Context không nêu cụ thể yếu tố nào, chỉ cho biết có nhiều yếu tố có thể ảnh hưởng đến kiến thức và kỹ năng tư vấn giáo dục sức khỏe của điều dưỡng, hộ sinh trong nghiên cứu tại Bệnh viện Phụ Sản Thiện An năm 2022.",
    "q_089": "Không. Nghiên cứu chỉ cho thấy mối liên quan giữa khối ngành học và kiến thức, không có dữ liệu về tỷ lệ mắc HIV theo nhóm ngành và không chứng minh quan hệ nhân quả trực tiếp.",
}


QUESTION_OVERRIDES = {
    "q_035": "Trong nghiên cứu về đường cong học tập kỹ thuật Snodgrass, các trường hợp được chia như thế nào để phân tích ảnh hưởng của kinh nghiệm phẫu thuật viên đến biến chứng?",
    "q_072": "Nghiên cứu này đánh giá kết quả điều trị bệnh lý nào và trên nhóm bệnh nhân nào?",
}

GROUND_TRUTH_OVERRIDES = {
    "q_003": "Đối với u tương bào ngoài tủy đơn độc ở phổi với tổn thương cô lập, phẫu thuật cắt bỏ là phương pháp điều trị được ưu tiên lựa chọn. Context nêu rõ đây là phương pháp điều trị được ưu tiên cho các tổn thương cô lập ở phổi.",
    "q_035": "Để phân tích ảnh hưởng của kinh nghiệm phẫu thuật viên đến biến chứng, nghiên cứu so sánh các nhóm khác nhau, mỗi nhóm gồm 30 trường hợp liên tiếp theo thứ tự thời gian.",
    "q_072": "Nghiên cứu đánh giá kết quả điều trị hẹp ống sống thắt lưng có mất vững do thoái hóa ở người lớn tuổi. Context tập trung vào đúng bệnh lý này ở nhóm bệnh nhân cao tuổi.",
}

SHORT_ANSWER_EXTRA_OVERRIDES = {
    "q_035": "Các trường hợp được chia thành các nhóm khác nhau, mỗi nhóm gồm 30 trường hợp liên tiếp theo thứ tự thời gian.",
    "q_071": "Context chưa cung cấp kết quả cụ thể về hạn chế EBP, nên chưa thể xác định điểm yếu cụ thể cần đào tạo.",
    "q_072": "Nghiên cứu tập trung vào hẹp ống sống thắt lưng có mất vững do thoái hóa ở người lớn tuổi.",
}

MUST_HAVE_CONCEPT_OVERRIDES = {
    "q_035": [
        "So sánh các nhóm khác nhau",
        "Mỗi nhóm gồm 30 trường hợp liên tiếp",
        "Sắp xếp theo thứ tự thời gian",
        "Dùng để phân tích ảnh hưởng của kinh nghiệm phẫu thuật viên",
    ],
    "q_070": [
        "Lo ngại về an toàn khi dùng dài hạn",
        "Chỉ định cần cá thể hóa",
        "Thuốc ức chế Janus kinase",
    ],
    "q_072": [
        "Hẹp ống sống thắt lưng có mất vững do thoái hóa",
        "Người lớn tuổi",
        "Nghiên cứu đánh giá kết quả điều trị",
    ],
}

MUST_NOT_CLAIM_OVERRIDES = {
    "q_035": [
        "Các trường hợp được chia ngẫu nhiên thành các nhóm điều trị",
        "Mỗi nhóm chỉ gồm 10 hoặc 20 trường hợp",
        "Phân nhóm không dựa trên thứ tự thời gian",
    ],
    "q_072": [
        "Nghiên cứu đánh giá trên người trẻ tuổi",
        "Context mô tả nhiều bệnh lý cột sống khác nhau thay vì hẹp ống sống thắt lưng có mất vững do thoái hóa",
        "Mục tiêu chỉ là so sánh kỹ thuật mà không xác định rõ bệnh lý và nhóm bệnh nhân",
    ],
}


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def load_json(path: Path) -> list[dict[str, Any]]:
    return json.loads(path.read_text(encoding="utf-8"))


def save_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def save_json(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")


def clean_context(query_id: str, context: str) -> str:
    cleaned = context

    if query_id in {"q_044", "q_045"}:
        cleaned = cleaned.replace("dieungan@gmail. com ", "")
        cleaned = cleaned.replace("do vi khuẩn. gram âm;", "do vi khuẩn gram âm;")

    if query_id == "q_052":
        cleaned = cleaned.replace(
            "*Bệnh viện Phục hồi chức năng Thanh Hóa, **Trường Đại học Y Hà Nội ",
            ""
        )

    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def clean_short_answer(query_id: str, short_answer: str) -> str:
    if query_id in SHORT_ANSWER_EXTRA_OVERRIDES:
        return SHORT_ANSWER_EXTRA_OVERRIDES[query_id]
    return SHORT_ANSWER_OVERRIDES.get(query_id, short_answer.strip())


def clean_question(query_id: str, question: str) -> str:
    return QUESTION_OVERRIDES.get(query_id, question.strip())


def clean_ground_truth(query_id: str, ground_truth: str) -> str:
    return GROUND_TRUTH_OVERRIDES.get(query_id, ground_truth.strip())


def clean_must_have_concepts(query_id: str, must_have_concepts: list[str]) -> list[str]:
    return MUST_HAVE_CONCEPT_OVERRIDES.get(query_id, must_have_concepts)


def clean_must_not_claim(query_id: str, must_not_claim: list[str]) -> list[str]:
    return MUST_NOT_CLAIM_OVERRIDES.get(query_id, must_not_claim)


def main() -> None:
    reviewed_rows = load_jsonl(REVIEWED_QUERIES_PATH)
    template_rows = load_jsonl(ANNOTATION_TEMPLATE_PATH)
    answer_rows = load_json(ANSWER_PACK_PATH)

    reviewed_map = {row["query_id"]: row for row in reviewed_rows}
    template_map = {row["query_id"]: row for row in template_rows}
    answer_map = {row["query_id"]: row for row in answer_rows}

    query_ids = sorted(answer_map.keys())

    if len(reviewed_map) != len(template_map) or len(template_map) != len(answer_map):
        raise ValueError(
            f"Row-count mismatch: reviewed={len(reviewed_map)}, template={len(template_map)}, answers={len(answer_map)}"
        )

    merged_rows: list[dict[str, Any]] = []
    for query_id in query_ids:
        reviewed = reviewed_map.get(query_id)
        template = template_map.get(query_id)
        answer = answer_map.get(query_id)
        if reviewed is None or template is None or answer is None:
            raise KeyError(f"Missing record for {query_id}")

        merged_rows.append(
            {
                "query_id": query_id,
                "dataset_id": f"vmj_synth_gold_{query_id}",
                "dataset_version": "v1_90",
                "source": reviewed["source"],
                "seed_id": reviewed["seed_id"],
                "review_status": reviewed["review_status"],
                "language": "vi",
                "split": "gold",
                "question": clean_question(query_id, reviewed["question"]),
                "context": clean_context(query_id, template["context"]),
                "query_type": reviewed["query_type"],
                "difficulty": reviewed["difficulty"],
                "expected_behavior": reviewed["expected_behavior"],
                "answerability": reviewed["answerability"],
                "topic": reviewed["topic"],
                "title": reviewed["title"],
                "ground_truth": clean_ground_truth(query_id, answer["ground_truth"]),
                "short_answer": clean_short_answer(query_id, answer["short_answer"]),
                "must_have_concepts": clean_must_have_concepts(query_id, answer["must_have_concepts"]),
                "must_not_claim": clean_must_not_claim(query_id, answer["must_not_claim"]),
            }
        )

    save_jsonl(JSONL_OUTPUT_PATH, merged_rows)
    save_json(JSON_OUTPUT_PATH, merged_rows)

    print(f"Gold dataset JSONL: {JSONL_OUTPUT_PATH}")
    print(f"Gold dataset JSON: {JSON_OUTPUT_PATH}")
    print(f"Total records: {len(merged_rows)}")


if __name__ == "__main__":
    main()
