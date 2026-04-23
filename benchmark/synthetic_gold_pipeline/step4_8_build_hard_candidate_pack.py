# -*- coding: utf-8 -*-
"""Build a safe hard-record candidate pack from the existing gold corpus."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


BASE_DIR = Path(__file__).resolve().parent
DATASET_DIR = BASE_DIR.parent / "datasets"
OUTPUT_DIR = BASE_DIR / "output"

SOURCE_DATASET_PATH = DATASET_DIR / "vmj_synthetic_gold_v1_90.json"
JSONL_OUTPUT_PATH = OUTPUT_DIR / "hard_record_candidates_v1.jsonl"
JSON_OUTPUT_PATH = OUTPUT_DIR / "hard_record_candidates_v1.json"
MD_OUTPUT_PATH = OUTPUT_DIR / "hard_record_candidates_v1.md"


CANDIDATE_SPECS: list[dict[str, Any]] = [
    {
        "candidate_id": "hc_001",
        "source_query_id": "q_002",
        "derived_from_query_ids": ["q_001", "q_002"],
        "question": "Dựa trên context này, có thể đề xuất trực tiếp một phác đồ phòng ngừa hoặc quản lý riêng cho thai phụ có từ hai yếu tố nguy cơ trở lên không? Vì sao?",
        "query_type": "bounded_partial",
        "difficulty": "hard",
        "expected_behavior": "bounded_partial",
        "answerability": "partial_only",
        "hard_type": "insufficient_but_useful",
        "rationale": "Buộc mô hình phân biệt giữa nhận diện nhóm nguy cơ cao và việc có đủ dữ liệu để đưa ra phác đồ quản lý cụ thể.",
        "ground_truth": "Chưa thể đề xuất trực tiếp một phác đồ phòng ngừa hoặc quản lý riêng chỉ từ context này. Context chỉ cho biết các yếu tố liên quan làm tăng nguy cơ đái tháo đường thai kỳ và cho thấy thai phụ có từ hai yếu tố nguy cơ trở lên hoặc trên ba yếu tố nguy cơ là nhóm nguy cơ cao hơn. Tuy nhiên, bài không nêu biện pháp phòng ngừa, lịch theo dõi hay chiến lược điều trị cụ thể cho từng nhóm nguy cơ.",
        "short_answer": "Chưa. Context chỉ xác định nhóm nguy cơ cao hơn, không nêu phác đồ phòng ngừa hay quản lý cụ thể.",
        "must_have_concepts": [
            "Chỉ xác định các yếu tố nguy cơ hoặc nhóm nguy cơ cao",
            "Thai phụ có nhiều yếu tố nguy cơ có nguy cơ cao hơn",
            "Không có phác đồ phòng ngừa hoặc quản lý cụ thể trong context",
        ],
        "must_not_claim": [
            "Context khuyến cáo sẵn chế độ ăn, insulin hoặc thuốc cụ thể",
            "Context đưa ra lịch tầm soát hoặc xử trí chi tiết theo từng mức nguy cơ",
        ],
    },
    {
        "candidate_id": "hc_002",
        "source_query_id": "q_010",
        "derived_from_query_ids": ["q_010"],
        "question": "Ở bệnh nhân ghép thận nghi mắc lao, vì sao việc chẩn đoán và điều trị phải được cân nhắc cùng nhau thay vì tách rời?",
        "query_type": "reasoning",
        "difficulty": "hard",
        "expected_behavior": "summary",
        "answerability": "answerable",
        "hard_type": "multi_hop",
        "rationale": "Yêu cầu nối thông tin về biểu hiện lâm sàng, hạn chế xét nghiệm và tương tác thuốc trong cùng một lập luận.",
        "ground_truth": "Ở bệnh nhân ghép thận, chẩn đoán và điều trị lao phải được cân nhắc cùng nhau vì bệnh thường biểu hiện không điển hình, nhiều trường hợp là lao ngoài phổi hoặc lao lan tỏa, trong khi xét nghiệm lại có độ nhạy thấp và trả kết quả chậm. Đồng thời, điều trị lao rất dễ tương tác với thuốc chống thải ghép, đặc biệt rifampicin làm giảm nồng độ tacrolimus và cyclosporine, làm tăng nguy cơ thải ghép. Thuốc kháng lao còn có thể gây độc tính và thời gian điều trị kéo dài, nên quyết định chẩn đoán ảnh hưởng trực tiếp đến chiến lược theo dõi và xử trí.",
        "short_answer": "Vì chẩn đoán lao ở bệnh nhân ghép thận vừa khó do biểu hiện không điển hình và xét nghiệm hạn chế, vừa gắn chặt với nguy cơ tương tác thuốc chống thải ghép và độc tính khi điều trị.",
        "must_have_concepts": [
            "Biểu hiện lâm sàng không điển hình hoặc lao ngoài phổi, lao lan tỏa",
            "Xét nghiệm chẩn đoán độ nhạy thấp hoặc trả kết quả chậm",
            "Tương tác rifampicin với tacrolimus hoặc cyclosporine",
            "Nguy cơ thải ghép hoặc cần theo dõi chỉnh liều chặt",
        ],
        "must_not_claim": [
            "Context cho thấy chẩn đoán lao ở bệnh nhân ghép thận là đơn giản",
            "Điều trị lao không ảnh hưởng đến thuốc chống thải ghép",
        ],
    },
    {
        "candidate_id": "hc_003",
        "source_query_id": "q_015",
        "derived_from_query_ids": ["q_015", "q_016"],
        "question": "Vì sao trong theo dõi MRD ở bệnh bạch cầu cấp dòng lympho B, việc dựa vào cả CD58, CD81 và tỉ số MFI CD81/CD58 hợp lý hơn là chỉ nhìn một dấu ấn đơn lẻ?",
        "query_type": "reasoning",
        "difficulty": "hard",
        "expected_behavior": "summary",
        "answerability": "answerable",
        "hard_type": "multi_hop",
        "rationale": "Đòi hỏi tổng hợp sự ổn định sau điều trị, khác biệt với quần thể bình thường và vai trò của chỉ số tỉ số MFI.",
        "ground_truth": "Việc dựa vào cả CD58, CD81 và tỉ số MFI CD81/CD58 hợp lý hơn vì hai dấu ấn này đều biểu hiện trên quần thể ác tính lúc chẩn đoán, tương đối ổn định sau điều trị và khác biệt rõ với quần thể B non bình thường hoặc hoạt hóa. CD58 thường tăng mạnh hơn còn CD81 giảm hơn trên quần thể ác tính, và tỉ số MFI CD81/CD58 giúp nhận diện quần thể B non bình thường hoặc hoạt hóa với độ chính xác cao hơn. Do đó, cách tiếp cận kết hợp giúp phân biệt tốt hơn giữa tế bào ác tính và quần thể không ác tính khi theo dõi MRD.",
        "short_answer": "Vì CD58 và CD81 đều khác biệt rõ, khá ổn định sau điều trị, còn tỉ số MFI CD81/CD58 giúp phân biệt tốt hơn quần thể ác tính với B non bình thường hoặc hoạt hóa.",
        "must_have_concepts": [
            "CD58 và CD81 đều hữu ích hoặc biểu hiện trên quần thể ác tính",
            "Mức biểu hiện tương đối ổn định sau điều trị",
            "Khác biệt với quần thể B non bình thường hoặc hoạt hóa",
            "Tỉ số MFI CD81/CD58 hỗ trợ nhận diện hoặc phân biệt",
        ],
        "must_not_claim": [
            "Context chứng minh một dấu ấn đơn lẻ là đủ trong mọi trường hợp",
            "Tỉ số MFI CD81/CD58 được dùng để chẩn đoán mọi bệnh huyết học khác",
        ],
    },
    {
        "candidate_id": "hc_004",
        "source_query_id": "q_027",
        "derived_from_query_ids": ["q_027"],
        "question": "Theo context, quyết định chọn CEA hay CAS và việc theo dõi sau can thiệp đều phải cá thể hóa dựa trên những nhóm yếu tố nào?",
        "query_type": "reasoning",
        "difficulty": "hard",
        "expected_behavior": "summary",
        "answerability": "answerable",
        "hard_type": "constraint_heavy",
        "rationale": "Buộc mô hình gom cả tiêu chí chọn thủ thuật và yêu cầu quản lý sau can thiệp trong một câu trả lời có ràng buộc.",
        "ground_truth": "Theo context, lựa chọn giữa CEA và CAS không thể áp dụng đồng loạt mà phải cá thể hóa theo mức độ hẹp động mạch, tình trạng có triệu chứng hay không, nguy cơ quanh thủ thuật và kỳ vọng sống còn của người bệnh. Sau can thiệp, việc theo dõi cũng phải tiếp tục cá thể hóa vì người bệnh vẫn cần điều trị nội khoa tối ưu, kiểm soát yếu tố nguy cơ tim mạch và theo dõi hình ảnh định kỳ để giảm nguy cơ tái hẹp và đột quỵ, chứ nguy cơ không được loại bỏ hoàn toàn.",
        "short_answer": "Cần cá thể hóa theo mức độ hẹp, triệu chứng, nguy cơ quanh thủ thuật và kỳ vọng sống còn; sau can thiệp vẫn phải điều trị nội khoa tối ưu, kiểm soát yếu tố nguy cơ và theo dõi hình ảnh định kỳ.",
        "must_have_concepts": [
            "Mức độ hẹp",
            "Có triệu chứng hay không",
            "Nguy cơ quanh thủ thuật",
            "Kỳ vọng sống còn",
            "Điều trị nội khoa tối ưu sau can thiệp",
            "Kiểm soát yếu tố nguy cơ tim mạch hoặc theo dõi hình ảnh định kỳ",
        ],
        "must_not_claim": [
            "CEA luôn tốt hơn CAS cho mọi bệnh nhân",
            "Sau can thiệp không cần theo dõi thêm",
        ],
    },
    {
        "candidate_id": "hc_005",
        "source_query_id": "q_030",
        "derived_from_query_ids": ["q_030"],
        "question": "Từ phần tổng quan này, có thể kết luận PIEB vượt trội tuyệt đối so với CEI ở mọi khía cạnh không? Có thể khẳng định chắc điều gì và chưa thể khẳng định điều gì?",
        "query_type": "bounded_partial",
        "difficulty": "hard",
        "expected_behavior": "bounded_partial",
        "answerability": "partial_only",
        "hard_type": "insufficient_but_useful",
        "rationale": "Đây là bài test chống overclaim dựa trên phần đặt vấn đề, không phải kết quả nghiên cứu hoàn chỉnh.",
        "ground_truth": "Chưa thể kết luận PIEB vượt trội tuyệt đối ở mọi khía cạnh chỉ từ context này. Có thể khẳng định rằng phần tổng quan nêu nhiều nghiên cứu cho thấy PIEB giúp giảm lượng thuốc tê, giảm ức chế vận động, hiệu quả giảm đau tốt hơn và không làm tăng các tác dụng phụ khác so với CEI. Tuy nhiên, context chỉ cung cấp cơ sở lý thuyết và lý do tiến hành nghiên cứu, chưa đủ để khẳng định ưu thế tuyệt đối trong mọi bối cảnh hoặc trên mọi nhóm sản phụ.",
        "short_answer": "Chưa. Context chỉ cho thấy PIEB có nhiều lợi thế được gợi ý trong tổng quan, chưa đủ để kết luận vượt trội tuyệt đối ở mọi bối cảnh.",
        "must_have_concepts": [
            "PIEB được nêu là có lợi thế như giảm thuốc tê hoặc giảm ức chế vận động hoặc giảm đau tốt hơn",
            "Không tăng tác dụng phụ khác trong các nghiên cứu được nêu",
            "Context chưa đủ để kết luận vượt trội tuyệt đối trong mọi trường hợp",
        ],
        "must_not_claim": [
            "Nghiên cứu hiện tại đã chứng minh dứt khoát PIEB tốt hơn CEI ở mọi tiêu chí",
            "PIEB hoàn toàn không có bất kỳ hạn chế nào",
        ],
    },
    {
        "candidate_id": "hc_006",
        "source_query_id": "q_040",
        "derived_from_query_ids": ["q_040", "q_041"],
        "question": "Theo tổng quan này, vai trò của troponin trong rung nhĩ mới khởi phát và trong rung nhĩ đã có khác nhau như thế nào?",
        "query_type": "reasoning",
        "difficulty": "hard",
        "expected_behavior": "summary",
        "answerability": "answerable",
        "hard_type": "multi_hop",
        "rationale": "Buộc tách hai nhiệm vụ lâm sàng khác nhau của cùng một biomarker: dự đoán ca mới và tiên lượng ca đã mắc.",
        "ground_truth": "Theo tổng quan, ở người chưa mắc rung nhĩ, troponin độ nhạy cao tăng có vai trò dự đoán nguy cơ xuất hiện rung nhĩ mới khởi phát, với mối liên quan mạnh và theo liều lượng ngay cả sau khi điều chỉnh các yếu tố nguy cơ truyền thống. Ngược lại, ở bệnh nhân đã mắc rung nhĩ, troponin tăng chủ yếu mang ý nghĩa tiên lượng xấu, liên quan nguy cơ tử vong mọi nguyên nhân và biến cố tim mạch bất lợi cao hơn, đặc biệt trong bối cảnh phòng cấp cứu.",
        "short_answer": "Troponin tăng dùng để dự đoán nguy cơ xuất hiện rung nhĩ mới ở người chưa mắc bệnh, còn ở bệnh nhân đã có rung nhĩ thì chủ yếu mang ý nghĩa tiên lượng xấu hơn.",
        "must_have_concepts": [
            "Dự đoán rung nhĩ mới khởi phát",
            "Troponin độ nhạy cao tăng liên quan nguy cơ hoặc theo liều lượng",
            "Ở bệnh nhân đã mắc rung nhĩ là ý nghĩa tiên lượng",
            "Liên quan tử vong hoặc MACE cao hơn",
        ],
        "must_not_claim": [
            "Troponin chỉ có một vai trò duy nhất trong rung nhĩ",
            "Context chứng minh troponin chẩn đoán xác định mọi ca rung nhĩ",
        ],
    },
    {
        "candidate_id": "hc_007",
        "source_query_id": "q_043",
        "derived_from_query_ids": ["q_043"],
        "question": "Vì sao ngưỡng PLT ≤ 20 × 10³/µL trong context này chỉ nên xem là ngưỡng báo động cần khảo sát thêm, chưa đủ để làm chỉ định truyền tiểu cầu áp dụng cho mọi bệnh nhân?",
        "query_type": "bounded_partial",
        "difficulty": "hard",
        "expected_behavior": "bounded_partial",
        "answerability": "partial_only",
        "hard_type": "insufficient_but_useful",
        "rationale": "Bài kiểm tra kiểu RAG khó: phải giữ được phần hữu ích của context nhưng không được biến mục tiêu nghiên cứu thành khuyến cáo lâm sàng đã xác lập.",
        "ground_truth": "Ngưỡng PLT ≤ 20 × 10³/µL trong context này mới chỉ là ngưỡng được khảo sát như một giá trị báo động tiềm năng. Bài nêu mục tiêu đánh giá thực trạng truyền tiểu cầu và khảo sát độ nhạy, độ đặc hiệu của ngưỡng này qua ROC, nhưng không cung cấp kết quả cụ thể để chứng minh ngưỡng đó đủ chính xác hoặc phù hợp cho mọi bệnh nhân. Vì vậy, chưa thể dùng nó như chỉ định truyền tiểu cầu áp dụng đại trà cho toàn bộ bệnh nhân.",
        "short_answer": "Vì context chỉ nêu đây là ngưỡng đang được khảo sát qua ROC, chưa có kết quả đủ để biến nó thành chỉ định truyền áp dụng cho mọi bệnh nhân.",
        "must_have_concepts": [
            "Ngưỡng này đang được khảo sát hoặc đánh giá",
            "Có nhắc đến ROC hoặc độ nhạy, độ đặc hiệu",
            "Không có kết quả cụ thể để khái quát cho mọi bệnh nhân",
        ],
        "must_not_claim": [
            "PLT ≤ 20 × 10³/µL đã được chứng minh là chỉ định truyền chuẩn cho tất cả bệnh nhân",
            "Ngưỡng này thay thế hoàn toàn đánh giá lâm sàng",
        ],
    },
    {
        "candidate_id": "hc_008",
        "source_query_id": "q_058",
        "derived_from_query_ids": ["q_058"],
        "question": "Những đánh đổi nào khiến BASIC-DPS vẫn được xem là khả thi trong điều kiện nguồn lực hạn chế?",
        "query_type": "reasoning",
        "difficulty": "hard",
        "expected_behavior": "summary",
        "answerability": "answerable",
        "hard_type": "constraint_heavy",
        "rationale": "Đây là câu trade-off rõ ràng: mô hình phải cân bằng lợi ích hạ tầng với hạn chế vận hành thực tế.",
        "ground_truth": "BASIC-DPS vẫn được xem là khả thi trong điều kiện nguồn lực hạn chế vì tận dụng kính hiển vi sẵn có, tích hợp được với HIS, LIS và PACS, hỗ trợ hội chẩn từ xa và có chi phí triển khai thấp hơn các hệ thống số hóa đầy đủ. Đánh đổi là hệ thống còn vận hành thủ công, tốc độ xử lý chậm khoảng 12 phút cho diện tích 1x1 cm và có tỷ lệ lỗi quét khoảng 5%. Nói cách khác, tính khả thi đến từ lợi ích triển khai thực tế dù hiệu năng chưa tối ưu.",
        "short_answer": "BASIC-DPS khả thi vì tận dụng thiết bị sẵn có, tích hợp tốt và hỗ trợ hội chẩn từ xa với chi phí thấp, dù phải đánh đổi bằng vận hành thủ công, quét chậm và có lỗi quét.",
        "must_have_concepts": [
            "Tận dụng thiết bị hoặc kính hiển vi sẵn có",
            "Tích hợp HIS/LIS/PACS hoặc hỗ trợ hội chẩn từ xa",
            "Chi phí thấp hoặc phù hợp nguồn lực hạn chế",
            "Đánh đổi là vận hành thủ công hoặc quét chậm hoặc có lỗi quét",
        ],
        "must_not_claim": [
            "BASIC-DPS đã tối ưu hoàn toàn như hệ thống số hóa đầy đủ",
            "Context cho thấy không có bất kỳ hạn chế vận hành nào",
        ],
    },
    {
        "candidate_id": "hc_009",
        "source_query_id": "q_060",
        "derived_from_query_ids": ["q_060"],
        "question": "Dựa trên context, vì sao TOETVA vẫn có thể là lựa chọn hợp lý cho ung thư tuyến giáp biệt hóa giai đoạn sớm dù thời gian mổ dài hơn mổ mở?",
        "query_type": "reasoning",
        "difficulty": "hard",
        "expected_behavior": "summary",
        "answerability": "answerable",
        "hard_type": "constraint_heavy",
        "rationale": "Câu này buộc mô hình xử lý đúng một trade-off kinh điển: hiệu quả thao tác kém hơn nhưng hồ sơ an toàn chấp nhận được.",
        "ground_truth": "TOETVA vẫn có thể là lựa chọn hợp lý vì dù thời gian phẫu thuật dài hơn rõ rệt so với mổ mở, nghiên cứu cho thấy tính an toàn phẫu thuật giữa hai nhóm là tương đương và khác biệt không có ý nghĩa thống kê. Context cũng kết luận đây là lựa chọn khả thi và an toàn ở bệnh nhân ung thư tuyến giáp biệt hóa giai đoạn sớm khi được chọn lọc phù hợp. Vì vậy, nhược điểm về thời gian không tự động loại trừ giá trị của phương pháp này.",
        "short_answer": "Vì dù thời gian mổ dài hơn, TOETVA vẫn cho hồ sơ an toàn tương đương và được xem là khả thi ở bệnh nhân giai đoạn sớm được chọn lọc phù hợp.",
        "must_have_concepts": [
            "Thời gian mổ dài hơn mổ mở",
            "Tính an toàn tương đương hoặc khác biệt không có ý nghĩa thống kê",
            "Lựa chọn khả thi ở bệnh nhân được chọn lọc phù hợp",
        ],
        "must_not_claim": [
            "TOETVA an toàn vượt trội hơn mổ mở",
            "TOETVA nên áp dụng cho mọi trường hợp ung thư tuyến giáp",
        ],
    },
    {
        "candidate_id": "hc_010",
        "source_query_id": "q_070",
        "derived_from_query_ids": ["q_070"],
        "question": "Nếu cân nhắc dùng thuốc ức chế Janus kinase dài hạn cho viêm khớp cột sống thể trục, context gợi ý phải cân đối những lợi ích nào với những giới hạn nào?",
        "query_type": "reasoning",
        "difficulty": "hard",
        "expected_behavior": "summary",
        "answerability": "answerable",
        "hard_type": "multi_hop",
        "rationale": "Buộc mô hình cân bằng lợi ích điều trị với cảnh báo an toàn và điều kiện chỉ định cá thể hóa.",
        "ground_truth": "Context gợi ý rằng khi cân nhắc dùng thuốc ức chế Janus kinase dài hạn, cần cân đối giữa các lợi ích như đường dùng thuận tiện, tác động lên nhiều cytokine và khả năng cải thiện nhanh triệu chứng với các giới hạn gồm tranh cãi về an toàn khi dùng dài hạn và yêu cầu cá thể hóa chỉ định cho từng người bệnh. Vì vậy, đây không phải lựa chọn có thể áp dụng đồng nhất cho mọi trường hợp.",
        "short_answer": "Cần cân đối lợi ích về đường dùng thuận tiện, tác động lên nhiều cytokine và cải thiện nhanh triệu chứng với lo ngại an toàn dài hạn và nhu cầu cá thể hóa chỉ định.",
        "must_have_concepts": [
            "Đường dùng thuận tiện",
            "Tác động lên nhiều cytokine hoặc cải thiện nhanh triệu chứng",
            "Lo ngại an toàn khi dùng dài hạn",
            "Chỉ định cần cá thể hóa",
        ],
        "must_not_claim": [
            "Thuốc an toàn tuyệt đối khi dùng lâu dài",
            "Có thể dùng giống nhau cho mọi bệnh nhân",
        ],
    },
    {
        "candidate_id": "hc_011",
        "source_query_id": "q_084",
        "derived_from_query_ids": ["q_084", "q_085"],
        "question": "Từ dữ liệu theo dõi 1, 3 và 6 tháng, có thể kết luận gì và không thể kết luận gì về ảnh hưởng của kích thước xoang sâu đến kết quả phục hồi bằng inlay sứ lai?",
        "query_type": "bounded_partial",
        "difficulty": "hard",
        "expected_behavior": "bounded_partial",
        "answerability": "partial_only",
        "hard_type": "insufficient_but_useful",
        "rationale": "Câu này ép mô hình tách được kết luận ngắn hạn có dữ liệu khỏi phần dài hạn chưa có dữ liệu.",
        "ground_truth": "Từ dữ liệu 1, 3 và 6 tháng, có thể kết luận rằng kích thước xoang sâu có liên quan đến tình trạng sau điều trị ở mốc 1 tháng, nhưng mức nhạy cảm này là nhẹ và giảm dần, trở về bình thường ở mốc 3 tháng. Đồng thời, tỷ lệ thành công của phục hồi bằng inlay sứ lai là 100% ở mốc 6 tháng. Tuy nhiên, không thể kết luận rằng kích thước xoang sâu ảnh hưởng lâu dài hoặc ảnh hưởng kéo dài quá 6 tháng, vì context không cung cấp dữ liệu theo dõi dài hơn.",
        "short_answer": "Có thể nói kích thước xoang sâu liên quan đến tình trạng ở mốc 1 tháng nhưng ảnh hưởng này giảm và trở về bình thường ở 3 tháng; không thể kết luận tác động lâu dài vượt quá 6 tháng.",
        "must_have_concepts": [
            "Có liên quan ở mốc 1 tháng",
            "Nhạy cảm nhẹ và giảm dần hoặc trở về bình thường ở 3 tháng",
            "Tỷ lệ thành công 6 tháng là 100%",
            "Không đủ dữ liệu để kết luận ảnh hưởng lâu dài hơn",
        ],
        "must_not_claim": [
            "Kích thước xoang sâu gây ảnh hưởng lâu dài đã được chứng minh",
            "Context có dữ liệu theo dõi sau 6 tháng",
        ],
    },
    {
        "candidate_id": "hc_012",
        "source_query_id": "q_089",
        "derived_from_query_ids": ["q_089"],
        "question": "Từ mối liên quan giữa khối ngành học và kiến thức về lây nhiễm HIV qua quan hệ tình dục, context cho phép suy ra điều gì và không cho phép suy ra điều gì về nguy cơ nhiễm HIV thực tế?",
        "query_type": "bounded_partial",
        "difficulty": "hard",
        "expected_behavior": "bounded_partial",
        "answerability": "partial_only",
        "hard_type": "insufficient_but_useful",
        "rationale": "Đây là câu chống nhảy từ association sang causation và từ kiến thức sang tỷ lệ mắc thực tế.",
        "ground_truth": "Context cho phép suy ra rằng sinh viên thuộc khối ngành sức khỏe có kiến thức tốt hơn về lây nhiễm HIV qua quan hệ tình dục so với sinh viên ở các khối ngành khác. Tuy nhiên, context không cho phép suy ra rằng sinh viên ngoài khối ngành sức khỏe có nguy cơ nhiễm HIV thực tế cao hơn, càng không cho phép kết luận về quan hệ nhân quả trực tiếp giữa kiến thức và tỷ lệ mắc HIV, vì nghiên cứu chỉ khảo sát mối liên quan với kiến thức chứ không đo tỷ lệ nhiễm HIV thực tế.",
        "short_answer": "Chỉ có thể suy ra khác biệt về mức độ kiến thức giữa các khối ngành; không thể suy ra trực tiếp nguy cơ nhiễm HIV thực tế hay quan hệ nhân quả với tỷ lệ mắc.",
        "must_have_concepts": [
            "Khối ngành sức khỏe có kiến thức tốt hơn hoặc có liên quan đến kiến thức",
            "Không có dữ liệu về tỷ lệ nhiễm HIV thực tế",
            "Không thể suy ra quan hệ nhân quả trực tiếp",
        ],
        "must_not_claim": [
            "Sinh viên ngoài khối ngành sức khỏe chắc chắn có tỷ lệ nhiễm HIV cao hơn",
            "Nghiên cứu chứng minh kiến thức kém là nguyên nhân trực tiếp làm tăng mắc HIV",
        ],
    },
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


def build_markdown(rows: list[dict[str, Any]]) -> str:
    lines = [
        "# Hard Record Candidates v1",
        "",
        "- Status: `needs_review`",
        "- Strategy: safe append-only candidate pack, do not merge into `vmj_synthetic_gold_v1_90` before manual review",
        f"- Total candidates: `{len(rows)}`",
        "",
        "## Coverage",
        "",
    ]

    counts: dict[str, int] = {}
    for row in rows:
        counts[row["hard_type"]] = counts.get(row["hard_type"], 0) + 1
    for key in sorted(counts):
        lines.append(f"- `{key}`: `{counts[key]}`")

    lines.extend(
        [
            "",
            "## Candidates",
            "",
        ]
    )

    for row in rows:
        lines.extend(
            [
                f"### {row['candidate_id']}",
                f"- Derived from: `{', '.join(row['derived_from_query_ids'])}`",
                f"- Topic: `{row['topic']}`",
                f"- Hard type: `{row['hard_type']}`",
                f"- Question: {row['question']}",
                f"- Review rationale: {row['rationale']}",
                "",
            ]
        )

    return "\n".join(lines) + "\n"


def main() -> None:
    source_rows = load_json(SOURCE_DATASET_PATH)
    source_map = {row["query_id"]: row for row in source_rows}

    candidate_rows: list[dict[str, Any]] = []
    for spec in CANDIDATE_SPECS:
        source_row = source_map[spec["source_query_id"]]
        derived_rows = [source_map[qid] for qid in spec["derived_from_query_ids"]]

        candidate_rows.append(
            {
                "candidate_id": spec["candidate_id"],
                "candidate_version": "v1",
                "candidate_status": "needs_review",
                "parent_dataset": "vmj_synthetic_gold_v1_90",
                "derived_from_query_ids": spec["derived_from_query_ids"],
                "source": source_row["source"],
                "seed_ids": [row["seed_id"] for row in derived_rows],
                "language": "vi",
                "split": "candidate",
                "question": spec["question"],
                "context": source_row["context"],
                "query_type": spec["query_type"],
                "difficulty": spec["difficulty"],
                "expected_behavior": spec["expected_behavior"],
                "answerability": spec["answerability"],
                "topic": source_row["topic"],
                "title": source_row["title"],
                "hard_type": spec["hard_type"],
                "rationale": spec["rationale"],
                "ground_truth": spec["ground_truth"],
                "short_answer": spec["short_answer"],
                "must_have_concepts": spec["must_have_concepts"],
                "must_not_claim": spec["must_not_claim"],
            }
        )

    save_jsonl(JSONL_OUTPUT_PATH, candidate_rows)
    save_json(JSON_OUTPUT_PATH, candidate_rows)
    MD_OUTPUT_PATH.write_text(build_markdown(candidate_rows), encoding="utf-8")

    print(f"Hard candidate JSONL: {JSONL_OUTPUT_PATH}")
    print(f"Hard candidate JSON: {JSON_OUTPUT_PATH}")
    print(f"Hard candidate review MD: {MD_OUTPUT_PATH}")
    print(f"Total candidates: {len(candidate_rows)}")


if __name__ == "__main__":
    main()
