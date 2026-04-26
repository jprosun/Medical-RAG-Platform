"""
Vietnamese Medical Specialty Dictionary
=========================================
Maps Vietnamese medical keywords to standardized English specialty names.
Used by vn_metadata_enricher.py for rule-based specialty detection.
"""

from __future__ import annotations

# Ordered by clinical significance for Medical QA
SPECIALTY_KEYWORDS: dict[str, list[str]] = {
    "cardiology": [
        "tăng huyết áp", "suy tim", "nhồi máu cơ tim", "rung nhĩ",
        "van tim", "động mạch vành", "stent", "holter", "huyết áp",
        "tim mạch", "mạch vành", "block nhĩ thất", "suy vành",
    ],
    "respiratory": [
        "hen phế quản", "COPD", "viêm phổi", "lao phổi", "hô hấp",
        "phế quản", "màng phổi", "khí phế thũng", "tràn khí",
        "tràn dịch", "nội soi phế quản", "ngừng thở khi ngủ",
        "thông khí", "oxy liệu pháp",
    ],
    "endocrinology": [
        "đái tháo đường", "tuyến giáp", "insulin", "cushing",
        "tiểu đường", "basedow", "suy thượng thận", "tuyến yên",
    ],
    "oncology": [
        "ung thư", "u ác tính", "hóa trị", "xạ trị", "di căn",
        "lymphoma", "carcinoma", "u bướu", "sinh thiết",
        "hắc tố", "u tủy",
    ],
    "neurology": [
        "đột quỵ", "động kinh", "alzheimer", "parkinson",
        "thần kinh", "co giật", "viêm não", "đau đầu",
        "chấn thương sọ não",
    ],
    "gastroenterology": [
        "dạ dày", "gan", "viêm gan", "đại tràng",
        "tiêu hóa", "xơ gan", "tụy", "thực quản",
        "ruột", "trào ngược",
    ],
    "nephrology": [
        "thận", "chạy thận", "lọc máu", "suy thận",
        "hội chứng thận hư", "ghép thận", "tiết niệu",
    ],
    "dermatology": [
        "da liễu", "viêm da", "vảy nến", "nấm da",
        "viêm da cơ địa", "mụn trứng cá", "lupus ban đỏ",
    ],
    "infectious_disease": [
        "HIV", "nhiễm khuẩn", "kháng sinh", "vi khuẩn",
        "sốt rét", "lao", "nhiễm trùng", "virus",
        "kháng thuốc", "kháng kháng sinh",
    ],
    "pharmacology": [
        "dược", "thuốc", "liều dùng", "tương tác",
        "chống chỉ định", "GMP", "GSP", "dược phẩm",
        "hoạt chất", "bào chế", "sinh khả dụng",
    ],
    "traditional_medicine": [
        "y học cổ truyền", "YHCT", "châm cứu",
        "thuốc cổ truyền", "dược liệu", "đông y",
        "thuốc nam", "bài thuốc",
    ],
    "obstetrics_gynecology": [
        "thai", "sản khoa", "phụ khoa", "tử cung",
        "buồng trứng", "sinh non", "tiền sản giật",
    ],
    "pediatrics": [
        "trẻ em", "nhi khoa", "sơ sinh", "nhũ nhi",
        "tiêm chủng", "trẻ nhỏ",
    ],
    "surgery": [
        "phẫu thuật", "ngoại khoa", "nội soi",
        "mổ", "phẫu thuật nội soi", "ghép tạng",
    ],
    "ophthalmology": [
        "mắt", "võng mạc", "nhãn khoa", "thủy tinh thể",
        "glaucoma", "đục thủy tinh thể",
    ],
    "hematology": [
        "máu", "đông máu", "hemophilia", "tiểu cầu",
        "bạch cầu", "thiếu máu", "hồng cầu",
        "thalassemia", "truyền máu",
    ],
    "psychiatry": [
        "tâm thần", "trầm cảm", "lo âu", "tâm thần phân liệt",
        "rối loạn tâm thần",
    ],
    "rehabilitation": [
        "phục hồi chức năng", "vật lý trị liệu",
        "phục hồi", "chức năng hô hấp",
    ],
    "emergency_medicine": [
        "cấp cứu", "hồi sức", "sốc", "ngừng tim",
        "hồi sức tích cực", "ICU",
    ],
    "radiology": [
        "chẩn đoán hình ảnh", "X quang", "CT", "MRI",
        "siêu âm", "cắt lớp vi tính", "cộng hưởng từ",
    ],
    "nutrition": [
        "dinh dưỡng", "suy dinh dưỡng", "BMI",
        "chế độ ăn", "dinh dưỡng lâm sàng",
    ],
    "public_health": [
        "y tế công cộng", "dịch tễ", "phòng bệnh",
        "sức khỏe cộng đồng", "sàng lọc", "tiêm chủng",
    ],
}


def detect_specialty(title: str, body_preview: str) -> str:
    """Detect medical specialty from title and body preview.

    Args:
        title: Document title.
        body_preview: First ~500 chars of body text.

    Returns:
        Best matching specialty name, or "general" if no match.
    """
    text = (title + " " + body_preview).lower()
    scores: dict[str, int] = {}

    for specialty, keywords in SPECIALTY_KEYWORDS.items():
        count = sum(1 for kw in keywords if kw.lower() in text)
        if count > 0:
            scores[specialty] = count

    if not scores:
        return "general"

    return max(scores, key=scores.get)  # type: ignore[arg-type]
