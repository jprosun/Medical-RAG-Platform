import os
import re
import json
import random
import hashlib
import unicodedata
from typing import Dict, Tuple, List

DATA_DIR = r"d:\CODE\DATN\LLM-MedQA-Assistant\data\data_final"
OUTPUT_DIR = r"d:\CODE\DATN\LLM-MedQA-Assistant\benchmark\synthetic_gold_pipeline\output"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "seed_contexts_vmj_v1.jsonl")

CONFIG = {
    "vmj_clean_candidates.jsonl": {"target": 120, "source_label": "VMJ"},
}

RANDOM_SEED = 42
MIN_BODY_LEN = 400
MIN_VALID_SENTENCES = 3
MIN_QUALITY_SCORE = 85
MAX_DIGIT_RATIO = 0.10
MAX_SPECIAL_RATIO = 0.10
MAX_BORDERLINE_RATIO = 0.15


def normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip()

def remove_diacritics(text: str) -> str:
    nfd = unicodedata.normalize('NFD', text)
    return "".join(c for c in nfd if unicodedata.category(c) != 'Mn')


def digit_ratio(text: str) -> float:
    if not text:
        return 1.0
    return sum(ch.isdigit() for ch in text) / max(1, len(text))


def special_ratio(text: str) -> float:
    """
    Tính tỷ lệ ký tự 'lạ' theo cách an toàn, không dùng regex phức tạp.
    Cho phép:
    - chữ cái
    - số
    - khoảng trắng
    - dấu câu cơ bản
    """
    if not text:
        return 1.0

    allowed_punct = set(".,;:!?()%/-+[]\"'–—")
    bad = 0
    for ch in text:
        if ch.isalpha() or ch.isdigit() or ch.isspace() or ch in allowed_punct:
            continue
        bad += 1
    return bad / max(1, len(text))


def alpha_ratio(text: str) -> float:
    if not text:
        return 0.0
    return sum(ch.isalpha() for ch in text) / max(1, len(text))


def narrative_sentence_count(body: str) -> int:
    sentences = re.split(r"[.!?]", body)
    valid = [s for s in sentences if len(s.strip().split()) > 5]
    return len(valid)


def table_heavy_score(body: str) -> int:
    lower = body.lower()
    patterns = ["bảng ", "biểu đồ ", "figure ", "table ", "n=", "%", "±"]
    return sum(lower.count(p) for p in patterns)


def make_chunk_id(doc_id: str, body: str) -> str:
    raw = f"{doc_id}::{body[:200]}"
    return hashlib.md5(raw.encode("utf-8")).hexdigest()[:16]


def is_numeric_or_symbolic_title(title: str) -> bool:
    t = title.strip()
    if not t:
        return True
    if alpha_ratio(t) < 0.25:
        return True
    if re.fullmatch(r"[\d\s\.\-_/\\()]+", t):
        return True
    if re.fullmatch(r"[.…•·\s]+", t):
        return True
    return False


def is_reference_like_title(title: str) -> bool:
    t = title.lower().strip()
    patterns = [
        r"^\d+\.\s*$",                       # 1. 2.
        r"^\d+\.\s*[a-z]*$",                 # 1. aaa
        r".*\bdoi\b.*",
        r".*\d+\(\d+\)\s*:\s*\d+.*",         # 12(3):123-129
        r".*\d{4};\s*\d+\s*:\s*\d+.*",       # 2011;56:433–50
        r".*\btr\.\s*\d+.*",
        r".*\bnhà xuất bản\b.*",
        r".*\bmed ultrason\b.*",
        r".*\bplast reconstr surg\b.*",
        r".*\bjournal\b.*",
        r".*mayo clin proc.*",
        r".*[a-z0-9]+\.\d{4}\.\d+.*",        # mayocpiqo.2021.01
        r"^\d+\.\s+[a-z]{1,4}\s+[a-z]{1,2}\.*", # 4. Gedde SJ
    ]
    return any(re.match(p, t) or re.search(p, t) for p in patterns)

def is_truncated_title(title: str) -> bool:
    t = title.lower().strip()
    prefixes = ["và ", "trong ", "của ", "với ", "tại "]
    for p in prefixes:
        if t.startswith(p):
            return True
    return False

def is_author_line(title: str) -> bool:
    t = title.strip()

    # Ví dụ: Nguyễn Văn A1, Trần Thị B2
    if re.search(r"[A-Za-zÀ-ỹ]+\s+[A-Za-zÀ-ỹ]+\s+[A-Za-zÀ-ỹ]+\d", t):
        return True

    # Nhiều tên + số
    if t.count(",") >= 1 and len(re.findall(r"\d", t)) >= 2 and len(t) < 140:
        return True

    return False


def is_equation_like_title(title: str) -> bool:
    t = title.lower().strip()
    if "=" in t:
        return True
    if re.search(r"\bod[a-z0-9\s]*=.*", t):
        return True
    if re.search(r"\bod[a-z0-9\s]*\+.*", t):
        return True
    return False


def has_reference_section(body: str) -> bool:
    b = body.lower()
    markers = ["tài liệu tham khảo", "references", "bibliography"]
    return any(m in b for m in markers)


def has_form_like_structure(title: str, body: str) -> bool:
    text = f"{title.lower()} {body.lower()}"
    patterns = [
        "phiếu",
        "biểu mẫu",
        "bộ công cụ",
        "checklist",
        "đúng/không đúng",
        "không biết",
        "hoàn thành phần",
        "stt câu hỏi",
        "câu hỏi trả lời",
        "thang đo",
        "thang điểm",
        "bảng điểm",
        "trắc nghiệm",
        "mini-cog",
        "mmse",
        "zung",
        "hachinski",
        "test ",
        "pcl-5",
        "bệnh án ra viện",
        "người bệnh chuyển khoa",
        "tổng kết bệnh án",
        "thông tin hành chính",
        "đánh giá hành vi",
    ]
    return any(p in text for p in patterns)


def has_admin_like_structure(title: str, body: str) -> bool:
    text = remove_diacritics(f"{title.lower()} {body.lower()[:700]}")
    patterns = [
        "cong hoa xa hoi chu nghia",
        "kt. bo truong",
        "kt.bo truong",
        "ban hanh kem",
        "quyet dinh so",
        "uy ban nhan dan",
        "chu bien",
        "chu tich hoi dong",
        "danh sach nhan cong van",
        "van ban > chi dao dieu hanh",
        "chuyen vien",
        "phong ke hoach tong hop",
        "ve nhap vien noi tru",
        "thong tin kham benh",
        "cong van",
        "co so kham, chua benh",
        "truong khoa",
        "giam doc",
    ]
    return any(p in text for p in patterns)


def has_conference_like_structure(title: str, body: str) -> bool:
    text = f"{title.lower()} {body.lower()[:300]}"
    patterns = [
        "hội nghị khoa học",
        "kỷ yếu",
        "kỷ yếu đặc biệt",
        "chuyên đề:",
        "hội thảo",
    ]
    return any(p in text for p in patterns)


def is_generic_heading(title: str) -> bool:
    t = title.lower().strip()

    generic_exact = {
        "dấu hiệu lâm sàng",
        "triệu chứng",
        "chẩn đoán",
        "điều trị",
        "theo dõi",
        "đại cương",
        "tổng quan",
        "b. thông tin khám bệnh",
    }

    generic_prefixes = [
        "nguyên tắc áp dụng",
        "nguyên tắc xây dựng",
        "các bước tiến hành",
        "những sai sót và xử trí",
        "tiêu chuẩn đánh giá",
        "nguyên nhân tử vong",
    ]

    if t in generic_exact:
        return True
    if any(t.startswith(p) for p in generic_prefixes):
        return True
    if re.match(r"^(\d+\.|(i|ii|iii|iv|v|vi|vii|viii)\.)\s", t):
        return True
    if re.match(r"^[a-z]\.\s", t):  # B. Thông tin khám bệnh
        return True
    return False


def has_clinical_signal(title: str, body: str) -> bool:
    text = f"{title.lower()} {body.lower()[:1200]}"
    patterns = [
        "bệnh ",
        "hội chứng",
        "viêm ",
        "ung thư",
        "suy ",
        "nhiễm ",
        "đái tháo đường",
        "cao huyết áp",
        "thận",
        "gan",
        "phổi",
        "tim",
        "chẩn đoán",
        "điều trị",
        "phác đồ",
        "triệu chứng",
        "chỉ định",
        "chống chỉ định",
        "lâm sàng",
        "cận lâm sàng",
        "thuốc",
    ]
    return any(p in text for p in patterns)


def analyze_chunk_quality(record: Dict, source_label: str) -> Tuple[str, str]:
    title = normalize_text(record.get("title_clean", record.get("title", "")))
    body = normalize_text(record.get("body_clean", record.get("body", "")))
    
    quality_score = record.get("quality_score", 100)

    # ---------- Hard rejects ----------
    if quality_score < MIN_QUALITY_SCORE:
        return "reject", "low_quality_score"

    if len(body) < MIN_BODY_LEN:
        return "reject", "too_short"

    if not title or len(title) < 15:
        return "reject", "weak_title_too_short"
        
    if is_truncated_title(title):
        return "reject", "truncated_title"

    if "\\" in title:
        return "reject", "encoding_garbage"

    if is_numeric_or_symbolic_title(title):
        return "reject", "numeric_or_symbolic_title"

    if is_reference_like_title(title):
        return "reject", "reference_like_title"

    if is_author_line(title):
        return "reject", "author_line_title"

    if is_equation_like_title(title):
        return "reject", "equation_lab_fragment"

    if has_reference_section(body):
        return "reject", "references"

    if has_admin_like_structure(title, body):
        return "reject", "administrative_cover"

    if has_conference_like_structure(title, body):
        return "reject", "conference_header"

    if has_form_like_structure(title, body):
        return "reject", "questionnaire_form"

    if is_generic_heading(title):
        return "reject", "generic_heading"

    # ---------- Source-specific ----------
    lower_body = body.lower()
    lower_title = title.lower()

    if source_label == "VMJ":
        if "doi:" in lower_body:
            return "reject", "bibliographic_fragment"
        if re.search(r"\d+\(\d+\)\s*:\s*\d+", lower_body):
            return "reject", "bibliographic_fragment"

    elif source_label == "BYT_KCB":
        if re.search(r"test\s*\d+\s*:", lower_title) or re.search(r"test\s*\d+\s*:", lower_body[:300]):
            return "reject", "non_medical_test"

        if not has_clinical_signal(title, body):
            return "reject", "lack_of_clinical_signal"

    # ---------- Text quality ----------
    if digit_ratio(body) > MAX_DIGIT_RATIO:
        return "reject", "high_digit_ratio"

    if special_ratio(body) > MAX_SPECIAL_RATIO:
        return "reject", "high_special_ratio"

    if narrative_sentence_count(body) < MIN_VALID_SENTENCES:
        return "reject", "lack_of_narrative"

    table_score = table_heavy_score(body)
    if table_score > 8 and len(body) < 1200:
        return "reject", "table_figure_heavy"

    # ---------- Borderline ----------
    if any(x in lower_title for x in ["ca lâm sàng", "nhân một trường hợp", "case report"]):
        return "borderline", "case_report"

    if table_score > 4 or digit_ratio(body) > 0.07:
        return "borderline", "slightly_heavy_data"

    return "high", "valid"


def sample_candidates(candidates: List[Dict], target_count: int) -> List[Dict]:
    high_candidates = [c for c in candidates if c["seed_quality"] == "high"]
    borderline_candidates = [c for c in candidates if c["seed_quality"] == "borderline"]

    max_borderline = int(target_count * MAX_BORDERLINE_RATIO)
    n_border = min(max_borderline, len(borderline_candidates))
    n_high = target_count - n_border

    if n_high > len(high_candidates):
        n_high = len(high_candidates)
        n_border = min(len(borderline_candidates), target_count - n_high)

    sampled = []
    if n_high > 0:
        sampled.extend(random.sample(high_candidates, n_high))
    if n_border > 0:
        sampled.extend(random.sample(borderline_candidates, n_border))
    return sampled


def main() -> None:
    random.seed(RANDOM_SEED)
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    all_selected_chunks = []

    for filename, cfg in CONFIG.items():
        filepath = os.path.join(OUTPUT_DIR, filename)
        target_count = cfg["target"]
        source_label = cfg["source_label"]

        if not os.path.exists(filepath):
            print(f"[!] Missing file: {filepath}")
            continue

        print(f"\n[*] Processing {filename} -> target={target_count} source={source_label}")

        candidates = []
        reject_stats = {}

        with open(filepath, "r", encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue
                try:
                    record = json.loads(line)
                    q_level, reason = analyze_chunk_quality(record, source_label)

                    if q_level in ("high", "borderline"):
                        record["seed_quality"] = q_level
                        record["quality_reason"] = reason
                        candidates.append(record)
                    else:
                        reject_stats[reason] = reject_stats.get(reason, 0) + 1
                except Exception:
                    reject_stats["json_or_parse_error"] = reject_stats.get("json_or_parse_error", 0) + 1

        print(" -> Reject stats:")
        for reason, count in sorted(reject_stats.items(), key=lambda x: x[1], reverse=True):
            print(f"    - {reason}: {count}")

        if len(candidates) < target_count:
            print(f"[!] Only {len(candidates)} valid candidates found for {source_label}, using all.")
            sampled = candidates
        else:
            sampled = sample_candidates(candidates, target_count)

        print(f" -> Selected {len(sampled)} chunks from {source_label}")

        # Chống trùng lặp Topic (<= 15 chunks/topic)
        topic_counts = {}
        topic_sampled = []
        for s in sampled:
            topic = s.get("topic", "unknown")
            if topic_counts.get(topic, 0) < 15:
                topic_counts[topic] = topic_counts.get(topic, 0) + 1
                topic_sampled.append(s)
            # Dù bị reject thì thay thế bằng borderline/high nếu thiếu (để bài toán đơn giản ta bỏ qua rườm rà)
            
        print(f" -> Selected {len(topic_sampled)} chunks after Topic Distribution Filtering (max 15/topic)")

        for s in topic_sampled:
            doc_id = str(s.get("doc_id", "unknown_doc"))
            body = normalize_text(s.get("body_clean", s.get("body", "")))
            chunk_id = str(s.get("chunk_id") or make_chunk_id(doc_id, body))

            clean_record = {
                "chunk_id": chunk_id,
                "doc_id": doc_id,
                "source": source_label,
                "doc_type": "case_report" if s.get("quality_reason") == "case_report" else s.get("doc_type", "unknown"),
                "seed_quality": s.get("seed_quality"),
                "quality_reason": s.get("quality_reason"),
                "topic": s.get("topic", s.get("specialty", "unknown")),
                "language": s.get("language", "vi"),
                "title": normalize_text(s.get("title_clean", s.get("title_raw", s.get("title", "")))),
                "context": body,
                "url": s.get("source_url", ""),
            }
            all_selected_chunks.append(clean_record)

    random.shuffle(all_selected_chunks)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as out:
        for row in all_selected_chunks:
            out.write(json.dumps(row, ensure_ascii=False) + "\n")

    print(f"\n[+] Saved {len(all_selected_chunks)} seed contexts to: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
