import os
import re
import json
import hashlib
import sys
from pathlib import Path
from typing import Dict, List, Tuple

REPO_ROOT = Path(__file__).resolve().parents[2]
BASE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(REPO_ROOT))

from services.utils.data_paths import preferred_dataset_records_path

OUTPUT_DIR = BASE_DIR / "output"
OUTPUT_FILE = OUTPUT_DIR / "vmj_clean_candidates.jsonl"
INPUT_DATASET_IDS = ("vmj_ojs_release_v2", "vmj_ojs_v2")


def _resolve_input_file() -> Path:
    for dataset_id in INPUT_DATASET_IDS:
        candidate = preferred_dataset_records_path(dataset_id)
        if candidate.exists():
            return candidate
    return preferred_dataset_records_path(INPUT_DATASET_IDS[0])


INPUT_FILE = _resolve_input_file()

# Lịch sử các bài đã gặp để chống trùng lặp
seen_hashes = set()

def normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip()

def strip_author_and_meta(body: str) -> Tuple[str, List[str]]:
    flags = []
    og_len = len(body)
    
    # Cắt khối thông tin submission (Ngày nhận, ngày duyệt, email, chịu trách nhiệm chính)
    if re.search(r"Chịu trách nhiệm chính:|Ngày nhận bài:|Email:", body, re.IGNORECASE):
        flags.append("removed_submission_meta")
        body = re.sub(r"Chịu trách nhiệm chính:.*?(?:Ngày duyệt|Ngày chấp nhận) bài:?[\s\d/]+", "", body, flags=re.DOTALL | re.IGNORECASE)
        body = re.sub(r"Ngày nhận bài:.*?(?:Ngày duyệt|Ngày chấp nhận) bài:?[\s\d/]+", "", body, flags=re.DOTALL | re.IGNORECASE)
        body = re.sub(r"Email:[\w\.-]+@[\w\.-]+\.\w+", "", body, flags=re.IGNORECASE)
    
    # Cắt author blocks (VD: 1Bệnh viện ABC... 2Trường Đại học...)
    if re.search(r"^\d*[a-zA-ZÀ-Ỹà-ỹ\s,]+Bệnh viện.*?\d{4}\n*", body, re.MULTILINE):
        flags.append("removed_author_block")
        body = re.sub(r"^\d*[a-zA-ZÀ-Ỹà-ỹ\s,]+Bệnh viện.*?\d{4}\n*", "", body, flags=re.MULTILINE)
        
    # Cắt footer hội nghị
    if re.search(r"HỘI NGHỊ KHOA HỌC|HỘI THẢO|CHUYÊN ĐỀ", body, re.IGNORECASE):
        flags.append("removed_conference_footer")
        body = re.sub(r"HỘI NGHỊ KHOA HỌC THƯỜNG NIÊN.*?\d{4}", "", body, flags=re.IGNORECASE)
        body = re.sub(r"HỘI THẢO CHUYÊN ĐỀ.*?\d{4}", "", body, flags=re.IGNORECASE)
        body = re.sub(r"CHUYÊN ĐỀ: HỘI THẢO.*?\d{4}", "", body, flags=re.IGNORECASE)

    return body.strip(), flags

def strip_reference_tail(body: str) -> Tuple[str, List[str]]:
    flags = []
    lower_body = body.lower()
    
    markers = ["tài liệu tham khảo", "references\n", "bibliography\n"]
    for m in markers:
        idx = lower_body.rfind(m)
        # Chỉ cắt nếu đuôi tài liệu tham khảo nằm ở nửa cuối hoặc 500 ký tự cuối.
        if idx != -1 and idx > len(body) / 2:
            body = body[:idx].strip()
            flags.append(f"removed_reference_tail")
            break
            
    return body, flags

def repair_title(title: str, body: str) -> Tuple[str, str, List[str]]:
    flags = []
    t = normalize_text(title)
    
    # 1. Truncated Title check (Nếu cụt, cố gắng móc nối với đoạn đầu của body nếu khớp)
    lower_t = t.lower()
    prefixes = ["và ", "trong ", "của ", "với ", "tại "]
    for p in prefixes:
        if lower_t.startswith(p):
            # Từ bỏ nếu không thể cứu vãn dễ dàng
            return "REJECT_TRUNCATED", body, flags
            
    # 2. Reference Citation Title check
    patterns = [
        r"^\d+\.\s*$", r"^\d+\.\s*[a-z]*$", r".*\bdoi\b.*",
        r".*\d+\(\d+\)\s*:\s*\d+.*", r".*\d{4};\s*\d+\s*:\s*\d+.*",
        r".*mayo clin proc.*", r".*[a-z0-9]+\.\d{4}\.\d+.*",
        r"^\d+\.\s+[a-z]{1,4}\s+[a-z]{1,2}\.*"
    ]
    if any(re.match(p, lower_t) or re.search(p, lower_t) for p in patterns):
        return "REJECT_REFERENCE_TITLE", body, flags
        
    return t, body, flags

def hash_content(title_clean: str, body_clean: str) -> str:
    raw = f"{title_clean}::{body_clean[:150]}"
    return hashlib.md5(raw.encode("utf-8")).hexdigest()

def process_record(record: dict) -> dict:
    original_title = record.get("title", "")
    original_body = record.get("body", "")
    flags_list = []
    
    # 1. Title Repair
    title_clean, body, title_flags = repair_title(original_title, original_body)
    if title_clean.startswith("REJECT_"):
        return {"reject_reason": title_clean}
    flags_list.extend(title_flags)
    
    # 2. Body Meta & Tail Stripping
    body_clean, meta_flags = strip_author_and_meta(body)
    flags_list.extend(meta_flags)
    
    body_clean, ref_flags = strip_reference_tail(body_clean)
    flags_list.extend(ref_flags)
    
    body_clean = normalize_text(body_clean)
    
    # 3. Near-Duplicate Check
    doc_hash = hash_content(title_clean, body_clean)
    if doc_hash in seen_hashes:
        return {"reject_reason": "REJECT_NEAR_DUPLICATE"}
    seen_hashes.add(doc_hash)
    
    return {
        "doc_id": record.get("doc_id", ""),
        "title_raw": normalize_text(original_title),
        "title_clean": title_clean,
        "body_clean": body_clean,
        "language": record.get("language", "vi"),
        "doc_type": record.get("doc_type", "unknown"),
        "topic": record.get("specialty", "unknown"),
        "source_url": record.get("source_url", ""),
        "repair_flags": flags_list,
        "quality_score": record.get("quality_score", 100)
    }

def main():
    if not os.path.exists(INPUT_FILE):
        print(f"[!] Input file missing: {INPUT_FILE}")
        return
        
    print(f"[*] Starting VMJ Repair on: {INPUT_FILE}")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    clean_candidates = []
    reject_stats = {}
    
    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip(): continue
            try:
                record = json.loads(line)
                processed = process_record(record)
                
                if "reject_reason" in processed:
                    reason = processed["reject_reason"]
                    reject_stats[reason] = reject_stats.get(reason, 0) + 1
                else:
                    # Kiểm định sương sương: chỉ lưu nếu còn đủ chữ sau khi chặt chém
                    if len(processed["body_clean"]) > 400:
                        clean_candidates.append(processed)
                    else:
                        reject_stats["REJECT_TOO_SHORT_AFTER_REPAIR"] = reject_stats.get("REJECT_TOO_SHORT_AFTER_REPAIR", 0) + 1
            except Exception:
                continue

    # Xuất file kết quả
    with open(OUTPUT_FILE, "w", encoding="utf-8") as out:
        for cand in clean_candidates:
            out.write(json.dumps(cand, ensure_ascii=False) + "\n")
            
    print("\n--- VMJ Repair Stats ---")
    print(f"Processed chunks: {len(clean_candidates) + sum(reject_stats.values())}")
    print(f"Clean Candidates Saved: {len(clean_candidates)}")
    for reason, count in sorted(reject_stats.items(), key=lambda x: x[1], reverse=True):
        print(f"  - {reason}: {count}")
        
    print(f"\n[+] Saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
