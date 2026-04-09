"""
VMJ Title Patcher v1
=====================
Post-process vmj_ojs.jsonl to fix corrupted/truncated titles.
Does NOT re-run ETL. Only patches the 'title' field.

Safety requirements:
  1. Keeps title_original in every record
  2. Adds _patch metadata (action, reason, method)
  3. Never uses "UNKNOWN" — unfixable titles keep original + flag
  4. Outputs patch_report.json for human review BEFORE re-embed

Usage:
  python title_patcher.py
"""
import json, re, os, sys, unicodedata
from collections import Counter
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[4]
INPUT = _ROOT / "data" / "data_final" / "vmj_ojs.jsonl"
OUTPUT = _ROOT / "data" / "data_final" / "vmj_ojs_patched.jsonl"
REPORT = _ROOT / "benchmark" / "reports" / "vmj" / "title_patch_report.json"
REPORT_MD = _ROOT / "benchmark" / "reports" / "vmj" / "title_patch_report.md"

# ── Detection patterns ──────────────────────────────────────────────

# Author line: Vietnamese names + superscript numbers
_RE_AUTHOR = re.compile(
    r'^[A-ZÀ-Ỹa-zà-ỹ\s.,*]+\d+[*,]?\s*$'
    r'|^[A-ZÀ-Ỹa-zà-ỹ\s]+\d+(,\s*\d+)*\s*$'
)

# Reference/bibliography line
_RE_REFERENCE = re.compile(
    r'^\d+\.\s+[A-ZÀ-Ỹa-zà-ỹ].*(?:et al|pp?\.\s*\d|\(\d{4}\))'
    r'|Luận văn|Nhà xuất bản|doi:\s*10\.|ISSN\s+\d'
    r'|PMID:\s*\d+|https?://'
    r'|^\d+\.\s+Bộ Y tế'
    r'|Dec;\d|Jan;\d|Feb;\d|Mar;\d|Apr;\d|May;\d|Jun;\d'
    r'|Jul;\d|Aug;\d|Sep;\d|Oct;\d|Nov;\d'
)

# Body text (method/result sections)
_RE_BODY_TEXT = re.compile(
    r'^Đối tượng và phương pháp'
    r'|^Nghiên cứu mô tả'
    r'|^Nghiên cứu cắt ngang'
    r'|^Phương pháp:'
    r'|^Mục tiêu:'
    r'|^Kết quả:'
    r'|^Đặt vấn đề:'
    r'|^Mở đầu:'
    r'|^Tóm tắt'
    r'|^vào ngày thứ \d'
    r'|^kết quả nghiên cứu'
    r'|^bám da mặt \['
    r'|^trong viêm tai giữa'
    r'|^ngưỡng cắt,'
, re.IGNORECASE)

# Fragment titles (starts with location/date, too generic)
_FRAGMENT_STARTS = [
    'TẠI BỆNH VIỆN', 'TẠI TRUNG TÂM', 'TẠI KHOA', 'TẠI CÁC',
    'TỪ THÁNG', 'TỪ NĂM', 'GIAI ĐOẠN', 'NĂM 20',
    'CÓ ', 'ĐƯỢC ', 'VÀ MỘT SỐ', 'VÀ CÁC YẾU TỐ',
    'NHÂN CÁC TRƯỜNG HỢP', 'NHÂN MỘT TRƯỜNG HỢP',
    'THÀNH PHỐ HỒ CHÍ MINH', 'BỆNH VIỆN',
]

# Bad chars in title
_RE_BAD_CHARS = re.compile(r'[\[\]\\]')

# ALL CAPS Vietnamese line (potential title)
_RE_ALLCAPS_VN = re.compile(r'^[A-ZÀ-ỸĐ\s\d,.:;\-–—()/"]+$')


def _normalize_title(title: str) -> str:
    """Normalize a title string."""
    # Unicode NFC normalize
    title = unicodedata.normalize('NFC', title)
    # Remove bad chars
    title = _RE_BAD_CHARS.sub('', title)
    # Remove trailing superscript numbers
    title = re.sub(r'\d{1,3}$', '', title)
    # Collapse whitespace
    title = re.sub(r'\s+', ' ', title).strip()
    return title


def _classify_bad_title(title: str) -> str | None:
    """Classify why a title is bad. Returns reason or None if OK."""
    t = title.strip()
    
    if not t:
        return "empty"
    
    if len(t) < 20:
        return "too_short"
    
    if _RE_BAD_CHARS.search(t):
        return "bad_chars"
    
    if _RE_AUTHOR.match(t):
        return "author_line"
    
    if _RE_REFERENCE.search(t):
        return "reference_line"
    
    if _RE_BODY_TEXT.match(t):
        return "body_text"
    
    # Fragment check
    t_upper = t.upper().strip()
    for frag in _FRAGMENT_STARTS:
        if t_upper.startswith(frag) and len(t) < 50:
            return "fragment_short"
    
    # All lowercase (unusual for VMJ titles)
    alpha = [c for c in t if c.isalpha()]
    if alpha and sum(1 for c in alpha if c.islower()) / len(alpha) > 0.9 and len(t) < 60:
        return "lowercase_suspect"
    
    return None  # Title is OK


def _extract_title_from_body(body: str) -> str | None:
    """Try to extract a proper FULL title from the body text.
    
    Key insight: the body typically starts with the fragment title,
    followed by author names, then TÓM TẮT. But the fragment IS the
    last part of a multi-line title. We need to find TÓM TẮT and look
    backwards to reconstruct the full title block.
    
    Strategy:
    1. Find TÓM TẮT/ABSTRACT anchor
    2. Look backwards: collect ALL CAPS lines = title block
    3. Skip author lines between title and TÓM TẮT
    4. If no anchor found, look for first ALL CAPS block
    """
    lines = body.splitlines()
    
    # Skip noise patterns
    _skip = re.compile(
        r'^(TẠP CHÍ|VIETNAM MEDICAL|SỐ \d|ISSN|DOI:|BÀI NGHIÊN CỨU|$)',
        re.IGNORECASE
    )
    _section = re.compile(
        r'^(TÓM TẮT|ABSTRACT|SUMMARY)',
        re.IGNORECASE
    )
    
    # Strategy 1: Find TÓM TẮT and look backwards
    anchor_idx = -1
    for i, line in enumerate(lines[:80]):
        stripped = line.strip()
        if stripped and re.match(r'^TÓM TẮT\d*\s*$', stripped, re.IGNORECASE):
            anchor_idx = i
            break
    
    if anchor_idx > 0:
        # Look backwards from anchor, collecting title parts
        title_parts = []
        for j in reversed(range(0, anchor_idx)):
            stripped = lines[j].strip()
            if not stripped:
                if title_parts:
                    break  # blank line = end of title block
                continue
            
            # Skip author lines
            if _RE_AUTHOR.match(stripped):
                continue
            if re.match(r'^[*]\s*Tác giả|^Chịu trách nhiệm|^Email:', stripped):
                continue
            # Skip affiliation lines (start with number + institution)
            if re.match(r'^\d+\s*(Trường|Bệnh viện|Khoa|Viện|Đại học|BV)', stripped):
                continue
            if _skip.match(stripped):
                continue
            
            # ALL CAPS line >= 15 chars = title part
            alpha = [c for c in stripped if c.isalpha()]
            if alpha and len(alpha) >= 10:
                upper_ratio = sum(1 for c in alpha if c.isupper()) / len(alpha)
                if upper_ratio >= 0.6:
                    title_parts.insert(0, stripped)
                    continue
            
            # If we hit a non-title, non-author line, stop
            if title_parts:
                break
        
        if title_parts:
            full_title = ' '.join(title_parts)
            full_title = _normalize_title(full_title)
            if len(full_title) >= 30:
                return full_title
    
    # Strategy 2: No anchor found — collect first ALL CAPS block
    title_parts = []
    for i, line in enumerate(lines[:40]):
        stripped = line.strip()
        if not stripped:
            if title_parts:
                break
            continue
        if _skip.match(stripped):
            continue
        
        alpha = [c for c in stripped if c.isalpha()]
        if alpha and len(alpha) >= 10:
            upper_ratio = sum(1 for c in alpha if c.isupper()) / len(alpha)
            if upper_ratio >= 0.6 and not _RE_REFERENCE.search(stripped):
                title_parts.append(stripped)
                continue
        
        # Non-title line
        if title_parts:
            break
    
    if title_parts:
        full_title = ' '.join(title_parts)
        full_title = _normalize_title(full_title)
        if len(full_title) >= 30:
            return full_title
    
    return None


def _extract_title_from_section_title(section_title: str) -> str | None:
    """Use section_title as fallback if it's better than current title."""
    if not section_title:
        return None
    st = section_title.strip()
    if len(st) >= 30 and _classify_bad_title(st) is None:
        return st
    return None


def main():
    print("=" * 60)
    print("  VMJ Title Patcher")
    print("=" * 60)
    
    # Load records
    records = []
    with open(INPUT, 'r', encoding='utf-8') as f:
        for line in f:
            if line.strip():
                records.append(json.loads(line))
    print(f"Loaded {len(records)} records from {INPUT.name}")
    
    # Phase 1: Classify all titles
    stats = Counter()
    actions = Counter()
    patched = 0
    unfixable = 0
    samples = {"patched": [], "unfixable": [], "normalized_only": []}
    
    for rec in records:
        original_title = rec['title']
        rec['title_original'] = original_title  # Always keep original
        
        reason = _classify_bad_title(original_title)
        
        if reason is None:
            # Title looks OK, just normalize
            normalized = _normalize_title(original_title)
            if normalized != original_title:
                rec['title'] = normalized
                rec['_patch'] = {
                    "action": "normalized",
                    "reason": "cleanup_only",
                    "method": "normalize",
                }
                actions["normalized"] += 1
                if len(samples["normalized_only"]) < 5:
                    samples["normalized_only"].append({
                        "before": original_title[:60],
                        "after": normalized[:60],
                    })
            else:
                rec['_patch'] = {"action": "kept", "reason": "ok"}
                actions["kept"] += 1
            stats["ok"] += 1
            continue
        
        stats[reason] += 1
        
        # Phase 2: Try to extract better title
        new_title = _extract_title_from_body(rec.get('body', ''))
        method = "body_extraction"
        
        if new_title is None:
            new_title = _extract_title_from_section_title(rec.get('section_title', ''))
            method = "section_title_fallback"
        
        if new_title and len(new_title) >= 20:
            new_title = _normalize_title(new_title)
            # Verify new title is actually better
            new_reason = _classify_bad_title(new_title)
            if new_reason is None and len(new_title) > len(original_title):
                rec['title'] = new_title
                rec['_patch'] = {
                    "action": "patched",
                    "reason": reason,
                    "method": method,
                }
                patched += 1
                actions["patched"] += 1
                if len(samples["patched"]) < 15:
                    samples["patched"].append({
                        "doc_id": rec['doc_id'],
                        "reason": reason,
                        "before": original_title[:80],
                        "after": new_title[:80],
                        "method": method,
                    })
                continue
        
        # Unfixable: keep original, flag it
        rec['title'] = _normalize_title(original_title)
        rec['_patch'] = {
            "action": "flagged_unfixable",
            "reason": reason,
            "method": "kept_original_normalized",
        }
        unfixable += 1
        actions["flagged_unfixable"] += 1
        if len(samples["unfixable"]) < 10:
            samples["unfixable"].append({
                "doc_id": rec['doc_id'],
                "reason": reason,
                "title": original_title[:80],
            })
    
    # Phase 3: Check dependencies on title
    # - doc_id: based on source_id + filename + section_idx → NOT dependent on title ✓
    # - chunk ID: based on source_name + doc_id + section_slug → NOT dependent on title ✓  
    # - metadata.title in Qdrant payload → WILL be updated ✓
    # - chunk text header "Title: ..." → WILL change → embeddings MUST be regenerated ✓
    # - gold set expected_title → MUST be checked after patch ✓
    
    # Write patched JSONL
    with open(OUTPUT, 'w', encoding='utf-8') as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False) + '\n')
    
    # Write report
    report = {
        "input": str(INPUT),
        "output": str(OUTPUT),
        "total_records": len(records),
        "title_classification": dict(stats),
        "actions": dict(actions),
        "patched_count": patched,
        "unfixable_count": unfixable,
        "samples": samples,
        "dependency_check": {
            "doc_id": "NOT dependent on title (hash of source+filename+section_idx)",
            "chunk_id": "NOT dependent on title (hash of source_name+doc_id+section_slug)",
            "qdrant_payload_title": "WILL be updated after re-ingest",
            "chunk_text_header": "WILL change (Title: ... prepended) → RE-EMBED REQUIRED",
            "gold_set_expected_title": "MUST verify after patch — some expected_titles may need updating",
        },
    }
    
    REPORT.parent.mkdir(parents=True, exist_ok=True)
    with open(REPORT, 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    
    # Write MD report
    with open(REPORT_MD, 'w', encoding='utf-8') as f:
        f.write("# VMJ Title Patch Report\n\n")
        f.write("## Summary\n\n")
        f.write(f"| Metric | Value |\n")
        f.write(f"|--------|-------|\n")
        f.write(f"| Total records | {len(records)} |\n")
        f.write(f"| Titles OK (kept/normalized) | {stats['ok']} ({stats['ok']/len(records)*100:.1f}%) |\n")
        f.write(f"| **Titles patched** | **{patched}** ({patched/len(records)*100:.1f}%) |\n")
        f.write(f"| Titles unfixable (flagged) | {unfixable} ({unfixable/len(records)*100:.1f}%) |\n\n")
        
        f.write("## Bad Title Classification\n\n")
        f.write("| Reason | Count |\n")
        f.write("|--------|-------|\n")
        for reason, count in stats.most_common():
            f.write(f"| {reason} | {count} |\n")
        
        f.write("\n## Actions Taken\n\n")
        f.write("| Action | Count |\n")
        f.write("|--------|-------|\n")
        for action, count in actions.most_common():
            f.write(f"| {action} | {count} |\n")
        
        f.write("\n## Sample Patches (review these!)\n\n")
        for s in samples["patched"][:15]:
            f.write(f"- **[{s['reason']}]** `{s['before']}` → `{s['after']}` ({s['method']})\n")
        
        if samples["unfixable"]:
            f.write(f"\n## Unfixable Titles (flagged, kept original)\n\n")
            for s in samples["unfixable"]:
                f.write(f"- **[{s['reason']}]** `{s['title']}`\n")
        
        f.write(f"\n## Dependency Check\n\n")
        f.write(f"| Component | Impact |\n")
        f.write(f"|-----------|--------|\n")
        f.write(f"| doc_id | ✅ NOT dependent on title |\n")
        f.write(f"| chunk_id | ✅ NOT dependent on title |\n")
        f.write(f"| Qdrant payload.title | ⚠️ Will update after re-ingest |\n")
        f.write(f"| Chunk text (has Title: header) | ⚠️ Will change → RE-EMBED required |\n")
        f.write(f"| Gold set expected_title | ⚠️ Must verify — some may need updating |\n")
    
    # Print summary
    print(f"\n  Classification:")
    for reason, count in stats.most_common():
        print(f"    {reason:25s}: {count}")
    print(f"\n  Actions:")
    for action, count in actions.most_common():
        print(f"    {action:25s}: {count}")
    print(f"\n  Patched: {patched} | Unfixable: {unfixable}")
    print(f"\n  Output:  {OUTPUT}")
    print(f"  Report:  {REPORT_MD}")
    print(f"\n  ⚠️  REVIEW the report before re-embedding!")


if __name__ == "__main__":
    main()
