"""
Sprint 2 - Pha B: vmj_ojs Article Boundary Splitter
Chạy pre-processing trên 1,336 files của tạp chí Y học Việt Nam (vmj_ojs).
Sử dụng mỏ neo TÓM TẮT và look-back để cắt bài, xuất ra thư mục data_intermediate.
"""
import re, json, sys, io
from pathlib import Path
from typing import List, Dict

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

BASE_DIR = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(BASE_DIR))

from services.utils.data_paths import preferred_processed_dir, source_intermediate_dir  # noqa: E402

VMJ_DIR = preferred_processed_dir("vmj_ojs")
OUTPUT_DIR = source_intermediate_dir("vmj_ojs", "split_articles")
MANIFEST_FILE = BASE_DIR / "benchmark" / "reports" / "vmj_split_manifest.jsonl"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
MANIFEST_FILE.parent.mkdir(parents=True, exist_ok=True)

# Regex Patterns
_RE_YAML_BOUNDARY = re.compile(r"^---\s*$")
_RE_ANCHORS = re.compile(r"^\s*(TÓM TẮT|ABSTRACT|I\.\s*ĐẶT VẤN ĐỀ)", re.IGNORECASE)
_RE_JOURNAL_HEADER = re.compile(r'(TẠP CHÍ Y|vietnam medical journal|VIETNAM MEDICAL)', re.IGNORECASE)

# Author name must end in a number (1, 2, 1,2) to strictly filter false positives in look-back
_RE_AUTHOR_STRICT = re.compile(r"([A-ZÀÁÂÃÈÉÊÌÍÒÓÔÕÙÚĂĐĨŨƠƯĂẠẢẤẦẨẪẬẮẰẲẴẶẸẺẼỀỀỂẾỄỆỈỊỌỎỐỒỔỖỘỚỜỞỠỢỤỦỨỪỬỮỰỲỴÝỶỸa-zàáâãèéêìíòóôõùúăđĩũơưăạảấầẩẫậắằẳẵặẹẻẽềềểếễệỉịọỏốồổỗộớờởỡợụủứừửữựỳỵỷỹ]+\s+){2,}.*?\d+(,\d+)*", re.IGNORECASE)

# Reject lines representing subsections or noise when searching for Title
_RE_SECTION = re.compile(r"^\s*(KẾT QUẢ|BÀN LUẬN|KẾT LUẬN|TÀI LIỆU THAM KHẢO|REFERENCES)", re.IGNORECASE)


def looks_like_title(line: str) -> bool:
    """Check if line is predominantly uppercase, typical of VMJ titles."""
    alpha_chars = [c for c in line if c.isalpha()]
    if len(alpha_chars) < 15: 
        return False
    upper = sum(1 for c in alpha_chars if c.isupper())
    return (upper / len(alpha_chars)) >= 0.65


def clean_journal_noise(lines: List[str]) -> List[str]:
    """Strip repeating Tạp chí Y học headers from the article body."""
    cleaned = []
    for line in lines:
        if not _RE_JOURNAL_HEADER.search(line):
            cleaned.append(line)
    return cleaned


def parse_file(filepath: Path) -> dict:
    """Parse YAML frontmatter and body lines."""
    text = filepath.read_text(encoding='utf-8')
    lines = text.splitlines()
    
    yaml_metadata = {}
    body_lines = lines
    yaml_end_idx = 0
    
    # Parse existing YAML
    if lines and _RE_YAML_BOUNDARY.match(lines[0]):
        in_yaml = True
        for i in range(1, len(lines)):
            if _RE_YAML_BOUNDARY.match(lines[i]):
                yaml_end_idx = i
                in_yaml = False
                break
            linestr = lines[i].strip()
            if ":" in linestr:
                k, v = linestr.split(":", 1)
                yaml_metadata[k.strip()] = v.strip()
                
        if not in_yaml:
            body_lines = lines[yaml_end_idx + 1:]
            
    return {"metadata": yaml_metadata, "lines": body_lines}


def find_boundaries(lines: List[str]) -> List[Dict]:
    """Find valid article boundaries using the TÓM TẮT-first strategy."""
    boundaries = []
    
    for i, line in enumerate(lines):
        if _RE_ANCHORS.match(line):
            anchor_text = _RE_ANCHORS.match(line).group(1)
            is_tom_tat = "TÓM TẮT" in anchor_text.upper()
            
            look_back_start = max(0, i - 15)
            context_lines = lines[look_back_start:i]
            
            found_title_idx = -1
            found_author = ""
            found_title_text = ""
            score = 3 if is_tom_tat else 1  # TÓM TẮT gets premium score
            
            # Look backwards from the anchor
            title_lines_collected = []  # (real_idx, text) pairs
            for j in reversed(range(len(context_lines))):
                ctx_line = context_lines[j].strip()
                real_idx = look_back_start + j
                
                if not ctx_line or _RE_JOURNAL_HEADER.search(ctx_line):
                    # Blank or noise: if we already have title lines, stop collecting
                    if title_lines_collected:
                        break
                    continue
                    
                # Abort if we hit an English abstract block from previous article or section heading
                if "ABSTRACT" in ctx_line.upper() or _RE_SECTION.match(ctx_line):
                    break
                    
                # Identify strict author line (Vietnamese names + superscript numbers)
                if not found_author and len(ctx_line) > 10 and not looks_like_title(ctx_line) and _RE_AUTHOR_STRICT.search(ctx_line):
                    found_author = ctx_line
                    score += 2
                    continue
                    
                # Identify Title lines — collect ALL consecutive ALL-CAPS lines
                if looks_like_title(ctx_line):
                    title_lines_collected.append((real_idx, ctx_line))
                    continue
                
                # Non-title, non-author, non-blank line → stop if we have titles
                if title_lines_collected:
                    break
            
            # Build full title from collected lines (they are in reverse order)
            if title_lines_collected:
                title_lines_collected.reverse()  # chronological order
                found_title_idx = title_lines_collected[0][0]  # topmost line
                found_title_text = " ".join(t[1] for t in title_lines_collected)
                score += 2
            
            # Validate boundary
            # Accept if we have a strong signal (Score >= 5 means Anchor + Author or Anchor + Title)
            # Typically a true boundary scores 7 (TÓM TẮT + Author + Title)
            if score >= 5 and found_title_idx != -1:
                boundaries.append({
                    "start_idx": found_title_idx, # The article technically starts at its title
                    "anchor_idx": i,
                    "score": score,
                    "title": found_title_text,
                    "author": found_author
                })
                
    # Deduplicate overlapping boundaries (if multiple anchors triggered the same title block)
    unique_boundaries = []
    for b in boundaries:
        # Check if we already have a boundary with roughly the same start_idx (within a few lines)
        is_dup = any(abs(ub["start_idx"] - b["start_idx"]) < 10 for ub in unique_boundaries)
        if not is_dup:
            unique_boundaries.append(b)
            
    return unique_boundaries


def process_files():
    files = sorted(VMJ_DIR.glob("*.txt"))
    total_files = len(files)
    
    print(f"Starting Splitter on {total_files} VMJ files...")
    
    # Reset manifest
    MANIFEST_FILE.write_text("", encoding='utf-8')
    manifest_handle = open(MANIFEST_FILE, "a", encoding='utf-8')
    
    total_articles_created = 0
    stats = {"single_article_files": 0, "multi_article_files": 0, "no_boundary_files": 0}
    
    # Limit to first 300 files for an E2E test, or process all? We'll process ALL in batches.
    for f in files:
        parsed = parse_file(f)
        lines = parsed["lines"]
        meta = parsed["metadata"]
        
        boundaries = find_boundaries(lines)
        
        if not boundaries:
            # Fallback: Treat as a single article if no boundaries found
            stats["no_boundary_files"] += 1
            boundaries = [{"start_idx": 0}]
        elif len(boundaries) == 1:
            # Shift the single boundary to line 0 to capture everything before the title (if any)
            stats["single_article_files"] += 1
            boundaries[0]["start_idx"] = 0
        else:
            stats["multi_article_files"] += 1
            # Adjust the first boundary to capture any preamble (like title starting at line 2)
            if boundaries[0]["start_idx"] < 50:
                 boundaries[0]["start_idx"] = 0
                 
        # Ensure we always end at EOF
        boundaries.append({"start_idx": len(lines)})
        
        for idx in range(len(boundaries) - 1):
            art_num = idx + 1
            start = boundaries[idx]["start_idx"]
            end = boundaries[idx + 1]["start_idx"]
            
            chunk = lines[start:end]
            chunk = clean_journal_noise(chunk)
            
            # Rebuild YAML
            new_meta = meta.copy()
            new_meta["article_index"] = str(art_num)
            if "file_url" not in new_meta:
                 new_meta["file_url"] = f"vmj_ojs://{f.name}#art_{art_num}"
            
            # Construct output text
            out_lines = ["---"]
            for k, v in new_meta.items():
                out_lines.append(f"{k}: {v}")
            out_lines.append("---")
            out_lines.append("")
            out_lines.extend(chunk)
            
            # Save Article File
            art_filename = f"{f.stem}__art_{art_num:03d}.txt"
            art_path = OUTPUT_DIR / art_filename
            art_path.write_text("\n".join(out_lines), encoding='utf-8')
            
            total_articles_created += 1
            
            # Log to manifest
            log_entry = {
                "issue_file": f.name,
                "article_file": art_filename,
                "lines": len(chunk),
                "boundary_score": boundaries[idx].get("score", -1),
                "extracted_title": boundaries[idx].get("title", "")
            }
            manifest_handle.write(json.dumps(log_entry, ensure_ascii=False) + "\n")

    manifest_handle.close()
    
    print("\n--- Splitter Results ---")
    print(f"Total RAW files processed: {total_files}")
    print(f"Total ARTICLE files generated: {total_articles_created}")
    print(f"Stats:")
    print(f"  - Files containing exactly 1 article:    {stats['single_article_files']}")
    print(f"  - Files containing MULTIPLE articles:    {stats['multi_article_files']}")
    print(f"  - Files with NO boundaries (fallback 1): {stats['no_boundary_files']}")
    print(f"\nSaved all split files to: {OUTPUT_DIR}")
    print(f"Saved manifest to: {MANIFEST_FILE}")


if __name__ == "__main__":
    process_files()
