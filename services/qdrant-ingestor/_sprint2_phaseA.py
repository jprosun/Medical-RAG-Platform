"""
Phase A: Validate the TÓM TẮT-first Boundary Detection Strategy
Reads a few small and large VMJ files, extracts candidate boundaries,
looks back for title/author, and prints the debug info.
"""
import re, json, sys, io
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

BASE_DIR = Path(r"d:\CODE\DATN\LLM-MedQA-Assistant")
VMJ_DIR = BASE_DIR / "rag-data" / "data_processed" / "vmj_ojs"

_RE_ANCHORS = re.compile(r"^\s*(TÓM TẮT|ABSTRACT|I\.\s*ĐẶT VẤN ĐỀ)", re.IGNORECASE)
_RE_JOURNAL_HEADER = re.compile(r'(TẠP CHÍ Y|vietnam medical journal|VIETNAM MEDICAL|Medical Journal)', re.IGNORECASE)

# Tên tác giả Việt Nam thường có số superscript ở cuối: Nguyễn Văn A1,2
_RE_AUTHOR = re.compile(r"([A-ZÀÁÂÃÈÉÊÌÍÒÓÔÕÙÚĂĐĨŨƠƯĂẠẢẤẦẨẪẬẮẰẲẴẶẸẺẼỀỀỂẾỄỆỈỊỌỎỐỒỔỖỘỚỜỞỠỢỤỦỨỪỬỮỰỲỴÝỶỸa-zàáâãèéêìíòóôõùúăđĩũơưăạảấầẩẫậắằẳẵặẹẻẽềềểếễệỉịọỏốồổỗộớờởỡợụủứừửữựỳỵỷỹ]+\s+){2,}[A-ZÀÁÂÃÈÉÊÌÍÒÓÔÕÙÚĂĐĨŨƠƯĂẠẢẤẦẨẪẬẮẰẲẴẶẸẺẼỀỀỂẾỄỆỈỊỌỎỐỒỔỖỘỚỜỞỠỢỤỦỨỪỬỮỰỲỴÝỶỸa-zàáâãèéêìíòóôõùúăđĩũơưăạảấầẩẫậắằẳẵặẹẻẽềềểếễệỉịọỏốồổỗộớờởỡợụủứừửữựỳỵỷỹ]+(\d+(,\d+)*)*", re.IGNORECASE)

def looks_like_title(line: str) -> bool:
    """Check if line is mostly uppercase (title)"""
    alpha_chars = [c for c in line if c.isalpha()]
    if len(alpha_chars) < 10: return False
    upper = sum(1 for c in alpha_chars if c.isupper())
    return (upper / len(alpha_chars)) > 0.65

def detect_boundaries(filepath: Path):
    text = filepath.read_text(encoding='utf-8')
    lines = text.splitlines()
    
    boundaries = []
    
    for i, line in enumerate(lines):
        if _RE_ANCHORS.match(line):
            anchor_type = _RE_ANCHORS.match(line).group(1)
            
            # Khởi tạo look-back block
            look_back_start = max(0, i - 15)
            context_lines = lines[look_back_start:i]
            
            # Cố gắng tìm Author/Title từ dưới lên (gần mỏ neo nhất)
            found_title = ""
            found_author = ""
            score = 3  # Trúng anchor TÓM TẮT/ABSTRACT được base 3đ
            
            # Quét look-back ngược từ sát anchor lên trên
            for j in reversed(range(len(context_lines))):
                ctx_line = context_lines[j].strip()
                if not ctx_line or _RE_JOURNAL_HEADER.search(ctx_line):
                    continue
                
                # Nếu chưa tìm thấy Author, mà dòng này giống Author
                if not found_author and len(ctx_line) > 10 and not looks_like_title(ctx_line) and _RE_AUTHOR.search(ctx_line):
                    # Check không chặn nhầm title tiếng anh
                    if "TÓM TẮT" not in ctx_line.upper():
                        found_author = ctx_line
                        score += 2
                        continue
                
                # Nếu đã có hoặc chưa có Author, tìm Title Block
                if looks_like_title(ctx_line):
                    found_title = ctx_line
                    score += 2
                    break # Chỉ lấy dòng title sát author nhất để debug tạm
                    
            boundaries.append({
                "anchor_line_idx": i,
                "anchor_text": line.strip()[:30],
                "score": score,
                "ext_author": found_author[:60],
                "ext_title": found_title[:80]
            })
            
    return boundaries

test_files = [
    VMJ_DIR / "11277_9844.txt",  # File nhỏ (2 bài)
    VMJ_DIR / "11278_9845.txt",  # File nhỏ (2 bài)
    VMJ_DIR / "10201_8922.txt",  # File lớn (43 bài)
]

for f in test_files:
    print(f"\n{'='*50}\nFILE: {f.name} ({f.stat().st_size//1024} KB)")
    bounds = detect_boundaries(f)
    for b in bounds:
        print(f" [Line {b['anchor_line_idx']:4d}] Score: {b['score']} | Anchor: {b['anchor_text']}")
        print(f"   + Title:  {b['ext_title']}")
        print(f"   + Author: {b['ext_author']}")
