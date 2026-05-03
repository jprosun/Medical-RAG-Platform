import json
import sys
import unicodedata
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from services.utils.data_paths import preferred_dataset_records_path

INPUT_PATH = preferred_dataset_records_path("vmj_ojs_pilot")
OUTPUT_PATH = REPO_ROOT / "services" / "qdrant-ingestor" / "d1_metrics_out.txt"


def _fold_text(text: str) -> str:
    normalized = unicodedata.normalize("NFD", text or "")
    return "".join(ch for ch in normalized if unicodedata.category(ch) != "Mn").lower()

try:
    with open(INPUT_PATH, "r", encoding="utf-8") as f:
        records = [json.loads(line) for line in f]

    total = len(records)
    go = sum(1 for r in records if r["quality_status"] == "go")
    hold = sum(1 for r in records if r["quality_status"] == "hold")

    weird_titles = 0
    reference_leaks = 0
    garbage_sections = 0
    for r in records:
        title = r["title"].strip()
        folded_title = _fold_text(title)
        folded_section = _fold_text(r["section_title"])
        if len(title) < 10 or title.islower() or folded_title.startswith("tom tat") or title.startswith("ABSTRACT"):
            weird_titles += 1
        if "tai lieu tham khao" not in folded_section:
            if sum(1 for x in ["[1]", "[2]", "[3]", "[4]"] if x in r["body"][:200]) >= 3:
                reference_leaks += 1
        if sum(1 for c in r["body"] if c.isalnum()) < 50 and len(r["body"]) > 100:
            garbage_sections += 1

    with open(OUTPUT_PATH, "w", encoding="utf-8") as out:
        out.write(f"Input: {INPUT_PATH}\n")
        out.write(f"Total: {total}\n")
        out.write(f"GO: {go/total*100:.1f}%\n")
        out.write(f"HOLD: {hold/total*100:.1f}%\n")
        out.write(f"Title Acc: {(total-weird_titles)/total*100:.1f}%\n")
        out.write(f"Leak Rate: {reference_leaks/total*100:.1f}%\n")
        out.write(f"Purity: {(total-garbage_sections)/total*100:.1f}%\n")
except Exception as e:
    with open(OUTPUT_PATH, "w", encoding="utf-8") as out:
        out.write(str(e))
