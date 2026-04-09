"""
Sprint 2 - Pha C: Boundary Quality Audits for VMJ OJS
This script automates the statistical and structural checks defined in review.md
before proceeding to full pipeline integration.
"""
import sys, io, json, random, re
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

BASE_DIR = Path(__file__).resolve().parents[2]
VMJ_DIR = BASE_DIR / "rag-data" / "data_processed" / "vmj_ojs"
SPLIT_DIR = BASE_DIR / "rag-data" / "data_intermediate" / "vmj_ojs_split_articles"
MANIFEST_FILE = BASE_DIR / "benchmark" / "reports" / "vmj_split_manifest.jsonl"

def perform_audits():
    if not MANIFEST_FILE.exists():
        print("Manifest not found. Run Splitter first.")
        return

    manifest = []
    with open(MANIFEST_FILE, "r", encoding="utf-8") as f:
        for line in f:
            manifest.append(json.loads(line))
            
    print("="*60)
    print("PHASE C AUDIT REPORT")
    print("="*60)

    # 1. Article Length Distribution & Duplicate Check
    lengths = [m["lines"] for m in manifest]
    lengths.sort()
    
    total = len(lengths)
    p10 = lengths[int(total * 0.1)]
    p50 = lengths[int(total * 0.5)]
    p90 = lengths[int(total * 0.9)]
    short_art = sum(1 for m in lengths if m < 80)
    long_art = sum(1 for m in lengths if m > 1500)
    
    print("\n[1] ARTICLE LENGTH DISTRIBUTION")
    print(f"Total articles: {total}")
    print(f"P10: {p10} lines | Median (P50): {p50} lines | P90: {p90} lines")
    print(f"Very short (<80 lines): {short_art} articles ({short_art/total*100:.1f}%)")
    print(f"Very long (>1500 lines): {long_art} articles ({long_art/total*100:.1f}%)")
    
    # 2. Metadata Inheritance Check
    print("\n[2] METADATA INHERITANCE TEST")
    sample_file = SPLIT_DIR / manifest[100]["article_file"]
    lines = sample_file.read_text(encoding='utf-8').splitlines()
    yaml_lines = [l for l in lines[:15] if l.startswith("source_id") or l.startswith("file_url") or l.startswith("article_index")]
    print(f"Checked {sample_file.name}. YAML extracted:")
    for y in yaml_lines:
        print(f"  {y}")
        
    # 3. Cross-Article Contamination Check (Tail of A vs Head of B)
    print("\n[3] CROSS-ARTICLE CONTAMINATION CHECK (Sample 3 pairs)")
    # Find 3 random issues that have multiple articles
    issue_counts = {}
    for m in manifest:
        issue_counts[m["issue_file"]] = issue_counts.get(m["issue_file"], 0) + 1
    
    multi_issues = [k for k,v in issue_counts.items() if v > 2]
    sampled_issues = random.sample(multi_issues, 3)
    
    for iss in sampled_issues:
        arts = [m for m in manifest if m["issue_file"] == iss]
        # Pick art 1 and art 2
        a1_path = SPLIT_DIR / arts[0]["article_file"]
        a2_path = SPLIT_DIR / arts[1]["article_file"]
        
        a1_lines = a1_path.read_text(encoding='utf-8').splitlines()
        a2_lines = a2_path.read_text(encoding='utf-8').splitlines()
        
        print(f"\n--- PAIR from {iss} ---")
        print(f"TAIL of Article A ({arts[0]['article_file']}):")
        for l in a1_lines[-5:]:
            if l.strip(): print(f"   {l.strip()}")
            
        print(f"HEAD of Article B ({arts[1]['article_file']}):")
        for l in a2_lines[10:15]: # Skip yaml
            if l.strip(): print(f"   {l.strip()}")

    # 4. No-Boundary Bucket Check
    print("\n[4] NO-BOUNDARY BUCKET AUDIT")
    no_bounds = [k for k,v in issue_counts.items() if v == 1]
    
    # Read the raw files of no_bounds to see if they actually contain "TÓM TẮT"
    false_no_bounds = 0
    _re_tomtat = re.compile(r"T[OÓ]M\s*T[AẮ]T", re.IGNORECASE)
    
    for nb in no_bounds:
        text = (VMJ_DIR / nb).read_text(encoding='utf-8', errors='ignore')
        if _re_tomtat.search(text):
            false_no_bounds += 1
    
    print(f"Total 'single-article' or 'no-boundary' files: {len(no_bounds)}")
    print(f"Files containing 'TÓM TẮT' but missed by splitter: {false_no_bounds} out of {len(no_bounds)}")
    if false_no_bounds > 0:
        print("  -> Could be missing primary anchor due to typo or formatting. Acceptable if < 10%.")

    print("\n=======================================================")
    print("AUDIT COMPLETE. Ready for human review in the chat.")

if __name__ == "__main__":
    random.seed(42) # Deterministic for report
    perform_audits()
