import sys, io, json, os, random, shutil, subprocess
from pathlib import Path

# Fix encoding
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

BASE_DIR = Path('d:/CODE/DATN/LLM-MedQA-Assistant')
MANIFEST_FILE = BASE_DIR / 'benchmark' / 'reports' / 'vmj_split_manifest.jsonl'
SRC_DIR = BASE_DIR / 'rag-data' / 'data_intermediate' / 'vmj_ojs_split_articles'
PILOT_DIR = BASE_DIR / 'rag-data' / 'data_intermediate' / 'vmj_ojs_d1_pilot'
JSONL_OUT = BASE_DIR / 'data' / 'data_final' / 'vmj_ojs_pilot.jsonl'

def select_pilot_files():
    if PILOT_DIR.exists():
        shutil.rmtree(PILOT_DIR)
    PILOT_DIR.mkdir(parents=True, exist_ok=True)
    
    manifest = []
    with open(MANIFEST_FILE, 'r', encoding='utf-8') as f:
        for line in f:
            manifest.append(json.loads(line))
            
    issue_to_arts = {}
    for m in manifest:
        issue_to_arts.setdefault(m['issue_file'], []).append(m)
        
    random.seed(42) # Deterministic
    sampled_issues = random.sample(list(issue_to_arts.keys()), min(30, len(issue_to_arts)))
    
    copied = 0
    for iss in sampled_issues:
        for m in issue_to_arts[iss]:
            src = SRC_DIR / m['article_file']
            if src.exists():
                shutil.copy2(src, PILOT_DIR / m['article_file'])
                copied += 1
    
    print(f"Sampled {len(sampled_issues)} issues -> Copied {copied} article files to {PILOT_DIR.name}")
    return copied

def run_etl():
    print("Running ETL pipeline on pilot...")
    cmd = [
        "python", "-m", "etl.vn.vn_txt_to_jsonl",
        "--source-dir", str(PILOT_DIR),
        "--output", str(JSONL_OUT),
        "--source-id", "vmj_ojs"
    ]
    env = os.environ.copy()
    env["PYTHONUTF8"] = "1"
    env["PYTHONPATH"] = str(BASE_DIR / 'services' / 'qdrant-ingestor')
    
    subprocess.run(cmd, env=env, cwd=str(BASE_DIR / 'services' / 'qdrant-ingestor'), check=True)

def evaluate_metrics():
    print("\nEvaluating D1 Metrics...")
    records = []
    with open(JSONL_OUT, 'r', encoding='utf-8') as f:
        for line in f:
            records.append(json.loads(line))
            
    total_records = len(records)
    if total_records == 0:
        print("No output records found!")
        return
        
    status_counts = {"go": 0, "review": 0, "hold": 0}
    for r in records:
        status_counts[r['quality_status']] += 1
        
    go_pct = status_counts["go"] / total_records * 100
    hold_pct = status_counts["hold"] / total_records * 100
    
    # Heuristics for Title precision / Reference leak
    num_weird_titles = 0
    num_ref_leaks = 0
    num_garbage_sections = 0
    for r in records:
        title = r['title']
        sec = r['section_title']
        body = r['body']
        
        # Weird title: Very short, lowercased entirely, or looks like random text
        ts = title.strip()
        if len(ts) < 10 or ts.islower() or ts.startswith('TÓM TẮT') or ts.startswith('ABSTRACT'):
            num_weird_titles += 1
            
        # Ref leak: section_title isn't reference, but contains heavy reference markers like "[1]", "[2]" combined with "TÀI LIỆU"
        if "tài liệu tham khảo" not in sec.lower():
            refs = sum(1 for x in ["[1]", "[2]", "[3]", "[4]"] if x in body[:200])
            if refs >= 3:
                num_ref_leaks += 1
                
        # Garbage sections: Sections containing extremely few alphanumeric chars
        alpha_chars = sum(c.isalnum() for c in body)
        if alpha_chars < 50 and len(body) > 100:
            num_garbage_sections += 1

    title_acc = ((total_records - num_weird_titles) / total_records) * 100
    ref_leak_rt = (num_ref_leaks / total_records) * 100
    section_purity = ((total_records - num_garbage_sections) / total_records) * 100

    print("="*40)
    print("D1 PILOT GATE METRICS")
    print("="*40)
    print(f"Total chunks generated: {total_records}")
    print(f"Status GO:   {go_pct:.1f}%  (Gate: >= 50%)")
    print(f"Status HOLD: {hold_pct:.1f}%  (Gate: <= 10%)")
    print(f"Title Accuracy (Heuristic): {title_acc:.1f}%  (Gate: >= 90%)")
    print(f"Reference Leak Rate:        {ref_leak_rt:.1f}%   (Gate: <= 5%)")
    print(f"Section Purity Rate:        {section_purity:.1f}%  (Gate: >= 75%)")
    
    print("\nSAMPLE TITLES EXTRACTED:")
    for r in random.sample(records, min(5, total_records)):
        print(f" - {r['title'][:100]}...")

if __name__ == "__main__":
    select_pilot_files()
    run_etl()
    evaluate_metrics()
