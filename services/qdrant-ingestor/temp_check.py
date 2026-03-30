import json
from pathlib import Path

BASE_DIR = Path('d:/CODE/DATN/LLM-MedQA-Assistant')
MANIFEST = BASE_DIR / 'benchmark' / 'reports' / 'vmj_split_manifest.jsonl'
SPLIT_DIR = BASE_DIR / 'rag-data' / 'data_intermediate' / 'vmj_ojs_split_articles'

manifest = []
with open(MANIFEST, 'r', encoding='utf-8') as f:
    for line in f:
        manifest.append(json.loads(line))

issue_to_arts = {}
for m in manifest:
    issue_to_arts.setdefault(m['issue_file'], []).append(m)

num_1_art = sum(1 for k,v in issue_to_arts.items() if len(v) == 1)
print(f"Issues with exactly 1 article: {num_1_art}")

short_arts = [m for m in manifest if m['lines'] < 80]
print(f"Short articles (<80 lines): {len(short_arts)}")
print("Sample shorts:")
for i, sa in enumerate(short_arts[:5]):
    content = (SPLIT_DIR / sa['article_file']).read_text(encoding='utf-8')
    lines = [l for l in content.splitlines() if not l.startswith('source_') and not l.startswith('file_') and not l.startswith('issue_') and not l.startswith('article_') and not l.startswith('split_')]
    print(f"\n--- {sa['article_file']} ({sa['lines']} lines) ---")
    print("\n".join(lines[:10]))
    print("...")
    print("\n".join(lines[-5:]))
