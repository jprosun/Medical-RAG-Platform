import json

INPUT = "benchmark/synthetic_gold_pipeline/output/Pasted_text4_final65_edited.jsonl"

issues = []
seeds = []
with open(INPUT, "r", encoding="utf-8") as f:
    for i, line in enumerate(f, 1):
        line = line.strip()
        if not line:
            continue
        try:
            d = json.loads(line)
        except json.JSONDecodeError as e:
            issues.append(f"Line {i}: JSON parse error - {e}")
            continue
        seeds.append(d)
        
        # Check accepted field
        if "accepted" not in d:
            issues.append(f"Line {i}: missing accepted field")
        elif d["accepted"] is not True:
            val = d["accepted"]
            issues.append(f"Line {i}: accepted is not true ({val})")
        
        # Check required fields
        for key in ["chunk_id", "doc_id", "title", "context", "topic", "source"]:
            if key not in d or not d[key]:
                issues.append(f"Line {i}: missing or empty '{key}'")
        
        # Check context length
        ctx = d.get("context", "")
        if len(ctx) < 200:
            title_short = d.get("title", "")[:60]
            issues.append(f"Line {i}: context too short ({len(ctx)} chars) - {title_short}")
        
        # Check title appears to be pure English (potential mismatch)
        title = d.get("title", "")
        if title and len(title) > 20:
            if all(ord(c) < 128 or not c.isalpha() for c in title):
                issues.append(f"Line {i}: title appears fully ASCII/English - {title[:80]}")

# Topic diversity
topics = {}
for s in seeds:
    t = s.get("topic", "unknown")
    topics[t] = topics.get(t, 0) + 1

print(f"Total seeds: {len(seeds)}")
print(f"Unique topics: {len(topics)}")
print(f"Topic distribution:")
for t, c in sorted(topics.items(), key=lambda x: -x[1]):
    print(f"  {t}: {c}")
print(f"\nIssues found: {len(issues)}")
for iss in issues:
    print(f"  - {iss}")
if not issues:
    print("  NONE - File is clean!")
