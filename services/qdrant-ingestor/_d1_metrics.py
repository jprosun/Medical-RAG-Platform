import json
try:
    with open('d:/CODE/DATN/LLM-MedQA-Assistant/data/data_final/vmj_ojs_pilot.jsonl', 'r', encoding='utf-8') as f:
        records = [json.loads(line) for line in f]

    total = len(records)
    go = sum(1 for r in records if r['quality_status'] == 'go')
    hold = sum(1 for r in records if r['quality_status'] == 'hold')

    wt = 0
    rl = 0
    gs = 0
    for r in records:
        t = r['title'].strip()
        if len(t) < 10 or t.islower() or t.startswith('TÓM TẮT') or t.startswith('ABSTRACT'): wt += 1
        if 'tài liệu tham khảo' not in r['section_title'].lower():
            if sum(1 for x in ['[1]', '[2]', '[3]', '[4]'] if x in r['body'][:200]) >= 3: rl += 1
        if sum(1 for c in r['body'] if c.isalnum()) < 50 and len(r['body']) > 100: gs += 1

    with open('d:/CODE/DATN/LLM-MedQA-Assistant/services/qdrant-ingestor/d1_metrics_out.txt', 'w', encoding='utf-8') as out:
        out.write(f"Total: {total}\n")
        out.write(f"GO: {go/total*100:.1f}%\n")
        out.write(f"HOLD: {hold/total*100:.1f}%\n")
        out.write(f"Title Acc: {(total-wt)/total*100:.1f}%\n")
        out.write(f"Leak Rate: {rl/total*100:.1f}%\n")
        out.write(f"Purity: {(total-gs)/total*100:.1f}%\n")
except Exception as e:
    with open('d:/CODE/DATN/LLM-MedQA-Assistant/services/qdrant-ingestor/d1_metrics_out.txt', 'w', encoding='utf-8') as out:
        out.write(str(e))
