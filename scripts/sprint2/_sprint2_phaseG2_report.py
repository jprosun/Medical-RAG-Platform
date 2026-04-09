import json
import statistics
from pathlib import Path

BASE_DIR = Path('d:/CODE/DATN/LLM-MedQA-Assistant')
INPUT_JSONL = BASE_DIR / 'data' / 'data_final' / 'vmj_ojs.jsonl'
REPORT_JSON = BASE_DIR / 'benchmark' / 'reports' / 'vmj_full_quality_report.json'
REPORT_MD = BASE_DIR / 'benchmark' / 'reports' / 'vmj_full_quality_summary.md'

def generate_report():
    print("Generating Phase 2 Quality Report...")
    
    total_chunks = 0
    status_counts = {"go": 0, "review": 0, "hold": 0}
    articles = {}  # doc_id -> list of chunks
    
    # Audit metrics
    valid_titles = 0
    ref_leaks = 0
    pure_sections = 0
    metadata_complete = 0
    
    seen_texts = set()
    duplicates = 0

    with open(INPUT_JSONL, 'r', encoding='utf-8') as f:
        for line in f:
            total_chunks += 1
            rec = json.loads(line)
            
            # Status
            st = rec.get("quality_status", "hold").lower()
            status_counts[st] = status_counts.get(st, 0) + 1
            
            # Article grouping
            title = rec.get("title", "").strip()
            group_key = title if title else rec.get("doc_id", "unknown")
            if group_key not in articles:
                articles[group_key] = []
            articles[group_key].append(rec)
            
            # Heuristics
            title = rec.get("title", "")
            if title and len(title) > 10 and "tài liệu" not in title.lower():
                valid_titles += 1
                
            text = rec.get("body", "")
            if text in seen_texts:
                duplicates += 1
            else:
                seen_texts.add(text)
                
            # Naive ref leak check
            if "tài liệu tham khảo" in text.lower()[:30] or "references" in text.lower()[:30]:
                ref_leaks += 1
                
            if rec.get("section_title") and rec.get("section_title") != title:
                pure_sections += 1
                
            if rec.get("source_url") and rec.get("title"):
                metadata_complete += 1
                
    # Distribution
    chunk_counts = [len(ch) for ch in articles.values()]
    chunk_counts.sort()
    
    articles_len = len(chunk_counts)
    if articles_len > 0:
        p10 = chunk_counts[int(articles_len * 0.1)]
        p50 = chunk_counts[int(articles_len * 0.5)]
        p90 = chunk_counts[int(articles_len * 0.9)]
        single_chunk_articles = sum(1 for c in chunk_counts if c == 1)
        too_many_chunks = sum(1 for c in chunk_counts if c > 30)
    else:
        p10 = p50 = p90 = single_chunk_articles = too_many_chunks = 0
        
    report = {
        "metrics": {
            "total_chunks": total_chunks,
            "total_articles": articles_len,
            "go_rate": status_counts["go"] / total_chunks,
            "hold_rate": status_counts["hold"] / total_chunks,
            "title_semantic_accuracy_estimate": valid_titles / total_chunks,
            "reference_leak_rate_estimate": ref_leaks / total_chunks,
            "section_purity_rate": pure_sections / total_chunks,
            "metadata_completeness": metadata_complete / total_chunks,
            "duplicate_suspect_rate": duplicates / total_chunks
        },
        "distribution": {
            "p10_chunks_per_article": p10,
            "p50_chunks_per_article": p50,
            "p90_chunks_per_article": p90,
            "single_chunk_articles_rate": single_chunk_articles / articles_len if articles_len else 0,
            "too_many_chunks_rate": too_many_chunks / articles_len if articles_len else 0
        }
    }
    
    # Save JSON
    with open(REPORT_JSON, 'w', encoding='utf-8') as f:
        json.dump(report, f, indent=4)
        
    # Build MD Dashboard using GitHub Alerts
    md = f"""# 📊 Báo cáo Chất lượng Giai đoạn 2 (Post-Ingest Quality Report)
    
## 1. Metrics Chất lượng (Gate G2)
| Metric | Giá trị thực tế | Threshold G2 | Đánh giá |
|--------|-----------------|--------------|----------|
| **Tỷ lệ GO** | {report['metrics']['go_rate']*100:.1f}% | ≥ 85% | {'✅ PASS' if report['metrics']['go_rate'] >= 0.85 else '❌ FAIL'} |
| **Tỷ lệ HOLD** | {report['metrics']['hold_rate']*100:.1f}% | ≤ 5% | {'✅ PASS' if report['metrics']['hold_rate'] <= 0.05 else '❌ FAIL'} |
| **Độ khớp Tiêu đề (Ước lượng)** | {report['metrics']['title_semantic_accuracy_estimate']*100:.1f}% | ≥ 95% | {'✅ PASS' if report['metrics']['title_semantic_accuracy_estimate'] >= 0.95 else '❌ FAIL'} |
| **Nhiễu Tham khảo (Ước lượng)** | {report['metrics']['reference_leak_rate_estimate']*100:.1f}% | ≤ 2% | {'✅ PASS' if report['metrics']['reference_leak_rate_estimate'] <= 0.02 else '❌ FAIL'} |
| **Thêm Section Headings** | {report['metrics']['section_purity_rate']*100:.1f}% | - | Thông tin tham khảo |
| **Trùng lặp Chunk (Duplicates)** | {report['metrics']['duplicate_suspect_rate']*100:.1f}% | ≤ 2% | {'✅ PASS' if report['metrics']['duplicate_suspect_rate'] <= 0.02 else '❌ FAIL'} |

## 2. Phân phối Chunk (Chunk Distribution)
- Tổng số bài: **{articles_len}**
- Trọng tâm phân phối: `P10={p10}` -> `Median={p50}` -> `P90={p90}` chunk / bài.
- Tỷ lệ bài báo chỉ có 1 Chunk: `{report['distribution']['single_chunk_articles_rate']*100:.1f}%`
- Tỷ lệ bài báo > 30 Chunks: `{report['distribution']['too_many_chunks_rate']*100:.1f}%`

> [!TIP]
> Các con số ước lượng Heuristic có thể không phản ánh đúng 100% ngữ nghĩa tự nhiên, nhưng giúp ta bắt được các lỗi hệ thống trầm trọng nếu có (ví dụ 1000 chunks trùng nhau, hoặc tất cả đều dính 'Tài liệu tham khảo' ở dòng đầu tiên).

"""
    with open(REPORT_MD, 'w', encoding='utf-8') as f:
        f.write(md)
    print("Done generating Phase 2 reports!")

if __name__ == "__main__":
    generate_report()
