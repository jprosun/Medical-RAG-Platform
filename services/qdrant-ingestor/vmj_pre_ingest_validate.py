import json, os, hashlib
from pathlib import Path

BASE_DIR = Path('d:/CODE/DATN/LLM-MedQA-Assistant')
INPUT_JSONL = BASE_DIR / 'data' / 'data_final' / 'vmj_ojs.jsonl'
REPORT_OUT = BASE_DIR / 'benchmark' / 'reports' / 'vmj_pre_ingest_validate.json'

def validate():
    print("Running Pre-Ingest Validator...")
    
    total_records = 0
    errors = []
    seen_ids = set()
    warnings = []
    
    with open(INPUT_JSONL, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, start=1):
            total_records += 1
            rec = json.loads(line)
            
            # Point ID / deterministic check
            point_id = rec.get("doc_id")
            if not point_id:
                errors.append(f"Line {line_num}: Missing 'doc_id'")
            else:
                if point_id in seen_ids:
                    errors.append(f"Line {line_num}: Duplicate 'doc_id': {point_id}")
                seen_ids.add(point_id)
                
            # Content checks
            title = rec.get("title", "").strip()
            if not title or title.lower() in ("pdf", "document"):
                errors.append(f"Line {line_num}: Invalid or empty 'title'")
                
            body = rec.get("body", "").strip()
            if not body or len(body) < 10:
                errors.append(f"Line {line_num}: Empty or too short 'body'")
                
            if len(body) < 100:
                warnings.append(f"Line {line_num}: Very short chunk ({len(body)} chars)")
                
            # Metadata checks
            source_id = rec.get("source_name", "") # In the schema, source_id is usually mapped to 'source_name' or just missing unless added back
            # Wait, our JSONL outputs source_name like "Tạp chí Y học Việt Nam", let's be careful.
            
            src_url = rec.get("source_url", "")
            if not src_url and not rec.get("file_url"):
                errors.append(f"Line {line_num}: Missing source_url and file_url")
                
    fail_rate = len(errors) / total_records if total_records else 0
    
    report = {
        "status": "PASS" if fail_rate <= 0.01 and len(errors) < 50 else "FAIL",
        "total_records": total_records,
        "unique_doc_ids": len(seen_ids),
        "total_errors": len(errors),
        "total_warnings": len(warnings),
        "error_rate": fail_rate,
        "sample_errors": errors[:10],
        "gate_g1_passed": fail_rate <= 0.01 and "Duplicate 'doc_id'" not in str(errors)
    }
    
    os.makedirs(REPORT_OUT.parent, exist_ok=True)
    with open(REPORT_OUT, 'w', encoding='utf-8') as f:
        json.dump(report, f, indent=4, ensure_ascii=False)
        
    print(f"Validation complete. Records: {total_records}. Errors: {len(errors)}. Gate Status: {report['status']}")
    
if __name__ == "__main__":
    validate()
