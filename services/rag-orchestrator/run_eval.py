import json
import time
import requests
import os

API_URL = "http://localhost:8000/api/chat"

def main():
    with open("eval_queries.json", "r") as f:
        queries = json.load(f)

    results = []
    print(f"Running evaluation on {len(queries)} queries...")
    
    for i, q in enumerate(queries):
        query_text = q["query"]
        expected_source = q["expected_source"]
        
        print(f"\n[{i+1}/{len(queries)}] Query: {query_text}")
        payload = {"message": query_text, "session_id": f"eval_session_{i}"}
        
        start_t = time.time()
        try:
            resp = requests.post(API_URL, json=payload, timeout=300)
            if resp.status_code == 200:
                data = resp.json()
                answer = data.get("answer", "")
                sources = data.get("sources", [])
                
                # Check hit@3
                hit = False
                source_names = []
                for s in sources:
                    s_name = s.get("title", "") + " / " + s.get("source_name", "") # the frontend format uses url/title
                    # wait, main.py might not return source_name explicitly, but let's check text
                    source_names.append(str(s))
                    if expected_source.lower() in str(s).lower():
                        hit = True
                
                print(f"  Hit expected source ({expected_source})? {hit}")
                
                results.append({
                    "query": query_text,
                    "expected_source": expected_source,
                    "hit": hit,
                    "answer": answer,
                    "sources": sources,
                    "latency": time.time() - start_t
                })
            else:
                print(f"  API Error: {resp.status_code} - {resp.text}")
                results.append({
                    "query": query_text,
                    "error": resp.text
                })
        except Exception as e:
            print(f"  Exception: {e}")
            results.append({
                "query": query_text,
                "error": str(e)
            })
            
    # Write report
    report_path = "../../data/eval_report.md"
    os.makedirs(os.path.dirname(report_path), exist_ok=True)
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("# RAG Retrieval Evaluation Report\n\n")
        
        hits = sum(1 for r in results if r.get("hit"))
        total = len(results)
        f.write(f"**Overall Source Hit@3 Rate**: {hits}/{total} ({(hits/total)*100:.1f}%)\n\n")
        
        for r in results:
            f.write(f"## Q: {r['query']}\n")
            f.write(f"- **Expected Source**: {r.get('expected_source')}\n")
            if "error" in r:
                f.write(f"- **Error**: {r['error']}\n\n")
                continue
            
            f.write(f"- **Hit Expected**: {'✅ YES' if r['hit'] else '❌ NO'}\n")
            f.write(f"- **Latency**: {r['latency']:.2f}s\n")
            
            f.write("### Sources Retrieved\n")
            for s in r["sources"]:
                f.write(f"- {s.get('title', 'Unknown Title')} (ID: {s.get('id', '')})\n")
                
            f.write("\n### LLM Answer\n")
            f.write(f"{r['answer']}\n\n")
            f.write("---\n\n")
            
    print(f"\n[DONE] Evaluation complete. Report written to {report_path}")

if __name__ == "__main__":
    main()
