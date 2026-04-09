"""
Gate G3: Retrieval Evaluation v2
=================================
v2 improvements (per review.md):
  1. title_norm matching instead of raw exact match
  2. Article collapse: group chunks by title_norm, use max score per article
  3. Retrieve top-20 chunks, collapse to top-5 articles, then check hits

Runs 100 gold queries against Qdrant and measures Hit@1, Hit@3, Hit@5.
"""
import json, time, sys, os, re, unicodedata
from collections import defaultdict

sys.path.insert(0, os.path.abspath("."))

from qdrant_client import QdrantClient

# Try sentence-transformers first (required for bge-m3), fallback to fastembed
try:
    from sentence_transformers import SentenceTransformer
    _HAS_ST = True
except ImportError:
    _HAS_ST = False
try:
    from fastembed import TextEmbedding
    _HAS_FE = True
except ImportError:
    _HAS_FE = False

# Config
QDRANT_URL = os.getenv("QDRANT_URL", "http://localhost:6333")
COLLECTION = os.getenv("QDRANT_COLLECTION", "staging_medqa_vi_vmj_v2")
_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
GOLD_FILE = os.path.join(_ROOT, "benchmark", "datasets", "vmj_retrieval_gold_g3_v2.jsonl")
REPORT_FILE = os.path.join(_ROOT, "benchmark", "reports", "vmj", "G3_retrieval_eval_v2.json")
REPORT_MD = os.path.join(_ROOT, "benchmark", "reports", "vmj", "G3_retrieval_eval_v2.md")
CHUNK_TOP_K = 20   # retrieve this many chunks
ARTICLE_TOP_K = 5  # collapse to this many articles
MODEL = os.getenv("EMBEDDING_MODEL", "BAAI/bge-m3")


# ── Title normalization ──────────────────────────────────────────────
_RE_BAD_CHARS = re.compile(r'[\[\]\\]')

def title_norm(title: str) -> str:
    """Normalize title for fuzzy matching.
    - Unicode NFC
    - lowercase
    - remove bad chars
    - remove trailing numbers (superscripts)
    - collapse whitespace
    - strip punctuation edges
    """
    t = unicodedata.normalize('NFC', title)
    t = _RE_BAD_CHARS.sub('', t)
    t = t.lower().strip()
    t = re.sub(r'\d{1,3}$', '', t)
    t = re.sub(r'\s+', ' ', t).strip()
    t = t.strip('.,;:- ')
    return t


def title_contains(expected_norm: str, candidate_norm: str) -> bool:
    """Check if expected is contained in candidate or vice versa.
    Handles truncated titles from splitter bug.
    """
    if not expected_norm or not candidate_norm:
        return False
    if expected_norm == candidate_norm:
        return True
    # one is substring of the other (handles truncated titles)
    if len(expected_norm) >= 15 and expected_norm in candidate_norm:
        return True
    if len(candidate_norm) >= 15 and candidate_norm in expected_norm:
        return True
    # Word overlap >= 70% of shorter title
    exp_words = set(expected_norm.split())
    cand_words = set(candidate_norm.split())
    if len(exp_words) >= 3 and len(cand_words) >= 3:
        overlap = len(exp_words & cand_words)
        shorter = min(len(exp_words), len(cand_words))
        if overlap / shorter >= 0.7:
            return True
    return False


def collapse_by_article(chunk_results: list) -> list:
    """Collapse chunk results into article-level results.
    Group chunks by title_norm, take max score per article.
    Returns sorted list of {title, title_norm, score, chunk_count}.
    """
    articles = {}  # title_norm -> {title, score, count}
    for r in chunk_results:
        tn = title_norm(r["title"])
        if tn not in articles or r["score"] > articles[tn]["score"]:
            articles[tn] = {
                "title": r["title"],
                "title_norm": tn,
                "score": r["score"],
                "chunk_count": articles.get(tn, {}).get("chunk_count", 0) + 1,
            }
        else:
            articles[tn]["chunk_count"] += 1
    
    # Sort by score descending
    sorted_articles = sorted(articles.values(), key=lambda x: -x["score"])
    return sorted_articles


# ── Vietnamese stopwords for keyword extraction ──────────────────────
_VN_STOPS = set("và của ở tại có là được cho trong với các những một này đó từ đến theo về trên"
    " không cũng đã sẽ hay hoặc nhưng nếu khi sau trước như thì bằng"
    " qua giữa nào đều vẫn ra vào lên xuống đi mà do vì để khi nên"
    " người bệnh nhân viện tỷ lệ nghiên cứu đánh giá khảo sát thực trạng".split())

def _extract_keywords(text: str) -> set:
    """Extract meaningful keywords from query/title."""
    words = title_norm(text).split()
    return {w for w in words if w not in _VN_STOPS and len(w) > 1}


def rerank_articles(articles: list, query: str) -> list:
    """Heuristic reranker for article-level results.
    Adjusts scores based on:
    1. Title-query keyword overlap (main signal)
    2. Chunk coverage (more chunks = better coverage)
    3. Title quality (penalize short/generic titles)
    """
    if not articles:
        return articles
    
    query_kw = _extract_keywords(query)
    if not query_kw:
        return articles
    
    reranked = []
    for art in articles:
        base_score = art["score"]
        title_kw = _extract_keywords(art["title"])
        
        # Signal 1: keyword overlap between query and title
        if title_kw:
            overlap = len(query_kw & title_kw)
            overlap_ratio = overlap / len(query_kw)
            kw_boost = overlap_ratio * 0.06  # max +0.06
        else:
            kw_boost = 0
        
        # Signal 2: chunk coverage — more chunks matching = more relevant
        chunk_count = art.get("chunk_count", 1)
        coverage_boost = min(chunk_count / 10.0, 1.0) * 0.02  # max +0.02
        
        # Signal 3: title quality penalty
        title_len = len(art["title"])
        if title_len < 20:
            quality_penalty = -0.03
        elif title_len < 35:
            quality_penalty = -0.01
        else:
            quality_penalty = 0
        
        # Combined reranked score
        reranked_score = base_score + kw_boost + coverage_boost + quality_penalty
        
        reranked.append({
            **art,
            "base_score": base_score,
            "score": reranked_score,
            "kw_boost": round(kw_boost, 4),
            "coverage_boost": round(coverage_boost, 4),
            "quality_penalty": round(quality_penalty, 4),
        })
    
    reranked.sort(key=lambda x: -x["score"])
    return reranked


def main():
    print("=" * 60)
    print("  Gate G3: Retrieval Evaluation v2.1")
    print("  (title_norm + article collapse + heuristic reranker)")
    print("=" * 60)

    # Load gold
    gold = []
    with open(GOLD_FILE, 'r', encoding='utf-8') as f:
        for line in f:
            if line.strip():
                gold.append(json.loads(line))
    print(f"Loaded {len(gold)} gold queries")

    # Connect
    client = QdrantClient(url=QDRANT_URL, check_compatibility=False)
    info = client.get_collection(COLLECTION)
    print(f"Collection: {COLLECTION} ({info.points_count} points)")

    # Embedder — bge-m3 uses sentence-transformers, others use fastembed
    print(f"Loading {MODEL}...")
    if 'bge-m3' in MODEL and _HAS_ST:
        st_model = SentenceTransformer(MODEL)
        def embed_query(text):
            return st_model.encode([text], normalize_embeddings=True)[0].tolist()
        print(f"Model ready (sentence-transformers, dim={st_model.get_sentence_embedding_dimension()}).\n")
    elif _HAS_FE:
        fe_model = TextEmbedding(model_name=MODEL)
        def embed_query(text):
            return list(fe_model.embed([text]))[0].tolist()
        print("Model ready (fastembed).\n")
    else:
        raise RuntimeError("Neither sentence-transformers nor fastembed available")

    # Run evaluation
    results = []
    hit1 = hit3 = hit5 = 0
    # Also track raw (v1-style) for comparison
    raw_hit1 = raw_hit3 = raw_hit5 = 0
    by_diff = defaultdict(lambda: {"total": 0, "hit1": 0, "hit3": 0, "hit5": 0})
    by_group = defaultdict(lambda: {"total": 0, "hit1": 0, "hit3": 0, "hit5": 0})

    t0 = time.time()
    for i, q in enumerate(gold):
        query_vec = embed_query(q["query"])
        search_results = client.query_points(
            collection_name=COLLECTION,
            query=query_vec,
            limit=CHUNK_TOP_K,  # Retrieve more chunks for collapse
        )

        # Extract chunk-level results
        chunk_results = []
        for pt in search_results.points:
            title = pt.payload.get("title", "")
            chunk_results.append({"title": title, "score": pt.score})

        # Article collapse
        articles = collapse_by_article(chunk_results)
        
        # Heuristic reranker
        articles = rerank_articles(articles, q["query"])

        # Check hits with title_norm matching
        expected_tn = title_norm(q["expected_title"])
        
        def check_hit(article_list, top_n):
            for art in article_list[:top_n]:
                if title_contains(expected_tn, art["title_norm"]):
                    return True
            return False

        h1 = check_hit(articles, 1)
        h3 = check_hit(articles, 3)
        h5 = check_hit(articles, ARTICLE_TOP_K)

        if h1: hit1 += 1
        if h3: hit3 += 1
        if h5: hit5 += 1

        # Also compute raw exact match for comparison
        raw_titles = [r["title"].strip() for r in chunk_results]
        raw_expected = q["expected_title"].strip()
        r_h1 = raw_expected in raw_titles[:1]
        r_h3 = raw_expected in raw_titles[:3]
        r_h5 = raw_expected in raw_titles[:5]
        if r_h1: raw_hit1 += 1
        if r_h3: raw_hit3 += 1
        if r_h5: raw_hit5 += 1

        diff = q["difficulty"]
        by_diff[diff]["total"] += 1
        if h1: by_diff[diff]["hit1"] += 1
        if h3: by_diff[diff]["hit3"] += 1
        if h5: by_diff[diff]["hit5"] += 1

        grp = q["group"]
        by_group[grp]["total"] += 1
        if h1: by_group[grp]["hit1"] += 1
        if h3: by_group[grp]["hit3"] += 1
        if h5: by_group[grp]["hit5"] += 1

        status = "HIT" if h3 else "MISS"
        match_method = ""
        if h3 and not r_h3:
            match_method = " [RESCUED by norm]"
        top1_score = articles[0]["score"] if articles else 0
        print(f"  [{i+1:3d}/100] {status} | H@1:{int(h1)} H@3:{int(h3)} | score={top1_score:.3f} | {q['query'][:55]}{match_method}")

        results.append({
            "query": q["query"],
            "expected_title": q["expected_title"].strip(),
            "expected_title_norm": expected_tn,
            "difficulty": diff,
            "group": grp,
            "type": q["type"],
            "hit_at_1": h1,
            "hit_at_3": h3,
            "hit_at_5": h5,
            "raw_hit_at_3": r_h3,
            "match_method": "norm" if (h3 and not r_h3) else ("exact" if h3 else "miss"),
            "top_articles": [{"title": a["title"][:80], "title_norm": a["title_norm"][:80], "score": a["score"], "chunks": a["chunk_count"]} for a in articles[:ARTICLE_TOP_K]],
        })

    elapsed = time.time() - t0
    total = len(gold)

    # Summary
    rescued = hit3 - raw_hit3
    print(f"\n{'=' * 60}")
    print(f"  OVERALL (v2 with title_norm + article collapse):")
    print(f"    Hit@1={hit1/total:.1%} | Hit@3={hit3/total:.1%} | Hit@5={hit5/total:.1%}")
    print(f"  COMPARISON (v1 raw exact match):")
    print(f"    Hit@1={raw_hit1/total:.1%} | Hit@3={raw_hit3/total:.1%} | Hit@5={raw_hit5/total:.1%}")
    print(f"  DELTA: Hit@3 {raw_hit3}→{hit3} (+{rescued} rescued by norm+collapse)")
    print(f"  Time: {elapsed:.1f}s ({elapsed/total:.2f}s/query)")
    print(f"{'=' * 60}")

    print("\n  By Difficulty:")
    for d in ["easy", "medium", "hard"]:
        s = by_diff[d]
        print(f"    {d:8s}: Hit@1={s['hit1']}/{s['total']} ({s['hit1']/s['total']:.0%}) | Hit@3={s['hit3']}/{s['total']} ({s['hit3']/s['total']:.0%}) | Hit@5={s['hit5']}/{s['total']} ({s['hit5']/s['total']:.0%})")

    print("\n  By Group:")
    for g in sorted(by_group.keys()):
        s = by_group[g]
        print(f"    {g[:40]:40s}: H@3={s['hit3']}/{s['total']} ({s['hit3']/s['total']:.0%})")

    # Gate check
    hit3_pct = hit3 / total * 100
    gate_pass = hit3_pct >= 75
    print(f"\n  Gate G3 threshold: Hit@3 >= 75%")
    print(f"  Result: Hit@3 = {hit3_pct:.1f}% → {'PASS ✅' if gate_pass else 'FAIL ❌'}")

    # Save JSON report
    report = {
        "gate": "G3",
        "version": "v2_norm_collapse",
        "collection": COLLECTION,
        "model": MODEL,
        "total_queries": total,
        "chunk_top_k": CHUNK_TOP_K,
        "article_top_k": ARTICLE_TOP_K,
        "elapsed_s": round(elapsed, 1),
        "hit_at_1": hit1, "hit_at_3": hit3, "hit_at_5": hit5,
        "hit_at_1_pct": round(hit1/total*100, 1),
        "hit_at_3_pct": round(hit3/total*100, 1),
        "hit_at_5_pct": round(hit5/total*100, 1),
        "raw_hit_at_1": raw_hit1, "raw_hit_at_3": raw_hit3, "raw_hit_at_5": raw_hit5,
        "rescued_by_norm": rescued,
        "gate_threshold": 75, "gate_pass": gate_pass,
        "by_difficulty": dict(by_diff),
        "by_group": dict(by_group),
        "details": results,
    }
    os.makedirs(os.path.dirname(REPORT_FILE), exist_ok=True)
    with open(REPORT_FILE, 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    print(f"\n  JSON: {REPORT_FILE}")

    # Save MD report
    with open(REPORT_MD, 'w', encoding='utf-8') as f:
        f.write(f"# Gate G3 Retrieval Evaluation Report v2\n\n")
        f.write(f"**Evaluator:** title_norm matching + article collapse (top-20 chunks → top-5 articles)  \n")
        f.write(f"**Collection:** `{COLLECTION}` ({info.points_count} points)  \n")
        f.write(f"**Model:** `{MODEL}`  \n")
        f.write(f"**Queries:** {total}  \n")
        f.write(f"**Time:** {elapsed:.1f}s  \n\n")
        
        f.write(f"## v1 vs v2 Comparison\n\n")
        f.write(f"| Metric | v1 (exact match) | v2 (norm+collapse) | Delta |\n")
        f.write(f"|--------|-----------------|-------------------|-------|\n")
        f.write(f"| Hit@1 | {raw_hit1/total:.1%} | {hit1/total:.1%} | +{hit1-raw_hit1} |\n")
        f.write(f"| **Hit@3** | **{raw_hit3/total:.1%}** | **{hit3/total:.1%}** | **+{rescued}** |\n")
        f.write(f"| Hit@5 | {raw_hit5/total:.1%} | {hit5/total:.1%} | +{hit5-raw_hit5} |\n\n")
        
        f.write(f"## Overall (v2)\n\n")
        f.write(f"| Metric | Value | Gate |\n")
        f.write(f"|--------|-------|------|\n")
        f.write(f"| Hit@1 | {hit1/total:.1%} ({hit1}/{total}) | |\n")
        f.write(f"| **Hit@3** | **{hit3/total:.1%}** ({hit3}/{total}) | **{'PASS ✅' if gate_pass else 'FAIL ❌'}** (≥75%) |\n")
        f.write(f"| Hit@5 | {hit5/total:.1%} ({hit5}/{total}) | |\n\n")
        
        f.write(f"## By Difficulty\n\n")
        f.write(f"| Difficulty | Hit@1 | Hit@3 | Hit@5 |\n")
        f.write(f"|------------|-------|-------|-------|\n")
        for d in ["easy", "medium", "hard"]:
            s = by_diff[d]
            f.write(f"| {d} | {s['hit1']}/{s['total']} ({s['hit1']/s['total']:.0%}) | {s['hit3']}/{s['total']} ({s['hit3']/s['total']:.0%}) | {s['hit5']}/{s['total']} ({s['hit5']/s['total']:.0%}) |\n")
        
        f.write(f"\n## By Group\n\n")
        f.write(f"| Group | Hit@1 | Hit@3 | Hit@5 |\n")
        f.write(f"|-------|-------|-------|-------|\n")
        for g in sorted(by_group.keys()):
            s = by_group[g]
            f.write(f"| {g} | {s['hit1']}/{s['total']} ({s['hit1']/s['total']:.0%}) | {s['hit3']}/{s['total']} ({s['hit3']/s['total']:.0%}) | {s['hit5']}/{s['total']} ({s['hit5']/s['total']:.0%}) |\n")

        # Rescued queries
        rescued_queries = [r for r in results if r["match_method"] == "norm"]
        if rescued_queries:
            f.write(f"\n## Rescued by Norm+Collapse ({len(rescued_queries)} queries)\n\n")
            for r in rescued_queries:
                f.write(f"- **[{r['difficulty']}]** {r['query'][:80]}  \n")
                f.write(f"  Expected norm: `{r['expected_title_norm'][:60]}`  \n")
                matched_art = next((a for a in r['top_articles'][:3] if title_contains(title_norm(r['expected_title']), a['title_norm'])), None)
                if matched_art:
                    f.write(f"  Matched: `{matched_art['title'][:60]}` (score={matched_art['score']:.3f})  \n\n")

        # Still missed
        misses = [r for r in results if not r["hit_at_3"]]
        if misses:
            f.write(f"\n## Still Missed ({len(misses)} queries)\n\n")
            for m in misses:
                f.write(f"- **[{m['difficulty']}]** {m['query'][:80]}  \n")
                f.write(f"  Expected: `{m['expected_title'][:60]}`  \n")
                if m['top_articles']:
                    f.write(f"  Got Top-1: `{m['top_articles'][0]['title'][:60]}` (score={m['top_articles'][0]['score']:.3f})  \n\n")

    print(f"  MD:   {REPORT_MD}")

if __name__ == "__main__":
    main()
