# Retrieval System — Chi tiết toàn bộ

> Mọi thông tin trích dẫn trực tiếp từ source code thực tế.

---

## Tổng quan: 8 bước từ Query → Context cho LLM

```
User Query
  │
  ▼
① Query Rewriter ──────── Có history? → LLM rewrite thành standalone query
  │                       Không?      → Giữ nguyên
  ▼
② Query Router ─────────── Rule-based → query_type + retrieval_profile
  │                                     (light/standard/deep → top_k = 8/12/20)
  ▼
③ Auto Filter Detector ─── Phân tích query → MetadataFilters
  │                        (source_name, audience, trust_tier, doc_type)
  ▼
④ Embedding ────────────── Query → bge-m3 → Vector 1024-dim
  │
  ▼
⑤ Qdrant Vector Search ── Cosine similarity + optional filters
  │                        → top-K chunks (thô)
  ▼
⑥ Post-processing ─────── Score threshold + dedup + token budget
  │                        → chunks sạch
  ▼
⑦ Article Aggregator ──── Group by title_norm → score articles
  │                        → 1 primary + 0-2 secondary
  ▼
⑧ Evidence Extractor ──── LLM hoặc regex → structured evidence pack
  │
  ▼
Context cho Answer Composer
```

---

## Bước ① — Query Rewriter

**File:** [query_rewriter.py](file:///d:/CODE/DATN/LLM-MedQA-Assistant/services/rag-orchestrator/app/query_rewriter.py)

### Mục đích
Biến câu follow-up ngắn (ví dụ "còn tác dụng phụ thì sao?") thành câu hỏi độc lập để retriever tìm đúng.

### Logic quyết định rewrite hay không

```python
def _needs_rewriting(message, history):
    if not history:           → return False   # Câu đầu tiên, không cần
    if len(words) <= 5:       → return True    # "Can it be cured?" → cần
    if chứa referent words:   → return True    # "it", "this", "what about"
    else:                     → return False   # Câu tự đủ nghĩa
```

Referent words: `it, this, that, those, they, them, what about, how about, and, also, too, else, more, the same, similar`

### 2 chế độ rewrite

| Chế độ | Khi nào dùng | Logic |
|---|---|---|
| **LLM rewrite** | Có LLM client | Gửi last 4 messages làm context + prompt: *"Rewrite as STANDALONE search query"* → temp=0.1, max_tokens=150 |
| **Rule fallback** | LLM fail hoặc không có | Lấy 8 words đầu câu hỏi trước → prepend `"Regarding {topic}: {message}"` |

### Ví dụ

```
History: "What is asthma?" → "Asthma is a chronic respiratory condition..."
User:    "Can it be cured?"

LLM rewrite → "Can asthma be cured treatment options"
Rule fallback → "Regarding What is asthma: Can it be cured?"
```

---

## Bước ② — Query Router

**File:** [query_router.py](file:///d:/CODE/DATN/LLM-MedQA-Assistant/services/rag-orchestrator/app/query_router.py)

### Mục đích
Phân loại query → xác định **retrieval profile** (bao nhiêu chunks cần lấy) và **có cần Evidence Extractor không**.

### Cách hoạt động — 100% Rule-based (không gọi LLM)

```python
scores = {
    "study_result_extraction": count("AUC", "OR", "HR", "kết quả", "cỡ mẫu"...),
    "research_appraisal":      count("hạn chế", "bias", "áp dụng"...),
    "comparative_synthesis":   count("so sánh", "khác nhau", "versus"...),
    "guideline_comparison":    count("guideline", "hướng dẫn", "phác đồ"...),
    "teaching_explainer":      count("giải thích", "cơ chế", "tại sao"...),
    "fact_extraction":         count("là gì", "bao nhiêu", "tiêu chuẩn"...),
}
best_type = max(scores)  # type có nhiều keyword match nhất
```

Nếu không match keyword nào: dựa vào **độ dài query**:
- \> 25 words → `research_appraisal`
- \> 15 words → `study_result_extraction`
- else → `fact_extraction`

### Output ảnh hưởng retrieval

| Query Type | retrieval_profile | top_k | needs_extractor |
|---|---|---|---|
| `fact_extraction` | **light** | **8** | ❌ |
| `study_result_extraction` | **standard** | **12** | ✅ |
| `research_appraisal` | **deep** | **20** | ✅ |
| `comparative_synthesis` | **deep** | **20** | ✅ |
| `guideline_comparison` | **standard** | **12** | ❌ |
| `teaching_explainer` | **standard** | **12** | ❌ |

---

## Bước ③ — Auto Filter Detector

**File:** [retriever.py](file:///d:/CODE/DATN/LLM-MedQA-Assistant/services/rag-orchestrator/app/retriever.py) → `detect_filters_from_query()`

### Mục đích
Phân tích query → tự động gán Qdrant payload filter để thu hẹp search space.

### 3 loại filter detection

**Loại 1 — Explicit source mention** (high confidence):

| Keyword trong query | Filter |
|---|---|
| `"medlineplus"`, `"medline plus"` | `source_name = "MedlinePlus"` |
| `"ncbi"`, `"statpearls"`, `"pubmed"` | `source_name = "NCBI Bookshelf"` |
| `"world health"` | `source_name = "WHO"` |
| `"who recommends/guideline/report..."` | `source_name = "WHO"` (regex check, tránh "who is at risk") |

**Loại 2 — Audience routing** (soft):

| Pattern | audience | source preferred |
|---|---|---|
| `"explain simply"`, `"easy to understand"`, `"patient"` | `patient` | MedlinePlus (soft) |
| `"mechanism"`, `"pathophysiology"`, `"textbook"` | `student` | NCBI Bookshelf (hard) |
| `"global"`, `"guideline"`, `"prevention strategy"` | `clinician` | WHO (hard) |

**Loại 3 — Trust tier** (from keywords):

| Keyword | trust_tier | doc_type |
|---|---|---|
| `"guideline"`, `"protocol"` | 1 | `guideline` |
| `"textbook"`, `"pathophysiology"` | 2 | — |

### Fallback khi filter quá chặt

```python
if qdrant_filter and len(results) < 2:
    # Retry WITHOUT filter, merge results
    unfiltered_results = search(no_filter)
    results = filtered + unfiltered  # filtered first
```

→ Đảm bảo luôn có kết quả, không bao giờ trả empty vì filter sai.

---

## Bước ④ — Embedding

### Model: `BAAI/bge-m3`

| Thuộc tính | Giá trị |
|---|---|
| Model | `BAAI/bge-m3` |
| Dimension | 1024 |
| Library | `sentence-transformers` (ưu tiên) hoặc `fastembed` (fallback) |
| Normalize | `normalize_embeddings=True` |
| Multilingual | ✅ (EN + VI cùng embedding space) |

```python
# sentence-transformers
vector = model.encode([query], normalize_embeddings=True)[0].tolist()

# fastembed fallback
vector = next(embedder.embed([query])).tolist()
```

### Cache model
Retriever object được cache toàn cục (`_RETRIEVER_CACHE`) → model chỉ load 1 lần khi startup.

---

## Bước ⑤ — Qdrant Vector Search

### Cấu hình

```python
QdrantRetriever(
    qdrant_url    = os.getenv("QDRANT_URL"),
    collection    = os.getenv("QDRANT_COLLECTION", "medical_docs"),
    embedding_model = os.getenv("EMBEDDING_MODEL", "BAAI/bge-small-en-v1.5"),
    top_k           = int(os.getenv("RAG_TOP_K", "4")),          # default, overridden by router
    score_threshold = float(os.getenv("RAG_MIN_SCORE", "0.25")), # minimum cosine score
    max_context_tokens = int(os.getenv("RAG_MAX_CONTEXT_TOKENS", "2048")),
    deduplicate     = True,
)
```

### Search flow

```
query_vector ──→ Qdrant.query_points(
                    collection = "staging_medqa_vi_vmj_v2",
                    query = vector_1024d,
                    limit = 8/12/20,        ← từ router profile
                    query_filter = {...},    ← từ auto filter
                    with_payload = True,
                 )
                 │
                 ▼
          List[ScoredPoint]  (id, score, payload)
```

### Distance metric: Cosine Similarity

Khi tạo collection:
```python
client.create_collection(
    vectors_config=qm.VectorParams(
        size=1024,                    # bge-m3 dimension
        distance=qm.Distance.COSINE,
    ),
)
```

---

## Bước ⑥ — Post-processing (3 cổng lọc)

### Cổng 1: Score threshold

```python
if p.score < self.score_threshold:   # default 0.25
    continue  # bỏ chunk có score quá thấp
```

### Cổng 2: Deduplication

```python
h = sha256(normalize(text))  # hash nội dung
if h in seen:
    continue  # bỏ chunk trùng nội dung (dù ID khác)
seen.add(h)
```

Normalize: lowercase + collapse whitespace → hash → so sánh.

### Cổng 3: Token budget

```python
tokens = max(1, len(text) // 4)   # ước lượng 1 token ≈ 4 chars
if used_tokens + tokens > max_context_tokens:   # default 2048
    break  # đã đủ context, không lấy thêm
used_tokens += tokens
```

### Output: `List[RetrievedChunk]`

```python
@dataclass
class RetrievedChunk:
    id: str                    # UUID point ID từ Qdrant
    text: str                  # Nội dung chunk (có context header)
    score: float               # Cosine similarity score
    metadata: Dict[str, Any]   # title, source_name, doc_type, trust_tier, ...
```

Metadata 15 fields: `doc_id, title, section_title, source_name, source_url, doc_type, specialty, audience, language, trust_tier, published_at, updated_at, heading_path, tags, chunk_index`

---

## Bước ⑦ — Article Aggregator

**File:** [article_aggregator.py](file:///d:/CODE/DATN/LLM-MedQA-Assistant/services/rag-orchestrator/app/article_aggregator.py)

### Mục đích
Chuyển top-K chunks thành **1 primary article + 0-2 secondary articles**.

### Bước 7.1: Group chunks by title_norm

```python
title_norm("KHẢO SÁT VÀ PHÁC THẢO DANH MỤC TƯƠNG TÁC...")
→ "khảo sát và phác thảo danh mục tương tác..."

# Normalize: NFC → remove []\\ → lowercase → remove trailing digits
#            → collapse whitespace → strip punctuation
```

20 chunks → 3-5 articles (chunks cùng title được gom lại).

### Bước 7.2: Tính article_score

```python
article_score =
    0.35 × max_chunk_score       # chunk có score cao nhất
  + 0.20 × avg_chunk_score       # trung bình scores
  + 0.15 × section_diversity     # min(chunk_count / 6, 1.0)
  + 0.15 × numeric_density       # % chunks có số liệu (OR, HR, AUC, n=)
  + 0.15 × keyword_overlap       # from query↔title keyword intersection
```

| Factor | Weight | Ý nghĩa |
|---|---|---|
| **max_chunk_score** | 35% | Chunks tốt nhất quan trọng nhất |
| **avg_chunk_score** | 20% | Consistency — nhiều chunks tốt hơn 1 chunk tốt |
| **section_diversity** | 15% | Bài có nhiều sections của content bao quát hơn |
| **numeric_density** | 15% | Bài có số liệu → phù hợp câu hỏi study |
| **keyword_overlap** | 15% | Title match query keywords → đúng chủ đề |

> ⚠️ **Không có trust_tier** trong formula — guideline không được ưu tiên hơn bài lẻ.

### Bước 7.3: Chọn primary + secondary

```python
articles.sort(key=lambda a: -a.article_score)
primary = articles[0]

secondary = []
for art in articles[1:]:
    if len(secondary) >= 2:
        break
    if art.article_score >= primary.article_score * 0.80:  # chỉ lấy nếu score ≥ 80% primary
        secondary.append(art)
```

**Chọn lọc chặt:** Secondary chỉ được chọn nếu score ≥ 80% primary — tránh thêm bài yếu.

### Ví dụ thực tế

```
Query: "Tương tác thuốc trong thực hành lâm sàng tại bệnh viện đa khoa"

20 chunks → 5 articles:
  #1 KHẢO SÁT... ĐỒNG NAI         score=0.759  13 chunks → PRIMARY ★
  #2 CAN THIỆP DƯỢC LÂM SÀNG...   score=0.715   3 chunks → secondary (0.715/0.759 = 94% ≥ 80%)
  #3 HIỆU QUẢ CAN THIỆP...        score=0.706   2 chunks → secondary (0.706/0.759 = 93% ≥ 80%)
  #4 XÂY DỰNG DANH MỤC...          score=0.684   1 chunk  → bỏ (0.684/0.759 = 90% → OK, nhưng max_secondary=2 rồi)
  #5 THỰC TRẠNG...                  score=0.671   1 chunk  → bỏ
```

---

## Bước ⑧ — Evidence Extractor

**File:** [evidence_extractor.py](file:///d:/CODE/DATN/LLM-MedQA-Assistant/services/rag-orchestrator/app/evidence_extractor.py)

### Mục đích
Chuyển raw chunks thành **structured evidence pack** có cấu trúc cho Answer Composer.

### 2 chế độ

| Chế độ | Khi nào | Cách trích |
|---|---|---|
| **LLM extraction** | `needs_extractor=True` AND có LLM client | Gửi chunks → LLM → JSON structured (population, sample_size, design, numbers...) |
| **Simple regex** | Mọi case khác | Regex tìm `n=`, `AUC=`, `OR=`, `HR=`, `p<` trong raw text |

### LLM Extraction — Chi tiết

Prompt system:
```
Bạn là Evidence Extractor cho hệ thống y khoa.
Chỉ dùng các article chunks được cung cấp.
Không suy luận bằng kiến thức ngoài.
```

Output JSON:
```json
{
  "source_type": "original_study | review | guideline",
  "population": "...",
  "sample_size": "n=146",
  "design": "nghiên cứu đoàn hệ hồi cứu",
  "setting": "đơn trung tâm",
  "intervention_or_exposure": "...",
  "comparator": "...",
  "outcomes": ["kết cục chính"],
  "key_findings": ["phát hiện 1", "phát hiện 2"],
  "numbers": [{"metric": "AUC", "value": "0.76", "unit": ""}],
  "limitations": ["cỡ mẫu nhỏ"],
  "conclusion": "..."
}
```

Config: `temperature=0.1`, `max_tokens=800`

### Regex extraction (fallback)

```python
patterns = [
    (r'n\s*=\s*(\d+)',                   'sample_size'),
    (r'AUC\s*[=:]\s*([\d.]+)',           'AUC'),
    (r'OR\s*[=:]\s*([\d.]+)',            'OR'),
    (r'HR\s*[=:]\s*([\d.]+)',            'HR'),
    (r'sensitivity\s*[=:]\s*([\d.]+%?)', 'sensitivity'),
    (r'specificity\s*[=:]\s*([\d.]+%?)', 'specificity'),
    (r'p\s*[<>=]\s*([\d.]+)',            'p-value'),
]
```

### Evidence Pack → Answer Composer

```
EvidencePack
  ├── primary_source (PrimaryEvidence)
  │     ├── title, source_type
  │     ├── population, sample_size, design, setting
  │     ├── intervention_or_exposure, comparator
  │     ├── outcomes[], key_findings[], numbers[], limitations[]
  │     ├── authors_conclusion
  │     └── raw_text (gốc, luôn có)
  ├── secondary_sources[] (PrimaryEvidence × 0-2)
  └── coverage (CoverageScores)
```

---

## Phía Ingestion — Chunks được tạo như thế nào

**File:** [ingest.py](file:///d:/CODE/DATN/LLM-MedQA-Assistant/services/qdrant-ingestor/app/ingest.py)

### Structure-aware chunking

```
Document body
  │
  ├── split_by_headings() → Section[]
  │     Section { title, heading_path, body }
  │
  └── Mỗi section:
        │
        ├── len ≤ 900 chars → 1 chunk (giữ nguyên)
        └── len > 900 chars → sub_chunk(chunk_size=900, overlap=150)

  Mỗi chunk được prepend Context Header:
    "Title: Viêm gan B mạn\n"
    "Section: Kết quả\n"
    "Source: Tạp chí Y học Việt Nam\n"
    "Updated: 2024-01-15\n"
    "Audience: clinician\n"
    "Body:\n"
    "{nội dung chunk}"
```

### Tại sao context header quan trọng?

Embedding model encode **cả header + body** → vector mang thêm semantic về title, source, section. Khi search "viêm gan B", chunks có header "Title: Viêm gan B mạn" sẽ có cosine score cao hơn.

### Stable chunk ID

```python
generate_stable_id("VMJ", "viêm_gan_b_mạn", "kết_quả", 2)
→ "vmj_viem_gan_b_man_ket_qua_chunk02"
```

Human-readable + deterministic → re-ingest không tạo duplicate.

### Chunk metadata payload

Mỗi chunk trong Qdrant mang **15 metadata fields** (top-level payload):

```json
{
  "text": "Title: ...\nBody:\n...",
  "doc_id": "vmj_ojs_12345",
  "title": "VIÊM GAN B MẠN...",
  "source_name": "Tạp chí Y học Việt Nam",
  "source_url": "https://...",
  "doc_type": "review",
  "specialty": "gastroenterology",
  "audience": "clinician",
  "language": "vi",
  "trust_tier": 2,
  "published_at": "2024-01-15",
  "heading_path": "Kết quả > Bảng 1",
  "chunk_index": 2,
  "human_id": "vmj_viem_gan_b_man_ket_qua_chunk02"
}
```

---

## Kiểm soát chất lượng Retrieval

### Gate G3 — Benchmark chính

**File:** [gate_g3_eval.py](file:///d:/CODE/DATN/LLM-MedQA-Assistant/services/qdrant-ingestor/gate_g3_eval.py)

### Cách hoạt động

```
100 gold queries                Gate G3 pipeline
      │                              │
      ▼                              │
  ① Embed query (bge-m3)             │
      │                              │
      ▼                              │
  ② Qdrant search top-20 chunks      │
      │                              │
      ▼                              │
  ③ collapse_by_article()            │
      │  Group chunks by title_norm  │
      │  Max score per article       │
      ▼                              │
  ④ rerank_articles()                │
      │  +kw_boost (max +0.06)       │
      │  +coverage_boost (max +0.02) │
      │  -quality_penalty (-0.03)    │
      ▼                              │
  ⑤ Top-5 articles                   │
      │                              │
      ▼                              │
  ⑥ title_contains(expected, top_k)  │
      │  Match? → HIT                │
      │  No?    → MISS               │
      ▼                              ▼
  Hit@1, Hit@3, Hit@5          Gate: Hit@3 ≥ 75%?
```

### Title matching — Fuzzy, không exact

```python
def title_contains(expected_norm, candidate_norm):
    if expected_norm == candidate_norm:          → True   # exact
    if expected_norm in candidate_norm:          → True   # substring (≥15 chars)
    if candidate_norm in expected_norm:          → True   # reverse substring
    if word_overlap ≥ 70% of shorter title:     → True   # word overlap
    else:                                       → False
```

### Heuristic Reranker — 3 tín hiệu

```python
reranked_score = base_score + kw_boost + coverage_boost + quality_penalty

kw_boost       = (keyword_overlap / query_keywords) × 0.06   # max +0.06
coverage_boost = min(chunk_count / 10, 1.0) × 0.02           # max +0.02
quality_penalty = -0.03 if title < 20 chars                   # penalize short titles
                  -0.01 if title < 35 chars
                   0.00 otherwise
```

### Kết quả thực tế

**VMJ (100 queries VN):**

| Metric | v1 (exact match) | v2 (norm+collapse+rerank) | Cải thiện |
|---|---|---|---|
| Hit@1 | 40% | **93%** | +53 |
| **Hit@3** | **42%** | **97%** | **+55** |
| Hit@5 | 42% | **97%** | +55 |

**EN (68 queries):**

| Collection | Src Hit@3 | Title Hit@3 | Title MRR |
|---|---|---|---|
| Combined (all) | 91.2% | 89.7% | 0.858 |
| WHO only | 100% | 100% | 1.000 |
| NCBI only | 100% | 100% | 1.000 |
| MedlinePlus only | 100% | 88.5% | 0.859 |

### 3 queries MISS (VN) — Nguyên nhân

| # | Query | Root cause |
|---|---|---|
| 1 | Viêm gan B mạn chưa điều trị | **Title lỗi ký tự** `V[` trong DB → norm match fail |
| 2 | PPOS ở phụ nữ vô sinh | **Semantic gap** — query paraphrase quá xa title |
| 3 | Khối phần phụ có cấu trúc nang | **Semantic gap** — query hỏi "dấu hiệu gợi ý ác tính" nhưng title nói "u buồng trứng" |

### 7 queries MISS (EN) — Nguyên nhân

| Query | Root cause |
|---|---|
| Glaucoma (2 queries) | **Không có trong corpus** MedlinePlus crawl position chữ G →  chưa crawl đến |
| Abdominal pain causes | NCBI "Appendicitis" outranked MedlinePlus "Abdominal Pain" |
| Sickle cell anemia | Title mismatch: "Sickle Cell Disease" ≠ "Anemia" |
| "Can it be cured?" | Multi-turn rewrite → tìm WHO thay vì MedlinePlus (source miss, topic hit) |

---

## Tổng kết: Retrieval pipeline từ A→Z

```
┌─────────────────────────────────────────────────────────────────┐
│                    INGESTION (offline, 1 lần)                  │
│                                                                 │
│  JSONL → validate → chunk_by_structure(900 chars, 150 overlap) │
│       → prepend context header                                  │
│       → embed (bge-m3, 1024-dim)                                │
│       → upsert to Qdrant (cosine, 80,871 points VMJ)           │
│       → metadata payload (15 fields per chunk)                  │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    QUERY TIME (mỗi câu hỏi)                   │
│                                                                 │
│  ① Rewrite (LLM/rule)                                          │
│  ② Route (rule-based → light/standard/deep)                    │
│  ③ Auto filter (source/audience/trust_tier)                    │
│  ④ Embed query (bge-m3)                                        │
│  ⑤ Qdrant cosine search (top 8/12/20)                          │
│  ⑥ Filter: score ≥ 0.25, dedup SHA256, token budget ≤ 2048    │
│  ⑦ Article collapse: group by title → score → 1 primary       │
│  ⑧ Evidence extract: LLM structured hoặc regex fallback       │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    QUALITY CONTROL                              │
│                                                                 │
│  Pre-ingest:  QA 3-layer (schema + content + chunks)           │
│               Composite score ≥ 80 → GO                        │
│                                                                 │
│  Post-ingest: Gate G3 (100 gold queries)                       │
│               Hit@3 ≥ 75% → PASS                              │
│               Actual: 97% VN, 89.7% EN                         │
│                                                                 │
│  Runtime:     Coverage scorer (high/medium/low)                │
│               → ép LLM nói "không đủ dữ liệu" khi low        │
└─────────────────────────────────────────────────────────────────┘
```

### Điểm mạnh

| # | Điểm mạnh | Evidence |
|---|---|---|
| 1 | **Multilingual embedding** | bge-m3 hỗ trợ VN+EN cùng vector space |
| 2 | **Structure-aware chunking** | Tách theo heading, giữ context header |
| 3 | **Article collapse** | 20 chunks → 5 articles, tránh 1 bài chiếm hết context |
| 4 | **Fallback chain** | Filter fail → retry unfiltered; LLM fail → regex; chunks=0 → abstain |
| 5 | **Deterministic retrieval** | Cosine similarity, no sampling → cùng query → cùng results |
| 6 | **Gold benchmark 168 queries** | 100 VN + 68 EN, đa chuyên khoa, paraphrased |

### Điểm cần cải thiện

| # | Điểm yếu | Vấn đề |
|---|---|---|
| 1 | **trust_tier không dùng trong ranking** | Guideline Tier 1 có thể bị bài lẻ Tier 2 đánh bại |
| 2 | **Không có cross-encoder reranker** | Chỉ dùng bi-encoder (bge-m3) + heuristic boost |
| 3 | **Auto filter có thể quá chặt** | `"textbook"` → hard filter `source_name = "NCBI Bookshelf"` → miss VN textbooks |
| 4 | **token budget tĩnh (2048)** | Không adaptive theo query complexity |
| 5 | **Chưa test latency production** | Chỉ có benchmark time (0.68s/query), chưa có real-user latency |
