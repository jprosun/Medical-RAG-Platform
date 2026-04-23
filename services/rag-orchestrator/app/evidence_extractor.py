"""
Evidence Extractor v1.5
========================
Builds structured evidence pack from article chunks per review.md §6.
Uses LLM to extract: population, sample_size, design, key_findings,
numbers, limitations, conclusion — each with support text.

v1.5 improvements:
  - claims now carry supporting_span and chunk_id for grounding
  - LLM prompt explicitly requests supporting spans
  - simple extraction also attaches chunk provenance

Only triggered for deep query types:
  - study_result_extraction
  - research_appraisal
  - comparative_synthesis
"""

from __future__ import annotations

import json
import re
import os
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any

from .query_router import RouterOutput
from .article_aggregator import AggregatedResult, ArticleGroup, _extract_keywords


# ── Data structures ──────────────────────────────────────────────────

@dataclass
class EvidenceField:
    """A single extracted field with its source text."""
    text: str
    support_text: str = ""  # raw chunk text it was extracted from


@dataclass
class NumberEvidence:
    """An extracted numeric result."""
    metric: str
    value: str
    unit: str = ""
    support_text: str = ""


@dataclass
class ClaimEvidence:
    """An extracted claim/finding with grounding info."""
    claim: str
    support_text: str = ""         # raw chunk text it was derived from
    supporting_span: str = ""      # v1.5: exact span that supports this claim
    chunk_id: str = ""             # v1.5: which chunk this claim came from
    section_title: str = ""        # v1.5: section where claim was found


@dataclass
class PrimaryEvidence:
    """Structured evidence from the primary source."""
    title: str = ""
    source_type: str = ""  # original_study | review | guideline | etc.
    population: Optional[EvidenceField] = None
    sample_size: Optional[EvidenceField] = None
    design: Optional[EvidenceField] = None
    setting: Optional[EvidenceField] = None
    intervention_or_exposure: Optional[EvidenceField] = None
    comparator: Optional[EvidenceField] = None
    outcomes: List[EvidenceField] = field(default_factory=list)
    direct_answer_spans: List[ClaimEvidence] = field(default_factory=list)
    key_findings: List[ClaimEvidence] = field(default_factory=list)
    numbers: List[NumberEvidence] = field(default_factory=list)
    limitations: List[ClaimEvidence] = field(default_factory=list)
    authors_conclusion: Optional[EvidenceField] = None
    raw_text: str = ""  # fallback: all chunks concatenated


@dataclass
class CoverageScores:
    """How well the evidence answers the query."""
    direct_answerability: float = 0.0
    numeric_coverage: float = 0.0
    methods_coverage: float = 0.0
    limitations_coverage: float = 0.0
    conflict_risk: float = 0.0


@dataclass
class EvidencePack:
    """Complete evidence pack for the answer composer."""
    query_type: str
    primary_source: PrimaryEvidence = field(default_factory=PrimaryEvidence)
    secondary_sources: List[PrimaryEvidence] = field(default_factory=list)
    coverage: CoverageScores = field(default_factory=CoverageScores)
    extractor_used: bool = False  # whether LLM extraction was run


# ── LLM Extraction Prompt ────────────────────────────────────────────

_EXTRACTOR_SYSTEM = """Bạn là Evidence Extractor (chuyên gia trích xuất bằng chứng y khoa).
Nhiệm vụ của bạn là bóc tách các trường dữ liệu một cách CỰC KỲ KHẮT KHE từ các article chunks được cung cấp.
Tuyệt đối KHÔNG suy luận bằng kiến thức ngoài. Nếu một trường không có dữ liệu thật trong văn bản, BẮT BUỘC trả về null.

Output JSON hợp lệ theo format:
{
  "source_type": "original_study | review | guideline | meta_analysis | case_report | other",
  "population": "mô tả quần thể nghiên cứu. Để null nếu không có.",
  "sample_size": "cỡ mẫu tổng thể (vd: n=146). KHÔNG gán nhầm số của subgroup thành sample size toàn bài. Phải bám sát mẫu câu n=...",
  "design": "thiết kế nghiên cứu (vd: mô tả cắt ngang, RCT). Phải có từ khóa thiết kế.",
  "setting": "nơi thực hiện",
  "intervention_or_exposure": "can thiệp hoặc phơi nhiễm",
  "comparator": "nhóm so sánh",
  "outcomes": ["kết cục chính", "kết cục phụ"],
  "key_findings": [
    {
      "claim": "phát hiện chính yếu nhất",
      "supporting_span": "trích CỰC KỲ CHÍNH XÁC nguyên văn câu/đoạn hỗ trợ từ chunk (không paraphrase)",
      "chunk_index": 0
    }
  ],
  "numbers": [
    {"metric": "tên chỉ số", "value": "giá trị", "unit": "đơn vị"}
  ],
  "limitations": [
    {
      "claim": "hạn chế",
      "supporting_span": "trích nguyên văn hạn chế được tác giả thừa nhận từ tài liệu"
    }
  ],
  "conclusion": "kết luận của tác giả"
}

Luật lệ Trích xuất Bắt buộc:
1. KHÔNG suy diễn. Nếu bài không có sample size, để field sample_size = null.
2. KHÔNG lấy số liệu nếu không chỉ rõ đoạn trích dẫn (supporting_span).
3. `supporting_span` phải là văn bản CHÍNH XÁC COPY 100% từ chunk. Không được viết lại hay tóm tắt.
4. `primary_endpoints` phải được tách riêng với kết quả phụ.
5. chunk_index là số index (0, 1, 2...) của chunk chứa thông tin.
"""


_DIRECT_ANSWER_STOPWORDS = {
    "the", "and", "for", "with", "that", "this", "from",
    "trong", "theo", "cua", "của", "mot", "một", "sau", "bao", "nhieu",
    "nhiêu", "la", "là", "nao", "nào", "duoc", "được", "nhu", "như",
    "the", "thế", "nao", "nào", "co", "có", "gi", "gì",
}
_SENTENCE_SPLIT_RE = re.compile(r'(?<=[\.\?!])\s+|\n+')
_DIRECT_NUMERIC_RE = re.compile(r'\d+[.,]?\d*\s*%|\d+[.,]?\d*')
_DIRECT_FRAGMENT_MARKERS = ("title:", "source:", "audience:", "body:", "keywords:")


def _normalize_text(text: str) -> str:
    import unicodedata

    base = unicodedata.normalize("NFKD", text or "")
    stripped = "".join(ch for ch in base if not unicodedata.combining(ch))
    return " ".join(stripped.lower().split())


def _content_tokens(text: str) -> list[str]:
    tokens = re.findall(r"\w+", _normalize_text(text), flags=re.UNICODE)
    return [tok for tok in tokens if len(tok) >= 3 and tok not in _DIRECT_ANSWER_STOPWORDS]


def _query_phrases(query: str) -> list[str]:
    tokens = _content_tokens(query)
    phrases = []
    for size in (3, 2):
        for i in range(len(tokens) - size + 1):
            phrase = " ".join(tokens[i:i + size])
            if phrase not in phrases:
                phrases.append(phrase)
    return phrases[:12]


def _split_candidate_sentences(text: str) -> list[str]:
    text = re.sub(r'(?i)\btitle:\s*.*?(?=\bsource:|\baudience:|\bbody:|$)', ' ', text or '')
    text = re.sub(r'(?i)\bsource:\s*.*?(?=\baudience:|\bbody:|$)', ' ', text)
    text = re.sub(r'(?i)\baudience:\s*.*?(?=\bbody:|$)', ' ', text)
    text = re.sub(r'(?i)\bbody:\s*', ' ', text)
    text = re.sub(r'(?i)\bkeywords?:\s*.*$', ' ', text)
    text = re.sub(r'(?<!\n)\n(?!\n)', ' ', text)
    text = re.sub(r'\n{2,}', '\n\n', text)
    text = re.sub(r'\s+[o•]\s+(?=[A-ZÀ-Ỵ])', '. ', text)
    text = re.sub(r'(?i)\btheo dõi sau mổ:\s*', '. Theo dõi sau mổ: ', text)
    text = re.sub(r'(?i)\btheo doi sau mo:\s*', '. Theo dõi sau mổ: ', text)
    text = re.sub(r'(?i)\bđiều trị nội khoa hỗ trợ:\s*', '. Điều trị nội khoa hỗ trợ: ', text)
    text = re.sub(r'(?i)\bdieu tri noi khoa ho tro:\s*', '. Điều trị nội khoa hỗ trợ: ', text)
    sentences = []
    for part in _SENTENCE_SPLIT_RE.split(text or ""):
        sent = re.sub(r"\s+", " ", part).strip(" -\t\r\n")
        if len(sent) >= 30:
            sent_norm = _normalize_text(sent)
            if any(marker in sent_norm for marker in _DIRECT_FRAGMENT_MARKERS):
                continue
            sentences.append(sent)
    return sentences


def _contains_any(text: str, markers: tuple[str, ...]) -> bool:
    text_norm = _normalize_text(text)
    return any(marker in text_norm for marker in markers)


def _looks_like_dense_table(sentence: str) -> bool:
    sent_norm = _normalize_text(sentence)
    digit_count = sum(ch.isdigit() for ch in sentence)
    return digit_count >= 12 or any(marker in sent_norm for marker in ("rct", "95% ci", "or (", "tv/dq", "nmct"))


def _score_direct_answer_sentence(
    sentence: str,
    query: str,
    query_terms: set[str],
    query_phrases: list[str],
    asks_numeric: bool,
    section_title: str = "",
) -> float:
    sentence_norm = _normalize_text(sentence)
    if any(marker in sentence_norm for marker in _DIRECT_FRAGMENT_MARKERS):
        return -1.0

    sent_terms = _extract_keywords(sentence)
    overlap = len(query_terms & sent_terms)
    score = overlap * 1.5

    for phrase in query_phrases:
        if phrase and phrase in sentence_norm:
            score += 2.0 if len(phrase.split()) >= 3 else 1.2

    if asks_numeric and _DIRECT_NUMERIC_RE.search(sentence):
        score += 2.0
    if "karnofsky" in _normalize_text(query) and "karnofsky" in sentence_norm:
        score += 3.0
    if "duy tri" in _normalize_text(query) and "duy tri" in sentence_norm:
        score += 1.5
    if "pho bien nhat" in _normalize_text(query) and "pho bien nhat" in sentence_norm:
        score += 2.0

    section_norm = _normalize_text(section_title)
    if any(marker in section_norm for marker in ("ket qua", "kết quả", "ket luan", "kết luận", "tom tat", "tóm tắt")):
        score += 0.8
    return score


def _select_direct_answer_spans(
    article: ArticleGroup,
    query: str,
    router_output: RouterOutput,
) -> list[ClaimEvidence]:
    if getattr(router_output, "answer_style", "") != "exact":
        return []

    query_terms = _extract_keywords(query)
    if not query_terms and not _DIRECT_NUMERIC_RE.search(query or ""):
        return []

    phrases = _query_phrases(query)
    asks_numeric = any(marker in _normalize_text(query) for marker in ("bao nhieu", "ty le", "phan tram", "karnofsky", "diem"))
    candidates: list[tuple[float, str, str, str]] = []
    for chunk in article.chunks:
        section_title = str(chunk.metadata.get("section_title", "") or "")
        for sentence in _split_candidate_sentences(chunk.text):
            score = _score_direct_answer_sentence(
                sentence,
                query,
                query_terms,
                phrases,
                asks_numeric=asks_numeric,
                section_title=section_title,
            )
            if score >= 2.0:
                candidates.append((score, sentence, chunk.id, section_title))

    candidates.sort(key=lambda item: item[0], reverse=True)
    selected: list[ClaimEvidence] = []
    seen = set()
    for score, sentence, chunk_id, section_title in candidates:
        norm = _normalize_text(sentence)
        if norm in seen:
            continue
        seen.add(norm)
        selected.append(
            ClaimEvidence(
                claim=sentence,
                support_text=sentence,
                supporting_span=sentence,
                chunk_id=chunk_id,
                section_title=section_title,
            )
        )
        if len(selected) >= 3:
            break
    return selected


def _select_query_focus_claims(
    article: ArticleGroup,
    query: str,
    router_output: Optional[RouterOutput],
) -> list[ClaimEvidence]:
    answer_style = getattr(router_output, "answer_style", "") if router_output else ""
    query_type = getattr(router_output, "query_type", "") if router_output else ""
    if answer_style not in {"summary", "bounded_partial"} and query_type not in {
        "teaching_explainer",
        "comparative_synthesis",
        "guideline_comparison",
    }:
        return []

    query_terms = _extract_keywords(query)
    if not query_terms:
        return []

    query_norm = _normalize_text(query)
    phrases = _query_phrases(query)
    candidates: list[tuple[float, str, str, str]] = []
    asks_criteria = any(marker in query_norm for marker in ("chi so", "tiêu chí", "tieu chi"))
    asks_why = any(marker in query_norm for marker in ("vì sao", "vi sao", "tại sao", "tai sao"))
    asks_group_factors = any(marker in query_norm for marker in ("nhóm yếu tố", "nhom yeu to", "dựa trên", "dua tren"))
    criteria_markers = ("lam sang", "can lam sang", "nhap vien", "tinh trang", "danh gia", "theo doi")
    metric_markers = ("phan so tong mau", "ef", "bnp", "huyet ap", "thuoc")
    interaction_markers = ("rifampicin", "tacrolimus", "cyclosporine", "thải ghép", "thai ghep")
    selection_factor_markers = ("muc do", "trieu chung", "nguy co quanh thu thuat", "song con")
    followup_factor_markers = (
        "noi khoa toi uu",
        "noi khoa ho tro",
        "nguy co tim mach",
        "theo doi hinh anh",
        "duplex",
        "kiem soat huyet ap",
        "dai thao duong",
        "roi loan lipid mau",
        "tai hep",
        "dot quy",
    )

    for chunk in article.chunks:
        section_title = str(chunk.metadata.get("section_title", "") or "")
        for sentence in _split_candidate_sentences(chunk.text):
            sentence_norm = _normalize_text(sentence)
            score = _score_direct_answer_sentence(
                sentence,
                query,
                query_terms,
                phrases,
                asks_numeric=False,
                section_title=section_title,
            )

            if asks_criteria:
                if _contains_any(sentence_norm, criteria_markers):
                    score += 1.6
                if _contains_any(sentence_norm, metric_markers) and not _contains_any(sentence_norm, criteria_markers):
                    score -= 1.2
            if asks_why:
                if any(
                    marker in sentence_norm
                    for marker in ("tương tác", "tuong tac", "nguy cơ", "nguy co", "thải ghép", "thai ghep", "độ nhạy", "do nhay", "kéo dài", "keo dai")
                ):
                    score += 1.2
            if asks_group_factors:
                if _contains_any(sentence_norm, selection_factor_markers + followup_factor_markers):
                    score += 1.8
                elif any(
                    marker in sentence_norm
                    for marker in ("mức độ", "muc do", "triệu chứng", "trieu chung", "nguy cơ", "nguy co", "sống còn", "song con", "nội khoa", "noi khoa", "hình ảnh", "hinh anh")
                ):
                    score += 1.0
                if _looks_like_dense_table(sentence) and not _contains_any(sentence_norm, selection_factor_markers + followup_factor_markers):
                    score -= 2.4
            if "ghep than" in query_norm and _contains_any(sentence_norm, interaction_markers):
                score += 4.0

            if score >= 2.0:
                candidates.append((score, sentence, chunk.id, section_title))

    candidates.sort(key=lambda item: item[0], reverse=True)
    if asks_criteria:
        preferred = [
            item for item in candidates
            if _contains_any(item[1], criteria_markers) and not _contains_any(item[1], metric_markers)
        ]
        if preferred:
            candidates = preferred + [item for item in candidates if item not in preferred]
    if asks_why and "ghep than" in query_norm:
        preferred = [item for item in candidates if _contains_any(item[1], interaction_markers)]
        if preferred:
            candidates = preferred + [item for item in candidates if item not in preferred]
    if asks_group_factors:
        preferred = [
            item for item in candidates
            if _contains_any(item[1], selection_factor_markers + followup_factor_markers)
        ]
        if preferred:
            candidates = preferred + [item for item in candidates if item not in preferred]

    selected: list[ClaimEvidence] = []
    seen = set()
    for _score, sentence, chunk_id, section_title in candidates:
        norm = _normalize_text(sentence)
        if norm in seen:
            continue
        seen.add(norm)
        selected.append(
            ClaimEvidence(
                claim=sentence,
                support_text=sentence,
                supporting_span=sentence,
                chunk_id=chunk_id,
                section_title=section_title,
            )
        )
        if len(selected) >= 4:
            break

    def _append_required_claim(markers: tuple[str, ...]) -> None:
        if any(_contains_any(claim.claim, markers) for claim in selected):
            return
        for _score, sentence, chunk_id, section_title in candidates:
            if not _contains_any(sentence, markers):
                continue
            norm = _normalize_text(sentence)
            if norm in seen:
                continue
            seen.add(norm)
            selected.append(
                ClaimEvidence(
                    claim=sentence,
                    support_text=sentence,
                    supporting_span=sentence,
                    chunk_id=chunk_id,
                    section_title=section_title,
                )
            )
            break

    if asks_why and "ghep than" in query_norm:
        _append_required_claim(interaction_markers)
    if asks_group_factors:
        _append_required_claim(selection_factor_markers)
        _append_required_claim(followup_factor_markers)

    return selected


def _build_extractor_prompt(
    article: ArticleGroup,
    query: str,
) -> list:
    """Build LLM prompt for evidence extraction."""
    # Concatenate article chunks
    chunk_texts = []
    for i, chunk in enumerate(article.chunks):
        chunk_texts.append(f"[Chunk {i+1}]\n{chunk.text}")
    article_text = "\n\n---\n\n".join(chunk_texts)

    messages = [
        {"role": "system", "content": _EXTRACTOR_SYSTEM},
        {
            "role": "user",
            "content": (
                f"Câu hỏi cần trả lời: {query}\n\n"
                f"Tài liệu: {article.title}\n\n"
                f"Nội dung:\n{article_text}\n\n"
                f"Hãy trích xuất evidence pack JSON."
            ),
        },
    ]
    return messages


def _parse_extractor_response(
    raw_response: str,
    article: ArticleGroup,
) -> PrimaryEvidence:
    """Parse LLM JSON response into PrimaryEvidence. Falls back gracefully."""
    evidence = PrimaryEvidence(title=article.title)
    raw_text = "\n\n".join(c.text for c in article.chunks)
    evidence.raw_text = raw_text

    try:
        # Extract JSON from response (handle markdown code blocks)
        json_match = re.search(r'\{[\s\S]*\}', raw_response)
        if not json_match:
            return evidence
        data = json.loads(json_match.group())
    except (json.JSONDecodeError, AttributeError):
        return evidence

    evidence.source_type = data.get("source_type", "")

    # Simple fields
    for field_name in ["population", "sample_size", "design", "setting",
                       "intervention_or_exposure", "comparator"]:
        val = data.get(field_name)
        if val and val != "null":
            setattr(evidence, field_name, EvidenceField(text=str(val)))

    # List fields: outcomes
    for o in (data.get("outcomes") or []):
        if o and o != "null":
            evidence.outcomes.append(EvidenceField(text=str(o)))

    # Key findings — v1.5: support structured claim objects
    for f in (data.get("key_findings") or []):
        if isinstance(f, dict):
            claim_text = f.get("claim", "")
            if claim_text and claim_text != "null":
                chunk_idx = f.get("chunk_index", 0)
                chunk_id = ""
                section = ""
                if isinstance(chunk_idx, int) and chunk_idx < len(article.chunks):
                    chunk_id = article.chunks[chunk_idx].id
                    section = article.chunks[chunk_idx].metadata.get("section_title", "")
                evidence.key_findings.append(ClaimEvidence(
                    claim=str(claim_text),
                    supporting_span=str(f.get("supporting_span", "")),
                    chunk_id=chunk_id,
                    section_title=section,
                ))
        elif f and f != "null":
            # Backward compatible: plain string findings
            evidence.key_findings.append(ClaimEvidence(claim=str(f)))

    # Numbers
    for n in (data.get("numbers") or []):
        if isinstance(n, dict) and n.get("metric"):
            evidence.numbers.append(NumberEvidence(
                metric=n.get("metric", ""),
                value=str(n.get("value", "")),
                unit=n.get("unit", ""),
            ))

    # Limitations — v1.5: support structured claim objects
    for lim in (data.get("limitations") or []):
        if isinstance(lim, dict):
            claim_text = lim.get("claim", "")
            if claim_text and claim_text != "null":
                evidence.limitations.append(ClaimEvidence(
                    claim=str(claim_text),
                    supporting_span=str(lim.get("supporting_span", "")),
                ))
        elif lim and lim != "null":
            evidence.limitations.append(ClaimEvidence(claim=str(lim)))

    # Conclusion
    conc = data.get("conclusion")
    if conc and conc != "null":
        evidence.authors_conclusion = EvidenceField(text=str(conc))

    return evidence


def _build_simple_evidence(
    article: ArticleGroup,
    query: str = "",
    router_output: Optional[RouterOutput] = None,
) -> PrimaryEvidence:
    """Build evidence without LLM — raw text + regex number extraction + chunk provenance."""
    evidence = PrimaryEvidence(title=article.title)
    raw_text = "\n\n".join(c.text for c in article.chunks)
    evidence.raw_text = raw_text
    if query and router_output is not None:
        evidence.direct_answer_spans = _select_direct_answer_spans(article, query, router_output)
        evidence.key_findings = _select_query_focus_claims(article, query, router_output)

    # Regex extraction for common medical numbers
    num_patterns = [
        (r'n\s*=\s*(\d+)', 'sample_size'),
        (r'AUC\s*[=:]\s*([\d.]+)', 'AUC'),
        (r'OR\s*[=:]\s*([\d.]+)', 'OR'),
        (r'HR\s*[=:]\s*([\d.]+)', 'HR'),
        (r'RR\s*[=:]\s*([\d.]+)', 'RR'),
        (r'(\d+[.,]\d+\s*%)', 'percentage'),
        (r'sensitivity\s*[=:]\s*([\d.]+\s*%?)', 'sensitivity'),
        (r'specificity\s*[=:]\s*([\d.]+\s*%?)', 'specificity'),
        (r'p\s*[<>=]\s*([\d.]+)', 'p-value'),
    ]

    # v1.5: track which chunk each number came from
    for chunk in article.chunks:
        for pattern, metric in num_patterns:
            matches = re.findall(pattern, chunk.text, re.IGNORECASE)
            for val in matches:
                evidence.numbers.append(NumberEvidence(
                    metric=metric,
                    value=str(val),
                    support_text=chunk.id,  # store chunk_id in support_text
                ))

    # Sample size from regex (first chunk that has it)
    for chunk in article.chunks:
        n_match = re.search(r'n\s*=\s*(\d+)', chunk.text)
        if n_match:
            evidence.sample_size = EvidenceField(text=f"n={n_match.group(1)}")
            break

    return evidence


def extract_evidence(
    aggregated: AggregatedResult,
    query: str,
    router_output: RouterOutput,
    llm_client=None,
) -> EvidencePack:
    """
    Build evidence pack from aggregated articles.

    If router says needs_extractor=True AND llm_client is available,
    uses LLM for structured extraction.
    Otherwise, uses simple regex-based extraction.
    """
    pack = EvidencePack(query_type=router_output.query_type)

    if not aggregated.primary.chunks:
        pack.primary_source = PrimaryEvidence(title="")
        return pack

    # Primary source extraction
    if router_output.needs_extractor and llm_client is not None:
        # LLM extraction for deep queries
        try:
            messages = _build_extractor_prompt(aggregated.primary, query)
            max_tokens = int(os.getenv("EXTRACTOR_MAX_TOKENS", "800"))
            temperature = float(os.getenv("EXTRACTOR_TEMPERATURE", "0.1"))
            raw_response = llm_client.generate(
                messages,
                max_tokens=max_tokens,
                temperature=temperature,
                attempt_budget=int(os.getenv("EXTRACTOR_MAX_ATTEMPTS", "1")),
            )
            pack.primary_source = _parse_extractor_response(
                raw_response, aggregated.primary
            )
            if not pack.primary_source.direct_answer_spans:
                pack.primary_source.direct_answer_spans = _select_direct_answer_spans(
                    aggregated.primary,
                    query,
                    router_output,
                )
            if not pack.primary_source.key_findings:
                pack.primary_source.key_findings = _select_query_focus_claims(
                    aggregated.primary,
                    query,
                    router_output,
                )
            pack.extractor_used = True
        except Exception as exc:
            print(f"[EvidenceExtractor] LLM extraction failed: {exc}")
            pack.primary_source = _build_simple_evidence(
                aggregated.primary,
                query=query,
                router_output=router_output,
            )
    else:
        # Simple extraction for lightweight queries
        pack.primary_source = _build_simple_evidence(
            aggregated.primary,
            query=query,
            router_output=router_output,
        )

    # Secondary sources — always simple extraction
    for sec_art in aggregated.secondary:
        sec_evidence = _build_simple_evidence(sec_art)
        pack.secondary_sources.append(sec_evidence)

    return pack
