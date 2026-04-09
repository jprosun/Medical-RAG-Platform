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
from .article_aggregator import AggregatedResult, ArticleGroup


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


def _build_simple_evidence(article: ArticleGroup) -> PrimaryEvidence:
    """Build evidence without LLM — raw text + regex number extraction + chunk provenance."""
    evidence = PrimaryEvidence(title=article.title)
    raw_text = "\n\n".join(c.text for c in article.chunks)
    evidence.raw_text = raw_text

    # Regex extraction for common medical numbers
    num_patterns = [
        (r'n\s*=\s*(\d+)', 'sample_size'),
        (r'AUC\s*[=:]\s*([\d.]+)', 'AUC'),
        (r'OR\s*[=:]\s*([\d.]+)', 'OR'),
        (r'HR\s*[=:]\s*([\d.]+)', 'HR'),
        (r'RR\s*[=:]\s*([\d.]+)', 'RR'),
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
            )
            pack.primary_source = _parse_extractor_response(
                raw_response, aggregated.primary
            )
            pack.extractor_used = True
        except Exception as exc:
            print(f"[EvidenceExtractor] LLM extraction failed: {exc}")
            pack.primary_source = _build_simple_evidence(aggregated.primary)
    else:
        # Simple extraction for lightweight queries
        pack.primary_source = _build_simple_evidence(aggregated.primary)

    # Secondary sources — always simple extraction
    for sec_art in aggregated.secondary:
        sec_evidence = _build_simple_evidence(sec_art)
        pack.secondary_sources.append(sec_evidence)

    return pack
