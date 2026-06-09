"""Export selected Hugging Face medical datasets to DocumentRecord JSONL.

The tool intentionally adapts data into the existing RAG corpus schema instead
of changing the ingest/chunking pipeline.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Iterable

REPO_ROOT = Path(__file__).resolve().parents[1]
INGESTOR_ROOT = REPO_ROOT / "services" / "qdrant-ingestor"
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(INGESTOR_ROOT))

from app.document_schema import DocumentRecord
from services.utils.data_lineage import file_sha256
from services.utils.data_paths import source_intermediate_dir, source_qa_dir, source_records_path


DATASETS: dict[str, dict[str, str]] = {
    "medquad": {
        "hf_dataset": "lavita/MedQuAD",
        "config": "default",
        "split": "train",
        "default_source": "hf_medquad",
    },
    "mtue29": {
        "hf_dataset": "mtue29/vietnamese-medical-dataset",
        "config": "default",
        "split": "train",
        "default_source": "hf_vi_mtue29_medical",
    },
    "ynguyen1010": {
        "hf_dataset": "ynguyen1010/medical_vietnamese_datasets",
        "config": "cleaned_format",
        "split": "train",
        "default_source": "hf_vi_ynguyen_medical",
    },
}

DATASET_VIEWER_BASE = "https://datasets-server.huggingface.co"
USER_AGENT = "MedQA-RAG-HF-ETL/1.0"
MEDICAL_TERMS_VI = (
    "bệnh",
    "triệu chứng",
    "điều trị",
    "chẩn đoán",
    "bác sĩ",
    "sức khỏe",
    "thuốc",
    "phẫu thuật",
    "xét nghiệm",
    "viêm",
    "ung thư",
    "đau",
    "nhiễm",
    "cơ thể",
    "bệnh viện",
)


def _utc_run_id(prefix: str) -> str:
    return f"{prefix}_{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}"


def _hash16(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:16]


def _normalize_ws(text: Any) -> str:
    if text is None:
        return ""
    value = str(text).replace("\r\n", "\n").replace("\r", "\n")
    value = re.sub(r"[ \t]+", " ", value)
    value = re.sub(r"\n{3,}", "\n\n", value)
    return value.strip()


def _dedup_key(*parts: str) -> str:
    joined = "\n".join(parts)
    joined = re.sub(r"\s+", " ", joined).casefold().strip()
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def _safe_specialty(value: str, default: str = "general") -> str:
    value = _normalize_ws(value).lower()
    if not value:
        return default
    value = value.replace("/", "_").replace("-", "_")
    value = re.sub(r"[^0-9a-zA-Z_à-ỹÀ-Ỹ]+", "_", value)
    value = re.sub(r"_+", "_", value).strip("_")
    return value[:80] or default


def _contains_think_or_chat_artifact(text: str) -> bool:
    lowered = text.lower()
    return any(marker in lowered for marker in ("<think>", "</think>", "role\":\"assistant", "role\":\"user"))


def _collapse_consecutive_duplicate_sentences(text: str) -> tuple[str, int]:
    parts = re.split(r"(?<=[.!?。！？])\s+", text)
    collapsed: list[str] = []
    removed = 0
    last_key = ""
    for part in parts:
        clean = part.strip()
        if not clean:
            continue
        key = re.sub(r"\s+", " ", clean).casefold()
        if key and key == last_key:
            removed += 1
            continue
        collapsed.append(clean)
        last_key = key
    return " ".join(collapsed).strip(), removed


def _split_letter_noise_ratio(text: str) -> float:
    tokens = re.findall(r"\b[\wÀ-ỹà-ỹ]+\b", text)
    if not tokens:
        return 0.0
    single_ascii = sum(1 for token in tokens if len(token) == 1 and token.isascii() and token.isalpha())
    return single_ascii / max(len(tokens), 1)


def _is_medical_vi(title: str, body: str) -> bool:
    text = f"{title}\n{body}".casefold()
    return any(term in text for term in MEDICAL_TERMS_VI)


def _request_json(url: str) -> dict[str, Any]:
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(request, timeout=60) as response:
        return json.loads(response.read().decode("utf-8"))


def _download(url: str, target: Path, expected_size: int = 0, overwrite: bool = False) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists() and not overwrite:
        if not expected_size or target.stat().st_size == expected_size:
            return

    tmp = target.with_suffix(target.suffix + ".tmp")
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(request, timeout=120) as response, open(tmp, "wb") as out:
        while True:
            chunk = response.read(1024 * 1024)
            if not chunk:
                break
            out.write(chunk)
    tmp.replace(target)


def _parquet_files(dataset_name: str, config: str, split: str) -> list[dict[str, Any]]:
    url = f"{DATASET_VIEWER_BASE}/parquet?dataset={urllib.parse.quote(dataset_name, safe='/')}"
    payload = _request_json(url)
    files = []
    for item in payload.get("parquet_files", []):
        if item.get("config") == config and item.get("split") == split:
            files.append(item)
    if not files:
        raise RuntimeError(f"No parquet files found for {dataset_name}/{config}/{split}")
    return files


def _iter_parquet_rows(paths: Iterable[Path], columns: list[str], batch_size: int) -> Iterable[dict[str, Any]]:
    try:
        import pyarrow.parquet as pq
    except ImportError as exc:  # pragma: no cover - depends on local env
        raise RuntimeError("pyarrow is required. Install it with: python -m pip install pyarrow") from exc

    for path in paths:
        parquet = pq.ParquetFile(path)
        for batch in parquet.iter_batches(batch_size=batch_size, columns=columns):
            for row in batch.to_pylist():
                yield row


def _make_record(
    *,
    doc_id: str,
    title: str,
    body: str,
    source_name: str,
    source_id: str,
    article_id: str,
    canonical_title: str,
    section_title: str,
    source_url: str = "",
    source_file: str = "",
    doc_type: str = "patient_education",
    specialty: str = "general",
    audience: str = "patient",
    language: str = "vi",
    trust_tier: int = 3,
    tags: list[str] | None = None,
    etl_run_id: str = "",
    quality_score: int = 80,
    quality_flags: list[str] | None = None,
) -> DocumentRecord:
    return DocumentRecord(
        doc_id=doc_id,
        title=title,
        body=body,
        source_name=source_name,
        section_title=section_title,
        source_url=source_url,
        source_id=source_id,
        source_file=source_file,
        article_id=article_id,
        source_sha256=_hash16(body),
        etl_run_id=etl_run_id,
        doc_type=doc_type,
        specialty=specialty,
        audience=audience,
        language=language,
        canonical_title=canonical_title,
        language_confidence=1.0,
        is_mixed_language=False,
        trust_tier=trust_tier,
        quality_score=quality_score,
        quality_status="go",
        quality_flags=quality_flags or ["hf_import"],
        tags=tags or [],
        heading_path=f"{canonical_title} > {section_title}" if section_title else canonical_title,
    )


def _medquad_record(row: dict[str, Any], source_id: str, source_file: str, etl_run_id: str) -> DocumentRecord | None:
    document_id = _normalize_ws(row.get("document_id"))
    question_id = _normalize_ws(row.get("question_id"))
    question_focus = _normalize_ws(row.get("question_focus"))
    question_type = _normalize_ws(row.get("question_type"))
    question = _normalize_ws(row.get("question"))
    answer = _normalize_ws(row.get("answer"))
    if len(answer) < 20 or _contains_think_or_chat_artifact(answer):
        return None

    title = question_focus or question[:120] or document_id or "MedQuAD medical topic"
    section_title = question_type or question[:120] or "medical question"
    body = f"Question: {question}\n\nAnswer: {answer}" if question else answer
    source = _normalize_ws(row.get("document_source")) or "MedQuAD"
    category = _normalize_ws(row.get("category"))
    semantic_group = _normalize_ws(row.get("umls_semantic_group"))
    raw_doc_key = document_id or title
    raw_row_key = f"{raw_doc_key}|{question_id or question}|{answer[:120]}"

    tags = ["huggingface", "medquad"]
    for value in (source, category, question_type, semantic_group):
        if value:
            tags.append(value)

    return _make_record(
        doc_id=f"{source_id}_{_hash16(raw_row_key)}",
        title=title,
        body=body,
        source_name=f"MedQuAD / {source}",
        source_id=source_id,
        source_file=source_file,
        article_id=f"{source_id}_{_hash16(raw_doc_key)}",
        canonical_title=title,
        section_title=section_title,
        source_url=_normalize_ws(row.get("document_url")),
        doc_type="patient_education",
        specialty=_safe_specialty(category),
        language="en",
        trust_tier=2,
        tags=tags,
        etl_run_id=etl_run_id,
        quality_score=95,
    )


def _mtue29_record(row: dict[str, Any], source_id: str, source_file: str, etl_run_id: str) -> DocumentRecord | None:
    meta = row.get("meta") or {}
    if not isinstance(meta, dict):
        meta = {}
    title = _normalize_ws(meta.get("title"))
    section_title = _normalize_ws(row.get("anchor"))
    body = _normalize_ws(row.get("positive"))
    if not title or len(body) < 120 or _contains_think_or_chat_artifact(body):
        return None
    category = _normalize_ws(meta.get("category"))
    original_article_id = _normalize_ws(meta.get("article_id"))
    article_id = f"{source_id}_{_hash16(title)}"
    doc_key = f"{title}|{section_title}|{body[:200]}"
    tags = ["huggingface", "vietnamese-medical-dataset"]
    if category:
        tags.append(category)
    if original_article_id:
        tags.append(f"hf_article_id:{original_article_id}")

    return _make_record(
        doc_id=f"{source_id}_{_hash16(doc_key)}",
        title=title,
        body=body,
        source_name="Hugging Face mtue29/vietnamese-medical-dataset",
        source_id=source_id,
        source_file=source_file,
        article_id=article_id,
        canonical_title=title,
        section_title=section_title,
        source_url="",
        doc_type="patient_education",
        specialty=_safe_specialty(category),
        language="vi",
        trust_tier=3,
        tags=tags,
        etl_run_id=etl_run_id,
        quality_score=82,
    )


def _ynguyen_record(
    row: dict[str, Any],
    source_id: str,
    source_file: str,
    etl_run_id: str,
    min_body_chars: int,
    max_split_letter_ratio: float,
    max_duplicate_sentence_ratio: float,
) -> tuple[DocumentRecord | None, str]:
    title = _normalize_ws(row.get("question_cleaned"))
    body = _normalize_ws(row.get("answer_cleaned"))
    if not title or len(body) < min_body_chars:
        return None, "short_or_missing"
    if _contains_think_or_chat_artifact(body):
        return None, "chat_artifact"
    if not _is_medical_vi(title, body):
        return None, "non_medical"
    if _split_letter_noise_ratio(body) > max_split_letter_ratio:
        return None, "split_letter_noise"

    collapsed, removed_duplicates = _collapse_consecutive_duplicate_sentences(body)
    sentence_count = max(len(re.split(r"(?<=[.!?。！？])\s+", body)), 1)
    duplicate_ratio = removed_duplicates / sentence_count
    if duplicate_ratio > max_duplicate_sentence_ratio:
        return None, "duplicate_sentence_noise"
    body = collapsed or body

    doc_key = f"{title}|{body[:240]}"
    record = _make_record(
        doc_id=f"{source_id}_{_hash16(doc_key)}",
        title=title,
        body=body,
        source_name="Hugging Face ynguyen1010/medical_vietnamese_datasets",
        source_id=source_id,
        source_file=source_file,
        article_id=f"{source_id}_{_hash16(title)}",
        canonical_title=title,
        section_title=title,
        source_url="",
        doc_type="patient_education",
        specialty="general",
        language="vi",
        trust_tier=3,
        tags=["huggingface", "medical_vietnamese_datasets"],
        etl_run_id=etl_run_id,
        quality_score=78,
        quality_flags=["hf_import", "filtered"],
    )
    return record, ""


def export_dataset(args: argparse.Namespace) -> dict[str, Any]:
    dataset_cfg = DATASETS[args.dataset]
    output_source = args.output_source or dataset_cfg["default_source"]
    records_path = source_records_path(output_source)
    qa_dir = source_qa_dir(output_source)
    parquet_dir = source_intermediate_dir(output_source, "hf_parquet")
    records_path.parent.mkdir(parents=True, exist_ok=True)
    qa_dir.mkdir(parents=True, exist_ok=True)
    parquet_dir.mkdir(parents=True, exist_ok=True)

    if records_path.exists() and not args.overwrite:
        raise SystemExit(f"Refusing to overwrite {records_path}. Use --overwrite if intentional.")

    etl_run_id = _utc_run_id(f"hf_{args.dataset}")
    parquet_items = _parquet_files(dataset_cfg["hf_dataset"], dataset_cfg["config"], dataset_cfg["split"])
    parquet_paths: list[Path] = []
    for item in parquet_items:
        filename = f"{dataset_cfg['config']}__{dataset_cfg['split']}__{item['filename']}"
        target = parquet_dir / filename
        print(f"[download] {item['url']} -> {target}", flush=True)
        _download(item["url"], target, expected_size=int(item.get("size") or 0), overwrite=args.refresh_downloads)
        parquet_paths.append(target)

    columns_by_dataset = {
        "medquad": [
            "document_id",
            "document_source",
            "document_url",
            "category",
            "umls_semantic_group",
            "question_id",
            "question_focus",
            "question_type",
            "question",
            "answer",
        ],
        "mtue29": ["anchor", "positive", "meta"],
        "ynguyen1010": ["question_cleaned", "answer_cleaned"],
    }

    seen: set[str] = set()
    stats: dict[str, Any] = {
        "dataset": args.dataset,
        "hf_dataset": dataset_cfg["hf_dataset"],
        "config": dataset_cfg["config"],
        "split": dataset_cfg["split"],
        "output_source": output_source,
        "records_path": str(records_path),
        "etl_run_id": etl_run_id,
        "input_rows": 0,
        "written_records": 0,
        "duplicates_skipped": 0,
        "filtered": {},
        "validation_errors": 0,
        "parquet_files": [str(path) for path in parquet_paths],
    }

    with open(records_path, "w", encoding="utf-8") as out:
        for row in _iter_parquet_rows(parquet_paths, columns_by_dataset[args.dataset], args.batch_size):
            stats["input_rows"] += 1
            if args.limit and stats["input_rows"] > args.limit:
                break

            source_file = str(parquet_paths[min(len(parquet_paths) - 1, 0)].relative_to(REPO_ROOT))
            record: DocumentRecord | None
            filtered_reason = ""
            if args.dataset == "medquad":
                record = _medquad_record(row, output_source, source_file, etl_run_id)
                filtered_reason = "quality_filter" if record is None else ""
            elif args.dataset == "mtue29":
                record = _mtue29_record(row, output_source, source_file, etl_run_id)
                filtered_reason = "quality_filter" if record is None else ""
            else:
                record, filtered_reason = _ynguyen_record(
                    row,
                    output_source,
                    source_file,
                    etl_run_id,
                    args.min_body_chars,
                    args.max_split_letter_ratio,
                    args.max_duplicate_sentence_ratio,
                )

            if record is None:
                reason = filtered_reason or "quality_filter"
                stats["filtered"][reason] = stats["filtered"].get(reason, 0) + 1
                continue

            key = _dedup_key(record.title, record.section_title, record.body)
            if key in seen:
                stats["duplicates_skipped"] += 1
                continue
            seen.add(key)

            validation = record.validate()
            if validation:
                stats["validation_errors"] += len(validation)
                stats["filtered"]["validation_error"] = stats["filtered"].get("validation_error", 0) + 1
                continue

            out.write(record.to_jsonl_line() + "\n")
            stats["written_records"] += 1
            if stats["written_records"] % args.progress_every == 0:
                print(
                    f"[progress] input={stats['input_rows']} written={stats['written_records']} "
                    f"duplicates={stats['duplicates_skipped']}",
                    flush=True,
                )

    stats["records_sha256"] = file_sha256(records_path)
    summary_path = qa_dir / f"hf_export_summary_{args.dataset}.json"
    with open(summary_path, "w", encoding="utf-8") as fh:
        json.dump(stats, fh, ensure_ascii=False, indent=2)
    stats["summary_path"] = str(summary_path)
    print(json.dumps(stats, ensure_ascii=False, indent=2))
    if stats["validation_errors"]:
        raise SystemExit(2)
    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description="Export Hugging Face medical datasets to DocumentRecord JSONL.")
    parser.add_argument("--dataset", choices=sorted(DATASETS), required=True)
    parser.add_argument("--output-source", default="", help="Source ID under rag-data/sources/")
    parser.add_argument("--batch-size", type=int, default=5000)
    parser.add_argument("--limit", type=int, default=0, help="Optional input row limit for smoke tests.")
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--refresh-downloads", action="store_true")
    parser.add_argument("--min-body-chars", type=int, default=500)
    parser.add_argument("--max-split-letter-ratio", type=float, default=0.08)
    parser.add_argument("--max-duplicate-sentence-ratio", type=float, default=0.35)
    parser.add_argument("--progress-every", type=int, default=25000)
    args = parser.parse_args()

    try:
        export_dataset(args)
    except urllib.error.URLError as exc:
        raise SystemExit(f"Network error while reading Hugging Face dataset: {exc}") from exc


if __name__ == "__main__":
    main()
