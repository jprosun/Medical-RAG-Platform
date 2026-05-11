from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from pipelines.crawl.extract_source import _processed_asset_stem
from pipelines.etl.source_groups import extract_gate_policy, is_medlineplus_multi_output
from services.utils.crawl_manifest import read_manifest
from services.utils.data_paths import source_processed_dir, source_qa_dir, source_records_path


def _unique_manifest_rows(rows: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    unique_rows: dict[str, dict[str, str]] = {}
    for row in rows:
        rel_path = (row.get("relative_path") or "").strip()
        if rel_path:
            unique_rows[rel_path] = row
    return unique_rows


def _status_counts(source_id: str) -> dict[str, int]:
    rows = read_manifest(source_id)
    unique_rows = _unique_manifest_rows(rows)
    counts = {
        "unique_assets": len(unique_rows),
        "done": 0,
        "failed": 0,
        "deferred": 0,
        "missing_assets": 0,
        "pending": 0,
    }
    for row in unique_rows.values():
        status = (row.get("extract_status") or "pending").strip() or "pending"
        if status == "done":
            counts["done"] += 1
        elif status == "failed":
            counts["failed"] += 1
        elif status == "deferred":
            counts["deferred"] += 1
        elif status == "missing_asset":
            counts["missing_assets"] += 1
        else:
            counts["pending"] += 1
    return counts


def _processed_file_count(source_id: str) -> int:
    processed_dir = source_processed_dir(source_id)
    if not processed_dir.exists():
        return 0
    return sum(1 for path in processed_dir.glob("*.txt") if path.is_file())


def _normalize_output_name(name: str) -> str:
    return name.lower() if os.name == "nt" else name


def _logical_done_output_count(source_id: str) -> int:
    rows = read_manifest(source_id)
    outputs: set[str] = set()
    for rel_path, row in _unique_manifest_rows(rows).items():
        if (row.get("extract_status") or "").strip() != "done":
            continue
        stem = _processed_asset_stem(rel_path, row.get("content_class", ""))
        outputs.add(_normalize_output_name(f"{stem}.txt"))
    return len(outputs)


def _record_count(source_id: str) -> int:
    path = source_records_path(source_id)
    if not path.exists():
        return 0
    with open(path, "r", encoding="utf-8") as fh:
        return sum(1 for raw in fh if raw.strip())


def _deferred_strategy_counts(source_id: str) -> dict[str, int]:
    rows = read_manifest(source_id)
    counts: dict[str, int] = {}
    for row in _unique_manifest_rows(rows).values():
        if (row.get("extract_status") or "").strip() != "deferred":
            continue
        strategy = (row.get("extract_strategy") or "").strip() or "deferred"
        counts[strategy] = counts.get(strategy, 0) + 1
    return dict(sorted(counts.items()))


def _missing_asset_breakdown(source_id: str) -> dict[str, int]:
    rows = read_manifest(source_id)
    groups: dict[str, list[dict[str, str]]] = {}
    for row in _unique_manifest_rows(rows).values():
        stem = Path((row.get("relative_path") or "").strip()).stem
        if stem:
            groups.setdefault(stem, []).append(row)

    stale_sibling_missing = 0
    needs_recrawl = 0
    for items in groups.values():
        has_done = any((item.get("extract_status") or "").strip() == "done" for item in items)
        for row in items:
            if (row.get("extract_status") or "").strip() != "missing_asset":
                continue
            if has_done:
                stale_sibling_missing += 1
            else:
                needs_recrawl += 1
    return {
        "stale_sibling_missing": stale_sibling_missing,
        "needs_recrawl": needs_recrawl,
    }


def evaluate_extract_gate(source_id: str) -> dict[str, Any]:
    policy = extract_gate_policy(source_id)
    counts = _status_counts(source_id)
    processed_files = _processed_file_count(source_id)
    logical_done_outputs = _logical_done_output_count(source_id)
    records_count = _record_count(source_id)
    missing_breakdown = _missing_asset_breakdown(source_id)
    deferred_counts = _deferred_strategy_counts(source_id)
    allowed_deferred_strategies = set(policy["allowed_deferred_strategies"])
    unexpected_deferred_assets = 0
    if policy["enforce_deferred_strategy_allowlist"]:
        unexpected_deferred_assets = sum(
            count for strategy, count in deferred_counts.items() if strategy not in allowed_deferred_strategies
        )

    extract_health_passed = False
    extract_health_reason = ""
    done_only_etl_allowed = False
    if is_medlineplus_multi_output(source_id):
        extract_health_passed = counts["done"] > 0 and (processed_files > 0 or records_count > 0)
        extract_health_reason = "medlineplus_multi_output_ok" if extract_health_passed else "missing_records_or_processed"
    else:
        if counts["pending"] > 0:
            extract_health_reason = "pending_assets_remaining"
        elif counts["missing_assets"] > 0 and policy["allow_stale_missing_only"] and missing_breakdown["needs_recrawl"] == 0:
            extract_health_reason = "stale_missing_asset_rows_present"
        elif counts["missing_assets"] > 0:
            extract_health_reason = "missing_assets_present"
        elif unexpected_deferred_assets > 0:
            extract_health_reason = "unexpected_deferred_assets"
        elif policy["max_failed_assets"] is not None and counts["failed"] > int(policy["max_failed_assets"]):
            extract_health_reason = "failed_assets_over_limit"
        elif processed_files != logical_done_outputs:
            extract_health_reason = "processed_manifest_mismatch"
        elif counts["done"] == 0 and policy["allow_done_zero_if_only_deferred"] and counts["deferred"] > 0:
            extract_health_passed = True
            extract_health_reason = "backlog_only_deferred_assets"
        elif counts["done"] == 0:
            extract_health_reason = "no_done_assets"
        else:
            extract_health_passed = True
            extract_health_reason = "ok"

    if (
        counts["done"] > 0
        and counts["pending"] == 0
        and unexpected_deferred_assets == 0
        and processed_files == logical_done_outputs
        and (policy["max_failed_assets"] is None or counts["failed"] <= int(policy["max_failed_assets"]))
        and (
            counts["missing_assets"] == 0
            or bool(policy.get("allow_done_only_etl_with_missing"))
        )
    ):
        done_only_etl_allowed = True

    partition_release_allowed = bool(
        policy.get("allow_partition_release_with_backlog")
        and done_only_etl_allowed
        and missing_breakdown["stale_sibling_missing"] == 0
    )

    gate_passed = False
    gate_reason = extract_health_reason
    if extract_health_passed or partition_release_allowed:
        if policy["block_article_batch"]:
            gate_reason = "source_excluded_from_article_batch"
        else:
            gate_passed = True
            gate_reason = "article_partition_backlog_isolated" if partition_release_allowed and not extract_health_passed else extract_health_reason

    quality_gate_status = "go" if gate_passed else ("review" if extract_health_passed or partition_release_allowed else "hold")

    report = {
        "source_id": source_id,
        "source_gate_profile": policy["profile"],
        **counts,
        "processed_files": processed_files,
        "logical_done_outputs": logical_done_outputs,
        "records_count": records_count,
        "deferred_strategy_counts": deferred_counts,
        "allowed_deferred_strategies": sorted(allowed_deferred_strategies),
        "unexpected_deferred_assets": unexpected_deferred_assets,
        "extract_health_passed": extract_health_passed,
        "extract_health_reason": extract_health_reason,
        "partition_release_allowed": partition_release_allowed,
        "gate_passed": gate_passed,
        "gate_reason": gate_reason,
        "quality_gate_status": quality_gate_status,
        "article_batch_blocked": bool(policy["block_article_batch"]),
        "missing_asset_breakdown": missing_breakdown,
        "etl_done_only_allowed": done_only_etl_allowed,
    }
    return report


def write_extract_gate_report(source_id: str) -> dict[str, Any]:
    report = evaluate_extract_gate(source_id)
    qa_dir = source_qa_dir(source_id)
    qa_dir.mkdir(parents=True, exist_ok=True)
    out_path = qa_dir / "extract_gate.json"
    out_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    return report
