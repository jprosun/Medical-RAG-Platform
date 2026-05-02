from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

from services.utils.data_lineage import make_run_id
from services.utils.data_paths import source_qa_dir

from .run_source import run_source


def _int_field(report: dict[str, object], key: str) -> int:
    value = report.get(key, 0)
    try:
        return int(value)  # type: ignore[arg-type]
    except Exception:
        return 0


def run_source_batches(
    *,
    source_id: str,
    batch_size: int,
    resume: bool = True,
    max_batches: int = 0,
    stop_after_idle_batches: int = 1,
    sleep_seconds: float = 0.0,
) -> dict[str, object]:
    if batch_size <= 0:
        raise ValueError("batch_size must be > 0")
    if stop_after_idle_batches <= 0:
        raise ValueError("stop_after_idle_batches must be > 0")

    run_id = make_run_id("crawl_batches", source_id)
    qa_dir = source_qa_dir(source_id)
    qa_dir.mkdir(parents=True, exist_ok=True)
    summary_path = qa_dir / f"{run_id}.json"

    batches: list[dict[str, object]] = []
    totals = {"downloaded": 0, "skipped": 0, "failed": 0}
    idle_batches = 0
    batch_index = 0

    while True:
        batch_index += 1
        report = run_source(
            source_id=source_id,
            resume=resume,
            max_items=batch_size,
        )
        downloaded = _int_field(report, "downloaded")
        skipped = _int_field(report, "skipped")
        failed = _int_field(report, "failed")

        batch_report = {
            "batch": batch_index,
            "source_id": source_id,
            "batch_size": batch_size,
            "report": report,
        }
        batches.append(batch_report)

        totals["downloaded"] += downloaded
        totals["skipped"] += skipped
        totals["failed"] += failed

        snapshot = {
            "run_id": run_id,
            "source_id": source_id,
            "batch_size": batch_size,
            "resume": resume,
            "max_batches": max_batches,
            "stop_after_idle_batches": stop_after_idle_batches,
            "totals": totals,
            "batches": batches,
        }
        summary_path.write_text(
            json.dumps(snapshot, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        print(
            json.dumps(
                {
                    "batch": batch_index,
                    "downloaded": downloaded,
                    "skipped": skipped,
                    "failed": failed,
                    "summary_path": str(summary_path),
                },
                ensure_ascii=False,
            ),
            flush=True,
        )

        if downloaded == 0:
            idle_batches += 1
        else:
            idle_batches = 0

        if max_batches and batch_index >= max_batches:
            break
        if idle_batches >= stop_after_idle_batches:
            break
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)

    return {
        "run_id": run_id,
        "source_id": source_id,
        "batch_size": batch_size,
        "batches": len(batches),
        "totals": totals,
        "summary_path": str(summary_path),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Run one source in repeated resume-safe crawl batches until idle.")
    parser.add_argument("--source-id", required=True)
    parser.add_argument("--batch-size", type=int, required=True)
    parser.add_argument("--resume", action="store_true", default=False)
    parser.add_argument("--max-batches", type=int, default=0)
    parser.add_argument("--stop-after-idle-batches", type=int, default=1)
    parser.add_argument("--sleep-seconds", type=float, default=0.0)
    args = parser.parse_args()

    report = run_source_batches(
        source_id=args.source_id,
        batch_size=args.batch_size,
        resume=args.resume,
        max_batches=args.max_batches,
        stop_after_idle_batches=args.stop_after_idle_batches,
        sleep_seconds=args.sleep_seconds,
    )
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
