from __future__ import annotations

import argparse
import json
import time

from pipelines.etl.extract_gate import write_extract_gate_report
from services.utils.data_lineage import make_run_id
from services.utils.data_paths import source_qa_dir

from .extract_source import extract_source
from .run_source import run_source


def _int_field(report: dict[str, object], key: str) -> int:
    value = report.get(key, 0)
    try:
        return int(value)  # type: ignore[arg-type]
    except Exception:
        return 0


def run_repair_cycles(
    *,
    source_id: str,
    repair_batch_size: int,
    max_cycles: int = 0,
    stop_after_idle_cycles: int = 1,
    sleep_seconds: float = 0.0,
) -> dict[str, object]:
    if repair_batch_size <= 0:
        raise ValueError("repair_batch_size must be > 0")
    if stop_after_idle_cycles <= 0:
        raise ValueError("stop_after_idle_cycles must be > 0")

    run_id = make_run_id("repair_cycles", source_id)
    qa_dir = source_qa_dir(source_id)
    qa_dir.mkdir(parents=True, exist_ok=True)
    summary_path = qa_dir / f"{run_id}.json"

    cycles: list[dict[str, object]] = []
    totals = {
        "repair_downloaded": 0,
        "repair_failed": 0,
    }
    idle_cycles = 0
    cycle_index = 0

    while True:
        cycle_index += 1
        repair_report = run_source(
            source_id=source_id,
            resume=True,
            max_items=repair_batch_size,
            repair_missing_assets=True,
            repair_only=True,
        )
        extract_report = extract_source(source_id)
        gate_report = write_extract_gate_report(source_id)

        repair_downloaded = _int_field(repair_report, "repair_downloaded")
        repair_failed = _int_field(repair_report, "repair_failed")

        cycle_report = {
            "cycle": cycle_index,
            "source_id": source_id,
            "repair_batch_size": repair_batch_size,
            "repair_report": repair_report,
            "extract_report": extract_report,
            "gate_report": gate_report,
        }
        cycles.append(cycle_report)

        totals["repair_downloaded"] += repair_downloaded
        totals["repair_failed"] += repair_failed

        snapshot = {
            "run_id": run_id,
            "source_id": source_id,
            "repair_batch_size": repair_batch_size,
            "max_cycles": max_cycles,
            "stop_after_idle_cycles": stop_after_idle_cycles,
            "totals": totals,
            "cycles": cycles,
        }
        summary_path.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2), encoding="utf-8")

        print(
            json.dumps(
                {
                    "cycle": cycle_index,
                    "repair_downloaded": repair_downloaded,
                    "repair_failed": repair_failed,
                    "missing_assets": gate_report.get("missing_assets", 0),
                    "pending": gate_report.get("pending", 0),
                    "summary_path": str(summary_path),
                },
                ensure_ascii=False,
            ),
            flush=True,
        )

        if repair_downloaded == 0:
            idle_cycles += 1
        else:
            idle_cycles = 0

        if max_cycles and cycle_index >= max_cycles:
            break
        if idle_cycles >= stop_after_idle_cycles:
            break
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)

    return {
        "run_id": run_id,
        "source_id": source_id,
        "repair_batch_size": repair_batch_size,
        "cycles": len(cycles),
        "totals": totals,
        "summary_path": str(summary_path),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Run repeated repair -> extract -> gate cycles for one source.")
    parser.add_argument("--source-id", required=True)
    parser.add_argument("--repair-batch-size", type=int, required=True)
    parser.add_argument("--max-cycles", type=int, default=0)
    parser.add_argument("--stop-after-idle-cycles", type=int, default=1)
    parser.add_argument("--sleep-seconds", type=float, default=0.0)
    args = parser.parse_args()

    report = run_repair_cycles(
        source_id=args.source_id,
        repair_batch_size=args.repair_batch_size,
        max_cycles=args.max_cycles,
        stop_after_idle_cycles=args.stop_after_idle_cycles,
        sleep_seconds=args.sleep_seconds,
    )
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
