from __future__ import annotations

import argparse
import json

from .extract_source import extract_source
from .run_source import run_source
from .source_registry import SOURCE_REGISTRY, source_ids_for_wave


def run_wave(
    *,
    wave: int,
    resume: bool = True,
    max_items: int = 0,
    extract: bool = True,
) -> dict[str, object]:
    source_ids = source_ids_for_wave(wave)
    if not source_ids:
        raise ValueError(f"No sources registered for wave {wave}")

    crawl_reports: list[dict[str, object]] = []
    extract_reports: list[dict[str, object]] = []

    for source_id in source_ids:
        crawl_reports.append(
            run_source(
                source_id=source_id,
                resume=resume,
                max_items=max_items,
            )
        )
        if extract:
            extract_reports.append(extract_source(source_id))

    return {
        "wave": wave,
        "sources": source_ids,
        "crawl_reports": crawl_reports,
        "extract_reports": extract_reports,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a crawl rollout wave sequentially from simple to complex sources.")
    parser.add_argument("--wave", type=int, required=True, choices=sorted({config.wave for config in SOURCE_REGISTRY.values()}))
    parser.add_argument("--resume", action="store_true", default=False)
    parser.add_argument("--max-items", type=int, default=0)
    parser.add_argument("--no-extract", action="store_true", default=False)
    args = parser.parse_args()

    report = run_wave(
        wave=args.wave,
        resume=args.resume,
        max_items=args.max_items,
        extract=not args.no_extract,
    )
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
