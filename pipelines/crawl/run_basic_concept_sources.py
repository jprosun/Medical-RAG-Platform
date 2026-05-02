from __future__ import annotations

import argparse
import json

from .run_source import run_source


DEFAULT_BASIC_SOURCES = [
    "medlineplus",
    "who",
    "ncbi_bookshelf",
    "nhs_health_a_z",
    "msd_manual_consumer",
    "msd_manual_professional",
    "cdc_health_topics",
    "mayo_diseases_conditions",
]


def extract_source(source_id: str) -> dict[str, object]:
    from .extract_source import extract_source as _extract_source

    return _extract_source(source_id)


def run_basic_concept_sources(
    *,
    source_ids: list[str] | None = None,
    resume: bool = True,
    max_items: int = 0,
    extract: bool = False,
) -> dict[str, object]:
    selected = source_ids or list(DEFAULT_BASIC_SOURCES)
    crawl_reports: list[dict[str, object]] = []
    extract_reports: list[dict[str, object]] = []

    for source_id in selected:
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
        "sources": selected,
        "crawl_reports": crawl_reports,
        "extract_reports": extract_reports,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Run crawl for foundational/basic concept medical sources.")
    parser.add_argument("--sources", nargs="*", default=None, help="Optional subset of source_ids to crawl.")
    parser.add_argument("--resume", action="store_true", default=False)
    parser.add_argument("--max-items", type=int, default=0)
    parser.add_argument("--extract", action="store_true", default=False)
    args = parser.parse_args()

    report = run_basic_concept_sources(
        source_ids=args.sources,
        resume=args.resume,
        max_items=args.max_items,
        extract=args.extract,
    )
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
