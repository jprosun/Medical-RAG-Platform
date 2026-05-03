from __future__ import annotations

import argparse
import json

from .run_source import run_source


DEFAULT_REFERENCE_SOURCES = [
    "uspstf_recommendations",
    "nccih_health",
    "nci_pdq",
    "vaac_hiv_aids",
    "vien_dinh_duong",
    "vncdc_documents",
    "nice_guidance",
]

REFERENCE_SOURCE_BATCHES = {
    "easy": [
        "uspstf_recommendations",
        "nccih_health",
        "nci_pdq",
        "vaac_hiv_aids",
        "vien_dinh_duong",
    ],
    "vietnam": [
        "vaac_hiv_aids",
        "vien_dinh_duong",
        "vncdc_documents",
    ],
    "broad": [
        "nice_guidance",
    ],
    "all": list(DEFAULT_REFERENCE_SOURCES),
}


def batch_source_ids(batch: str | None) -> list[str] | None:
    if not batch:
        return None
    if batch not in REFERENCE_SOURCE_BATCHES:
        raise ValueError(f"Unknown reference source batch: {batch}")
    return list(REFERENCE_SOURCE_BATCHES[batch])


def extract_source(source_id: str) -> dict[str, object]:
    from .extract_source import extract_source as _extract_source

    return _extract_source(source_id)


def run_reference_concept_sources(
    *,
    source_ids: list[str] | None = None,
    batch: str | None = None,
    resume: bool = True,
    max_items: int = 0,
    extract: bool = False,
) -> dict[str, object]:
    selected = source_ids or batch_source_ids(batch) or list(DEFAULT_REFERENCE_SOURCES)
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
    parser = argparse.ArgumentParser(description="Run crawl for guideline/reference/basic concept medical sources.")
    parser.add_argument("--sources", nargs="*", default=None, help="Optional subset of source_ids to crawl.")
    parser.add_argument("--batch", choices=sorted(REFERENCE_SOURCE_BATCHES), default=None, help="Named batch preset, e.g. easy, vietnam, broad, all.")
    parser.add_argument("--resume", action="store_true", default=False)
    parser.add_argument("--max-items", type=int, default=0)
    parser.add_argument("--extract", action="store_true", default=False)
    args = parser.parse_args()

    report = run_reference_concept_sources(
        source_ids=args.sources,
        batch=args.batch,
        resume=args.resume,
        max_items=args.max_items,
        extract=args.extract,
    )
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
