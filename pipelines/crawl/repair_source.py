from __future__ import annotations

import argparse
import json

from services.utils.crawl_manifest import build_corpus_catalog, bootstrap_source_manifest, read_manifest, write_manifest
from services.utils.data_paths import ensure_rag_data_layout

from .run_source import _repair_vmj_ojs_rows


def repair_source(source_id: str) -> dict[str, int | str]:
    ensure_rag_data_layout([source_id])
    bootstrap_source_manifest(source_id)
    rows = read_manifest(source_id)

    repaired = 0
    if source_id == "vmj_ojs":
        repaired = _repair_vmj_ojs_rows(rows)

    write_manifest(source_id, rows)
    build_corpus_catalog()
    return {"source_id": source_id, "repaired": repaired}


def main() -> None:
    parser = argparse.ArgumentParser(description="Run source-specific manifest/raw repair without crawling.")
    parser.add_argument("--source-id", required=True)
    args = parser.parse_args()
    print(json.dumps(repair_source(args.source_id), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
