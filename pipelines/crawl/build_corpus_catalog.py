from __future__ import annotations

import argparse
import json

from services.utils.crawl_manifest import build_corpus_catalog
from services.utils.data_paths import KNOWN_SOURCE_IDS


def main() -> None:
    parser = argparse.ArgumentParser(description="Build rag-data/corpus_catalog.csv from per-source manifests.")
    parser.add_argument("--source-id", action="append", default=[], dest="source_ids")
    args = parser.parse_args()

    source_ids = args.source_ids or list(KNOWN_SOURCE_IDS)
    report = build_corpus_catalog(source_ids=source_ids)
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

