"""Audit parity between canonical `rag-data/` records and legacy data outputs."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from services.utils.data_audit import audit_records, write_migration_audit
from services.utils.data_paths import KNOWN_DATASET_IDS, KNOWN_SOURCE_IDS


def main() -> None:
    parser = argparse.ArgumentParser(description="Audit canonical rag-data records against legacy outputs.")
    parser.add_argument("--source-id", action="append", dest="source_ids", default=[])
    parser.add_argument("--dataset-id", action="append", dest="dataset_ids", default=[])
    parser.add_argument("--all-known", action="store_true", help="Audit all known source and dataset IDs.")
    parser.add_argument("--output", default="", help="Output JSON path. Defaults to rag-data/qa/migration_audit.json")
    args = parser.parse_args()

    source_ids = tuple(args.source_ids)
    dataset_ids = tuple(args.dataset_ids)
    if args.all_known:
        source_ids = KNOWN_SOURCE_IDS
        dataset_ids = KNOWN_DATASET_IDS

    report = audit_records(source_ids=source_ids, dataset_ids=dataset_ids)
    target = write_migration_audit(report, args.output or None)
    print(json.dumps({"output": str(target), "sources": len(source_ids), "datasets": len(dataset_ids)}, indent=2))


if __name__ == "__main__":
    main()
