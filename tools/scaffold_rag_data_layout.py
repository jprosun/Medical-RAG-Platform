from __future__ import annotations

import argparse
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from services.utils.data_paths import (  # noqa: E402
    DEFAULT_EMBEDDING_PROFILE,
    KNOWN_DATASET_IDS,
    KNOWN_SOURCE_IDS,
    ensure_rag_data_layout,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Create the canonical rag-data directory scaffold.")
    parser.add_argument(
        "--sources",
        nargs="*",
        default=list(KNOWN_SOURCE_IDS),
        help="Optional list of source IDs to scaffold.",
    )
    parser.add_argument(
        "--datasets",
        nargs="*",
        default=list(KNOWN_DATASET_IDS),
        help="Optional list of dataset IDs to scaffold.",
    )
    parser.add_argument(
        "--profiles",
        nargs="*",
        default=[DEFAULT_EMBEDDING_PROFILE],
        help="Embedding profile directories to scaffold for each dataset.",
    )
    args = parser.parse_args()

    created = ensure_rag_data_layout(args.sources, args.datasets, args.profiles)
    print(f"Created/ensured {len(created)} directories:")
    for path in created:
        print(f"  - {path}")


if __name__ == "__main__":
    main()
