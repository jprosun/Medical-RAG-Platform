"""
Compatibility package for legacy `etl.*` imports.

The ETL implementation now lives under `pipelines.etl`. We point this package's
search path at the new location first so old commands such as
`python -m etl.normalize_all` continue to work while callers migrate.
"""

from __future__ import annotations

from pathlib import Path


_CURRENT_DIR = Path(__file__).resolve().parent
_PIPELINES_ETL_DIR = Path(__file__).resolve().parents[3] / "pipelines" / "etl"

__path__ = [str(_PIPELINES_ETL_DIR), str(_CURRENT_DIR)]
