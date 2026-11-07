"""Where the dashboard reads its data from.

Three folders (or S3 prefixes) feed the dashboard. Each can be a local path or an
``s3://`` URI, and each is read through DuckDB, so both behave the same way.

  queue    one scoring run's published files:
             accounts.csv, transfers.parquet, ranking.json, evaluation.json (optional)
  curated  the Prepare job's curated zone, version prefix included:
             transfers/, transfer_labels/, rings/
  sharing  the BQ2 comparison table: comparison.csv (optional until it exists)

Environment variables
  DASH_SOURCE        sample (default) | local | s3  -- only changes the banner and defaults
  DASH_ROOT          one folder holding queue/, curated/ and sharing/
  DASH_QUEUE_DIR     override the queue folder
  DASH_CURATED_DIR   override the curated folder
  DASH_SHARING_DIR   override the sharing folder
"""
from __future__ import annotations

import os
from pathlib import Path

HERE = Path(__file__).resolve().parent.parent

SOURCE = os.getenv("DASH_SOURCE", "sample").strip().lower()
IS_SAMPLE = SOURCE == "sample"

if IS_SAMPLE:
    ROOT = (HERE / "sample_data").as_posix()
else:
    ROOT = os.getenv("DASH_ROOT", "").strip()


def _clean(path: str) -> str:
    return path.replace("\\", "/").rstrip("/")


def _dir(env: str, sub: str) -> str:
    explicit = os.getenv(env, "").strip()
    if explicit and not IS_SAMPLE:
        return _clean(explicit)
    return _clean(f"{ROOT}/{sub}") if ROOT else ""


QUEUE_DIR = _dir("DASH_QUEUE_DIR", "queue")
CURATED_DIR = _dir("DASH_CURATED_DIR", "curated")
SHARING_DIR = _dir("DASH_SHARING_DIR", "sharing")

USES_S3 = any(p.startswith("s3://") for p in (QUEUE_DIR, CURATED_DIR, SHARING_DIR))
CONFIGURED = bool(QUEUE_DIR and CURATED_DIR)

# Colours: reference palette slots 1 and 2 (blue, orange), validated as a pair, plus
# a neutral for everything that is not being called out.
BLUE = "#2a78d6"
ORANGE = "#eb6834"
NEUTRAL = "#8a8985"
