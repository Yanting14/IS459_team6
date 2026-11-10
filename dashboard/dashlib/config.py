"""Where the dashboard reads its data from.

DASH_SOURCE=athena (the real data path, and what the proposal promised): every table is read by
SQL on Amazon Athena. See ``athena/README.md`` for the one-time setup (workgroup, tables, queue table).

DASH_SOURCE=sample (the default) and DASH_SOURCE=local|s3 are the offline paths. They read files
through DuckDB, which is used for nothing else, and is not imported at all in athena mode.

For the file paths, four folders (or S3 prefixes) feed the dashboard. Each can be a local path or an
``s3://`` URI.

  queue    one BQ1 pipeline execution's published queues, one folder per scored day:
             date=<day>/accounts.csv, date=<day>/ranking.json
  metrics  the BQ1 evaluation files, copied unchanged from S3:
             models.json (LR and GBT), rules.json, rules_tuned.json
  curated  the Prepare job's curated zone, version prefix included:
             transfers/, transfer_labels/, rings/
  sharing  the BQ2 comparison table: comparison.csv, from bq2/ (the page skips section 2 without it)

Environment variables
  DASH_SOURCE        sample (default) | local | s3 | athena
  DASH_ATHENA_WORKGROUP  Athena workgroup (default is459-team6)
  DASH_ATHENA_DATABASE   Glue database holding the tables (default is459_team6)
  DASH_ATHENA_REGION     default us-east-1
  DASH_QUEUE_URI     athena only, optional: the S3 folder of the queue run, used for ranking.json (a small
                     JSON file, read directly from S3). By default it is read from the Glue table
                     queue_files, which each pipeline run re-points at its own queue.
  DASH_RULES_URI / DASH_RULES_TUNED_URI   athena only: the two rules baselines' output folders
  DASH_ROOT          one folder holding queue/, metrics/, curated/ and sharing/
  DASH_QUEUE_DIR     override the queue folder
  DASH_METRICS_DIR   override the metrics folder
  DASH_CURATED_DIR   override the curated folder
  DASH_SHARING_DIR   override the sharing folder
"""
from __future__ import annotations

import os
from pathlib import Path

HERE = Path(__file__).resolve().parent.parent

SOURCE = os.getenv("DASH_SOURCE", "sample").strip().lower()
IS_SAMPLE = SOURCE == "sample"
USES_ATHENA = SOURCE == "athena"

GOLD = "s3://is459-team6-gold-207567757101"
ATHENA_WORKGROUP = os.getenv("DASH_ATHENA_WORKGROUP", "is459-team6").strip()
ATHENA_DATABASE = os.getenv("DASH_ATHENA_DATABASE", "is459_team6").strip()
ATHENA_REGION = os.getenv("DASH_ATHENA_REGION", "us-east-1").strip()
QUEUE_URI = os.getenv("DASH_QUEUE_URI", "").strip().rstrip("/")   # empty: ask Glue (data.queue_uri)
RULES_URI = os.getenv("DASH_RULES_URI", f"{GOLD}/bq1/baseline/t4-20261108").strip().rstrip("/")
RULES_TUNED_URI = os.getenv("DASH_RULES_TUNED_URI", f"{GOLD}/bq1/baseline-tuned/20261109").strip().rstrip("/")

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
METRICS_DIR = _dir("DASH_METRICS_DIR", "metrics")
CURATED_DIR = _dir("DASH_CURATED_DIR", "curated")
SHARING_DIR = _dir("DASH_SHARING_DIR", "sharing")

USES_S3 = any(p.startswith("s3://") for p in (QUEUE_DIR, METRICS_DIR, CURATED_DIR, SHARING_DIR))
CONFIGURED = USES_ATHENA or bool(QUEUE_DIR and CURATED_DIR)

# Colours: reference palette slots 1 and 2 (blue, orange), validated as a pair, plus
# a neutral for everything that is not being called out.
BLUE = "#2a78d6"
ORANGE = "#eb6834"
NEUTRAL = "#8a8985"
