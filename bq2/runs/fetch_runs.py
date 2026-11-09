"""Download the scores of the BQ2 runs and write the manifest that `bq2.sharing compare` reads.

For each bank-view execution in jobs.tsv, and for the shared execution of each day, read the gold receipt,
check that it completed with the expected options, download its transfers.parquet, and record the local path.

Usage (from bq2/runs):
    python fetch_runs.py --jobs jobs.tsv --out downloads --manifest runs.json
    python -m bq2.sharing compare --curated <local curated> --manifest bq2/runs/runs.json --out-dir bq2/runs/sharing
"""
import argparse
import json
import os
import pathlib
import subprocess

EXECUTIONS = ("s3://is459-team6-gold-207567757101/ibm-aml/e2e-20261105-152512/scoring/executions")


def fetch(execution_id: str, out: pathlib.Path, bank: str | None, day: str, executions: str) -> str:
    """Download one run's receipt and transfers.parquet into out/<execution_id>/ and return the parquet path."""
    d = out / execution_id
    d.mkdir(parents=True, exist_ok=True)
    cp = ["aws", "s3", "cp", "--only-show-errors", "--region", os.environ.get("AWS_REGION", "us-east-1")]
    subprocess.run([*cp, f"{executions}/{execution_id}.json", str(d / "receipt.json")], check=True)
    r = json.loads((d / "receipt.json").read_text())
    opts = r["options"]
    if r["status"] != "completed" or (opts.get("bank") or None) != bank or opts["start"] != day:
        raise SystemExit(f"{execution_id}: unexpected receipt (status={r['status']}, options={opts})")
    for name in ("transfers.parquet", "ranking.json"):
        subprocess.run([*cp, r["artifacts"][name]["uri"], str(d / name)], check=True)
    return str((d / "transfers.parquet").resolve())


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--jobs", default="jobs.tsv", help="execution ids written by submit_batch.sh")
    ap.add_argument("--shared-id", default="bq2-shared-{day}", help="execution id of each day's shared run")
    ap.add_argument("--executions", default=EXECUTIONS, help="gold prefix that holds the receipts")
    ap.add_argument("--out", default="downloads")
    ap.add_argument("--manifest", default="runs.json")
    args = ap.parse_args()

    out = pathlib.Path(args.out)
    manifest: dict = {}
    for line in pathlib.Path(args.jobs).read_text().splitlines():
        execution_id = line.split("\t")[0]
        _, _, bank, day = execution_id.split("-", 3)  # bq2-bank-<bank>-<YYYY-MM-DD>
        if day not in manifest:
            shared = fetch(args.shared_id.format(day=day), out, None, day, args.executions)
            manifest[day] = {"shared": shared, "banks": {}}
        manifest[day]["banks"][bank] = fetch(execution_id, out, bank, day, args.executions)
        print("ok", execution_id)
    pathlib.Path(args.manifest).write_text(json.dumps(manifest, indent=1, sort_keys=True))
    print({day: len(m["banks"]) for day, m in manifest.items()})


if __name__ == "__main__":
    main()
