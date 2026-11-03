"""Clean the raw zone's transfers into the curated zone.

Runs as a Spark job on EMR Serverless, and locally in tests. Locations may be S3 URIs
or local folders. Safe to re-run: if the success marker exists under the version
prefix the job does nothing, and the marker is written only after all output.

Arguments:
  --input           raw zone root, e.g. s3://<raw>/ibm-aml/v1
  --output          curated zone root, e.g. s3://<curated>
  --size            dataset size to read (default medium)
  --version-prefix  key prefix for this build under --output (default ibm-aml/v1)
"""

import argparse
import json
import logging
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("prepare")

# The raw header has two columns named "Account", so name them by position instead.
RAW_COLUMNS = ["timestamp", "sender_bank_id", "sender_account_number", "receiver_bank_id",
               "receiver_account_number", "amount_received", "receiving_currency",
               "amount_paid", "payment_currency", "payment_format", "is_laundering"]
RAW_SCHEMA = ", ".join(f"{c} STRING" for c in RAW_COLUMNS)


def hadoop_path(spark: SparkSession, path: str):
    p = spark._jvm.org.apache.hadoop.fs.Path(path)
    return p.getFileSystem(spark._jsc.hadoopConfiguration()), p


def exists(spark: SparkSession, path: str) -> bool:
    fs, p = hadoop_path(spark, path)
    return fs.exists(p)


def write_text(spark: SparkSession, path: str, text: str) -> None:
    fs, p = hadoop_path(spark, path)
    out = fs.create(p, True)
    out.write(bytearray(text.encode()))
    out.close()


def read_transfers(spark: SparkSession, raw: str, size: str):
    path = f"{raw}/transactions/variant=HI/size={size}/HI-{size.capitalize()}_Trans.csv"
    log.info("Reading %s", path)
    return spark.read.csv(path, schema=RAW_SCHEMA, header=True)


def clean(df):
    return (df
            .withColumn("timestamp", F.to_timestamp("timestamp", "yyyy/MM/dd HH:mm"))
            .withColumn("amount_received", F.col("amount_received").cast("double"))
            .withColumn("amount_paid", F.col("amount_paid").cast("double"))
            .withColumn("is_laundering", F.col("is_laundering").cast("int"))
            .withColumn("date", F.to_date("timestamp")))


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--size", default="medium")
    parser.add_argument("--version-prefix", default="ibm-aml/v1")
    args = parser.parse_args(argv)
    out = f"{args.output.rstrip('/')}/{args.version_prefix.strip('/')}"

    marker = f"{out}/_SUCCESS"

    spark = SparkSession.builder.appName("prepare").getOrCreate()
    if exists(spark, marker):
        log.info("%s exists, nothing to do", marker)
        return
    # Timestamps carry no zone; parse and partition them as given.
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    raw = read_transfers(spark, args.input.rstrip("/"), args.size)
    transfers = clean(raw)
    # Leading with date matches the writer's own required sort, so it keeps the bank order.
    (transfers.repartition("date").sortWithinPartitions("date", "sender_bank_id")
     .write.mode("overwrite").partitionBy("date").parquet(f"{out}/transfers"))

    report = {
        "run_id": run_id,
        "size": args.size,
        "rows_in": raw.count(),
        "rows_accepted": spark.read.parquet(f"{out}/transfers").count(),
    }
    write_text(spark, f"{out}/quality/{run_id}.json", json.dumps(report, indent=2))
    log.info("Quality report: %s", report)
    write_text(spark, marker, "")
    log.info("Prepare complete, marker written to %s", marker)


if __name__ == "__main__":
    main()
