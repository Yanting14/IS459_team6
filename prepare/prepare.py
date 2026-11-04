"""Clean the raw zone's transfers into the curated zone.

Runs as a Spark job on EMR Serverless, and locally in tests. Locations may be S3 URIs
or local folders. Safe to re-run: if the success marker exists under the version
prefix the job does nothing, and the marker is written only after all output.

Arguments:
  --input                  raw zone root, e.g. s3://<raw>/ibm-aml/v1
  --output                 curated zone root, e.g. s3://<curated>
  --size                   dataset size to read (default medium)
  --version-prefix         key prefix for this build under --output (default ibm-aml/v1)
  --quarantine-threshold   share of input rows that may be quarantined before the job
                           fails, as a fraction (default 0.01, i.e. 1%)
"""

from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("prepare")

# The raw header has two columns named "Account", so name them by position instead.
RAW_COLUMNS = ["timestamp", "sender_bank_id", "sender_account_number", "receiver_bank_id",
               "receiver_account_number", "amount_received", "receiving_currency",
               "amount_paid", "payment_currency", "payment_format", "laundering_mark"]
RAW_SCHEMA = ", ".join(f"{c} STRING" for c in RAW_COLUMNS)
TIMESTAMP_FORMAT = "yyyy/MM/dd HH:mm"

# The dataset's documented values; anything else is a quarantined row. Not yet checked
# against the real HI-Medium file -- confirm before the first full run (#10).
KNOWN_CURRENCIES = ["Australian Dollar", "Bitcoin", "Brazil Real", "Canadian Dollar", "Euro",
                    "Mexican Peso", "Ruble", "Rupee", "Saudi Riyal", "Shekel", "Swiss Franc",
                    "UK Pound", "US Dollar", "Yen", "Yuan"]
KNOWN_PAYMENT_FORMATS = ["ACH", "Bitcoin", "Cash", "Cheque", "Credit Card", "Reinvestment", "Wire"]


def fs_and_path(spark: SparkSession, path: str):
    p = spark._jvm.org.apache.hadoop.fs.Path(path)
    return p.getFileSystem(spark._jsc.hadoopConfiguration()), p


def exists(spark: SparkSession, path: str) -> bool:
    fs, p = fs_and_path(spark, path)
    return fs.exists(p)


def write_text(spark: SparkSession, path: str, text: str) -> None:
    fs, p = fs_and_path(spark, path)
    out = fs.create(p, True)
    out.write(bytearray(text.encode()))
    out.close()


def read_transfers(spark: SparkSession, raw: str, size: str):
    path = f"{raw}/transactions/variant=HI/size={size}/HI-{size.capitalize()}_Trans.csv"
    log.info("Reading %s", path)
    return spark.read.csv(path, schema=RAW_SCHEMA, header=True)


def with_source(df):
    """Tag each row with where it came from: its file and its position after the header."""
    numbered = (df.withColumn("source_file", F.input_file_name())
                .withColumn("_read_order", F.monotonically_increasing_id()))
    # shortcut: numbering sorts a whole source file in one task; switch to per-partition
    # offsets if one file ever outgrows a single executor.
    window = Window.partitionBy("source_file").orderBy("_read_order")
    return numbered.withColumn("row_number", F.row_number().over(window)).drop("_read_order")


def quarantine_reason():
    """The first rule a raw row breaks, or null when the row is sound.

    Being a self-transfer is not one of the rules, so a sound self-transfer is accepted.
    An amount that is not a number has its own reason, apart from zero-or-below, so the
    quality report says which of the two it was.
    """
    received, paid = (F.col(c).cast("double") for c in ("amount_received", "amount_paid"))
    unparsable_amount = received.isNull() | paid.isNull()
    non_positive = (received <= 0) | (paid <= 0)
    unknown_currency = (~F.col("receiving_currency").isin(KNOWN_CURRENCIES)
                        | ~F.col("payment_currency").isin(KNOWN_CURRENCIES))
    missing = " OR ".join(f"(`{c}` IS NULL OR trim(`{c}`) = '')" for c in RAW_COLUMNS)
    return (F.when(F.expr(missing), "missing_field")
            .when(F.to_timestamp("timestamp", TIMESTAMP_FORMAT).isNull(), "unparsable_timestamp")
            .when(unparsable_amount, "unparsable_amount")
            .when(non_positive, "non_positive_amount")
            .when(unknown_currency, "unknown_currency")
            .when(~F.col("payment_format").isin(KNOWN_PAYMENT_FORMATS), "unknown_payment_format"))


def clean(df):
    return (df
            .withColumn("timestamp", F.to_timestamp("timestamp", TIMESTAMP_FORMAT))
            .withColumn("amount_received", F.col("amount_received").cast("double"))
            .withColumn("amount_paid", F.col("amount_paid").cast("double"))
            .withColumn("laundering_mark", F.col("laundering_mark").cast("int"))
            .withColumn("date", F.to_date("timestamp")))


def write_report(spark: SparkSession, out: str, report: dict) -> None:
    write_text(spark, f"{out}/quality/{report['run_id']}.json", json.dumps(report, indent=2))
    log.info("Quality report: %s", report)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--size", default="medium")
    parser.add_argument("--version-prefix", default="ibm-aml/v1")
    parser.add_argument("--quarantine-threshold", type=float, default=0.01)
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
    raw_rows = with_source(read_transfers(spark, args.input.rstrip("/"), args.size))
    checked = raw_rows.withColumn("quarantine_reason", quarantine_reason()).cache()

    # Write the rejected rows before the gate below can fail the job, so they survive it.
    quarantined = checked.where("quarantine_reason IS NOT NULL")
    (quarantined.select("quarantine_reason", "source_file", "row_number", *RAW_COLUMNS)
     .withColumn("run_id", F.lit(run_id))
     .write.mode("append").partitionBy("run_id").parquet(f"{out}/quarantine"))

    by_reason = {r["quarantine_reason"]: r["count"]
                 for r in quarantined.groupBy("quarantine_reason").count().collect()}
    rows_in = checked.count()
    rows_quarantined = sum(by_reason.values())
    report = {
        "run_id": run_id,
        "size": args.size,
        "rows_in": rows_in,
        "rows_accepted": rows_in - rows_quarantined,
        "rows_quarantined": rows_quarantined,
        "quarantined_by_reason": by_reason,
    }

    share = rows_quarantined / rows_in if rows_in else 0.0
    if share > args.quarantine_threshold:
        write_report(spark, out, report)
        raise RuntimeError(f"Quarantined {rows_quarantined} of {rows_in} rows ({share:.2%}), over "
                           f"the {args.quarantine_threshold:.2%} threshold; quality report and "
                           f"quarantined rows written for run {run_id}")

    transfers = clean(checked.where("quarantine_reason IS NULL"))
    # Leading with date matches the writer's own required sort, so it keeps the bank order.
    (transfers.select(*(c for c in RAW_COLUMNS if c != "laundering_mark"), "date")
     .repartition("date").sortWithinPartitions("date", "sender_bank_id")
     .write.mode("overwrite").partitionBy("date").parquet(f"{out}/transfers"))

    write_report(spark, out, report)
    write_text(spark, marker, "")
    log.info("Prepare complete, marker written to %s", marker)


if __name__ == "__main__":
    main()
