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
USD = "US Dollar"


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


def usd_rates(transfers) -> dict[str, float]:
    """Units of each currency per US dollar, derived from the dataset itself.

    A currency's rate is the median of amount received over amount paid across transfers
    paid in USD and received in that currency, so an amount in that currency becomes USD
    by dividing by its rate. USD is 1. Rates are never chained through a third currency:
    a currency the data uses but never pays for in USD fails the job by name, because a
    silently wrong USD amount is worse than no run at all.
    """
    paid_in_usd = transfers.where(F.col("payment_currency") == USD)
    rates = {row["receiving_currency"]: row["rate"] for row in
             paid_in_usd.groupBy("receiving_currency")
             .agg(F.expr("percentile(amount_received / amount_paid, 0.5)").alias("rate"))
             .collect()}
    rates[USD] = 1.0

    in_use = {row[0] for row in transfers.select("payment_currency")
              .union(transfers.select("receiving_currency")).distinct().collect()}
    missing = sorted(in_use - rates.keys())
    if missing:
        raise RuntimeError(f"No transfer paid in {USD} and received in "
                           f"{', '.join(missing)}, so no USD rate can be derived for it; "
                           f"rates are never chained through another currency")
    return {currency: rates[currency] for currency in sorted(in_use)}


# --- Features -----------------------------------------------------------------

# The days the dataset covers. Features are built for these days only, and `history_days`
# counts window days from the first of them, so a bank view agrees with the shared view on
# it (ADR 0002). Not yet checked against the real HI-Medium file -- confirm before the
# first full run (#10).
FIRST_DAY = "2022-09-01"
LAST_DAY = "2022-09-16"
WINDOW_DAYS = 7

ACCOUNT = ["bank_id", "account_number"]  # the glossary's account: never the number alone
FEATURE_KEYS = [*ACCOUNT, "date"]
# The only two features computed from a counterparty's own transfers rather than the
# account's, so the only two a bank view may change (ADR 0002).
COUNTERPARTY_FEATURES = ["cp_max_fan_in", "cp_max_fan_out"]
FEATURES = ["fan_in", "fan_out", "cp_banks", "cross_bank_share", *COUNTERPARTY_FEATURES,
            "usd_in", "usd_out", "max_usd", "pass_through", "hrs_in_to_out",
            "max_txn_per_day", "active_days", "n_pay_formats", "ach_share", "n_currencies",
            "history_days"]


def legs(transfers):
    """One row per account per transfer: outgoing for its sender, incoming for its receiver.

    A self-transfer moves no money between parties, so it is excluded from every feature.
    The currency and amount are the ones that account dealt in: what it paid, or what it
    received. The two differ on a cross-currency transfer.
    """
    moved = transfers.where("NOT (sender_bank_id = receiver_bank_id "
                            "AND sender_account_number = receiver_account_number)")
    both_sides = [F.col("timestamp"), F.to_date("timestamp").alias("date"),
                  F.col("payment_format"),
                  (F.col("sender_bank_id") != F.col("receiver_bank_id")).alias("cross_bank")]
    outgoing = moved.select(
        F.col("sender_bank_id").alias("bank_id"),
        F.col("sender_account_number").alias("account_number"),
        F.col("receiver_bank_id").alias("cp_bank_id"),
        F.col("receiver_account_number").alias("cp_account_number"),
        F.lit(True).alias("outgoing"), F.col("payment_currency").alias("currency"),
        F.col("amount_paid").alias("amount"), *both_sides)
    incoming = moved.select(
        F.col("receiver_bank_id").alias("bank_id"),
        F.col("receiver_account_number").alias("account_number"),
        F.col("sender_bank_id").alias("cp_bank_id"),
        F.col("sender_account_number").alias("cp_account_number"),
        F.lit(False).alias("outgoing"), F.col("receiving_currency").alias("currency"),
        F.col("amount_received").alias("amount"), *both_sides)
    return outgoing.unionByName(incoming)


def counterparty_features(windows, window_legs):
    """The highest `fan_in` among the accounts an account pays, and the highest `fan_out`
    among the accounts paying it, read off those counterparties' own rows for the same
    window and day.

    Every transfer gives both of its accounts a leg over the same account-days, so a
    counterparty always has a row of its own to read -- even on a day it was itself idle,
    which is why `windows` is joined before the idle days are dropped. An account with no
    counterparty on one side keeps a null there.
    """
    counterparties = windows.select("account_day", "fan_in", "fan_out",
                                    *(F.col(c).alias(f"cp_{c}") for c in ACCOUNT))
    return (window_legs.join(counterparties, ["account_day", *(f"cp_{c}" for c in ACCOUNT)])
            .groupBy(*ACCOUNT, "account_day")
            .agg(F.max(F.when(F.col("outgoing"), F.col("fan_in"))).alias("cp_max_fan_in"),
                 F.max(F.when(~F.col("outgoing"), F.col("fan_out"))).alias("cp_max_fan_out")))


def build_features(transfers, rates: dict[str, float]):
    """Account-day features over each day's 7-day lookback window.

    A pure function: no I/O, no label, and nothing but the transfers it is given, the rates
    from `usd_rates` and the dataset's fixed day range. Analyse calls it with transfers
    filtered to one bank's view to get that bank's features from the same code (ADR 0002),
    so the two views may differ only in `COUNTERPARTY_FEATURES`: every other feature must
    depend on the account's own transfers alone, all of which any view of its bank holds.
    A bank view is therefore given the rates derived from the whole dataset.
    """
    # A day's own transfer count is the same in every window that contains that day.
    day_legs = (legs(transfers).withColumn("transfers_that_day", F.count(F.lit(1)).over(
        Window.partitionBy(*ACCOUNT, "date")))
        .withColumn("usd", F.col("amount") / F.create_map(
            [F.lit(v) for pair in sorted(rates.items()) for v in pair])[F.col("currency")]))

    # A transfer falls in the windows of the 7 days from its own day onwards.
    window_legs = (day_legs
                   .withColumn("account_day", F.explode(F.sequence(
                       F.col("date"), F.date_add("date", WINDOW_DAYS - 1))))
                   .where(F.col("account_day").between(F.to_date(F.lit(FIRST_DAY)),
                                                       F.to_date(F.lit(LAST_DAY)))))

    # Hours since the latest incoming transfer strictly before each outgoing one. Timestamps
    # are whole minutes, so a bound one second back is exactly "earlier".
    earlier_in_window = (Window.partitionBy(*ACCOUNT, "account_day")
                         .orderBy(F.col("timestamp").cast("long"))
                         .rangeBetween(Window.unboundedPreceding, -1))
    last_incoming = F.max(F.when(~F.col("outgoing"),
                                 F.col("timestamp").cast("long"))).over(earlier_in_window)
    window_legs = window_legs.withColumn("hrs_since_in", F.when(
        F.col("outgoing"), (F.col("timestamp").cast("long") - last_incoming) / 3600))

    counterparty = F.array("cp_bank_id", "cp_account_number")  # never the number alone
    windows = (window_legs.groupBy(*ACCOUNT, "account_day").agg(
        F.count_distinct(F.when(~F.col("outgoing"), counterparty)).alias("fan_in"),
        F.count_distinct(F.when(F.col("outgoing"), counterparty)).alias("fan_out"),
        F.count_distinct("cp_bank_id").alias("cp_banks"),
        F.avg(F.col("cross_bank").cast("int")).alias("cross_bank_share"),
        # No money came in is 0 in, not an unknown; but the largest of no transfers at all
        # is genuinely missing, so `max_usd` stays null for an account that only received.
        F.coalesce(F.sum(F.when(~F.col("outgoing"), F.col("usd"))), F.lit(0.0)).alias("usd_in"),
        F.coalesce(F.sum(F.when(F.col("outgoing"), F.col("usd"))), F.lit(0.0)).alias("usd_out"),
        F.max(F.when(F.col("outgoing"), F.col("usd"))).alias("max_usd"),
        F.expr("percentile(hrs_since_in, 0.5)").alias("hrs_in_to_out"),
        F.max("transfers_that_day").alias("max_txn_per_day"),
        F.count_distinct("date").alias("active_days"),
        F.count_distinct("payment_format").alias("n_pay_formats"),
        F.avg((F.col("payment_format") == "ACH").cast("int")).alias("ach_share"),
        F.count_distinct("currency").alias("n_currencies"),
        # An account-day exists only where the account itself moved money that day.
        F.max((F.col("date") == F.col("account_day")).cast("int")).alias("active_on_the_day")))

    return (windows.where("active_on_the_day = 1")
        .join(counterparty_features(windows, window_legs), [*ACCOUNT, "account_day"], "left")
        .withColumnRenamed("account_day", "date")
        # A window day before the dataset's first day is not short history: it does not exist.
        .withColumn("history_days", F.least(
            F.lit(WINDOW_DAYS), F.datediff("date", F.to_date(F.lit(FIRST_DAY))) + 1))
        # Money that came in and left again in equal measure, where 1 is a pure conduit.
        # Nothing moved either way is not a conduit, and the ratio would be 0/0.
        .withColumn("pass_through", F.when(F.greatest("usd_in", "usd_out") == 0, F.lit(0.0))
                    .otherwise(F.least("usd_in", "usd_out") / F.greatest("usd_in", "usd_out")))
        .select(*FEATURE_KEYS, *FEATURES))


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

    # The feature builder is never handed the laundering mark (ADR 0002).
    transfers = (clean(checked.where("quarantine_reason IS NULL"))
                 .select(*(c for c in RAW_COLUMNS if c != "laundering_mark"), "date"))
    # Derived before anything is written, so a currency without a USD rate stops the job
    # rather than leaving half a build behind.
    rates = usd_rates(transfers)
    report["usd_rates"] = rates

    # Leading with date matches the writer's own required sort, so it keeps the bank order.
    (transfers.repartition("date").sortWithinPartitions("date", "sender_bank_id")
     .write.mode("overwrite").partitionBy("date").parquet(f"{out}/transfers"))

    (build_features(transfers, rates)
     .repartition("date").sortWithinPartitions("date", "bank_id")
     .write.mode("overwrite").partitionBy("date").parquet(f"{out}/features"))

    write_report(spark, out, report)
    write_text(spark, marker, "")
    log.info("Prepare complete, marker written to %s", marker)


if __name__ == "__main__":
    main()
