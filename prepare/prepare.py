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
# A transfer between one account and itself: it moves no money between parties, so no
# feature counts it, but it is still part of that account's timeline.
SELF_TRANSFER = ("sender_bank_id = receiver_bank_id "
                 "AND sender_account_number = receiver_account_number")

# Each ring in the patterns file is a block of transfer rows wrapped in a BEGIN and an END
# line, both naming the ring's shape. The real HI-Medium file names it either on its own
# ("STACK") or followed by that ring's size parameter ("CYCLE:  Max 10 hops"), so the shape
# is what precedes the colon: the eight pattern types, not seventy-odd variants of them.
RING_BEGIN = r"^BEGIN LAUNDERING ATTEMPT\s*-\s*([^:]*[^:\s])"
TRANSFER_LINE = r"^\d{4}/\d{2}/\d{2} "
LABEL = "laundered_24h"


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


def read_patterns(spark: SparkSession, raw: str, size: str):
    path = f"{raw}/patterns/variant=HI/size={size}/HI-{size.capitalize()}_Patterns.txt"
    log.info("Reading %s", path)
    return spark.read.text(path)


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

# Each size's primary period of activity, as the dataset's data card gives it, and the
# only days features are built for. `history_days` counts window days from the first of
# them, so a bank view agrees with the shared view on it (ADR 0002). HI and LI share these
# days, so the variant does not enter.
#
# Every file also holds rows past its last day: laundering patterns still playing out when
# the simulation stopped, which the data card says are kept on purpose so that a pattern
# can be seen completing. Counted over HI-Small, HI-Medium and HI-Large, that tail is about
# 0.02% of rows and about 59% laundering. Transfers and rings keep it, because a ring that
# finishes there is still a ring Q2 has to see whole; no account-day is built from it,
# because its lookback window holds almost nothing.
PRIMARY_PERIOD = {
    "small": ("2022-09-01", "2022-09-10"),
    "medium": ("2022-09-01", "2022-09-16"),
    "large": ("2022-08-01", "2022-11-05"),
}
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
    moved = transfers.where(f"NOT ({SELF_TRANSFER})")
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


def build_features(transfers, rates: dict[str, float], period: tuple[str, str]):
    """Account-day features over each day's 7-day lookback window.

    A pure function: no I/O, no label, and nothing but the transfers it is given, the rates
    from `usd_rates` and the `period` of days to build for, one of `PRIMARY_PERIOD`.
    Analyse calls it with transfers filtered to one bank's view to get that bank's features
    from the same code (ADR 0002), so the two views may differ only in
    `COUNTERPARTY_FEATURES`: every other feature must depend on the account's own transfers
    alone, all of which any view of its bank holds. A bank view is therefore given the
    rates derived from the whole dataset, and the same period, since a view that moved
    either of them would no longer be comparable with the shared view.
    """
    first_day, last_day = period

    # A day's own transfer count is the same in every window that contains that day.
    day_legs = (legs(transfers).withColumn("transfers_that_day", F.count(F.lit(1)).over(
        Window.partitionBy(*ACCOUNT, "date")))
        .withColumn("usd", F.col("amount") / F.create_map(
            [F.lit(v) for pair in sorted(rates.items()) for v in pair])[F.col("currency")]))

    # A transfer falls in the windows of the 7 days from its own day onwards.
    window_legs = (day_legs
                   .withColumn("account_day", F.explode(F.sequence(
                       F.col("date"), F.date_add("date", WINDOW_DAYS - 1))))
                   .where(F.col("account_day").between(F.to_date(F.lit(first_day)),
                                                       F.to_date(F.lit(last_day)))))

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
            F.lit(WINDOW_DAYS), F.datediff("date", F.to_date(F.lit(first_day))) + 1))
        # Money that came in and left again in equal measure, where 1 is a pure conduit.
        # Nothing moved either way is not a conduit, and the ratio would be 0/0.
        .withColumn("pass_through", F.when(F.greatest("usd_in", "usd_out") == 0, F.lit(0.0))
                    .otherwise(F.least("usd_in", "usd_out") / F.greatest("usd_in", "usd_out")))
        .select(*FEATURE_KEYS, *FEATURES))


# --- Labels and rings ---------------------------------------------------------
# Both are the label side of the curated zone: Training joins them deliberately, and the
# features table never carries them (ADR 0002).


def build_labels(features, accepted):
    """`laundered_24h` for every account-day the features hold, and for no other.

    1 when the account sent or received a transfer marked as laundering that day. The
    account-days are taken from the features themselves, so the two tables share a grain
    exactly and an account-day with no laundering transfer is a 0, not a missing row.
    A self-transfer marked as laundering counts, since the account did take part in it,
    but can never add an account-day: an account-day needs a transfer to someone else.
    """
    marked = accepted.where("laundering_mark = 1")

    def party(side):
        return marked.select(F.col(f"{side}_bank_id").alias("bank_id"),
                             F.col(f"{side}_account_number").alias("account_number"), "date")

    took_part = party("sender").unionByName(party("receiver")).distinct()
    return (features.select(*FEATURE_KEYS)
            .join(took_part.withColumn(LABEL, F.lit(1)), FEATURE_KEYS, "left")
            .fillna({LABEL: 0}))


def build_rings(lines):
    """One row per transfer the patterns file lists, with its ring's id and shape.

    The file names no ring, so a ring's id is its block's position in the file: the running
    count of BEGIN lines. Everything that is not a transfer row -- the BEGIN and END lines
    themselves -- is dropped.
    """
    # shortcut: one global order puts the whole patterns file through a single task; number
    # the blocks per partition if that file ever outgrows one executor.
    in_file_order = Window.orderBy("_order")
    shape = F.regexp_extract("value", RING_BEGIN, 1)
    numbered = (lines.withColumn("_order", F.monotonically_increasing_id())
                .withColumn("_shape", F.when(shape != "", shape)))
    fields = F.split("value", ",")
    # shortcut: a transfer row sitting between two blocks would be read as part of the
    # first; the patterns file has none, so the END lines are dropped rather than tracked.
    return clean(numbered
                 .withColumn("ring_id", F.count("_shape").over(in_file_order))
                 .withColumn("pattern_type", F.last("_shape", True).over(in_file_order))
                 .where(F.col("_shape").isNull() & F.col("value").rlike(TRANSFER_LINE))
                 .select("ring_id", "pattern_type",
                         *(fields[i].alias(c) for i, c in enumerate(RAW_COLUMNS))))


def write_report(spark: SparkSession, out: str, report: dict) -> None:
    write_text(spark, f"{out}/quality/{report['run_id']}.json", json.dumps(report, indent=2))
    log.info("Quality report: %s", report)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    # Rejected here rather than after a 3 GB read, and the table is the list of sizes.
    parser.add_argument("--size", default="medium", choices=sorted(PRIMARY_PERIOD))
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

    period = PRIMARY_PERIOD[args.size]
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    raw = args.input.rstrip("/")
    raw_rows = with_source(read_transfers(spark, raw, args.size))
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

    accepted = clean(checked.where("quarantine_reason IS NULL"))
    # The feature builder is never handed the laundering mark (ADR 0002).
    transfers = accepted.select(*(c for c in RAW_COLUMNS if c != "laundering_mark"), "date")
    # Derived before anything is written, so a currency without a USD rate stops the job
    # rather than leaving half a build behind.
    rates = usd_rates(transfers)
    report["usd_rates"] = rates

    self_transfers = accepted.where(SELF_TRANSFER)
    report["self_transfers"] = self_transfers.count()
    report["self_transfers_laundering"] = self_transfers.where("laundering_mark = 1").count()

    # The days and banks the run actually saw, rather than the ones FIRST_DAY, LAST_DAY and
    # ADR-0001 assume. Reported here because the curated tables are not in the Glue catalog,
    # so reading these back afterwards would cost another job.
    seen = accepted.agg(F.min("date"), F.max("date")).first()
    report["first_day"], report["last_day"] = str(seen[0]), str(seen[1])
    report["banks"] = (accepted.select("sender_bank_id")
                       .union(accepted.select("receiver_bank_id")).distinct().count())
    # Which days were featurised, beside the days the input held: the two differ by the
    # tail, and a run should say so rather than leave it to be worked out.
    report["primary_period"] = list(period)

    # Leading with date matches the writer's own required sort, so it keeps the bank order.
    (transfers.repartition("date").sortWithinPartitions("date", "sender_bank_id")
     .write.mode("overwrite").partitionBy("date").parquet(f"{out}/transfers"))

    features = build_features(transfers, rates, period).cache()
    (features.repartition("date").sortWithinPartitions("date", "bank_id")
     .write.mode("overwrite").partitionBy("date").parquet(f"{out}/features"))

    (build_labels(features, accepted)
     .write.mode("overwrite").partitionBy("date").parquet(f"{out}/labels"))
    (build_rings(read_patterns(spark, raw, args.size))
     .write.mode("overwrite").parquet(f"{out}/rings"))

    write_report(spark, out, report)
    write_text(spark, marker, "")
    log.info("Prepare complete, marker written to %s", marker)


if __name__ == "__main__":
    main()
