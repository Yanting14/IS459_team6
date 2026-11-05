"""Runs the Prepare job end to end on a hand-made raw zone and reads the curated zone back.

S3 is never mocked: the job takes local folders in place of S3 URIs.
"""

import json
import hashlib
from pathlib import Path

import pytest

import prepare

HEADER = ("Timestamp,From Bank,Account,To Bank,Account,Amount Received,Receiving Currency,"
          "Amount Paid,Payment Currency,Payment Format,Is Laundering")

SELF_TRANSFER = ("2022/09/01 00:20,3208,8000F4580,3208,8000F4580,50.00,US Dollar,50.00,"
                 "US Dollar,Reinvestment,0")
LAUNDERING_TRANSFER = ("2022/09/01 09:15,1,800A1B2C3,010,8000EBD30,700.00,Euro,800.00,"
                       "US Dollar,ACH,1")
# A self-transfer that is itself marked as laundering, so the account took part in a
# laundering transfer on a day that is no account-day at all.
LAUNDERING_SELF_TRANSFER = ("2022/09/03 18:00,010,800D4D4D4,010,800D4D4D4,25.00,US Dollar,"
                            "25.00,US Dollar,Reinvestment,1")
# A few banks (one with a leading zero) over three days. Accounts A = (010, 8000EBD30),
# B = (3208, 8000F4580) and C = (1, 800A1B2C3) both send and receive; D = (010, 800D4D4D4)
# only ever sends, and only within its own bank. C -> A -> B on 1 Sep is the two-hop
# chain the counterparty features are hand-computed from: A pays an account that is itself
# paid, and is paid by an account that itself pays. The last two rows fall outside the
# dataset's 1 to 16 Sep, so they belong in the clean transfers but in no account-day.
MEDIUM_ROWS = [
    "2022/09/01 00:20,010,8000EBD30,3208,8000F4580,1000.00,US Dollar,1000.00,US Dollar,Wire,0",
    SELF_TRANSFER,
    LAUNDERING_TRANSFER,
    "2022/09/01 12:00,3208,8000F4580,1,800A1B2C3,300.00,US Dollar,300.00,US Dollar,Cheque,0",
    "2022/09/02 06:00,010,800D4D4D4,010,8000EBD30,400.00,US Dollar,400.00,US Dollar,Wire,0",
    "2022/09/02 08:00,3208,8000F4580,010,8000EBD30,200.00,US Dollar,200.00,US Dollar,Wire,0",
    "2022/09/02 10:30,010,8000EBD30,1,800A1B2C3,150.00,US Dollar,150.00,US Dollar,ACH,0",
    "2022/09/02 23:59,1,800A1B2C3,3208,8000F4580,90.00,US Dollar,90.00,US Dollar,Credit Card,0",
    "2022/09/03 07:45,010,8000EBD30,3208,8000F4580,60.00,US Dollar,60.00,US Dollar,Cash,0",
    LAUNDERING_SELF_TRANSFER,
    "2022/08/31 12:00,7,800E5E5E5,7,800F6F6F6,10.00,US Dollar,10.00,US Dollar,Cash,0",
    "2022/09/17 12:00,010,8000EBD30,3208,8000F4580,20.00,US Dollar,20.00,US Dollar,Cash,0",
]
FEATURES = ["fan_in", "fan_out", "cp_banks", "cross_bank_share", "cp_max_fan_in",
            "cp_max_fan_out", "usd_in", "usd_out", "max_usd", "pass_through",
            "hrs_in_to_out", "max_txn_per_day", "active_days", "n_pay_formats", "ach_share",
            "n_currencies", "history_days"]
# The only cross-currency transfer pays 800 US Dollar for 700 Euro, so the Euro's rate is
# 700/800 = 0.875 Euro per dollar and the 700 Euro that account received is 800 USD.
EXPECTED_RATES = {"Euro": 0.875, "US Dollar": 1.0}
# Hand-computed from MEDIUM_ROWS, one row per account-day, ordered by bank then account
# then day. Columns: the three keys, then FEATURES in order. Floats rounded to 6 places.
EXPECTED_FEATURES = [
    ("010", "8000EBD30", "2022-09-01", 1, 1, 2, 1.0, 1, 1,
     800.0, 1000.0, 1000.0, 0.8, None, 2, 1, 2, 0.5, 2, 1),
    ("010", "8000EBD30", "2022-09-02", 3, 2, 3, 0.8, 2, 2,
     1400.0, 1150.0, 1000.0, 0.821429, 2.5, 3, 2, 2, 0.4, 2, 2),
    ("010", "8000EBD30", "2022-09-03", 3, 2, 3, 0.833333, 2, 2,
     1400.0, 1210.0, 1000.0, 0.864286, 13.125, 3, 3, 3, 0.333333, 2, 3),
    ("010", "800D4D4D4", "2022-09-02", 0, 1, 1, 0.0, 3, None,
     0.0, 400.0, 400.0, 0.0, None, 1, 1, 1, 0.0, 1, 2),
    ("1", "800A1B2C3", "2022-09-01", 1, 1, 2, 1.0, 1, 1,
     300.0, 800.0, 800.0, 0.375, None, 2, 1, 2, 0.5, 1, 1),
    ("1", "800A1B2C3", "2022-09-02", 2, 2, 2, 1.0, 3, 2,
     450.0, 890.0, 800.0, 0.505618, 13.483333, 2, 2, 3, 0.5, 1, 2),
    ("3208", "8000F4580", "2022-09-01", 1, 1, 2, 1.0, 1, 1,
     1000.0, 300.0, 300.0, 0.3, 11.666667, 2, 1, 2, 0.0, 1, 1),
    ("3208", "8000F4580", "2022-09-02", 2, 2, 2, 1.0, 3, 2,
     1090.0, 500.0, 300.0, 0.458716, 21.666667, 2, 2, 3, 0.0, 1, 2),
    ("3208", "8000F4580", "2022-09-03", 2, 2, 2, 1.0, 3, 2,
     1150.0, 500.0, 300.0, 0.434783, 21.666667, 2, 3, 4, 0.0, 1, 3),
]
# The second row is the one that makes the size matter: 12 Sep is inside HI-Medium's days
# and past HI-Small's, so the same raw zone featurises it under one size and not the other.
SMALL_ROWS = [
    "2022/09/05 01:00,1,800A1B2C3,010,8000EBD30,5.00,US Dollar,5.00,US Dollar,Cash,0",
    "2022/09/12 01:00,1,800A1B2C3,010,8000EBD30,7.00,US Dollar,7.00,US Dollar,Cash,0",
]
MEDIUM_PERIOD = ("2022-09-01", "2022-09-16")
# One row per quarantine rule, appended after MEDIUM_ROWS so they are rows 9 to 14.
BROKEN_ROWS = [
    "2022/09/04 08:00,010,8000EBD30,3208,,70.00,US Dollar,70.00,US Dollar,Wire,0",
    "2022-09-04 08:00,010,8000EBD30,3208,8000F4580,70.00,US Dollar,70.00,US Dollar,Wire,0",
    "2022/09/04 08:00,010,8000EBD30,3208,8000F4580,abc,US Dollar,abc,US Dollar,Wire,0",
    "2022/09/04 08:00,010,8000EBD30,3208,8000F4580,0.00,US Dollar,0.00,US Dollar,Wire,0",
    "2022/09/04 08:00,010,8000EBD30,3208,8000F4580,70.00,Klingon Darsek,70.00,US Dollar,Wire,0",
    "2022/09/04 08:00,010,8000EBD30,3208,8000F4580,70.00,US Dollar,70.00,US Dollar,Telepathy,0",
]
BROKEN_REASONS = ["missing_field", "unparsable_timestamp", "unparsable_amount",
                  "non_positive_amount", "unknown_currency", "unknown_payment_format"]
# The patterns format, copied from the real HI-Medium file: each ring is a block of
# transfer rows between a BEGIN and an END line naming its shape, and a blank line follows
# each block. The second ring's shape carries the size parameter the real file puts on most
# of them, so the expectation below pins that only the shape becomes the pattern type. The
# rows are the laundering transfers from MEDIUM_ROWS, two of them in the second ring so
# that one ring id covers all of its rows.
PATTERN_LINES = [
    "BEGIN LAUNDERING ATTEMPT - STACK",
    LAUNDERING_TRANSFER,
    "END LAUNDERING ATTEMPT - STACK",
    "",
    "BEGIN LAUNDERING ATTEMPT - CYCLE:  Max 3 hops",
    LAUNDERING_SELF_TRANSFER,
    LAUNDERING_TRANSFER,
    "END LAUNDERING ATTEMPT - CYCLE:  Max 3 hops",
    "",
]
TRANS_CSV = "HI-Medium_Trans.csv"
PREFIX = "ibm-aml/v1"


def write_raw(root, size, rows):
    folder = root / "transactions" / "variant=HI" / f"size={size}"
    folder.mkdir(parents=True, exist_ok=True)
    (folder / f"HI-{size.capitalize()}_Trans.csv").write_text("\n".join([HEADER, *rows]) + "\n")
    patterns = root / "patterns" / "variant=HI" / f"size={size}"
    patterns.mkdir(parents=True, exist_ok=True)
    (patterns / f"HI-{size.capitalize()}_Patterns.txt").write_text(
        "\n".join(PATTERN_LINES) + "\n")
    return root


@pytest.fixture
def raw(tmp_path):
    root = tmp_path / "raw"
    for size, rows in (("medium", MEDIUM_ROWS), ("small", SMALL_ROWS)):
        write_raw(root, size, rows)
    return root


@pytest.fixture
def raw_with_broken_rows(tmp_path):
    """A raw zone whose quarantined share (6 of 14) is far over the default 1% threshold."""
    return write_raw(tmp_path / "raw-broken", "medium", [*MEDIUM_ROWS, *BROKEN_ROWS])


@pytest.fixture
def curated(tmp_path):
    return tmp_path / "curated"


@pytest.fixture
def quarantine_run(raw_with_broken_rows, curated, spark):
    """A finished run over the broken-row zone, with the gate opened wide enough to pass."""
    run_job(raw_with_broken_rows, curated, "--quarantine-threshold", "1")
    return curated


def run_job(raw, curated, *extra):
    prepare.main(["--input", str(raw), "--output", str(curated), "--version-prefix", PREFIX, *extra])


def transfers(spark, curated):
    return spark.read.parquet(str(curated / PREFIX / "transfers"))


def quarantined(spark, curated):
    return spark.read.parquet(str(curated / PREFIX / "quarantine"))


def features(spark, curated):
    """The features table as plain tuples, in EXPECTED_FEATURES' column and row order."""
    rows = (spark.read.parquet(str(curated / PREFIX / "features"))
            .selectExpr("bank_id", "account_number", "cast(date AS string) AS date", *FEATURES)
            .orderBy("bank_id", "account_number", "date").collect())
    return [tuple(round(v, 6) if isinstance(v, float) else v for v in row) for row in rows]


def labels(spark, curated):
    return spark.read.parquet(str(curated / PREFIX / "labels"))


def only_report(curated):
    reports = list((curated / PREFIX / "quality").glob("*.json"))
    assert len(reports) == 1
    return json.loads(reports[0].read_text())


def test_clean_transfers_carry_sender_and_receiver_accounts(spark, raw, curated):
    run_job(raw, curated)

    df = transfers(spark, curated)
    types = dict(df.dtypes)
    assert types["sender_bank_id"] == "string"
    assert types["receiver_bank_id"] == "string"
    assert types["timestamp"] == "timestamp"
    row = (df.where("payment_format = 'Wire' AND date = '2022-09-01'")
           .selectExpr("date_format(timestamp, 'yyyy/MM/dd HH:mm') AS ts", "sender_bank_id",
                       "sender_account_number", "receiver_bank_id", "receiver_account_number")
           .collect())
    assert [r.asDict() for r in row] == [{
        "ts": "2022/09/01 00:20", "sender_bank_id": "010", "sender_account_number": "8000EBD30",
        "receiver_bank_id": "3208", "receiver_account_number": "8000F4580",
    }]


def test_curated_transfers_carry_no_laundering_mark(spark, raw, curated):
    run_job(raw, curated)

    assert not [c for c in transfers(spark, curated).columns if "laundering" in c]


def test_transfer_identity_and_labels_survive_sorting_and_quarantine(spark, quarantine_run):
    df = transfers(spark, quarantine_run)
    rows = df.orderBy("source_row_id").collect()
    assert [r.source_row_id for r in rows] == list(range(len(MEDIUM_ROWS)))
    digest = hashlib.sha256(Path(rows[0].source_file).read_bytes()).hexdigest()
    assert [r.transfer_id for r in rows] == [f"{digest}:{i}" for i in range(len(MEDIUM_ROWS))]
    marks = spark.read.parquet(str(quarantine_run / PREFIX / "transfer_labels"))
    joined = df.join(marks.select("transfer_id", "laundering_mark"), "transfer_id")
    assert joined.count() == len(MEDIUM_ROWS)
    assert [r.laundering_mark for r in joined.orderBy('source_row_id').collect()] == [
        int(row.rsplit(',', 1)[1]) for row in MEDIUM_ROWS]


def test_identity_does_not_depend_on_input_split_size(spark, raw):
    first = prepare.read_transfers(spark, str(raw), 'medium').orderBy('source_row_id').collect()
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    key = 'mapreduce.input.fileinputformat.split.maxsize'
    old = conf.get(key)
    try:
        conf.set(key, '128')
        second = prepare.read_transfers(spark, str(raw), 'medium').orderBy('source_row_id').collect()
    finally:
        if old is None:
            conf.unset(key)
        else:
            conf.set(key, old)
    assert first == second


def test_self_transfers_are_kept(spark, raw, curated):
    run_job(raw, curated)

    df = transfers(spark, curated)
    self_transfers = df.where("sender_bank_id = receiver_bank_id "
                              "AND sender_account_number = receiver_account_number").collect()
    assert sorted((r.sender_bank_id, r.sender_account_number, r.payment_format)
                  for r in self_transfers) == [("010", "800D4D4D4", "Reinvestment"),
                                               ("3208", "8000F4580", "Reinvestment")]


def test_partitioned_by_date_and_sorted_by_sender_bank(spark, raw, curated):
    run_job(raw, curated)

    root = curated / PREFIX / "transfers"
    dates = sorted(p.name for p in root.glob("date=*"))
    assert dates == ["date=2022-08-31", "date=2022-09-01", "date=2022-09-02",
                     "date=2022-09-03", "date=2022-09-17"]
    for date in dates:
        files = list((root / date).glob("*.parquet"))
        assert len(files) == 1
        banks = [r.sender_bank_id for r in spark.read.parquet(str(files[0])).collect()]
        assert banks == sorted(banks)
    assert transfers(spark, curated).where("date = '2022-09-01'").count() == 4


def test_reads_the_requested_dataset_size(spark, raw, curated, tmp_path):
    run_job(raw, curated)
    run_job(raw, tmp_path / "small", "--size", "small")

    assert transfers(spark, curated).count() == len(MEDIUM_ROWS)
    assert transfers(spark, tmp_path / "small").count() == len(SMALL_ROWS)


def test_quality_report_counts_rows_in_and_accepted(raw, curated, spark):
    run_job(raw, curated)

    report = only_report(curated)
    assert report["rows_in"] == len(MEDIUM_ROWS)
    assert report["rows_accepted"] == len(MEDIUM_ROWS)
    assert report["rows_accepted"] == transfers(spark, curated).count()


def test_success_marker_is_written_after_all_output(raw, curated, spark):
    run_job(raw, curated)

    out = curated / PREFIX
    marker = out / "_SUCCESS"
    assert marker.exists()
    # .crc files are local-filesystem checksums written alongside each file, not output.
    others = [p for p in out.rglob("*") if p.is_file() and p != marker and p.suffix != ".crc"]
    assert max(p.stat().st_mtime_ns for p in others) <= marker.stat().st_mtime_ns


def test_failed_run_writes_no_success_marker(raw, curated, spark):
    with pytest.raises(Exception):
        run_job(raw, curated, "--size", "large")  # no large file in the raw zone

    assert not (curated / PREFIX / "_SUCCESS").exists()


def test_run_with_success_marker_present_does_no_work(tmp_path, curated, spark):
    out = curated / PREFIX
    out.mkdir(parents=True)
    (out / "_SUCCESS").touch()

    run_job(tmp_path / "no-raw-zone", curated)  # would fail if it tried to read

    assert [p.name for p in out.iterdir()] == ["_SUCCESS"]


def test_each_rule_quarantines_its_row_with_reason_source_file_and_row_number(
        spark, quarantine_run):
    rows = sorted(quarantined(spark, quarantine_run).collect(), key=lambda r: r.row_number)
    first_broken = len(MEDIUM_ROWS) + 1
    assert [(r.row_number, r.quarantine_reason) for r in rows] == list(
        zip(range(first_broken, first_broken + len(BROKEN_ROWS)), BROKEN_REASONS))
    tail = f"/raw-broken/transactions/variant=HI/size=medium/{TRANS_CSV}"
    assert all(r.source_file.endswith(tail) for r in rows)
    assert all(r.transfer_id == f"{r.source_sha256}:{r.source_row_id}" for r in rows)
    assert all(r.source_row_id == r.row_number - 1 for r in rows)
    assert only_report(quarantine_run)['schema_version'] == 2


def test_quarantined_rows_are_partitioned_by_run(spark, quarantine_run):
    runs = [p.name for p in (quarantine_run / PREFIX / "quarantine").glob("run_id=*")]
    assert len(runs) == 1
    assert only_report(quarantine_run)["run_id"] == runs[0].removeprefix("run_id=")


def test_quarantined_rows_are_not_in_clean_transfers(spark, quarantine_run):
    df = transfers(spark, quarantine_run)
    assert df.count() == len(MEDIUM_ROWS)
    assert df.where("date = '2022-09-04' OR amount_paid <= 0 OR amount_paid IS NULL").count() == 0


def test_quality_report_counts_quarantined_rows_by_reason(quarantine_run):
    report = only_report(quarantine_run)
    assert report["rows_in"] == len(MEDIUM_ROWS) + len(BROKEN_ROWS)
    assert report["rows_accepted"] == len(MEDIUM_ROWS)
    assert report["rows_quarantined"] == len(BROKEN_ROWS)
    assert report["quarantined_by_reason"] == {reason: 1 for reason in BROKEN_REASONS}


def test_sound_rows_are_never_quarantined(spark, raw, curated):
    run_job(raw, curated)

    assert not list((curated / PREFIX / "quarantine").glob("run_id=*"))
    assert only_report(curated)["quarantined_by_reason"] == {}


def test_a_self_transfer_is_accepted_not_quarantined(spark, quarantine_run):
    """Row 2 of MEDIUM_ROWS is a self-transfer, alongside rows that are genuinely broken."""
    self_transfer = ("sender_bank_id = '3208' AND sender_account_number = '8000F4580' AND "
                     "receiver_bank_id = '3208' AND receiver_account_number = '8000F4580'")
    assert quarantined(spark, quarantine_run).where(self_transfer).count() == 0
    assert transfers(spark, quarantine_run).where(self_transfer).count() == 1


def test_run_over_the_threshold_fails_but_keeps_its_evidence(spark, raw_with_broken_rows, curated):
    with pytest.raises(RuntimeError, match="over the 1.00% threshold"):
        run_job(raw_with_broken_rows, curated)

    assert only_report(curated)["rows_quarantined"] == len(BROKEN_ROWS)
    assert quarantined(spark, curated).count() == len(BROKEN_ROWS)
    assert not (curated / PREFIX / "_SUCCESS").exists()
    assert not (curated / PREFIX / "transfers").exists()


def test_a_share_exactly_at_the_threshold_does_not_fail(spark, raw_with_broken_rows, curated):
    """The spec says rows must *exceed* the threshold, so the gate is strictly greater-than."""
    exact = len(BROKEN_ROWS) / (len(MEDIUM_ROWS) + len(BROKEN_ROWS))
    run_job(raw_with_broken_rows, curated, "--quarantine-threshold", repr(exact))

    assert (curated / PREFIX / "_SUCCESS").exists()


def test_threshold_can_be_changed_by_job_argument(spark, raw_with_broken_rows, curated):
    run_job(raw_with_broken_rows, curated, "--quarantine-threshold", "0.5")

    assert (curated / PREFIX / "_SUCCESS").exists()


@pytest.fixture
def raw_without_self_transfers(tmp_path):
    self_transfers = (SELF_TRANSFER, LAUNDERING_SELF_TRANSFER)
    return write_raw(tmp_path / "raw-no-self", "medium",
                     [row for row in MEDIUM_ROWS if row not in self_transfers])


def test_every_account_day_has_its_hand_computed_feature_values(spark, raw, curated):
    """Also pins the grain: an idle account gets no row, so C has no 3 Sep and D only 2 Sep."""
    run_job(raw, curated)

    assert features(spark, curated) == EXPECTED_FEATURES


def test_no_account_day_falls_outside_1_to_16_sep(spark, raw, curated):
    run_job(raw, curated)

    dates = sorted(p.name for p in (curated / PREFIX / "features").glob("date=*"))
    assert dates == ["date=2022-09-01", "date=2022-09-02", "date=2022-09-03"]
    # The two out-of-range transfers are in the clean table; only the features are capped.
    assert transfers(spark, curated).where("date IN ('2022-08-31', '2022-09-17')").count() == 2


def test_a_self_transfer_changes_no_feature_value(spark, raw, curated,
                                                  raw_without_self_transfers, tmp_path):
    run_job(raw, curated)
    run_job(raw_without_self_transfers, tmp_path / "curated-no-self")

    assert features(spark, curated) == features(spark, tmp_path / "curated-no-self")


def test_an_account_that_only_sends_has_no_hrs_in_to_out(spark, raw, curated):
    run_job(raw, curated)

    rows = (spark.read.parquet(str(curated / PREFIX / "features"))
            .where("account_number = '800D4D4D4'").collect())
    assert [(r.fan_in, r.fan_out, r.hrs_in_to_out) for r in rows] == [(0, 1, None)]


def test_features_carry_no_label_column(spark, raw, curated):
    run_job(raw, curated)

    columns = spark.read.parquet(str(curated / PREFIX / "features")).columns
    assert sorted(columns) == sorted(["bank_id", "account_number", "date", *FEATURES])


def test_features_are_partitioned_by_date_and_sorted_by_bank(spark, raw, curated):
    run_job(raw, curated)

    root = curated / PREFIX / "features"
    for date in sorted(p.name for p in root.glob("date=*")):
        files = list((root / date).glob("*.parquet"))
        assert len(files) == 1
        banks = [r.bank_id for r in spark.read.parquet(str(files[0])).collect()]
        assert banks == sorted(banks)


@pytest.fixture
def raw_with_an_unpriced_currency(tmp_path):
    """A Yen amount paid for in Euro, so no transfer prices the Yen against the dollar."""
    unpriced = ("2022/09/04 10:00,010,8000EBD30,3208,8000F4580,1000.00,Yen,8.00,Euro,Wire,0")
    return write_raw(tmp_path / "raw-unpriced", "medium", [*MEDIUM_ROWS, unpriced])


def test_a_cross_currency_transfer_prices_its_currency_in_the_quality_report(raw, curated, spark):
    run_job(raw, curated)

    assert only_report(curated)["usd_rates"] == EXPECTED_RATES


def test_a_currency_that_never_trades_against_usd_fails_the_job_by_name(
        spark, raw_with_an_unpriced_currency, curated):
    with pytest.raises(RuntimeError, match="Yen"):
        run_job(raw_with_an_unpriced_currency, curated)

    assert not (curated / PREFIX / "_SUCCESS").exists()


def test_a_laundering_transfer_labels_its_account_day_and_leaves_the_others_zero(
        spark, raw, curated):
    run_job(raw, curated)

    df = labels(spark, curated)
    ones = sorted((r.bank_id, r.account_number, str(r.date))
                  for r in df.where("laundered_24h = 1").collect())
    assert ones == [("010", "8000EBD30", "2022-09-01"), ("1", "800A1B2C3", "2022-09-01")]
    assert df.where("laundered_24h = 0").count() == len(EXPECTED_FEATURES) - len(ones)


def test_labels_have_exactly_the_account_features_grain(spark, raw, curated):
    """The laundering self-transfer is what pins this: (010, 800D4D4D4) moved money only to
    itself on 3 Sep, so that day is no account-day, has no features row, and so gets no
    label row either -- even though the account took part in a laundering transfer on it.
    """
    run_job(raw, curated)

    keys = ["bank_id", "account_number", "date"]
    feature_days = spark.read.parquet(str(curated / PREFIX / "features")).select(*keys)
    label_days = labels(spark, curated).select(*keys)
    assert label_days.count() == feature_days.count() == len(EXPECTED_FEATURES)
    assert feature_days.exceptAll(label_days).count() == 0
    assert label_days.exceptAll(feature_days).count() == 0
    assert sorted(p.name for p in (curated / PREFIX / "labels").glob("date=*")) == [
        "date=2022-09-01", "date=2022-09-02", "date=2022-09-03"]


def ring_line(r):
    """A ring row written back as its patterns-file line, so the fixture is the expectation."""
    return ",".join([r.ts, r.sender_bank_id, r.sender_account_number, r.receiver_bank_id,
                     r.receiver_account_number, f"{r.amount_received:.2f}",
                     r.receiving_currency, f"{r.amount_paid:.2f}", r.payment_currency,
                     r.payment_format, str(r.laundering_mark)])


def test_rings_carry_a_ring_id_a_pattern_type_and_the_transfers_own_fields(spark, raw, curated):
    run_job(raw, curated)

    rows = (spark.read.parquet(str(curated / PREFIX / "rings"))
            .selectExpr("*", "date_format(timestamp, 'yyyy/MM/dd HH:mm') AS ts")
            .orderBy("ring_id", "ts").collect())
    assert [(r.ring_id, r.pattern_type, ring_line(r)) for r in rows] == [
        (1, "STACK", LAUNDERING_TRANSFER),
        (2, "CYCLE", LAUNDERING_TRANSFER),
        (2, "CYCLE", LAUNDERING_SELF_TRANSFER),
    ]
    assert not list((curated / PREFIX / "rings").glob("*=*"))  # not partitioned


def test_the_feature_window_follows_the_dataset_size(spark, raw, tmp_path):
    """Each size has its own primary period, so the same transfer is in or out by size.

    HI-Large runs from 1 Aug to 5 Nov, so a window fixed at HI-Medium's days would have
    featurised sixteen days of it and dropped the rest without saying so.
    """
    out = tmp_path / "small"
    run_job(raw, out, "--size", "small")

    assert transfers(spark, out).count() == len(SMALL_ROWS)  # every row is still kept
    days = (spark.read.parquet(str(out / PREFIX / "features"))
            .selectExpr("cast(date AS string) AS date").distinct().collect())
    assert sorted(r.date for r in days) == ["2022-09-05"]  # 12 Sep is past HI-Small's days
    assert only_report(out)["primary_period"] == ["2022-09-01", "2022-09-10"]


def test_quality_report_records_the_days_and_banks_seen(raw, curated, spark):
    run_job(raw, curated)

    report = only_report(curated)
    # The range is what the accepted transfers hold, not the feature window: MEDIUM_ROWS
    # reaches a day past either end of it.
    assert (report["first_day"], report["last_day"]) == ("2022-08-31", "2022-09-17")
    assert report["banks"] == 4  # 010, 3208, 1 and 7, each counted once


def test_quality_report_counts_self_transfers_and_the_laundering_ones(raw, curated, spark):
    run_job(raw, curated)

    report = only_report(curated)
    assert report["self_transfers"] == 2
    assert report["self_transfers_laundering"] == 1


def one_transfer(spark, amount):
    """A lone transfer from A to B, as `build_features` takes it: no raw zone, no job run."""
    return spark.sql(f"""
        SELECT timestamp('2022-09-01 00:20') AS timestamp,
               '010' AS sender_bank_id, '8000EBD30' AS sender_account_number,
               '3208' AS receiver_bank_id, '8000F4580' AS receiver_account_number,
               CAST({amount} AS DOUBLE) AS amount_paid,
               CAST({amount} AS DOUBLE) AS amount_received,
               'US Dollar' AS payment_currency, 'US Dollar' AS receiving_currency,
               'Wire' AS payment_format""")


def test_pass_through_is_zero_when_no_money_moved(spark):
    """Both sides zero can only reach `build_features` directly: the job quarantines a
    zero amount, and this is the contract Analyse calls, so it must not divide 0 by 0."""
    rows = prepare.build_features(one_transfer(spark, 0), EXPECTED_RATES,
                                  MEDIUM_PERIOD).collect()

    assert [(r.usd_in, r.usd_out, r.pass_through) for r in rows] == [(0.0, 0.0, 0.0)] * 2


def test_a_counterparty_feature_is_null_where_that_side_has_no_counterparty(spark):
    """With a single transfer the sender pays an account but no account pays it, and the
    receiver the other way round, so each is missing exactly one of the two."""
    rows = (prepare.build_features(one_transfer(spark, 100), EXPECTED_RATES, MEDIUM_PERIOD)
            .orderBy("bank_id").collect())

    assert [(r.bank_id, r.cp_max_fan_in, r.cp_max_fan_out) for r in rows] == [
        ("010", 1, None), ("3208", None, 1)]


def test_a_bank_view_changes_only_the_counterparty_features(spark, raw, curated):
    """Seam 2: `build_features` is the contract with Analyse, so it is called directly here.

    Every transfer of an account involves that account's bank, so a bank view holds all of
    its own accounts' transfers; only its view of their counterparties is narrower. So the
    two views agree on every feature but the two counterparty ones, which is ADR 0002's
    guarantee and the reason Analyse can read a change in its top K as a change in what one
    bank can see rather than in the code that computed it.
    """
    run_job(raw, curated)
    clean = transfers(spark, curated)
    bank_view = clean.where("sender_bank_id = '010' OR receiver_bank_id = '010'")
    # Rates come from the whole dataset, as Prepare derived them; a bank view is not asked
    # to re-derive them from the fraction of transfers it can see.
    rates = prepare.usd_rates(clean)
    own = [c for c in prepare.FEATURES if c not in prepare.COUNTERPARTY_FEATURES]

    def features_of_bank_010(df, columns):
        return (prepare.build_features(df, rates, MEDIUM_PERIOD).where("bank_id = '010'")
                .orderBy(*prepare.FEATURE_KEYS).select(*columns).collect())

    assert bank_view.count() < clean.count()  # the view really is narrower
    assert len(features_of_bank_010(bank_view, own)) == 4
    assert (features_of_bank_010(bank_view, [*prepare.FEATURE_KEYS, *own])
            == features_of_bank_010(clean, [*prepare.FEATURE_KEYS, *own]))
    # ... and this fixture is one where the two views really do disagree on them.
    assert (features_of_bank_010(bank_view, prepare.COUNTERPARTY_FEATURES)
            != features_of_bank_010(clean, prepare.COUNTERPARTY_FEATURES))
