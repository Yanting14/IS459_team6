"""Runs the Prepare job end to end on a hand-made raw zone and reads the curated zone back.

S3 is never mocked: the job takes local folders in place of S3 URIs.
"""

import json

import pytest

import prepare

HEADER = ("Timestamp,From Bank,Account,To Bank,Account,Amount Received,Receiving Currency,"
          "Amount Paid,Payment Currency,Payment Format,Is Laundering")

# A few banks (one with a leading zero) over three days, including a self-transfer.
MEDIUM_ROWS = [
    "2022/09/01 00:20,010,8000EBD30,3208,8000F4580,1000.00,US Dollar,1000.00,US Dollar,Wire,0",
    "2022/09/01 00:20,3208,8000F4580,3208,8000F4580,50.00,US Dollar,50.00,US Dollar,Reinvestment,0",
    "2022/09/01 09:15,1,800A1B2C3,010,8000EBD30,700.00,Euro,800.00,US Dollar,ACH,1",
    "2022/09/01 12:00,3208,8000F4580,1,800A1B2C3,300.00,US Dollar,300.00,US Dollar,Cheque,0",
    "2022/09/02 08:00,3208,8000F4580,010,8000EBD30,200.00,US Dollar,200.00,US Dollar,Wire,0",
    "2022/09/02 10:30,010,8000EBD30,1,800A1B2C3,150.00,US Dollar,150.00,US Dollar,ACH,0",
    "2022/09/02 23:59,1,800A1B2C3,3208,8000F4580,90.00,US Dollar,90.00,US Dollar,Credit Card,0",
    "2022/09/03 07:45,010,8000EBD30,3208,8000F4580,60.00,US Dollar,60.00,US Dollar,Cash,0",
]
SMALL_ROWS = [
    "2022/09/05 01:00,1,800A1B2C3,010,8000EBD30,5.00,US Dollar,5.00,US Dollar,Cash,0",
]
PREFIX = "ibm-aml/v1"


@pytest.fixture
def raw(tmp_path):
    root = tmp_path / "raw"
    for size, rows in (("medium", MEDIUM_ROWS), ("small", SMALL_ROWS)):
        folder = root / "transactions" / "variant=HI" / f"size={size}"
        folder.mkdir(parents=True)
        (folder / f"HI-{size.capitalize()}_Trans.csv").write_text("\n".join([HEADER, *rows]) + "\n")
    return root


@pytest.fixture
def curated(tmp_path):
    return tmp_path / "curated"


def run_job(raw, curated, *extra):
    prepare.main(["--input", str(raw), "--output", str(curated), "--version-prefix", PREFIX, *extra])


def transfers(spark, curated):
    return spark.read.parquet(str(curated / PREFIX / "transfers"))


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


def test_self_transfers_are_kept(spark, raw, curated):
    run_job(raw, curated)

    df = transfers(spark, curated)
    self_transfers = df.where("sender_bank_id = receiver_bank_id "
                              "AND sender_account_number = receiver_account_number").collect()
    assert [(r.sender_bank_id, r.payment_format) for r in self_transfers] == [("3208", "Reinvestment")]


def test_partitioned_by_date_and_sorted_by_sender_bank(spark, raw, curated):
    run_job(raw, curated)

    root = curated / PREFIX / "transfers"
    dates = sorted(p.name for p in root.glob("date=*"))
    assert dates == ["date=2022-09-01", "date=2022-09-02", "date=2022-09-03"]
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

    reports = list((curated / PREFIX / "quality").glob("*.json"))
    assert len(reports) == 1
    report = json.loads(reports[0].read_text())
    assert report["rows_in"] == len(MEDIUM_ROWS)
    assert report["rows_accepted"] == len(MEDIUM_ROWS)


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
