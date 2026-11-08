"""Runs the three BQ1 modes on a tiny hand-made curated zone of local folders."""

import csv
import json
from pathlib import Path

import pytest
from pyspark.sql import functions as F
from pyspark.sql import types as T

import bq1
import prepare

DAYS = ["2022-09-01", "2022-09-02", "2022-09-03", "2022-09-04", "2022-09-05"]
DATE_ARGS = ["--train-from", "2022-09-01", "--train-to", "2022-09-03",
             "--val", "2022-09-04", "--test", "2022-09-05", "--k", "3"]
BANKS = ["010", "3208", "1"]
ACCOUNTS = 60
INTS = {"fan_in", "fan_out", "cp_banks", "cp_max_fan_in", "cp_max_fan_out", "max_txn_per_day",
        "active_days", "n_pay_formats", "n_currencies", "history_days"}
SCHEMA = T.StructType([T.StructField("bank_id", T.StringType()),
                       T.StructField("account_number", T.StringType()),
                       T.StructField("date", T.StringType()),
                       *(T.StructField(f, T.LongType() if f in INTS else T.DoubleType())
                         for f in bq1.FEATURES)])


def account_day(i: int, d: int) -> dict:
    """Every tenth account launders, and looks it: many senders, money straight back out."""
    bad = i % 10 == 0
    wobble = (i * 7 + d * 3) % 5
    return {
        "fan_in": 20 + wobble if bad else 1 + wobble % 3,
        "fan_out": 2 + wobble % 2,
        "cp_banks": 3 + wobble if bad else 1 + wobble % 2,
        "cross_bank_share": 0.9 if bad else 0.1 * wobble,
        "cp_max_fan_in": None if i % 11 == 3 else 2 + wobble,
        "cp_max_fan_out": 1 + wobble,
        "usd_in": 5000.0 + 100 * wobble if bad else 100.0 + 10 * i,
        "usd_out": 4900.0 + 90 * wobble if bad else 80.0 + 5 * i,
        "max_usd": 900.0 if bad else 50.0 + i,
        "pass_through": 0.98 if bad else 0.1 * wobble,
        "hrs_in_to_out": None if i % 7 == 1 else (0.5 if bad else 10.0 + wobble),
        "max_txn_per_day": 8 + wobble if bad else 1 + wobble % 2,
        "active_days": 1 + d,
        "n_pay_formats": 1 + wobble % 3,
        "ach_share": 0.7 if bad else 0.05 * wobble,
        "n_currencies": 1 + wobble % 2,
        "history_days": 1 + d,
    }


def write_curated(spark, root: Path, labels: bool = True, tweak=None, label_of=None) -> str:
    rows, label_rows = [], []
    for d, day in enumerate(DAYS):
        for i in range(ACCOUNTS):
            values = account_day(i, d)
            if tweak:
                tweak(day, values)
            key = (BANKS[i % 3], f"00{i:04d}", day)
            rows.append((*key, *(values[f] for f in bq1.FEATURES)))
            label_rows.append((*key, label_of(i, day) if label_of else int(i % 10 == 0)))
    (spark.createDataFrame(rows, SCHEMA).write.partitionBy("date")
     .parquet(str(root / "features")))
    if labels:
        (spark.createDataFrame(label_rows, "bank_id string, account_number string, date string, "
                                           f"{bq1.LABEL} int")
         .write.partitionBy("date").parquet(str(root / "labels")))
    return str(root)


@pytest.fixture(scope="module")
def curated(spark, tmp_path_factory):
    return write_curated(spark, tmp_path_factory.mktemp("curated"))


@pytest.fixture(scope="module")
def trained(spark, curated, tmp_path_factory):
    out = tmp_path_factory.mktemp("models") / "v1"
    metrics = bq1.main(["train", "--curated", curated, "--out", str(out), *DATE_ARGS,
                        "--neg-fraction", "0.5"])
    return out, metrics


def score(curated, model, out, date="2022-09-05"):
    return bq1.main(["score", "--curated", curated, "--model", str(model), "--date", date,
                     "--k", "3", "--out", str(out)])


def read_queue(out: Path) -> list[dict]:
    with open(out / "accounts.csv", newline="") as f:
        return list(csv.DictReader(f))


def assert_success_last(out: Path):
    marker = out / "_SUCCESS"
    assert marker.exists()
    others = [p for p in out.rglob("*") if p.is_file() and p != marker and p.suffix != ".crc"]
    assert max(p.stat().st_mtime_ns for p in others) <= marker.stat().st_mtime_ns


# --- Contract with prepare -----------------------------------------------------

def test_copied_constants_equal_prepare():
    assert bq1.FEATURES == prepare.FEATURES
    assert bq1.LABEL == prepare.LABEL
    assert bq1.FEATURE_KEYS == prepare.FEATURE_KEYS


# --- Split and sampling --------------------------------------------------------

def test_the_default_split_puts_every_date_in_one_set_only():
    args = bq1.parse(["train", "--curated", "c", "--out", "o"])
    splits = bq1.split_days(args.train_from, args.train_to, args.val, args.test)
    days = [d for s in splits.values() for d in s]
    assert len(days) == len(set(days))
    assert splits["train"] == bq1.day_range("2022-09-07", "2022-09-12")
    assert splits["test"] == ["2022-09-15", "2022-09-16"]


def test_a_date_in_two_sets_is_refused():
    with pytest.raises(ValueError, match="2022-09-04"):
        bq1.split_days("2022-09-01", "2022-09-04", ["2022-09-04"], ["2022-09-05"])


def test_sampling_keeps_every_positive_and_is_deterministic(spark, curated):
    data = bq1.labelled(spark, curated, DAYS[:3])
    keys = lambda df: sorted(map(tuple, df.select(*bq1.FEATURE_KEYS).collect()))
    first = bq1.sample_negatives(data, 0.5, seed=1)
    assert first.where(f"{bq1.LABEL} = 1").count() == data.where(f"{bq1.LABEL} = 1").count()
    assert keys(first) == keys(bq1.sample_negatives(data.repartition(3), 0.5, seed=1))
    assert keys(first) != keys(bq1.sample_negatives(data, 0.5, seed=2))
    negatives = first.where(f"{bq1.LABEL} = 0").count()
    assert 0 < negatives < data.where(f"{bq1.LABEL} = 0").count()


def test_a_label_missing_from_its_feature_row_fails_the_join(spark, tmp_path):
    curated = write_curated(spark, tmp_path / "c")
    (spark.createDataFrame([("010", "999999", "2022-09-01", 0)],
                           f"bank_id string, account_number string, date string, {bq1.LABEL} int")
     .write.mode("append").partitionBy("date").parquet(f"{curated}/labels"))
    with pytest.raises(ValueError, match="1:1"):
        bq1.labelled(spark, curated, DAYS)


# --- Pipeline inputs -----------------------------------------------------------

@pytest.mark.parametrize("family", ["lr", "gbt"])
def test_no_label_column_reaches_the_assembler(family):
    pipeline = bq1.build_pipeline(family, {}, seed=1)
    assembler = next(s for s in pipeline.getStages() if isinstance(s, bq1.VectorAssembler))
    inputs = assembler.getInputCols()
    assert inputs == [f"{f}_in" for f in bq1.FEATURES] + [bq1.MISSING]
    assert not any("label" in c.lower() or "launder" in c.lower() for c in inputs)


# --- Metrics -------------------------------------------------------------------

def test_precision_and_recall_at_k_on_a_hand_checked_example(spark):
    # 09-01: five eligible, positives at ranks 1 and 4 and 5, so k=2 catches one of three.
    # 09-02: one eligible and positive, fewer than k, so it is reviewed in full.
    rows = [("1", "a", "2022-09-01", 0.9, 1), ("1", "b", "2022-09-01", 0.8, 0),
            ("1", "c", "2022-09-01", 0.7, 0), ("1", "d", "2022-09-01", 0.6, 1),
            ("1", "e", "2022-09-01", 0.6, 1), ("1", "f", "2022-09-02", 0.1, 1)]
    df = spark.createDataFrame(rows, f"bank_id string, account_number string, date string, "
                                     f"score double, {bq1.LABEL} int")
    days = bq1.daily_metrics(bq1.rank_daily(df, bq1.model_order()), k=2)
    assert days == [
        {"date": "2022-09-01", "eligible": 5, "reviewed": 2, "positives": 3, "tp": 1,
         "precision_at_k": 0.5, "recall_at_k": 1 / 3},
        {"date": "2022-09-02", "eligible": 1, "reviewed": 1, "positives": 1, "tp": 1,
         "precision_at_k": 1.0, "recall_at_k": 1.0},
    ]


def test_score_ties_are_broken_by_the_account(spark):
    rows = [("2", "a", "2022-09-01", 0.5), ("1", "b", "2022-09-01", 0.5),
            ("1", "a", "2022-09-01", 0.5)]
    df = spark.createDataFrame(rows, "bank_id string, account_number string, date string, "
                                     "score double")
    ranked = bq1.rank_daily(df, bq1.model_order()).orderBy("rank").collect()
    assert [(r["bank_id"], r["account_number"]) for r in ranked] == [("1", "a"), ("1", "b"),
                                                                    ("2", "a")]


# --- Train ---------------------------------------------------------------------

def test_train_reports_every_grid_entry_and_the_rule(trained):
    out, _ = trained
    metrics = json.loads((out / "metrics.json").read_text())
    assert [(g["model"], g["params"]) for g in metrics["grid"]] == \
        [(m, p) for m, p in bq1.GRID]
    assert len(metrics["grid"]) == 7
    assert metrics["selection_rule"] == bq1.SELECTION_RULE
    assert {metrics["winner"], metrics["runner_up"]} == {"lr", "gbt"}
    assert (out / "model").is_dir() and (out / "runner_up").is_dir()
    assert_success_last(out)


def test_train_never_samples_validation_or_test_and_tests_on_test_days_only(trained):
    _, metrics = trained
    for entry in metrics["grid"]:
        assert [d["date"] for d in entry["val"]] == ["2022-09-04"]
        assert entry["val"][0]["eligible"] == ACCOUNTS
    for result in metrics["results"].values():
        assert [d["date"] for d in result["test"]] == ["2022-09-05"]
        assert result["test"][0]["eligible"] == ACCOUNTS
        assert result["test"][0]["positives"] == ACCOUNTS // 10
    assert metrics["train_rows"]["positives"] == 3 * ACCOUNTS // 10
    assert metrics["train_rows"]["negatives_sampled"] < 3 * ACCOUNTS * 9 // 10


def test_the_saved_model_assembles_no_label(trained):
    out, _ = trained
    inputs = bq1.assembler_inputs(bq1.PipelineModel.load(str(out / "model")))
    assert bq1.LABEL not in inputs and not any("label" in c.lower() for c in inputs)


def test_train_refuses_an_existing_output(spark, curated, trained):
    out, _ = trained
    with pytest.raises(FileExistsError):
        bq1.main(["train", "--curated", curated, "--out", str(out), *DATE_ARGS])


# --- Baseline ------------------------------------------------------------------

def test_baseline_thresholds_come_from_training_days_only(spark, curated, tmp_path):
    def inflate_later_days(day, values):
        if day not in DAYS[:3]:
            for f in ("fan_in", "fan_out", "pass_through", "max_usd"):
                values[f] = values[f] * 1000

    changed = write_curated(spark, tmp_path / "changed", tweak=inflate_later_days)
    first = bq1.main(["baseline", "--curated", curated, "--out", str(tmp_path / "a"), *DATE_ARGS])
    second = bq1.main(["baseline", "--curated", changed, "--out", str(tmp_path / "b"), *DATE_ARGS])
    assert first["thresholds"] == second["thresholds"]
    assert set(first["thresholds"]) == {f"{f}@{p}" for _, c in bq1.RULES for f, p, _ in c}
    assert [d["date"] for d in first["test"]] == ["2022-09-05"]
    assert_success_last(tmp_path / "a")


def test_baseline_counts_rules_fired_and_ranks_the_launderers_first(spark, curated, tmp_path):
    metrics = bq1.main(["baseline", "--curated", curated, "--out", str(tmp_path / "b"),
                        *DATE_ARGS])
    assert metrics["test"][0]["tp"] == 3


def test_tuned_baseline_reports_the_test_days_with_weights_chosen_on_validation(spark, curated,
                                                                                 tmp_path):
    plain = bq1.main(["baseline", "--curated", curated, "--out", str(tmp_path / "p"), *DATE_ARGS])
    tuned = bq1.main(["baseline-tuned", "--curated", curated, "--out", str(tmp_path / "t"),
                      *DATE_ARGS])
    assert set(tuned["weights"]) == {name for name, _ in bq1.RULES}
    assert "(4095)" in tuned["grid"]
    assert tuned["thresholds"] == plain["thresholds"]
    # The label-free weighting is in the grid, so the tuned rules cannot do worse on validation.
    assert tuned["val_mean_precision_at_k"] >= bq1.mean_precision(plain["val"])
    assert [d["date"] for d in tuned["val"]] == ["2022-09-04"]
    assert [d["date"] for d in tuned["test"]] == ["2022-09-05"]
    assert_success_last(tmp_path / "t")


def test_test_day_labels_cannot_change_the_tuned_weights(spark, curated, tmp_path):
    def other_test_labels(i, day):
        return int(i % 10 == 5) if day == "2022-09-05" else int(i % 10 == 0)

    changed = write_curated(spark, tmp_path / "changed", label_of=other_test_labels)
    first = bq1.main(["baseline-tuned", "--curated", curated, "--out", str(tmp_path / "a"),
                      *DATE_ARGS])
    second = bq1.main(["baseline-tuned", "--curated", changed, "--out", str(tmp_path / "b"),
                       *DATE_ARGS])
    assert first["weights"] == second["weights"]
    assert first["val"] == second["val"]
    assert first["test"] != second["test"]


def test_tuning_measures_every_weighting_exactly_as_the_spark_ranking_does(spark):
    # Two days, overlapping rules, tied and missing usd_out, and k cutting through ties.
    rows = []
    for day in ("2022-09-01", "2022-09-02"):
        for i in range(40):
            bits = [int((i * (r + 3) + len(day) * r) % (r + 4) == 0) for r in range(len(bq1.RULES))]
            usd = None if i % 9 == 0 else float((i * 7) % 5)
            rows.append(("1", f"{i:03d}", day, usd, int((i * 5) % 7 < 2), *bits))
    flags = [f"rule_{r}" for r in range(len(bq1.RULES))]
    df = spark.createDataFrame(rows, "bank_id string, account_number string, date string, "
                                     f"usd_out double, {bq1.LABEL} int, "
                                     + ", ".join(f"{f} int" for f in flags))
    k = 5
    eligible = {"2022-09-01": 40, "2022-09-02": 40}
    _, _, results = bq1.tune_weights(bq1.tuning_candidates(df, k), eligible, k)
    measured = dict(results)
    for w in [(1,) * 6, (0, 0, 0, 0, 0, 1), (3, 0, 1, 2, 0, 1), (0, 2, 0, 0, 3, 0), (1, 3, 2, 0, 1, 1)]:
        scored = df.withColumn("score", sum((df[f] * x for f, x in zip(flags, w)), F.lit(0)))
        days = bq1.daily_metrics(bq1.rank_daily(scored, bq1.rule_order()), k)
        assert measured[w] == pytest.approx(bq1.mean_precision(days), abs=1e-12), w


# --- Score ---------------------------------------------------------------------

@pytest.fixture(scope="module")
def unlabelled(spark, tmp_path_factory):
    """The same features with no labels table at all, so scoring cannot have read one."""
    return write_curated(spark, tmp_path_factory.mktemp("unlabelled"), labels=False)


def test_score_writes_a_queue_without_reading_labels(unlabelled, trained, tmp_path):
    model, _ = trained
    out = tmp_path / "queue"
    ranking = score(unlabelled, model, out)
    queue = read_queue(out)
    assert list(queue[0]) == ["rank", "bank_id", "account_id", "score", "model_version",
                              "window_start", "cutoff", "reason_1", "reason_2", "reason_3"]
    assert [r["rank"] for r in queue] == ["1", "2", "3"]
    assert all(r["model_version"] == "v1" and r["window_start"] == "2022-09-05"
               and r["cutoff"] == "2022-09-06" for r in queue)
    assert all(r["reason_1"] and r["reason_2"] and r["reason_3"] for r in queue)
    assert ranking["eligible"] == ACCOUNTS and ranking["reviewed"] == 3
    assert "not model attributions" in ranking["reasons"]
    assert "not a probability" in ranking["score"]
    assert_success_last(out)


def test_score_keeps_leading_zeros_in_bank_and_account(unlabelled, trained, tmp_path):
    model, _ = trained
    out = tmp_path / "queue"
    score(unlabelled, model, out)
    queue = read_queue(out)
    assert all(len(r["account_id"]) == 6 and r["account_id"].startswith("00") for r in queue)
    assert {r["bank_id"] for r in queue} <= set(BANKS)
    assert "010" in {r["bank_id"] for r in queue}  # account 0 is bank 010 and launders


def test_score_refuses_an_existing_output(unlabelled, trained, tmp_path):
    model, _ = trained
    (tmp_path / "queue").mkdir()
    with pytest.raises(FileExistsError):
        score(unlabelled, model, tmp_path / "queue")


def test_score_refuses_an_unfinished_model(unlabelled, tmp_path):
    (tmp_path / "half-trained" / "model").mkdir(parents=True)
    with pytest.raises(FileNotFoundError, match="_SUCCESS"):
        score(unlabelled, tmp_path / "half-trained", tmp_path / "queue")


# --- Reasons -------------------------------------------------------------------

def test_every_feature_has_a_reason_direction():
    assert set(bq1.REASON_DIRECTION) == set(bq1.FEATURES)
    assert bq1.REASON_DIRECTION["hrs_in_to_out"] == bq1.LOW
    assert bq1.REASON_DIRECTION["history_days"] == bq1.NEVER
    assert bq1.REASON_DIRECTION["active_days"] == bq1.NEVER


def test_a_low_hrs_in_to_out_is_a_reason_and_history_never_is():
    row = {}
    for f in bq1.FEATURES:
        row[f] = 1
        row[f"{f}_share"] = 90
    row.update(hrs_in_to_out=0.25, hrs_in_to_out_share=1, fan_in=412, fan_in_share=2,
               usd_out=1234.5, usd_out_share=5)
    # The never-reasons would be the most extreme of all if they were looked at.
    row.update(history_days_share=0, active_days_share=0)
    assert bq1.reasons(row, eligible=1000) == ["hrs_in_to_out in lowest 0.1% (0.25)",
                                               "fan_in in top 0.2% (412)",
                                               "usd_out in top 0.5% (1234.50)"]


def test_a_missing_value_is_never_a_reason():
    row = {}
    for f in bq1.FEATURES:
        row[f] = 1
        row[f"{f}_share"] = 50
    row.update(hrs_in_to_out=None, hrs_in_to_out_share=None)
    assert not any("hrs_in_to_out" in r for r in bq1.reasons(row, eligible=100))
