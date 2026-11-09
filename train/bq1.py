"""BQ1: rank account-days for AML review from the seven-day features in the curated zone.

Runs as a Spark job on EMR Serverless, and locally in tests. Locations may be S3 URIs or
local folders. Every mode refuses an output that already exists and writes `_SUCCESS` last.

Modes:
  train     fit logistic regression and gradient-boosted trees on the bank-visible features,
            pick the winner on the validation days, report both on the test days, save both
            models
  baseline  hand-written AML rules on the same features, thresholds from training days only
  baseline-tuned
            the same rules, weighted 0-3 each to do best on the validation days' labels: a
            best case for rules tuned by hand, reported on the test days
  score     rank one day's account-days with a pinned model; reads no labels

Arguments (train, baseline and baseline-tuned):
  --curated       curated version prefix, e.g. s3://<curated>/ibm-aml/v2
  --out           output folder, e.g. s3://<models>/bq1/<version>
  --train-from, --train-to, --val, --test, --k, --neg-fraction, --seed

Arguments (score):
  --curated, --model (a train output), --date, --k, --out
"""

from __future__ import annotations

import argparse
import csv
import io
import itertools
import json
import logging
from datetime import date, datetime, timedelta, timezone

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import GBTClassifier, LogisticRegression
from pyspark.ml.feature import Imputer, SQLTransformer, StandardScaler, VectorAssembler
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("bq1")

# Copied from prepare.py, which is not shipped to EMR beside this script; a test keeps
# the two equal.
LABEL = "laundered_24h"
ACCOUNT = ["bank_id", "account_number"]
FEATURE_KEYS = [*ACCOUNT, "date"]
FEATURES = ["fan_in", "fan_out", "cp_banks", "cross_bank_share", "cp_max_fan_in",
            "cp_max_fan_out", "usd_in", "usd_out", "max_usd", "pass_through", "hrs_in_to_out",
            "max_txn_per_day", "active_days", "n_pay_formats", "ach_share", "n_currencies",
            "history_days"]
# The model sees only what the account's own bank can compute from its accounts' transfers.
# cp_max_fan_in and cp_max_fan_out count the counterparties' other transfers, which sit
# at other banks, so BQ1 leaves them out. prepare.py still writes them.
NOT_BANK_VISIBLE = ("cp_max_fan_in", "cp_max_fan_out")
MODEL_FEATURES = [f for f in FEATURES if f not in NOT_BANK_VISIBLE]

MISSING = "hrs_in_to_out_missing"
# Counts and USD amounts are heavy-tailed; logistic regression sees them through log1p.
LOGGED = ["fan_in", "fan_out", "cp_banks", "usd_in", "usd_out", "max_usd", "max_txn_per_day",
          "n_pay_formats", "n_currencies"]

GRID = [("lr", {"regParam": r}) for r in (0.0, 0.01, 0.1)] + \
       [("gbt", {"maxDepth": d, "maxIter": i}) for d in (3, 5) for i in (50, 100)]
SELECTION_RULE = ("Each model family's setting, and then the overall winner, is the one with "
                  "the highest mean daily Precision@K over the validation days; ties go to "
                  "the earlier entry in the grid. Fixed before any test day was scored.")

# Which way a feature's value is unusual when it is given as a reason. Never-reasons
# describe how long an account has been seen, not what it did.
HIGH, LOW, NEVER = "high", "low", "never"
REASON_DIRECTION = {f: HIGH for f in MODEL_FEATURES}
REASON_DIRECTION.update(hrs_in_to_out=LOW, history_days=NEVER, active_days=NEVER)
REASONS_NOTE = ("Reasons are descriptive flags, not model attributions: each of the account's "
                "features is placed against that day's eligible account-days, and the three "
                "where the account is most extreme are listed. 'top x%' means x% of eligible "
                "account-days have this value or higher; 'lowest x%' means this value or lower.")
SCORE_NOTE = ("score is a ranking value, not a probability: negatives were sampled in "
              "training, so its scale is not calibrated.")

# Rule name, then (feature, percentile, direction) conditions that must all hold.
RULES = [
    ("rapid_pass_through", [("pass_through", 0.99, ">="), ("hrs_in_to_out", 0.10, "<=")]),
    ("funnel", [("fan_in", 0.99, ">=")]),
    ("fan_out", [("fan_out", 0.99, ">=")]),
    ("bursts", [("max_txn_per_day", 0.99, ">=")]),
    ("cross_bank_spread", [("cp_banks", 0.99, ">=")]),
    ("large_payment", [("max_usd", 0.99, ">=")]),
]

# The tuned baseline weighs each rule 0-3, where 0 drops it, choosing on validation labels.
WEIGHTS = (0, 1, 2, 3)
TUNED_RULE = ("The weighting with the highest mean daily Precision@K over the validation days "
              "wins; ties go to the earlier weighting in itertools.product order. Thresholds are "
              "the label-free baseline's.")
TUNED_NOTE = ("Uses the validation days' labels to choose rules and weights, so it is a best case "
              "for hand-tuned rules, not label-free monitoring. Test days are never used to choose.")


# --- Files ---------------------------------------------------------------------

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


def write_json(spark: SparkSession, path: str, data: dict) -> None:
    write_text(spark, path, json.dumps(data, indent=2, sort_keys=True) + "\n")


def refuse_existing(spark: SparkSession, out: str) -> None:
    if exists(spark, out):
        raise FileExistsError(f"{out} already exists; reruns write to a new location")


# --- Data ----------------------------------------------------------------------

def day_range(first: str, last: str) -> list[str]:
    start, end = date.fromisoformat(first), date.fromisoformat(last)
    return [str(start + timedelta(days=n)) for n in range((end - start).days + 1)]


def split_days(train_from: str, train_to: str, val: list[str], test: list[str]) -> dict:
    splits = {"train": day_range(train_from, train_to), "val": sorted(val), "test": sorted(test)}
    seen = {}
    for name, days in splits.items():
        if not days:
            raise ValueError(f"the {name} split has no days")
        for day in days:
            if day in seen:
                raise ValueError(f"{day} is in both {seen[day]} and {name}")
            seen[day] = name
    return splits


def read_table(spark: SparkSession, curated: str, table: str, days: list[str]):
    # The date is a partition column; read it back as the ISO string it was written from.
    return (spark.read.parquet(f"{curated.rstrip('/')}/{table}")
            .withColumn("date", F.col("date").cast("string"))
            .where(F.col("date").isin(days)))


def labelled(spark: SparkSession, curated: str, days: list[str]):
    """Features joined to labels, after checking the two tables share one grain exactly."""
    features = read_table(spark, curated, "features", days).select(*FEATURE_KEYS, *MODEL_FEATURES)
    labels = read_table(spark, curated, "labels", days).select(*FEATURE_KEYS, LABEL)
    joined = features.join(labels, FEATURE_KEYS, "inner").cache()
    n_features, n_labels, n_joined = features.count(), labels.count(), joined.count()
    n_keys = joined.select(*FEATURE_KEYS).distinct().count()
    if not n_features == n_labels == n_joined == n_keys:
        raise ValueError(f"features ({n_features}) and labels ({n_labels}) do not join 1:1 "
                         f"({n_joined} joined, {n_keys} distinct account-days)")
    return joined


def sample_negatives(df, fraction: float, seed: int):
    """Every positive, and a share of negatives chosen by a hash of the account-day.

    A hash rather than rand() keeps the sample the same however Spark partitions the data.
    """
    draw = (F.abs(F.xxhash64(*FEATURE_KEYS, F.lit(seed))) % 1_000_000) / 1_000_000
    return df.where((F.col(LABEL) == 1) | (draw < fraction))


# --- Models --------------------------------------------------------------------

def model_inputs() -> list[str]:
    inputs = [f"{f}_in" for f in MODEL_FEATURES] + [MISSING]
    assert not any(LABEL in c or "label" in c.lower() for c in inputs)
    return inputs


def build_pipeline(family: str, params: dict, seed: int) -> Pipeline:
    cast = ", ".join(f"CAST({f} AS DOUBLE) AS {f}_raw" for f in MODEL_FEATURES)
    stages = [
        SQLTransformer(statement=f"SELECT *, {cast}, "
                                 f"CAST(hrs_in_to_out IS NULL AS DOUBLE) AS {MISSING} "
                                 f"FROM __THIS__"),
        Imputer(strategy="median", inputCols=[f"{f}_raw" for f in MODEL_FEATURES],
                outputCols=[f"{f}_imp" for f in MODEL_FEATURES]),
    ]
    if family == "lr":
        moved = ", ".join(f"LOG1P({f}_imp) AS {f}_in" if f in LOGGED else f"{f}_imp AS {f}_in"
                          for f in MODEL_FEATURES)
    else:
        moved = ", ".join(f"{f}_imp AS {f}_in" for f in MODEL_FEATURES)
    stages.append(SQLTransformer(statement=f"SELECT *, {moved} FROM __THIS__"))
    stages.append(VectorAssembler(inputCols=model_inputs(), outputCol="assembled"))
    if family == "lr":
        stages.append(StandardScaler(inputCol="assembled", outputCol="vector",
                                     withMean=True, withStd=True))
        stages.append(LogisticRegression(featuresCol="vector", labelCol=LABEL,
                                         maxIter=100, **params))
    else:
        stages.append(GBTClassifier(featuresCol="assembled", labelCol=LABEL, seed=seed,
                                    **params))
    return Pipeline(stages=stages)


def assembler_inputs(model: PipelineModel) -> list[str]:
    return next(s.getInputCols() for s in model.stages if isinstance(s, VectorAssembler))


# pyspark.ml.functions.vector_to_array would need pandas, which EMR does not promise.
positive_class = F.udf(lambda v: float(v[1]), T.DoubleType())


def with_score(model: PipelineModel, df):
    return (model.transform(df)
            .withColumn("score", positive_class("probability"))
            .select(*FEATURE_KEYS, *MODEL_FEATURES,
                    *([LABEL] if LABEL in df.columns else []), "score"))


def importances(family: str, model: PipelineModel) -> dict:
    names = assembler_inputs(model)
    last = model.stages[-1]
    values = last.coefficients.toArray() if family == "lr" else last.featureImportances.toArray()
    return {n.removesuffix("_in"): float(v) for n, v in zip(names, values)}


# --- Ranking and metrics ------------------------------------------------------

def rank_daily(df, order):
    return df.withColumn("rank", F.row_number().over(Window.partitionBy("date").orderBy(*order)))


def model_order():
    return [F.desc("score"), *(F.asc(c) for c in ACCOUNT)]


def daily_metrics(ranked, k: int) -> list[dict]:
    """Per day: eligible account-days, positives, true positives in the top k, and
    Precision@k and Recall@k. A day with fewer than k eligible reviews all of them, and its
    precision is over that smaller number, as in the GIN evaluation."""
    rows = (ranked.groupBy("date")
            .agg(F.count("*").alias("eligible"),
                 F.sum(LABEL).alias("positives"),
                 F.sum(F.when(F.col("rank") <= k, F.col(LABEL)).otherwise(0)).alias("tp"))
            .orderBy("date").collect())
    out = []
    for r in rows:
        reviewed = min(k, r["eligible"])
        out.append({"date": r["date"], "eligible": r["eligible"], "reviewed": reviewed,
                    "positives": int(r["positives"]), "tp": int(r["tp"]),
                    "precision_at_k": r["tp"] / reviewed if reviewed else None,
                    "recall_at_k": r["tp"] / r["positives"] if r["positives"] else None})
    return out


def mean_precision(days: list[dict]) -> float:
    return sum(d["precision_at_k"] or 0.0 for d in days) / len(days)


# --- Modes ---------------------------------------------------------------------

def train(spark: SparkSession, args) -> dict:
    out = args.out.rstrip("/")
    refuse_existing(spark, out)
    splits = split_days(args.train_from, args.train_to, args.val, args.test)
    data = labelled(spark, args.curated, [d for days in splits.values() for d in days])

    train_set = sample_negatives(data.where(F.col("date").isin(splits["train"])),
                                 args.neg_fraction, args.seed).cache()
    val_set = data.where(F.col("date").isin(splits["val"])).cache()
    counts = {r[LABEL]: r["count"] for r in train_set.groupBy(LABEL).count().collect()}
    log.info("Training on %s positives and %s sampled negatives",
             counts.get(1, 0), counts.get(0, 0))

    grid, fitted = [], {}
    for i, (family, params) in enumerate(GRID):
        model = build_pipeline(family, params, args.seed).fit(train_set)
        val = daily_metrics(rank_daily(with_score(model, val_set), model_order()), args.k)
        entry = {"model": family, "params": params, "val": val,
                 "val_mean_precision_at_k": mean_precision(val)}
        log.info("%s %s: validation Precision@%d %.4f", family, params, args.k,
                 entry["val_mean_precision_at_k"])
        grid.append(entry)
        fitted[i] = model

    best = {}
    for i, entry in enumerate(grid):
        family = entry["model"]
        if family not in best or entry["val_mean_precision_at_k"] > \
                grid[best[family]]["val_mean_precision_at_k"]:
            best[family] = i
    ordered = sorted(best.items(), key=lambda kv: (-grid[kv[1]]["val_mean_precision_at_k"], kv[1]))
    (winner, w), (runner_up, r) = ordered

    test_set = data.where(F.col("date").isin(splits["test"]))
    results = {}
    for family, i in ((winner, w), (runner_up, r)):
        ranked = rank_daily(with_score(fitted[i], test_set), model_order())
        results[family] = {"params": grid[i]["params"], "val": grid[i]["val"],
                           "test": daily_metrics(ranked, args.k),
                           "feature_importances": importances(family, fitted[i])}

    fitted[w].write().save(f"{out}/model")
    fitted[r].write().save(f"{out}/runner_up")
    metrics = {
        "created": datetime.now(timezone.utc).isoformat(),
        "curated": args.curated, "k": args.k, "splits": splits,
        "neg_fraction": args.neg_fraction, "seed": args.seed,
        "train_rows": {"positives": counts.get(1, 0), "negatives_sampled": counts.get(0, 0)},
        "selection_rule": SELECTION_RULE, "grid": grid,
        "winner": winner, "runner_up": runner_up, "results": results,
        "score_note": SCORE_NOTE,
        "importance_note": "lr: coefficients on standardised inputs; gbt: Spark's "
                           "featureImportances",
    }
    write_json(spark, f"{out}/metrics.json", metrics)
    write_text(spark, f"{out}/_SUCCESS", "")
    log.info("Winner %s, written to %s", winner, out)
    return metrics


def rule_thresholds(train_rows) -> dict:
    wanted = sorted({(f, p) for _, conditions in RULES for f, p, _ in conditions})
    row = train_rows.agg(*(F.expr(f"percentile(CAST({f} AS DOUBLE), {p})").alias(f"{f}@{p}")
                           for f, p in wanted)).first()
    return {f"{f}@{p}": row[f"{f}@{p}"] for f, p in wanted}


def rule_flags(thresholds: dict) -> list:
    """One 0/1 column per rule, in RULES order; a missing value never fires a rule."""
    fired = []
    for _, conditions in RULES:
        test = None
        for f, p, op in conditions:
            t = thresholds[f"{f}@{p}"]
            c = F.col(f) >= t if op == ">=" else F.col(f) <= t
            test = c if test is None else test & c
        fired.append(F.coalesce(test, F.lit(False)).cast("int"))
    return fired


def rule_score(thresholds: dict, weights=None):
    flags = rule_flags(thresholds)
    weights = weights or [1] * len(flags)
    terms = [flag * w for flag, w in zip(flags, weights)]
    return sum(terms[1:], terms[0])


def rule_order():
    return [F.desc("score"), F.desc_nulls_last("usd_out"), *(F.asc(c) for c in ACCOUNT)]


def baseline(spark: SparkSession, args) -> dict:
    out = args.out.rstrip("/")
    refuse_existing(spark, out)
    splits = split_days(args.train_from, args.train_to, args.val, args.test)
    # Thresholds read the training days' features only, never a label.
    thresholds = rule_thresholds(read_table(spark, args.curated, "features", splits["train"]))
    data = labelled(spark, args.curated, splits["val"] + splits["test"])
    scored = data.withColumn("score", rule_score(thresholds))
    order = rule_order()
    metrics = {
        "created": datetime.now(timezone.utc).isoformat(),
        "curated": args.curated, "k": args.k, "splits": splits,
        "rules": [{"name": n, "conditions": [f"{f} {op} p{round(p * 100)} ({f}@{p})"
                                             for f, p, op in c]} for n, c in RULES],
        "thresholds": thresholds,
        "score": "number of rules fired; ties by usd_out descending, then the account",
    }
    for split in ("val", "test"):
        ranked = rank_daily(scored.where(F.col("date").isin(splits[split])), order)
        metrics[split] = daily_metrics(ranked, args.k)
    write_json(spark, f"{out}/metrics.json", metrics)
    write_text(spark, f"{out}/_SUCCESS", "")
    log.info("Baseline written to %s", out)
    return metrics


def weight_grid() -> list[tuple]:
    return [w for w in itertools.product(WEIGHTS, repeat=len(RULES)) if any(w)]


def tuning_candidates(scored, k: int):
    """Per day, every account-day that fires a rule plus the first k that fire none, in the
    rules' tie order. A non-negative weighting cannot put any other row in a day's top k:
    it scores 0 and at least k rows scoring 0 come before it."""
    flags = [f"rule_{i}" for i in range(len(RULES))]
    fired = sum((F.col(f) for f in flags[1:]), F.col(flags[0])) > 0
    tie = rule_order()[1:]
    rows = (scored
            .withColumn("tie", F.row_number().over(Window.partitionBy("date").orderBy(*tie)))
            .withColumn("quiet_rank", F.row_number().over(
                Window.partitionBy("date", fired).orderBy(*tie)))
            .where(fired | (F.col("quiet_rank") <= k))
            .select("date", "tie", LABEL, *flags).collect())
    by_day = {}
    for r in rows:
        by_day.setdefault(r["date"], []).append(r)
    return by_day


def tune_weights(by_day: dict, eligible: dict, k: int):
    """Exact daily Precision@k of every weighting on the candidates; the highest mean wins,
    ties to the earlier weighting in grid order."""
    import numpy as np

    days = []
    for day in sorted(by_day):
        rows = by_day[day]
        bits = np.array([[r[f"rule_{i}"] for i in range(len(RULES))] for r in rows], dtype=np.int64)
        tie = np.array([r["tie"] for r in rows], dtype=np.int64)
        label = np.array([r[LABEL] for r in rows], dtype=np.int64)
        days.append((day, bits, tie, label, min(k, eligible[day])))
    best, best_mean, results = None, -1.0, []
    for w in weight_grid():
        precisions = []
        for day, bits, tie, label, reviewed in days:
            s = bits @ np.array(w, dtype=np.int64)
            # Higher score first, then the tie order: one integer key, smallest first.
            key = (s.max() - s) * (int(tie.max()) + 1) + tie
            top = np.argpartition(key, reviewed - 1)[:reviewed] if reviewed < len(key) else key.argsort()
            precisions.append(int(label[top].sum()) / reviewed if reviewed else 0.0)
        mean = sum(precisions) / len(precisions)
        results.append((w, mean))
        if mean > best_mean:
            best, best_mean = w, mean
    return best, best_mean, results


def baseline_tuned(spark: SparkSession, args) -> dict:
    out = args.out.rstrip("/")
    refuse_existing(spark, out)
    splits = split_days(args.train_from, args.train_to, args.val, args.test)
    # The same label-free thresholds as the baseline; only the weights see labels, and only
    # the validation days' labels.
    thresholds = rule_thresholds(read_table(spark, args.curated, "features", splits["train"]))
    data = labelled(spark, args.curated, splits["val"] + splits["test"])
    flagged = data.select("*", *(f.alias(f"rule_{i}") for i, f in enumerate(rule_flags(thresholds))))
    val = flagged.where(F.col("date").isin(splits["val"])).cache()
    eligible = {r["date"]: r["count"] for r in val.groupBy("date").count().collect()}
    weights, val_mean, results = tune_weights(tuning_candidates(val, args.k), eligible, args.k)
    log.info("Best weights %s: validation Precision@%d %.4f", weights, args.k, val_mean)

    weighted = sum((F.col(f"rule_{i}") * w for i, w in enumerate(weights) if w), F.lit(0))
    metrics = {
        "created": datetime.now(timezone.utc).isoformat(),
        "curated": args.curated, "k": args.k, "splits": splits,
        "rules": [{"name": n, "conditions": [f"{f} {op} p{round(p * 100)} ({f}@{p})"
                                             for f, p, op in c]} for n, c in RULES],
        "thresholds": thresholds,
        "weights": {name: w for (name, _), w in zip(RULES, weights)},
        "grid": f"every weighting in {list(WEIGHTS)}^{len(RULES)} except all zero "
                f"({len(results)}); 0 drops a rule",
        "selection_rule": TUNED_RULE,
        "label_use": TUNED_NOTE,
        "score": "weighted sum of rules fired; ties by usd_out descending, then the account",
    }
    for split in ("val", "test"):
        ranked = rank_daily(flagged.where(F.col("date").isin(splits[split]))
                            .withColumn("score", weighted), rule_order())
        metrics[split] = daily_metrics(ranked, args.k)
    if abs(mean_precision(metrics["val"]) - val_mean) > 1e-12:
        raise AssertionError(f"tuning measured {val_mean} on validation, Spark ranking measured "
                             f"{mean_precision(metrics['val'])}")
    metrics["val_mean_precision_at_k"] = val_mean
    write_json(spark, f"{out}/metrics.json", metrics)
    write_text(spark, f"{out}/_SUCCESS", "")
    log.info("Tuned baseline written to %s", out)
    return metrics


def reason_text(feature: str, share: float, value) -> str:
    pct = f"{share * 100:.2g}"
    if float(value).is_integer():
        shown = str(int(value))
    else:
        shown = f"{value:.2f}"
    side = "top" if REASON_DIRECTION[feature] == HIGH else "lowest"
    return f"{feature} in {side} {pct}% ({shown})"


def reasons(row, eligible: int, n: int = 3) -> list[str]:
    shares = [(row[f"{f}_share"] / eligible, f) for f, d in REASON_DIRECTION.items()
              if d != NEVER and row[f] is not None]
    return [reason_text(f, s, row[f]) for s, f in sorted(shares)[:n]]


def score(spark: SparkSession, args) -> dict:
    out = args.out.rstrip("/")
    refuse_existing(spark, out)
    model_dir = args.model.rstrip("/")
    if not exists(spark, f"{model_dir}/_SUCCESS"):
        raise FileNotFoundError(f"{model_dir} has no _SUCCESS; it is not a finished train run")
    model = PipelineModel.load(f"{model_dir}/model")
    version = model_dir.rsplit("/", 1)[-1]

    # No labels are read in this mode.
    day = (read_table(spark, args.curated, "features", [args.date])
           .select(*FEATURE_KEYS, *MODEL_FEATURES))
    scored = with_score(model, day)
    for f, d in REASON_DIRECTION.items():
        if d == NEVER:
            continue
        # The window's default frame runs to the last tie, so this counts every
        # account-day with this value or a more extreme one.
        w = Window.orderBy(F.desc_nulls_last(f) if d == HIGH else F.asc_nulls_last(f))
        scored = scored.withColumn(f"{f}_share", F.when(F.col(f).isNotNull(),
                                                        F.count("*").over(w)))
    ranked = rank_daily(scored, model_order()).cache()
    eligible = ranked.count()
    top = ranked.where(F.col("rank") <= args.k).orderBy("rank").collect()

    cutoff = str(date.fromisoformat(args.date) + timedelta(days=1))
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerow(["rank", "bank_id", "account_id", "score", "model_version", "window_start",
                     "cutoff", "reason_1", "reason_2", "reason_3"])
    for row in top:
        writer.writerow([row["rank"], row["bank_id"], row["account_number"], repr(row["score"]),
                         version, args.date, cutoff, *reasons(row, eligible)])
    write_text(spark, f"{out}/accounts.csv", buf.getvalue())
    ranking = {"k": args.k, "date": args.date, "window_start": args.date, "cutoff": cutoff,
               "eligible": eligible, "reviewed": len(top), "model": model_dir,
               "model_version": version, "reasons": REASONS_NOTE, "score": SCORE_NOTE,
               "order": "score descending, then bank_id, then account_number"}
    write_json(spark, f"{out}/ranking.json", ranking)
    write_text(spark, f"{out}/_SUCCESS", "")
    log.info("Scored %d eligible account-days on %s, top %d written to %s",
             eligible, args.date, len(top), out)
    return ranking


def parse(argv: list[str] | None):
    parser = argparse.ArgumentParser()
    modes = parser.add_subparsers(dest="mode", required=True)
    for name in ("train", "baseline", "baseline-tuned"):
        m = modes.add_parser(name)
        m.add_argument("--curated", required=True)
        m.add_argument("--out", required=True)
        m.add_argument("--train-from", default="2022-09-07")
        m.add_argument("--train-to", default="2022-09-12")
        m.add_argument("--val", type=lambda s: s.split(","), default=["2022-09-13", "2022-09-14"])
        m.add_argument("--test", type=lambda s: s.split(","), default=["2022-09-15", "2022-09-16"])
        m.add_argument("--k", type=int, default=1000)
        if name == "train":
            m.add_argument("--neg-fraction", type=float, default=0.1)
            m.add_argument("--seed", type=int, default=1)
    m = modes.add_parser("score")
    m.add_argument("--curated", required=True)
    m.add_argument("--model", required=True)
    m.add_argument("--date", required=True)
    m.add_argument("--k", type=int, default=1000)
    m.add_argument("--out", required=True)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> dict:
    args = parse(argv)
    spark = SparkSession.builder.appName(f"bq1-{args.mode}").getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    modes = {"train": train, "baseline": baseline, "baseline-tuned": baseline_tuned, "score": score}
    return modes[args.mode](spark, args)


if __name__ == "__main__":
    main()
