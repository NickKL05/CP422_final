"""Local check of Member 4's model logic (notebooks/model_common.py) on small synthetic weekly tables.

It runs the same functions the 06_escalation_model notebook calls, on hand-made rows shaped like Member 3's
dyad_weekly_features table:
  1. the "sharp rise" label and the naive baseline, worked out by hand (4-week baseline, ratio vs minimum rise,
     NULL label in the last week);
  2. the time split (test = most recent weeks, a gap week, the sample-mode case without a gap), class weights and
     contiguous week-block folds;
  3. precision / recall / F1 / PR area on hand-made predictions;
  4. lead-lag: a series built so conflict change = -(tone change one week earlier) gives corr = -1 at lag +1 only;
  5. end to end on 12 weeks: CrossValidator for all three models, no label column among the features, a learnable
     signal beats the naive baseline, the best PipelineModel saves and reloads with identical predictions, the learning
     curve runs;
  6. the sample-mode shape (3 weeks, so 1 training week whose trailing columns are all NULL) runs without errors.

Needs only PySpark. From the repo root:
  PYSPARK_PYTHON=$(which python) python tests/test_model_local.py

AI citation: Claude Code (Claude Opus 5.5, Anthropic) drafted this test. Reviewed and tested by Romin Gandhi.
"""
import datetime as dt
import math
import random
import sys
import tempfile

sys.path.insert(0, "notebooks")
import model_common as mc  # noqa: E402
from pyspark.ml import PipelineModel  # noqa: E402
from pyspark.sql import SparkSession, functions as F  # noqa: E402

spark = (SparkSession.builder.master("local[2]").appName("cp422-model-test")
         .config("spark.sql.shuffle.partitions", "4")
         .config("spark.sql.session.timeZone", "UTC")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
print(f"Spark {spark.version}, ANSI mode = {spark.conf.get('spark.sql.ansi.enabled')}")

MONDAY = dt.date(2026, 1, 5)
COUNT_COLS = ["n_events", "active_days", "verbal_coop_events", "material_coop_events", "verbal_conflict_events",
              "material_conflict_events", "root_material_conflict_events", "material_conflict_lag1", "weeks_of_history",
              "label_material_conflict_next_week"]
DOUBLE_COLS = ["sum_mentions", "tone_wmean", "tone_mean", "tone_logw_mean", "goldstein_mean",
               "material_conflict_mentions", "conflict_share", "tone_wmean_lag1", "tone_change_1w",
               "tone_trailing4_wmean", "tone_drop_vs_trailing4", "material_conflict_trailing4_mean",
               "material_conflict_trailing4_std", "n_events_trailing4_mean"]
SCHEMA = ", ".join(["row_key STRING", "dyad STRING", "country_a STRING", "country_b STRING", "week_start DATE",
                    "week_end DATE"] + [f"{c} BIGINT" for c in COUNT_COLS] + [f"{c} DOUBLE" for c in DOUBLE_COLS])


def weekly_rows(series):
    """{dyad: ([material conflict count per week], [tone drop per week])} -> rows shaped like Member 3's table.
    Only the columns the tests look at are realistic; the rest are simple functions of the count and the tone."""
    rows = []
    for dyad, (conf, drops) in series.items():
        a, b = dyad.split("_")
        for t, c in enumerate(conf):
            ws = MONDAY + dt.timedelta(weeks=t)
            prev = conf[max(0, t - 4):t]
            tone = -2.0 + drops[t]
            rows.append({
                "row_key": f"{dyad}#{ws:%Y%m%d}", "dyad": dyad, "country_a": a, "country_b": b,
                "week_start": ws, "week_end": ws + dt.timedelta(days=6),
                "n_events": 3 * c + 5, "active_days": 7, "verbal_coop_events": c + 2, "material_coop_events": 1,
                "verbal_conflict_events": c, "material_conflict_events": c, "root_material_conflict_events": c // 2,
                "material_conflict_lag1": conf[t - 1] if t else None, "weeks_of_history": t,
                "label_material_conflict_next_week": conf[t + 1] if t + 1 < len(conf) else None,
                "sum_mentions": float(10 * c + 20), "tone_wmean": tone, "tone_mean": tone, "tone_logw_mean": tone,
                "goldstein_mean": -1.0, "material_conflict_mentions": float(5 * c), "conflict_share": c / (3 * c + 5),
                "tone_wmean_lag1": -2.0 + drops[t - 1] if t else None,
                "tone_change_1w": drops[t] - drops[t - 1] if t else None,
                "tone_trailing4_wmean": -2.0 if t else None, "tone_drop_vs_trailing4": drops[t] if t else None,
                "material_conflict_trailing4_mean": sum(prev) / len(prev) if prev else None,
                "material_conflict_trailing4_std": None,  # all NULL on purpose: usable_features must drop it
                "n_events_trailing4_mean": float(3 * sum(prev) / len(prev) + 5) if prev else None,
            })
    return spark.createDataFrame(rows, SCHEMA)


# 1. label and naive baseline -----------------------------------------------------------------------------------
# Small dyad: thresholds are max(1.5 x baseline, baseline + 3); the +3 binds. Week 4's baseline is weeks 1-4 only
# (2, 10, 4, 4 -> 5, threshold 8), so next week's 7 is NOT a rise. A 5-week baseline (4.0, threshold 7) would say it is.
# Big dyad: the ratio binds (baseline 100 -> threshold 150, so 140 is not a rise).
lab = mc.add_label(weekly_rows({"AAA_BBB": ([0, 2, 10, 4, 4, 7], [0.0] * 6),
                                "CCC_DDD": ([100, 100, 100, 100, 140, 200], [0.0] * 6)}))
got = {(r["dyad"], r["weeks_of_history"]): r for r in lab.collect()}
small = [(got[("AAA_BBB", t)]["conflict_baseline4"], got[("AAA_BBB", t)]["rise_threshold"],
          got[("AAA_BBB", t)]["label"], got[("AAA_BBB", t)]["naive_pred"]) for t in range(6)]
assert small == [(0.0, 3.0, 0.0, 0.0), (1.0, 4.0, 1.0, 0.0), (4.0, 7.0, 0.0, 1.0), (4.0, 7.0, 0.0, 0.0),
                 (5.0, 8.0, 0.0, 0.0), (6.25, 9.375, None, 0.0)], small
big = [(got[("CCC_DDD", t)]["rise_threshold"], got[("CCC_DDD", t)]["label"]) for t in range(6)]
assert big == [(150.0, 0.0), (150.0, 0.0), (150.0, 0.0), (150.0, 0.0), (165.0, 1.0), (202.5, None)], big
assert got[("AAA_BBB", 2)]["naive_score"] == 3.0  # 10 - 7
frame_cols = mc.model_frame(lab).columns
assert not any(c.startswith("label_") for c in mc.FEATURES) and "label" not in mc.FEATURES
assert "next_week_conflict" not in mc.FEATURES and "rise_threshold" not in mc.FEATURES
assert set(mc.FEATURES) <= set(frame_cols)
print(f"[1] label OK: small dyad {[(s[1], s[2]) for s in small]}, big dyad {big}")

# 2. split, weights, folds ------------------------------------------------------------------------------------------
weeks50 = [MONDAY + dt.timedelta(weeks=i) for i in range(50)]
s = mc.time_split(weeks50)
assert (len(s["train"]), len(s["gap"]), len(s["test"])) == (36, 1, 13) and not s["gap_skipped"]
assert max(s["train"]) < min(s["gap"]) < min(s["test"]) and s["test"][-1] == weeks50[-1]
s2 = mc.time_split(weeks50[:2])
assert (s2["train"], s2["gap"], s2["test"], s2["gap_skipped"]) == ([weeks50[0]], [], [weeks50[1]], True)
try:
    mc.time_split(weeks50[:1])
    raise AssertionError("one week must be rejected")
except ValueError:
    pass

w_pos, w_neg = mc.class_weights(10, 90)
assert math.isclose(w_pos * 10, 50) and math.isclose(w_neg * 90, 50), (w_pos, w_neg)
assert mc.class_weights(0, 5) == (1.0, 0.5)  # no positives: no division by zero

seven = weeks50[:7]
fold_df = spark.createDataFrame([("X_Y", w) for w in seven], "dyad STRING, week_start DATE")
folded, how = mc.with_folds(spark, fold_df, seven, 3)
assert [r["fold"] for r in folded.orderBy("week_start").collect()] == [0, 0, 0, 1, 1, 2, 2], how
hashed, how2 = mc.with_folds(spark, spark.createDataFrame([(f"D{i}", seven[0]) for i in range(50)],
                                                          "dyad STRING, week_start DATE"), seven[:1], 3)
assert {r["fold"] for r in hashed.collect()} == {0, 1, 2}, how2
print(f"[2] split 36/1/13 (sample: gap skipped), weights {w_pos:.2f}/{w_neg:.3f}, folds: {how}; fallback: {how2}")

# 3. metrics ----------------------------------------------------------------------------------------------------------
pred = spark.createDataFrame([(1.0, 1.0, 0.9), (1.0, 0.0, 0.8), (0.0, 1.0, 0.7), (0.0, 0.0, 0.2), (0.0, 0.0, 0.1)],
                             "label DOUBLE, prediction DOUBLE, score DOUBLE")
m = mc.binary_metrics(pred, "score", "prediction")
assert (m["tp"], m["fp"], m["fn"], m["tn"]) == (1, 1, 1, 2) and m["precision"] == m["recall"] == m["f1"] == 0.5, m
perfect = mc.binary_metrics(pred.withColumn("score", F.col("label")), "score", "prediction")
assert math.isclose(perfect["auc_pr"], 1.0), perfect
assert mc.binary_metrics(pred.where("label = 0"), "score", "prediction")["auc_pr"] is None  # no positives: no PR area
print(f"[3] metrics OK: P/R/F1 = 0.5, perfect ranking PR area = {perfect['auc_pr']:.2f}")

# 4. lead-lag -------------------------------------------------------------------------------------------------------
# log(1 + conflict) at t+1 = log(1 + conflict) at t - (tone change at t): conflict change at t+1 = -(tone change at t).
rng = random.Random(7)
ll_rows = []
for d in range(30):
    level = 3.0
    for t in range(20):
        d_tone = rng.gauss(0, 1)
        ll_rows.append((f"D{d}", MONDAY + dt.timedelta(weeks=t), d_tone, math.expm1(level)))
        level -= d_tone
ll = mc.lead_lag(spark.createDataFrame(ll_rows, "dyad STRING, week_start DATE, tone_change_1w DOUBLE, "
                                                "material_conflict_events DOUBLE"), max_lag=3)
by_lag = {r["lag_weeks"]: r for r in ll}
assert by_lag[1]["corr"] < -0.999, by_lag[1]
assert all(abs(by_lag[k]["corr"]) < 0.3 for k in (-3, -2, -1, 2, 3)), ll
# d_conf exists from week 1 on: 19 pairs per dyad at lags 0 and +1, 18 at -1.
assert (by_lag[0]["pairs"], by_lag[1]["pairs"], by_lag[-1]["pairs"]) == (30 * 19, 30 * 19, 30 * 18), by_lag
print(f"[4] lead-lag OK: corr at +1 = {by_lag[1]['corr']:.3f}, at -1 = {by_lag[-1]['corr']:.3f}")


# 5 and 6. end to end -----------------------------------------------------------------------------------------------
def synthetic(n_dyads, n_weeks, seed):
    """A tone drop of more than 1 point in week t is followed by a conflict spike in week t+1. Otherwise counts
    wander around the dyad's own level, so the naive "next week = this week" baseline has little to go on."""
    rng = random.Random(seed)
    out = {}
    for d in range(n_dyads):
        level = rng.randint(2, 40)
        drops = [rng.choice([0.0, 0.0, 0.0, -2.0]) + rng.gauss(0, 0.2) for _ in range(n_weeks)]
        conf = [max(0, int(level + rng.gauss(0, 1)))]
        for t in range(1, n_weeks):
            conf.append(3 * level + 8 if drops[t - 1] < -1 else max(0, int(level + rng.gauss(0, 1))))
        out[f"A{d:02d}_B{d:02d}"] = (conf, drops)
    return out


def run_pipeline(weekly, label):
    """The notebook's steps in order, returning what the assertions need."""
    data = mc.model_frame(mc.add_label(weekly))
    weeks = sorted(r[0] for r in data.where("label IS NOT NULL").select("week_start").distinct().collect())
    split = mc.time_split(weeks)
    train = data.where(F.col("week_start").isin(split["train"]))
    test = data.where(F.col("week_start").isin(split["test"]))
    train, (wp, wn, n_pos, n_neg) = mc.with_class_weight(train)
    train, how = mc.with_folds(spark, train, split["train"], 3)
    feats, dropped = mc.usable_features(train)
    results = mc.cross_validate(train, feats, mc.model_specs(), 3)
    best = max(results, key=lambda k: results[k]["cv_auc_pr"])
    scored = mc.with_p_rise(results[best]["best_model"].transform(test))
    metrics = mc.binary_metrics(scored, "p_rise", "prediction")
    naive = mc.binary_metrics(test, "naive_score", "naive_pred")
    print(f"    {label}: {len(split['train'])}/{len(split['gap'])}/{len(split['test'])} weeks, {n_pos} pos / "
          f"{n_neg} neg in train, folds {how}, dropped {dropped}")
    print("    CV PR area: " + ", ".join(f"{k} {v['cv_auc_pr']:.3f}" for k, v in results.items())
          + f"; best {best}: test PR area {metrics['auc_pr']}, F1 {metrics['f1']:.3f}; naive F1 {naive['f1']:.3f}")
    return data, split, train, test, feats, dropped, results, best, scored, metrics, naive


data, split, train, test, feats, dropped, results, best, scored, metrics, naive = run_pipeline(
    weekly_rows(synthetic(40, 12, 1)), "12 weeks")
assert dropped == ["material_conflict_trailing4_std"] and "material_conflict_trailing4_std" not in feats, dropped
assert set(results) == {"logistic_regression", "random_forest", "gbt"}
assert all(len(v["all_metrics"]) == 6 if k == "logistic_regression" else len(v["all_metrics"]) == 4
           for k, v in results.items())
assert metrics["auc_pr"] > 0.9 and metrics["f1"] > naive["f1"], (metrics, naive)
for name, res in results.items():
    fw = mc.feature_weights(res["best_model"], feats)
    assert [f for f, _ in fw] == feats, name

with tempfile.TemporaryDirectory() as tmp:
    results[best]["best_model"].write().overwrite().save(f"{tmp}/model")
    reloaded = mc.with_p_rise(PipelineModel.load(f"{tmp}/model").transform(test)).select("row_key", "p_rise")
    diff = (scored.select("row_key", F.col("p_rise").alias("p0")).join(reloaded, "row_key")
            .agg(F.max(F.abs(F.col("p0") - F.col("p_rise"))), F.count(F.lit(1))).first())
    assert diff[0] < 1e-12 and diff[1] == test.count(), diff

curve = mc.learning_curve(results[best]["pipeline"], results[best]["param_map"], train, test, fractions=(0.5, 1.0))
assert [r["fraction"] for r in curve] == [0.5, 1.0] and curve[-1]["train_rows"] == train.count(), curve
print(f"[5] end to end OK: best {best}, save/load identical, learning curve {[(r['fraction'], r['test_auc_pr']) for r in curve]}")

# Sample mode: 3 complete weeks -> 2 labeled -> 1 training week (weeks_of_history = 0, so every trailing column is
# NULL there and must be dropped), folds by dyad hash, no gap.
_, split_s, _, _, feats_s, dropped_s, _, _, _, metrics_s, _ = run_pipeline(weekly_rows(synthetic(60, 3, 2)),
                                                                           "sample shape (3 weeks)")
assert split_s["gap_skipped"] and len(split_s["train"]) == 1 and len(split_s["test"]) == 1
assert {"tone_trailing4_wmean", "material_conflict_lag1", "tone_change_1w"} <= set(dropped_s), dropped_s
assert metrics_s["rows"] == 60
print(f"[6] sample-mode shape OK: {len(feats_s)} features kept, {len(dropped_s)} all-NULL ones dropped")
print("ALL CHECKS PASSED")
spark.stop()
