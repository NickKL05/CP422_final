"""Helpers for Member 4's escalation model notebook (06_escalation_model).

Kept free of Databricks-only APIs (dbutils, display) and of the RDD API and .cache(), for the same reasons as
dyad_common.py: the notebook imports it from the Git folder, it has to run on serverless (Spark Connect), and
tests/test_model_local.py runs the exact same functions on a local Spark with synthetic rows.

Input: Member 3's weekly feature table (one row per dyad per complete Monday-to-Sunday week, dense calendar).
See docs/handoff-member3-features.md for its columns and the "rules that keep the model honest".

AI citation: Claude Code (Claude Opus 5.5, Anthropic) drafted this module (label and naive baseline, time split, class
weights, week-block folds, MLlib pipelines and grids, metrics, lead-lag, learning curve). Reviewed and tested by Romin Gandhi.
"""
import math

# --- the label ------------------------------------------------------------------------------------------------
# A "sharp rise" in week t+1: next week's material-conflict count is at least RISE_RATIO times the dyad's recent
# baseline AND at least MIN_RISE events above it. The ratio makes the rule fair to big and small dyads; the minimum
# stops 0 -> 1 on a quiet dyad from counting as an escalation.
RISE_RATIO = 1.5
MIN_RISE = 3
# Baseline = mean material-conflict count over this week and the 3 before it. It includes the current week, so it is
# never NULL (the first week of the range has a baseline too), and it only uses weeks up to t, so it never sees t+1.
BASELINE_WEEKS = 4

LABEL_SOURCE = "label_material_conflict_next_week"  # Member 3's LEAD column: next week's count. Target, never a feature.

# Features: every backward-looking column of the weekly table, plus the baseline above. Heavy-tailed counts get
# log1p, so one huge dyad (thousands of events) doesn't dominate a linear model. Tone and shares stay as they are.
LOG_FEATURES = ["n_events", "sum_mentions", "verbal_coop_events", "material_coop_events", "verbal_conflict_events",
                "material_conflict_events", "material_conflict_mentions", "root_material_conflict_events",
                "material_conflict_lag1", "material_conflict_trailing4_mean", "material_conflict_trailing4_std",
                "n_events_trailing4_mean", "conflict_baseline4"]
RAW_FEATURES = ["active_days", "tone_wmean", "tone_mean", "tone_logw_mean", "goldstein_mean", "conflict_share",
                "tone_wmean_lag1", "tone_change_1w", "tone_trailing4_wmean", "tone_drop_vs_trailing4",
                "weeks_of_history"]
FEATURES = LOG_FEATURES + RAW_FEATURES

# Columns carried next to the features for splitting, evaluation and the backtest. None of them goes into the model.
KEEP = ["row_key", "dyad", "country_a", "country_b", "week_start", "label", "naive_pred", "naive_score",
        "conflict_events_raw", "next_week_conflict", "rise_threshold"]


def add_label(weekly, ratio=RISE_RATIO, min_rise=MIN_RISE, baseline_weeks=BASELINE_WEEKS):
    """Add the baseline, the binary label and the naive baseline's prediction.

    The ROWS frame counts calendar weeks because Member 3's table is a dense calendar (every selected dyad has a row
    for every week, with 0 counts in empty weeks).
    label is NULL in the last week (no next week inside the range) and must be dropped before training.
    naive_pred is the "next week looks like this week" baseline pushed through the same rule: predict a rise if THIS
    week's count already clears the threshold. naive_score is its margin, used to rank rows for the PR curve.
    """
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
    w = Window.partitionBy("dyad").orderBy("week_start").rowsBetween(-(baseline_weeks - 1), 0)
    base = F.avg(F.col("material_conflict_events").cast("double")).over(w)
    nxt = F.col(LABEL_SOURCE)
    return (weekly.withColumn("conflict_baseline4", base)
            .withColumn("rise_threshold", F.greatest(F.lit(float(ratio)) * F.col("conflict_baseline4"),
                                                     F.col("conflict_baseline4") + F.lit(float(min_rise))))
            .withColumn("label", F.when(nxt.isNotNull(), (nxt >= F.col("rise_threshold")).cast("double")))
            .withColumn("naive_pred", (F.col("material_conflict_events") >= F.col("rise_threshold")).cast("double"))
            .withColumn("naive_score", F.col("material_conflict_events") - F.col("rise_threshold"))
            .withColumn("conflict_events_raw", F.col("material_conflict_events"))
            .withColumn("next_week_conflict", nxt))


def model_frame(labeled):
    """Id/evaluation columns plus the features as DOUBLE (log1p on the heavy-tailed counts). MLlib's Imputer only takes
    float/double columns. log1p is a fixed formula with nothing fitted, so applying it before the split leaks nothing."""
    from pyspark.sql import functions as F
    feats = [(F.log1p(F.col(c).cast("double")) if c in LOG_FEATURES else F.col(c).cast("double")).alias(c)
             for c in FEATURES]
    return labeled.select(*KEEP, *feats)


# --- time-based split -----------------------------------------------------------------------------------------
def time_split(labeled_weeks, test_fraction=0.25, gap_weeks=1):
    """Split sorted labeled week starts into train / gap / test, oldest weeks first.

    The test set is the most recent ceil(test_fraction x weeks) weeks. gap_weeks between them are dropped: the last
    training week's label IS the first test week's conflict count (handoff rule 4). If there are too few weeks for a
    gap (sample mode has only 2 labeled weeks), the gap is skipped and the result says so.
    """
    weeks = sorted(labeled_weeks)
    if len(weeks) < 2:
        raise ValueError(f"need at least 2 labeled weeks for a time split, got {len(weeks)}")
    n_test = max(1, math.ceil(len(weeks) * test_fraction))
    gap = gap_weeks if len(weeks) - n_test - gap_weeks >= 1 else 0
    n_train = len(weeks) - n_test - gap
    return {"train": weeks[:n_train], "gap": weeks[n_train:n_train + gap], "test": weeks[n_train + gap:],
            "gap_skipped": gap < gap_weeks}


def class_weights(n_pos, n_neg):
    """Balanced weights: each class carries half the total weight, so the rare 'rise' rows count as much as the rest.
    Computed on the training weeks only."""
    n = n_pos + n_neg
    return (n / (2.0 * n_pos) if n_pos else 1.0), (n / (2.0 * n_neg) if n_neg else 1.0)


def with_class_weight(train):
    """train with a `weight` column, plus (w_pos, w_neg, n_pos, n_neg)."""
    from pyspark.sql import functions as F
    r = train.agg(F.sum("label").alias("pos"), F.count("label").alias("n")).first()
    n_pos = int(r["pos"] or 0)
    n_neg = int(r["n"]) - n_pos
    w_pos, w_neg = class_weights(n_pos, n_neg)
    return train.withColumn("weight", F.when(F.col("label") == 1.0, w_pos).otherwise(w_neg)), (w_pos, w_neg, n_pos, n_neg)


def with_folds(spark, train, train_weeks, k=3):
    """Add an integer `fold` column (0 .. k-1) for CrossValidator's foldCol.

    Folds are contiguous blocks of weeks, so rows of one week never sit on both sides of a fold, and the validation
    fold is a stretch of time, not a random scatter of rows. (Blocked CV still trains some folds on later weeks than
    it validates on; the final test set is strictly later than all training data.)
    With fewer training weeks than folds (sample mode), folds are a hash of the dyad instead, so CV can still run.
    """
    from pyspark.sql import functions as F
    weeks = sorted(train_weeks)
    if len(weeks) >= k:
        fold_df = spark.createDataFrame([(w, i * k // len(weeks)) for i, w in enumerate(weeks)],
                                        "week_start DATE, fold INT")
        return train.join(F.broadcast(fold_df), "week_start"), f"{k} contiguous blocks of weeks"
    return (train.withColumn("fold", F.expr(f"CAST(pmod(xxhash64(dyad), {k}) AS INT)")),
            f"{k} folds by dyad hash (only {len(weeks)} training week(s))")


def usable_features(train, features=FEATURES):
    """Features with at least one non-NULL value in the training rows. A column that is entirely NULL there (e.g. the
    trailing columns when the only training week is the first week, in sample mode) can't be imputed and is dropped."""
    from pyspark.sql import functions as F
    counts = train.agg(*[F.count(c).alias(c) for c in features]).first().asDict()
    return [c for c in features if counts[c] > 0], [c for c in features if counts[c] == 0]


# --- models ---------------------------------------------------------------------------------------------------
def build_pipeline(classifier, features):
    """Imputer -> VectorAssembler -> StandardScaler -> classifier, as ONE Pipeline, so every fitted step (medians,
    means, standard deviations) is learned from the training fold only and travels with the saved model."""
    from pyspark.ml import Pipeline
    from pyspark.ml.feature import Imputer, StandardScaler, VectorAssembler
    imputed = [f"{c}__imp" for c in features]
    return Pipeline(stages=[
        # Median, not mean: the tone and count columns are skewed. NULL tone means "no events that week".
        Imputer(inputCols=features, outputCols=imputed, strategy="median"),
        VectorAssembler(inputCols=imputed, outputCol="raw_features"),
        # Scaling matters for the regularised logistic regression; the tree models ignore it.
        StandardScaler(inputCol="raw_features", outputCol="features", withMean=True, withStd=True),
        classifier,
    ])


def model_specs(seed=42):
    """The three classifiers and their grids. Every one uses the class-weight column. Grids are small on purpose:
    Free Edition compute is shared and quota-limited, and CrossValidator fits every point once per fold."""
    from pyspark.ml.classification import GBTClassifier, LogisticRegression, RandomForestClassifier
    from pyspark.ml.tuning import ParamGridBuilder
    common = {"featuresCol": "features", "labelCol": "label", "weightCol": "weight"}
    lr = LogisticRegression(maxIter=100, **common)
    rf = RandomForestClassifier(seed=seed, **common)
    gbt = GBTClassifier(maxIter=40, seed=seed, **common)
    return {
        "logistic_regression": (lr, ParamGridBuilder().addGrid(lr.regParam, [0.001, 0.01, 0.1])
                                .addGrid(lr.elasticNetParam, [0.0, 0.5]).build()),
        "random_forest": (rf, ParamGridBuilder().addGrid(rf.numTrees, [50, 100])
                          .addGrid(rf.maxDepth, [5, 8]).build()),
        "gbt": (gbt, ParamGridBuilder().addGrid(gbt.maxDepth, [3, 5])
                .addGrid(gbt.stepSize, [0.05, 0.1]).build()),
    }


def pr_evaluator(score_col="rawPrediction"):
    from pyspark.ml.evaluation import BinaryClassificationEvaluator
    # Unweighted on purpose: the class weights shape training, but the metric must reflect the real class balance.
    return BinaryClassificationEvaluator(labelCol="label", rawPredictionCol=score_col, metricName="areaUnderPR")


def cross_validate(train, features, specs, num_folds, parallelism=2, seed=42):
    """CrossValidator per model, folds from the `fold` column, area under the PR curve as the metric.
    Returns {name: {cv_auc_pr, best_params, best_model, pipeline, param_map, all_metrics}}."""
    import numpy as np
    from pyspark.ml.tuning import CrossValidator
    out = {}
    for name, (clf, grid) in specs.items():
        pipe = build_pipeline(clf, features)
        cv = CrossValidator(estimator=pipe, estimatorParamMaps=grid, evaluator=pr_evaluator(), numFolds=num_folds,
                            foldCol="fold", parallelism=parallelism, seed=seed)
        m = cv.fit(train)
        best = int(np.argmax(m.avgMetrics))  # the same choice CrossValidator made for bestModel
        out[name] = {"cv_auc_pr": float(m.avgMetrics[best]),
                     "best_params": {p.name: v for p, v in grid[best].items()},
                     "best_model": m.bestModel, "pipeline": pipe, "param_map": grid[best],
                     "all_metrics": [float(x) for x in m.avgMetrics]}
    return out


def with_p_rise(predictions):
    """Probability of class 1 as a plain double column (for metrics, tables and the backtest)."""
    from pyspark.ml.functions import vector_to_array
    from pyspark.sql import functions as F
    return predictions.withColumn("p_rise", vector_to_array(F.col("probability"))[1])


def binary_metrics(df, score_col, pred_col, label_col="label"):
    """Precision, recall and F1 for class 1, and area under the PR curve, on rows with a label.
    The PR area of a random ranking equals the positive rate, which is printed next to it for scale."""
    from pyspark.sql import functions as F
    y, p = F.col(label_col) == 1.0, F.col(pred_col) == 1.0
    r = df.agg(F.count(F.when(y & p, 1)).alias("tp"), F.count(F.when(~y & p, 1)).alias("fp"),
               F.count(F.when(y & ~p, 1)).alias("fn"), F.count(F.when(~y & ~p, 1)).alias("tn")).first()
    tp, fp, fn, tn = (int(r[k]) for k in ("tp", "fp", "fn", "tn"))
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    n_pos, n = tp + fn, tp + fp + fn + tn
    auc_pr = pr_evaluator(score_col).evaluate(df) if 0 < n_pos < n else None
    return {"rows": n, "positives": n_pos, "positive_rate": n_pos / n if n else None, "tp": tp, "fp": fp, "fn": fn,
            "tn": tn, "precision": precision, "recall": recall, "f1": f1, "auc_pr": auc_pr}


def feature_weights(pipeline_model, features):
    """(feature, value) per input feature: coefficients for logistic regression (on standardised features, so their
    sizes are comparable), importances for the tree models."""
    clf = pipeline_model.stages[-1]
    vals = clf.coefficients.toArray() if hasattr(clf, "coefficients") else clf.featureImportances.toArray()
    return list(zip(features, [float(v) for v in vals]))


# --- (A&B) lead-lag -------------------------------------------------------------------------------------------
def lead_lag(weekly, max_lag=4):
    """Correlation between this week's CHANGE in tone and the change in material conflict k weeks later, pooled over
    all dyads, for k = -max_lag .. max_lag.

    Changes (first differences), not levels: levels of both series drift slowly, so their correlation is similar at
    every offset and says nothing about order. k > 0 pairs tone at t with conflict at t + k ("tone leads"); k < 0 pairs
    it with earlier conflict ("conflict leads"). If tone leads conflict, the k > 0 side should be clearly more negative
    (tone falls, conflict rises afterwards) than the mirror k < 0 side.
    """
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
    w = Window.partitionBy("dyad").orderBy("week_start")
    conflict = F.log1p(F.col("material_conflict_events").cast("double"))

    # One row per dyad-week: this week's tone change and this week's conflict change.
    changes = weekly.select("dyad", "week_start", F.col("tone_change_1w").alias("d_tone"),
                            (conflict - F.lag(conflict, 1).over(w)).alias("d_conf"))

    rows = []
    for k in range(-max_lag, max_lag + 1):
        # Put the conflict change from k weeks later (k > 0) or earlier (k < 0) next to this week's tone change.
        if k > 0:
            shifted = F.lead("d_conf", k).over(w)
        elif k < 0:
            shifted = F.lag("d_conf", -k).over(w)
        else:
            shifted = F.col("d_conf")
        pairs = changes.withColumn("d_conf_k", shifted).where("d_tone IS NOT NULL AND d_conf_k IS NOT NULL")
        r = pairs.agg(F.corr("d_tone", "d_conf_k").alias("corr"), F.count(F.lit(1)).alias("pairs")).first()
        rows.append({"lag_weeks": k, "corr": r["corr"], "pairs": int(r["pairs"])})
    return rows


# --- (A&B) learning curve -------------------------------------------------------------------------------------
def learning_curve(pipeline, param_map, train, test, fractions=(0.1, 0.25, 0.5, 0.75, 1.0), seed=42):
    """Refit the chosen model (fixed hyper-parameters, no CV) on growing random shares of the training rows and score
    each fit on the same training subset and on the full test set. A gap that closes as rows grow means more data
    helps; a flat test curve means the model is limited by the features, not by the amount of data."""
    rows = []
    for f in fractions:
        sub = train if f >= 1.0 else train.sample(fraction=f, seed=seed)
        n = sub.count()
        model = pipeline.fit(sub, param_map)
        tr = binary_metrics(with_p_rise(model.transform(sub)), "p_rise", "prediction")
        te = binary_metrics(with_p_rise(model.transform(test)), "p_rise", "prediction")
        rows.append({"fraction": f, "train_rows": n, "train_auc_pr": tr["auc_pr"], "test_auc_pr": te["auc_pr"],
                     "test_f1": te["f1"]})
    return rows
