# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · Escalation prediction model (Member 4)
# MAGIC
# MAGIC AI citation: Claude Code (Claude Opus 5.5, Anthropic) drafted this notebook (label, time split, class weights, CrossValidator tuning, evaluation, model save, lead-lag, backtest, learning curve). Reviewed and tested by Romin Gandhi.
# MAGIC
# MAGIC **Research question:** does news tone lead conflict? Member 3's weekly table gives, for every active pair of states
# MAGIC (a *dyad*) and every complete week, the week's tone, conflict counts and the dyad's own recent history. This notebook
# MAGIC asks: **from what is known at the end of week *t*, can we predict a sharp rise in the dyad's material conflict in
# MAGIC week *t + 1*?** It then checks whether tone changes come before conflict changes (lead-lag), looks at the model around
# MAGIC the biggest rises it had to predict (backtest), and measures how much more data would help (learning curve).
# MAGIC
# MAGIC | Step | What happens | Output |
# MAGIC | --- | --- | --- |
# MAGIC | 1 | Read Member 3's weekly feature table (or its CSV export) | |
# MAGIC | 2 | Define the "sharp rise" label and the naive baseline; build the model input | `m4_model_input_*` |
# MAGIC | 3 | Time-based split, class weights, cross-validation folds | |
# MAGIC | 4 | Tune logistic regression, random forest and GBT with `CrossValidator` + `ParamGridBuilder` | |
# MAGIC | 5 | Evaluate every model and the baseline on the test weeks | `m4_model_results_*`, `m4_test_predictions_*` |
# MAGIC | 6 | Save the best `PipelineModel` for Member 5; feature weights | model folder in the volume, `m4_feature_weights_*` |
# MAGIC | 7 | (A&B) Lead-lag: tone change vs conflict change at different week offsets | `m4_lead_lag_*` |
# MAGIC | 8 | (A&B) Backtest around the largest rises in the test weeks (plus any events listed by hand) | `m4_backtest_*` |
# MAGIC | 9 | (A&B) Learning curve: score vs training-set size | `m4_learning_curve_*` |
# MAGIC | 10 | Run parameters | `m4_run_params_*` |
# MAGIC
# MAGIC **Sample and full runs write to different tables** (suffix `_sample` or `_full`), like notebook 03, so a sample-mode
# MAGIC Run All never overwrites the full-data results. The sample month has only 3 complete weeks, so sample-mode scores are a
# MAGIC smoke test that the pipeline runs, not results. **Quote only the `_full` tables in the report.**
# MAGIC
# MAGIC The logic lives in `model_common.py` next to this notebook, so `tests/test_model_local.py` checks the same code
# MAGIC locally on hand-made rows. This notebook explains each step and runs it.

# COMMAND ----------

# AI citation: Claude Code (Claude Opus 5.5, Anthropic) drafted this cell (sample_mode widget). Reviewed and tested by Romin Gandhi.
# Declared here, not only in 00_config: a widget created inside a %run notebook does not appear in this notebook's
# parameter bar. Every top-level notebook in the repo starts with this cell.
dbutils.widgets.dropdown("sample_mode", "true", ["true", "false"], "SAMPLE_MODE")

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: settings and output names
# MAGIC
# MAGIC | Setting | Value | Why |
# MAGIC | --- | --- | --- |
# MAGIC | `TEST_FRACTION` | 0.25 | The most recent quarter of the labeled weeks is the test set: about 3 months on the full year |
# MAGIC | `GAP_WEEKS` | 1 | One week between train and test is dropped: the last training week's label *is* the first test week's count |
# MAGIC | `NUM_FOLDS` | 3 | Cross-validation folds inside the training weeks. Each fold is a contiguous block of weeks |
# MAGIC | `CV_PARALLELISM` | 4 | Grid points fitted at the same time. The data is small, so this mostly hides per-job overhead |
# MAGIC | `BACKTEST_EVENTS` | empty | Optional real events to look at, as `(dyad, date, description)`. The dyad is the two codes sorted, e.g. `ISR_PSE` |
# MAGIC
# MAGIC The label settings (`RISE_RATIO`, `MIN_RISE`, `BASELINE_WEEKS`) live in `model_common.py`, because the local test
# MAGIC checks them by hand. They are printed below and saved in the run parameters.

# COMMAND ----------

# AI citation: Claude Code (Claude Opus 5.5, Anthropic) drafted this cell (settings, output names, save helper). Reviewed and tested by Romin Gandhi.
import datetime as dt
import json
import math
import os
import pandas as pd
from pyspark.sql import functions as F
import model_common as mc

TEST_FRACTION = 0.25
GAP_WEEKS = 1
NUM_FOLDS = 3
CV_PARALLELISM = 4
SEED = 42
SELECTION_FRACTION = 0.5     # Member 3 picks dyads on the first half of the weeks; training must cover them
LEAD_LAG_MAX = 4             # weeks either side
BACKTEST_TOP_N = 5           # largest rises in the test weeks to plot
BACKTEST_WINDOW_WEEKS = 4    # weeks shown either side of each backtest week
BACKTEST_EVENTS = []         # e.g. [("AAA_BBB", "2026-08-12", "what happened")]; any date inside the week works

SUFFIX = "_sample" if SAMPLE_MODE else "_full"
T = lambda name: f"{CATALOG}.{SCHEMA}.{name}{SUFFIX}"
T_WEEKLY_IN = T("dyad_weekly_features")                                  # written by 03_dyad_tone_index
CSV_IN = f"{VOLUME_ROOT}/exports/member3/dyad_weekly_features{SUFFIX}.csv"  # Member 3's export, for other workspaces
T_INPUT, T_RESULTS, T_PRED = T("m4_model_input"), T("m4_model_results"), T("m4_test_predictions")
T_WEIGHTS, T_LEADLAG, T_BACKTEST = T("m4_feature_weights"), T("m4_lead_lag"), T("m4_backtest")
T_CURVE, T_PARAMS = T("m4_learning_curve"), T("m4_run_params")
MODEL_PATH = f"{VOLUME_ROOT}/models/m4_escalation_model{SUFFIX}"

def save(df, table):
    """Overwrite a result table. overwriteSchema lets a re-run with changed columns replace the old table cleanly."""
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)

print(f"SAMPLE_MODE={SAMPLE_MODE}  outputs end in '{SUFFIX}'  model -> {MODEL_PATH}")
print(f"label: next week's material conflict >= max({mc.RISE_RATIO} x baseline, baseline + {mc.MIN_RISE}), "
      f"baseline = mean of this week and the {mc.BASELINE_WEEKS - 1} before")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Input: Member 3's weekly feature table
# MAGIC
# MAGIC One row per selected dyad per complete Monday-to-Sunday week (`week_start`), on a **dense calendar**: a week with no
# MAGIC events is still a row, with 0 counts and NULL tone. The table and its rules are described in
# MAGIC `docs/handoff-member3-features.md`.
# MAGIC
# MAGIC In a full Run All, notebook 03 has just written the table in this workspace. Each of us also has our own Free
# MAGIC Edition workspace, so if the table isn't here, the notebook reads Member 3's CSV export from the volume instead (dates
# MAGIC arrive as `yyyy-MM-dd` text and are turned back into dates).

# COMMAND ----------

# AI citation: Claude Code (Claude Opus 5.5, Anthropic) drafted this cell (weekly-table input with CSV fallback and column check). Reviewed and tested by Romin Gandhi.
if spark.catalog.tableExists(T_WEEKLY_IN):
    weekly, INPUT_SOURCE = spark.table(T_WEEKLY_IN), T_WEEKLY_IN
elif os.path.exists(CSV_IN):
    weekly = (spark.read.option("header", True).option("inferSchema", True).csv(CSV_IN)
              .withColumn("week_start", F.to_date("week_start")).withColumn("week_end", F.to_date("week_end")))
    INPUT_SOURCE = CSV_IN
else:
    raise RuntimeError(f"No weekly features: run 03_dyad_tone_index first with sample_mode={str(SAMPLE_MODE).lower()}, "
                       f"or upload Member 3's dyad_weekly_features{SUFFIX}.csv to {CSV_IN}")

needed = {"row_key", "dyad", "country_a", "country_b", "week_start", mc.LABEL_SOURCE} | set(mc.FEATURES)
missing = sorted(needed - {"conflict_baseline4"} - set(weekly.columns))  # conflict_baseline4 is added in step 2
if missing:
    raise RuntimeError(f"{INPUT_SOURCE} is missing columns {missing}; check them against Member 3's handoff note")

shape = weekly.agg(F.count(F.lit(1)).alias("rows"), F.countDistinct("dyad").alias("dyads"),
                   F.countDistinct("week_start").alias("weeks"), F.min("week_start").alias("first"),
                   F.max("week_start").alias("last")).first()
print(f"input: {INPUT_SOURCE}")
print(f"  {shape['rows']:,} rows = {shape['dyads']:,} dyads x {shape['weeks']} weeks ({shape['first']}..{shape['last']})")
assert shape["rows"] == shape["dyads"] * shape["weeks"], "expected a dense dyad x week calendar"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · The label: a sharp rise in material conflict next week
# MAGIC
# MAGIC Material conflict is `QuadClass = 4` (CAMEO root codes 15–20). For dyad *d* and week *t*:
# MAGIC
# MAGIC $$\text{baseline}_t = \text{mean material-conflict count over weeks } t-3 \ldots t$$
# MAGIC $$\text{label}_t = 1 \iff \text{conflict}_{t+1} \ge \max(1.5 \cdot \text{baseline}_t,\ \text{baseline}_t + 3)$$
# MAGIC
# MAGIC - **Relative to the dyad's own level.** A rise from 100 to 120 is normal noise for a large dyad; a rise from 2 to 12
# MAGIC   is not. The 1.5× ratio scales with the dyad.
# MAGIC - **With a minimum of 3 events.** Without it, 0 → 1 on a quiet dyad would count as a 50 %+ escalation.
# MAGIC - **The baseline includes week *t*** and only weeks before it. It is never NULL (even in the first week, which
# MAGIC   matters in sample mode) and it never sees week *t + 1*. It is also given to the model as a feature.
# MAGIC - The rows are a dense calendar, so "the 3 weeks before" are calendar weeks, not the last 3 weeks with events.
# MAGIC - The last week has no next week inside the range, so its label is NULL and it is never trained or tested on.
# MAGIC
# MAGIC **Naive baseline ("next week looks like this week").** Predict next week's count = this week's count, then apply the
# MAGIC same rule. It predicts a rise whenever this week's count already clears the threshold. Its margin (this week's count
# MAGIC minus the threshold) ranks rows for its PR curve.
# MAGIC
# MAGIC **Features.** Every backward-looking column of Member 3's table plus the baseline (24 in all): this week's tone
# MAGIC three ways, its change from last week and from the trailing 4 weeks (`tone_drop_vs_trailing4`, the research
# MAGIC question's signal), Goldstein, the four `QuadClass` counts, mentions, and the dyad's trailing conflict history.
# MAGIC Heavy-tailed counts get `log1p`, a fixed formula with nothing fitted, so applying it before the split leaks nothing.
# MAGIC **Never features:** the `label_` columns (next week's values), the ids, and the actor-network centrality (computed
# MAGIC over the whole period, which would leak later weeks into earlier rows).

# COMMAND ----------

# AI citation: Claude Code (Claude Opus 5.5, Anthropic) drafted this cell (label, naive baseline and model input table). Reviewed and tested by Romin Gandhi.
save(mc.model_frame(mc.add_label(weekly)), T_INPUT)
data = spark.table(T_INPUT)

label_rate = data.agg(F.count("label").alias("labeled"), F.sum("label").alias("pos")).first()
print(f"{T_INPUT}: {label_rate['labeled']:,} labeled rows, {int(label_rate['pos'] or 0):,} sharp rises "
      f"({(label_rate['pos'] or 0) / max(label_rate['labeled'], 1):.1%})")
display(data.where("label IS NOT NULL").groupBy("week_start")
        .agg(F.count(F.lit(1)).alias("rows"), F.avg("label").alias("rise_rate")).orderBy("week_start"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Time-based split, class weights and folds
# MAGIC
# MAGIC **Split by `week_start`, never randomly.** A random split would let the model train on week *t + 1* and be tested on
# MAGIC week *t* of the same dyad. Here every test week is later than every training week:
# MAGIC
# MAGIC `| training weeks (oldest) | 1 gap week (dropped) | test weeks (most recent 25 %) |`
# MAGIC
# MAGIC - **The gap week.** The last training week's label is the first test week's conflict count, so that week is dropped
# MAGIC   (handoff rule 4). In sample mode there are only 2 labeled weeks, so the gap is skipped and the notebook says so.
# MAGIC - **Training covers Member 3's dyad-selection weeks.** Member 3 chose the dyads on the first half of the weeks, so the
# MAGIC   training period must include all of them. Otherwise the test weeks would have influenced which dyads are in the
# MAGIC   data. This is checked (an error in full mode, a warning in sample mode, which only has 3 weeks).
# MAGIC
# MAGIC **Class imbalance.** Sharp rises are rare. Each training row gets a weight so both classes carry half the total
# MAGIC weight: positives `n / (2 · n_pos)`, negatives `n / (2 · n_neg)`, computed on the training weeks only. All three
# MAGIC classifiers take it through `weightCol`. The metrics are **not** weighted, so they reflect the real class balance.
# MAGIC
# MAGIC **Folds for `CrossValidator`.** Instead of random folds, the `fold` column splits the training weeks into 3 contiguous
# MAGIC blocks (`foldCol`), so one week's rows are never on both sides of a fold. With fewer training weeks than folds (sample
# MAGIC mode), folds fall back to a hash of the dyad, so the code path still runs.
# MAGIC
# MAGIC **Imputation and scaling** are stages *inside* the pipeline (step 4), so their medians, means and standard deviations
# MAGIC are fitted on the training folds only. A feature that is entirely NULL in the training weeks can't be imputed and is
# MAGIC dropped. In sample mode that's the trailing columns, because the only training week is the first week.

# COMMAND ----------

# AI citation: Claude Code (Claude Opus 5.5, Anthropic) drafted this cell (time split, gap week, selection check, class weights, folds). Reviewed and tested by Romin Gandhi.
labeled_weeks = sorted(r[0] for r in data.where("label IS NOT NULL").select("week_start").distinct().collect())
all_weeks = sorted(r[0] for r in data.select("week_start").distinct().collect())
split = mc.time_split(labeled_weeks, TEST_FRACTION, GAP_WEEKS)
TRAIN_WEEKS, GAP, TEST_WEEKS = split["train"], split["gap"], split["test"]

selection_weeks = max(1, math.ceil(len(all_weeks) * SELECTION_FRACTION))
covers_selection = len(TRAIN_WEEKS) >= selection_weeks
msg = (f"training weeks ({len(TRAIN_WEEKS)}) must cover Member 3's {selection_weeks} dyad-selection weeks "
       f"({all_weeks[0]}..{all_weeks[selection_weeks - 1]})")
if not covers_selection and not SAMPLE_MODE:
    raise RuntimeError(msg)

train = data.where(F.col("week_start").isin(TRAIN_WEEKS) & F.col("label").isNotNull())
test = data.where(F.col("week_start").isin(TEST_WEEKS) & F.col("label").isNotNull())
train, (W_POS, W_NEG, N_POS, N_NEG) = mc.with_class_weight(train)
train, FOLD_HOW = mc.with_folds(spark, train, TRAIN_WEEKS, NUM_FOLDS)
FEATURES, DROPPED = mc.usable_features(train)

test_counts = test.agg(F.count(F.lit(1)).alias("n"), F.sum("label").alias("pos")).first()
print(f"train {TRAIN_WEEKS[0]}..{TRAIN_WEEKS[-1]} ({len(TRAIN_WEEKS)} weeks): {N_POS + N_NEG:,} rows, {N_POS:,} rises "
      f"({N_POS / max(N_POS + N_NEG, 1):.1%})")
print(f"gap   {', '.join(map(str, GAP)) or 'none'}{'  (skipped: too few weeks)' if split['gap_skipped'] else ''}")
print(f"test  {TEST_WEEKS[0]}..{TEST_WEEKS[-1]} ({len(TEST_WEEKS)} weeks): {test_counts['n']:,} rows, "
      f"{int(test_counts['pos'] or 0):,} rises")
print(f"class weights: rise {W_POS:.3f}, no rise {W_NEG:.3f};  folds: {FOLD_HOW}")
print(f"{len(FEATURES)} features" + (f"; dropped (all NULL in training): {DROPPED}" if DROPPED else ""))
print(("OK: " if covers_selection else "WARNING (sample mode): ") + msg)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Three models, tuned with `CrossValidator` and `ParamGridBuilder`
# MAGIC
# MAGIC Each model is one `Pipeline`: `Imputer` (median) → `VectorAssembler` → `StandardScaler` → classifier. Because the whole
# MAGIC pipeline is what `CrossValidator` fits, the imputation and scaling are re-learned inside every training fold, and
# MAGIC the saved model carries them along.
# MAGIC
# MAGIC | Model | Grid | Why this model |
# MAGIC | --- | --- | --- |
# MAGIC | Logistic regression | `regParam` {0.001, 0.01, 0.1} × `elasticNetParam` {0, 0.5} | Interpretable: the sign of the tone coefficients answers the question directly |
# MAGIC | Random forest | `numTrees` {50, 100} × `maxDepth` {5, 8} | Non-linear, robust to skewed features and interactions |
# MAGIC | Gradient-boosted trees | `maxDepth` {3, 5} × `stepSize` {0.05, 0.1}, 40 rounds | Usually strongest on tabular data |
# MAGIC
# MAGIC **Metric: area under the precision-recall curve.** With rare positives, ROC AUC looks good even for weak models,
# MAGIC because it rewards ranking the many easy negatives. The PR area only rewards finding the rises. A random model
# MAGIC scores the positive rate.
# MAGIC
# MAGIC **Choosing the best model uses the cross-validation score only.** The test weeks are looked at once, in step 5, after
# MAGIC the choice is made. The grids are small on purpose: Free Edition compute is shared and quota-limited, and
# MAGIC `CrossValidator` fits every grid point once per fold (6 + 4 + 4 points × 3 folds, plus one refit each).

# COMMAND ----------

# AI citation: Claude Code (Claude Opus 5.5, Anthropic) drafted this cell (CrossValidator tuning of the three models). Reviewed and tested by Romin Gandhi.
results = mc.cross_validate(train, FEATURES, mc.model_specs(SEED), NUM_FOLDS, CV_PARALLELISM, SEED)
BEST = max(results, key=lambda k: results[k]["cv_auc_pr"])
for name, res in results.items():
    print(f"{name:20s} CV PR area {res['cv_auc_pr']:.4f}  best {res['best_params']}  "
          f"(all grid points: {[round(m, 4) for m in res['all_metrics']]})")
print(f"chosen on cross-validation: {BEST}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Evaluation on the test weeks
# MAGIC
# MAGIC Every model and the naive baseline are scored on the same test rows. Precision, recall and F1 are for the "rise"
# MAGIC class at the default 0.5 probability cut-off (the class weights already move that cut-off towards the rare class).
# MAGIC The PR area doesn't depend on a cut-off. `positive_rate` is the PR area of random guessing, for scale.

# COMMAND ----------

# AI citation: Claude Code (Claude Opus 5.5, Anthropic) drafted this cell (test-set evaluation and prediction table). Reviewed and tested by Romin Gandhi.
rows = [{"model": "naive_persistence", "cv_auc_pr": None, "best_params": "",
         **mc.binary_metrics(test, "naive_score", "naive_pred")}]
for name, res in results.items():
    scored = mc.with_p_rise(res["best_model"].transform(test))
    rows.append({"model": name, "cv_auc_pr": res["cv_auc_pr"], "best_params": json.dumps(res["best_params"]),
                 **mc.binary_metrics(scored, "p_rise", "prediction")})
# float, so a metric that is None everywhere (no rises in the test weeks) becomes NaN instead of an untypable column
results_pd = pd.DataFrame(rows).astype({"cv_auc_pr": float, "auc_pr": float, "positive_rate": float})
results_pd["chosen"] = results_pd["model"] == BEST
save(spark.createDataFrame(results_pd), T_RESULTS)
display(results_pd[["model", "chosen", "cv_auc_pr", "auc_pr", "precision", "recall", "f1", "positive_rate", "tp", "fp",
                    "fn", "tn", "best_params"]])

best_scored = mc.with_p_rise(results[BEST]["best_model"].transform(test))
save(best_scored.select("row_key", "dyad", "country_a", "country_b", "week_start", "label", "p_rise", "prediction",
                        "naive_pred", "conflict_events_raw", "next_week_conflict", "rise_threshold"), T_PRED)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · Save the best model (for Member 5) and look inside it
# MAGIC
# MAGIC The chosen `PipelineModel` is saved to the volume. To use it, load it with `PipelineModel.load(path)` and transform
# MAGIC rows that have the feature columns listed in the run parameters, built the same way as here:
# MAGIC `mc.model_frame(mc.add_label(weekly))` on a table shaped like Member 3's. The imputer and scaler are inside the model.
# MAGIC
# MAGIC **Feature weights.** For logistic regression these are coefficients on standardised features, so their sizes are
# MAGIC comparable and the sign shows the direction. A negative weight on `tone_drop_vs_trailing4` means a tone drop raises
# MAGIC the predicted risk. For the tree models they are importances (share of impurity reduction): always positive, with
# MAGIC no direction.

# COMMAND ----------

# AI citation: Claude Code (Claude Opus 5.5, Anthropic) drafted this cell (model save and feature weights). Reviewed and tested by Romin Gandhi.
results[BEST]["best_model"].write().overwrite().save(MODEL_PATH)
print(f"saved {BEST} PipelineModel to {MODEL_PATH}")

weights_pd = pd.DataFrame([(name, f, v) for name, res in results.items()
                           for f, v in mc.feature_weights(res["best_model"], FEATURES)],
                          columns=["model", "feature", "weight"])
save(spark.createDataFrame(weights_pd), T_WEIGHTS)
display(weights_pd.assign(abs_weight=weights_pd["weight"].abs())
        .sort_values(["model", "abs_weight"], ascending=[True, False]).groupby("model").head(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 · (A&B) Lead-lag: does a tone change come before a conflict change?
# MAGIC
# MAGIC For every offset *k* from −4 to +4 weeks: the correlation between a dyad's **change** in tone in week *t*
# MAGIC (`tone_change_1w`) and its change in log material conflict in week *t + k*, pooled over all dyads and weeks.
# MAGIC
# MAGIC - **Changes, not levels.** Both series drift slowly, so their levels correlate at every offset and say nothing
# MAGIC   about order.
# MAGIC - **How to read it:** *k* > 0 pairs tone with *later* conflict ("tone leads"); *k* < 0 pairs it with *earlier*
# MAGIC   conflict ("conflict leads"). If tone leads, the *k* > 0 side should be clearly more negative (tone falls, conflict
# MAGIC   rises afterwards) than its mirror on the *k* < 0 side.
# MAGIC - It is a correlation over all weeks, not a causal test, and not part of the model's evaluation. In sample mode
# MAGIC   there are only 3 weeks, so most offsets have few or no pairs.

# COMMAND ----------

# AI citation: Claude Code (Claude Opus 5.5, Anthropic) drafted this cell (lead-lag table). Reviewed and tested by Romin Gandhi.
lead_lag_pd = pd.DataFrame(mc.lead_lag(weekly, LEAD_LAG_MAX)).astype({"corr": float})  # None (no pairs) -> NaN
save(spark.createDataFrame(lead_lag_pd), T_LEADLAG)
display(lead_lag_pd)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8 · (A&B) Backtest: the model around the biggest rises it had to predict
# MAGIC
# MAGIC The best model scores every week of every dyad. The table then shows the weeks around:
# MAGIC - the `BACKTEST_TOP_N` largest rises in the **test** weeks (by how far next week's count cleared the threshold), and
# MAGIC - any real-world events listed in `BACKTEST_EVENTS` in the setup cell.
# MAGIC
# MAGIC Each row has the week's tone, its material-conflict count, the predicted probability of a rise next week, and
# MAGIC whether one happened. The `split` column marks weeks the model was trained on (`train`), whose scores are not an
# MAGIC honest test. Only `test` weeks are out-of-sample.

# COMMAND ----------

# AI citation: Claude Code (Claude Opus 5.5, Anthropic) drafted this cell (backtest around the largest test-period rises). Reviewed and tested by Romin Gandhi.
scored_all = mc.with_p_rise(results[BEST]["best_model"].transform(data)).withColumn(
    "split", F.when(F.col("week_start").isin(TRAIN_WEEKS), "train").when(F.col("week_start").isin(TEST_WEEKS), "test")
              .when(F.col("week_start").isin(GAP), "gap").otherwise("unlabeled"))

top = (scored_all.where("split = 'test' AND label = 1")
       .orderBy(F.desc(F.col("next_week_conflict") - F.col("rise_threshold")))
       .select("dyad", "week_start").limit(BACKTEST_TOP_N).collect())
picks = [(r["dyad"], r["week_start"], "largest rise in the test weeks") for r in top]
for dyad, day, what in BACKTEST_EVENTS:
    day = dt.date.fromisoformat(day)
    picks.append((dyad, day - dt.timedelta(days=day.weekday()), what))  # Monday of that week

if picks:
    picks_df = spark.createDataFrame(picks, "dyad STRING, focus_week DATE, why STRING")
    backtest = (scored_all.join(F.broadcast(picks_df), "dyad")
                .where(F.abs(F.datediff("week_start", "focus_week")) <= 7 * BACKTEST_WINDOW_WEEKS)
                .select("why", "dyad", "focus_week", "week_start", "split", "tone_wmean", "conflict_events_raw",
                        "p_rise", "label", "next_week_conflict", "rise_threshold")
                .orderBy("focus_week", "dyad", "week_start"))
    save(backtest, T_BACKTEST)
    display(spark.table(T_BACKTEST))
else:
    print("no rises in the test weeks and no BACKTEST_EVENTS listed; nothing to backtest")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9 · (A&B) Learning curve: score vs training-set size
# MAGIC
# MAGIC The chosen model, with its chosen hyper-parameters (no new tuning), is refitted on 10 %, 25 %, 50 %, 75 % and 100 %
# MAGIC of the training rows (a seeded random sample), and each fit is scored on the same training subset and on the full test
# MAGIC weeks. If the test score is still climbing at 100 %, more history would help. If it is flat while the training score
# MAGIC stays higher, the model is limited by the features, not by the amount of data.

# COMMAND ----------

# AI citation: Claude Code (Claude Opus 5.5, Anthropic) drafted this cell (learning curve). Reviewed and tested by Romin Gandhi.
curve_pd = pd.DataFrame(mc.learning_curve(results[BEST]["pipeline"], results[BEST]["param_map"], train, test,
                                          seed=SEED)).astype({"train_auc_pr": float, "test_auc_pr": float})
save(spark.createDataFrame(curve_pd), T_CURVE)
display(curve_pd)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10 · Run parameters
# MAGIC
# MAGIC One row with every setting and count of this run, for the report and for Member 5 (the model path and its feature
# MAGIC list).

# COMMAND ----------

# AI citation: Claude Code (Claude Opus 5.5, Anthropic) drafted this cell (run parameters table). Reviewed and tested by Romin Gandhi.
params = {
    "run_ts": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"), "sample_mode": SAMPLE_MODE,
    "input_source": INPUT_SOURCE, "dyads": shape["dyads"], "weeks": shape["weeks"],
    "rise_ratio": mc.RISE_RATIO, "min_rise": mc.MIN_RISE, "baseline_weeks": mc.BASELINE_WEEKS,
    "train_first_week": str(TRAIN_WEEKS[0]), "train_last_week": str(TRAIN_WEEKS[-1]), "train_weeks": len(TRAIN_WEEKS),
    "gap_weeks": len(GAP), "gap_skipped": split["gap_skipped"],
    "test_first_week": str(TEST_WEEKS[0]), "test_last_week": str(TEST_WEEKS[-1]), "test_weeks": len(TEST_WEEKS),
    "train_rows": N_POS + N_NEG, "train_rises": N_POS, "test_rows": int(test_counts["n"]),
    "test_rises": int(test_counts["pos"] or 0), "weight_rise": W_POS, "weight_no_rise": W_NEG,
    "folds": FOLD_HOW, "covers_selection_weeks": covers_selection,
    "features": json.dumps(FEATURES), "dropped_features": json.dumps(DROPPED),
    "best_model": BEST, "best_params": json.dumps(results[BEST]["best_params"]), "model_path": MODEL_PATH,
}
save(spark.createDataFrame(pd.DataFrame([params])), T_PARAMS)
display(spark.table(T_PARAMS))
