<!-- Draft v1 (Member 4), 2026-11-26. [TBD: ...] marks a number that comes from the full-data Databricks run.
     Sources for each number: m4_run_params_full, m4_model_results_full, m4_feature_weights_full, m4_lead_lag_full,
     m4_backtest_full, m4_learning_curve_full, and the printed output of notebook 06.
     AI citation: Claude Code (Claude Opus 5.5, Anthropic) drafted this section. Reviewed by Romin Gandhi.
     This AI use must also be listed in the report's References (Member 5). -->

## Predictive Modeling & Machine Learning

The research question is whether news tone leads conflict. The modelling stage turns it into a prediction task: from
what is known at the end of week *t*, predict whether a dyad's material conflict rises sharply in week *t + 1*. It runs
in one notebook (`06_escalation_model`) with Spark MLlib on Databricks Free Edition. The input is the weekly feature
table from the Spark stage (Section [TBD: Spark methodology]): [TBD: dyads] dyads × [TBD: weeks] complete weeks, from
2025-11-03 to [TBD: last week].

#### Prediction target

Material conflict is `QuadClass = 4` (CAMEO root codes 15–20). For each dyad and week we compute a baseline, the mean
weekly material-conflict count over the current week and the three before it. A week is labelled positive when the
following week's count is at least 1.5 times the baseline **and** at least 3 events above it:

$$y_t = 1 \iff c_{t+1} \ge \max(1.5\, b_t,\ b_t + 3), \qquad b_t = \tfrac{1}{4}\textstyle\sum_{i=0}^{3} c_{t-i}$$

The ratio makes the rule relative to each dyad's own level: 100 → 120 is ordinary variation for a large dyad, while
2 → 12 is not. The minimum of 3 events stops 0 → 1 on a quiet dyad from counting as an escalation. The baseline uses
only weeks up to *t*. The calendar is dense (weeks without events are rows with zero counts), so "the three weeks
before" are calendar weeks. [TBD: positive rate]% of labelled dyad-weeks are positive.

#### Features and leakage control

The model uses 24 features: the 23 backward-looking columns of the weekly table plus the baseline:
- this week's tone, measured three ways (mentions-weighted, log-weighted, unweighted)
- the change in tone from last week, and from the trailing four weeks (`tone_drop_vs_trailing4`, the signal in the
  research question)
- the mean Goldstein score
- the event counts per `QuadClass`, mentions, and active days
- the dyad's trailing conflict history, and the baseline above

Heavy-tailed counts are log-transformed (`log1p`).

Five measures keep test information out of training:
1. **No future columns.** The next-week columns are used only to build the label. Network centrality is excluded
   because it is computed over the whole year.
2. **A time-based split.** The most recent 25 % of labelled weeks ([TBD: test weeks] weeks, [TBD: test range]) form the
   test set, and the week before them is dropped. That week's label is the first test week's count.
3. **Training covers the dyad-selection weeks.** The Spark stage chose dyads on the first half of the weeks, and the
   training period covers all of those weeks, so the test period never influenced which dyads are in the data.
4. **Fitted steps live inside the pipeline.** Median imputation and standardisation are `Pipeline` stages, so they are
   re-fitted on each training fold and never see validation or test rows.
5. **Folds are blocks of weeks.** Cross-validation folds are three contiguous blocks of training weeks (`foldCol`),
   not random rows.

#### Class imbalance, models and tuning

Positives are rare, so each training row is weighted to give both classes equal total weight: *n* / (2 *n*<sub>pos</sub>)
for positives and *n* / (2 *n*<sub>neg</sub>) for negatives ([TBD: weights]). All three classifiers take the weight
through `weightCol`. The evaluation metrics are unweighted.

| Model | Grid (`ParamGridBuilder`) |
| --- | --- |
| Logistic regression | `regParam` ∈ {0.001, 0.01, 0.1} × `elasticNetParam` ∈ {0, 0.5} |
| Random forest | `numTrees` ∈ {50, 100} × `maxDepth` ∈ {5, 8} |
| Gradient-boosted trees | `maxDepth` ∈ {3, 5} × `stepSize` ∈ {0.05, 0.1}, 40 iterations |

Each model is tuned with `CrossValidator` on the area under the precision–recall curve (PR AUC). We chose PR AUC over
ROC AUC because, with rare positives, ROC AUC rewards ranking the many easy negatives. A random ranking scores the
positive rate on PR AUC. The final model is chosen on the cross-validation score alone. The test weeks are scored once,
after that choice. The grids are deliberately small, because Free Edition compute is shared and quota-limited.

The reference point is a naive persistence baseline: next week looks like this week. It predicts a rise whenever this
week's count already clears the threshold.

#### Results

[TBD: one or two sentences on the outcome, written after the full run.]

| Model | CV PR AUC | Test PR AUC | Precision | Recall | F1 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Naive persistence | — | [TBD] | [TBD] | [TBD] | [TBD] |
| Logistic regression | [TBD] | [TBD] | [TBD] | [TBD] | [TBD] |
| Random forest | [TBD] | [TBD] | [TBD] | [TBD] | [TBD] |
| Gradient-boosted trees | [TBD] | [TBD] | [TBD] | [TBD] | [TBD] |

Test set: [TBD: rows] dyad-weeks, [TBD: positives] positives (positive rate [TBD], the PR AUC of a random ranking).
Precision, recall and F1 are for the positive class at a 0.5 probability cut-off.

**What the model uses.** [TBD: top features from m4_feature_weights_full. For logistic regression, give the sign of
the tone coefficients: a negative weight on `tone_drop_vs_trailing4` means a fall in tone raises the predicted risk.]

The selected model ([TBD: model and hyper-parameters]) is saved as a Spark ML `PipelineModel`, including its imputer
and scaler, for the serving stage.

#### Above and beyond

**Lead–lag analysis.** For offsets *k* = −4 … +4 weeks we correlate a dyad's week-to-week change in tone with its change
in log material conflict *k* weeks later, pooled over all dyads ([TBD: pairs] pairs per offset). We use changes rather
than levels because both series drift slowly, so their levels correlate at every offset. If tone leads conflict, the
correlations at *k* > 0 should be more negative than their mirror images at *k* < 0. [TBD: result, e.g. "r = … at
+1 week vs … at −1 week".] This is a correlation, not a causal test.

**Backtest.** The selected model scored every week of the dyads with the [TBD: N] largest rises in the test period.
[TBD: describe one or two cases: did the tone fall and the predicted probability rise the week before?]

**Learning curve.** The selected model was refitted on 10–100 % of the training rows with its hyper-parameters fixed.
[TBD: does the test PR AUC still rise at 100 % (more history would help) or flatten (the features are the limit)?]

#### Limitations

- GDELT counts media reports of events, not the events themselves. A "rise" can be a rise in coverage.
- The label's thresholds (1.5×, +3 events) are a modelling choice. Other thresholds change the positive rate and the
  scores.
- One year of data gives about [TBD: weeks] weekly observations per dyad, and the test period covers a single season.
- Blocked cross-validation still trains some folds on weeks later than the ones they validate. Only the final test is
  strictly forward in time.
- A week's features are complete only N days after it ends ([TBD: N] days), because of late reports. A live system
  would make its prediction for week *t + 1* N days into that week.
