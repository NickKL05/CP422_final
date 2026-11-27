# Handoff: escalation model (Member 4)

<!-- AI citation: drafted with Claude Code (Claude Opus 5.5, Anthropic) as an internal team note. Reviewed by Romin Gandhi.
     This is not report text and must not be copied into the report. -->

What `notebooks/06_escalation_model` reads and writes, and what other members need from it. Values marked
`[TBD: ...]` are filled in after the full-data run.

## TL;DR

- **Input:** Member 3's `workspace.gdelt.dyad_weekly_features_{sample,full}`. If that table isn't in the workspace, the
  notebook reads Member 3's CSV export from `/Volumes/workspace/gdelt/raw/exports/member3/` instead.
- **Run order:** `01_load_gdelt` → `02_clean_events` → `03_dyad_tone_index` → `06_escalation_model`. It starts with
  the usual `sample_mode` widget cell and `%run ./00_config`, and writes `_sample` or `_full` tables like notebook 03.
- **Model for Member 5:** `/Volumes/workspace/gdelt/raw/models/m4_escalation_model_{sample,full}`, a Spark ML
  `PipelineModel`. The full-data model is `[TBD: best model]`.
- **Local test:** `PYSPARK_PYTHON=$(which python) python tests/test_model_local.py` (PySpark only, about 3 minutes).

## Answers to Member 3's open questions

| Question | Answer |
| --- | --- |
| "Sharp rise" definition | `label = 1` if next week's material-conflict count ≥ max(1.5 × baseline, baseline + 3), where baseline = mean material-conflict count of this week and the 3 before it. Constants in `model_common.py` (`RISE_RATIO`, `MIN_RISE`, `BASELINE_WEEKS`) |
| Train/test cutoff | Time split on `week_start`: the most recent 25 % of labeled weeks are the test set, with 1 gap week dropped before them. On the full year that is train 2025-11-03..`[TBD]`, test `[TBD]`..`[TBD]`. The notebook fails in full mode if training doesn't cover your first-half selection weeks |
| CSV or Parquet? | CSV is fine. The notebook reads your `dyad_weekly_features_full.csv` as-is |

## Tables written (suffix `_sample` or `_full`)

| Table | Grain | What |
| --- | --- | --- |
| `m4_model_input_*` | dyad × week | label, naive baseline, features after `log1p` |
| `m4_model_results_*` | model | CV and test PR area, precision, recall, F1, confusion counts, chosen hyper-parameters |
| `m4_test_predictions_*` | dyad × test week | best model's `p_rise` and prediction next to the truth |
| `m4_feature_weights_*` | model × feature | LR coefficients (standardised) / tree importances |
| `m4_lead_lag_*` | offset −4..+4 weeks | corr(Δtone at t, Δlog conflict at t+k), pairs |
| `m4_backtest_*` | dyad × week | weeks around the largest test-period rises (and any hand-listed events) |
| `m4_learning_curve_*` | training share | train and test PR area vs training rows |
| `m4_run_params_*` | one row | every setting, split, count, feature list and the model path |

## Using the model (Member 5)

```python
from pyspark.ml import PipelineModel
import model_common as mc
model = PipelineModel.load("/Volumes/workspace/gdelt/raw/models/m4_escalation_model_full")
rows = mc.model_frame(mc.add_label(weekly))   # weekly: a table shaped like dyad_weekly_features
scored = mc.with_p_rise(model.transform(rows)) # p_rise = probability of a sharp rise next week
```

The imputer and scaler are inside the model. The input needs the feature columns listed in
`m4_run_params_full.features`, built by `model_frame` (it applies `log1p` to the count columns). The model predicts
from a **complete week**, so it fits a weekly batch job better than the 15-minute stream. A week's features are final
N days after the week ends (Member 3's handoff, rule 6).

## Not done / open

- MLlib on Free Edition serverless was not tested here (only on local Spark 4.3). Run the notebook in sample mode once
  in a Free Edition workspace before relying on it.
- `BACKTEST_EVENTS` in the setup cell is empty. Add real events, as `(dyad, date, description)`, if we want named
  examples in the report.
- Predictions use the default 0.5 probability cut-off. No cut-off tuning.
