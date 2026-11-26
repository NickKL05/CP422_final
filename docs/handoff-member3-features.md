# Handoff: dyad features and daily index (Member 3)

This is the data contract for the tables built by `notebooks/03_dyad_tone_index`: what each table contains, how it
is keyed, and the rules that keep the model honest. Member 4 uses the **weekly feature table**, Member 5 uses the
**daily rolling index** for HBase. Notebooks `04_actor_network` and `05_skew_salting_ab` produce report material only.

Values marked `[TBD: ...]` are filled in after the full-data run on Databricks (by end of Nov 27).

## TL;DR

- **Weekly features (Member 4):** `workspace.gdelt.dyad_weekly_features_full`. Also exported as
  `dyad_weekly_features_full.csv` (see "Getting the files", below), because each of us has a separate workspace.
- **Daily index (Member 5):** `workspace.gdelt.dyad_daily_index_sample` (the same October 2026 month as the local
  cluster). Exported as `dyad_daily_index_sample.csv`.
- **One row = one undirected pair of states (a dyad) per Monday-to-Sunday week, or per day.**
- **Row key:** `dyad#yyyyMMdd`, e.g. `CHN_USA#20261005`. The two CAMEO codes are sorted alphabetically and joined
  with `_`. The date is `week_start` (weekly table) or `event_date` (daily table).
- **Time axis is `SQLDATE`** (the event date), with a recency filter `0 <= DATEADDED - SQLDATE <= N`.
  N = `[TBD: N from the full run]` days.
- **`label_` columns are targets, never features.**

## Tables and modes

Every table name ends in `_sample` (`sample_mode = true`, October 2026 files) or `_full` (`sample_mode = false`,
files from Nov 2025 to Oct 2026). A sample-mode Run All therefore never overwrites full-data results.

| Table | Grain | For |
| --- | --- | --- |
| `dyad_weekly_features_{sample,full}` | dyad × complete week (dense: every selected dyad has every week) | Member 4 |
| `dyad_daily_index_{sample,full}` | dyad × day with at least one event | Member 5 |
| `m3_dyad_events_*` | one clean event (both actors states, recency-filtered, on a complete day) | notebooks 04 and 05 |
| `m3_dyad_day_*` | dyad × day sums (the intermediate that both outputs are built from) | internal |
| `m3_run_params_*` | one row: N, date ranges, week range, events source, partition settings | everyone, and the report |
| `m3_cleaning_funnel_*`, `m3_lag_distribution_*` | rows removed per rule; lag distribution used to choose N | report |
| `actor_edges_*`, `actor_centrality_*` | directed state graph, centrality rankings | report only (not features) |
| `m3_skew_results_*` | skew and salting experiment results | report only |

## Getting the files

The notebook writes the exports to the Unity Catalog volume of whoever runs it:

```
/Volumes/workspace/gdelt/raw/exports/member3/dyad_weekly_features_{sample,full}.csv
/Volumes/workspace/gdelt/raw/exports/member3/dyad_daily_index_sample.csv      (sample mode only)
```

Download them from **Catalog → workspace → gdelt → raw → exports/member3**. Member 3 will share the full-data CSV in
the group drive by `[TBD: date and time on Nov 27]`. To load it in your own workspace, upload it to a volume and
read it with `spark.read.option("header", True).option("inferSchema", True).csv(path)`. Then check the types
against the schema below; dates arrive as `yyyy-MM-dd` strings.

## Conventions both tables share

- **Dyad.** An unordered pair of states. A USA→CHN event and a CHN→USA event both count for `CHN_USA`.
  `country_a` is the alphabetically first code and `country_b` the second. Direction is only kept in the actor
  network (notebook 04).
- **States.** CAMEO country codes from `lookups/CAMEO.country.txt`, minus 25 region codes (`EUR`, `MEA`, ...). The
  sub-state codes `WSB` and `GZS` are mapped to `PSE`, and `BAG` to `IRQ`, before anything else; this cleaning rule
  is shared with Member 2. Events within one state (`USA`–`USA`) are not dyads and are dropped.
- **Cleaning.** Duplicate `GLOBALEVENTID` rows are dropped (the earliest report is kept), as are rows with a missing
  actor country, an unknown code, or a missing tone, `QuadClass` or `NumMentions` (or `NumMentions` below 1). Numbers
  are parsed with `try_cast`, so a malformed value removes a row instead of failing the run. Rows removed per rule
  are in `m3_cleaning_funnel_*`.
- **Event date and recency.** Days and weeks use `SQLDATE`. A row is kept only if it was reported at most N days
  after the event (`0 <= DATEADDED - SQLDATE <= N`). That drops the many mentions of events from months or years
  earlier. N is chosen from the data, as the smallest N where one more day would add less than 0.5 % of rows; the
  distribution is in `m3_lag_distribution_*`.
- **Complete days.** Only event days whose reports are fully inside the loaded files are used: from `range_start`
  to `range_end − N`. Full run: `[TBD: first_day]` to `[TBD: last_day]`.
- **Weeks.** Monday to Sunday, keyed by `week_start` (a date). Only weeks lying entirely inside the complete days are
  used. Full run: `[TBD: n_weeks]` weeks from `[TBD: first_week]` (expected 2025-11-03) to `[TBD: last_week]`
  (expected 2026-10-19 if N ≤ 5). Sample run: 3 weeks (Oct 5, 12, 19).
- **Material conflict** = `QuadClass = 4` (CAMEO root codes 15–20).
- **Tone weighting.** `tone_wmean = Σ(AvgTone × NumMentions) / Σ NumMentions` over all events in the window, a ratio
  of sums rather than an average of daily averages.

## Weekly feature table (Member 4)

**Which dyads.** A dyad is included if it averages at least 5 events per week over the **first half** of the weeks:
`[TBD: selected_dyads]` dyads in the full run. Only the first half is used, so that the test period never decides
which dyads are in the data. **Member 4's training period must therefore cover at least the first half of the weeks**
(`[TBD: first_week]` to `[TBD: last selection week]`).

**Dense calendar.** Every selected dyad has a row for every complete week. In a week with no events, the counts are 0
and every tone column is NULL. `LAG` and the trailing columns therefore always refer to calendar weeks, never "the
last week that had events".

| Column | Type | Role | Meaning |
| --- | --- | --- | --- |
| `row_key` | string | id | `dyad#yyyyMMdd` of `week_start` |
| `dyad`, `country_a`, `country_b` | string | id | the pair, e.g. `CHN_USA`, `CHN`, `USA` |
| `week_start`, `week_end` | date | id / split | Monday and Sunday of the week. **Use `week_start` for the time-based split** |
| `n_events` | bigint | feature | events this week |
| `active_days` | bigint | feature | days this week with at least one event (0–7) |
| `sum_mentions` | double | feature | Σ NumMentions |
| `tone_wmean` | double | feature | mentions-weighted tone this week (NULL if no events) |
| `tone_mean` | double | feature | unweighted mean tone |
| `tone_logw_mean` | double | feature | tone weighted by log(1 + NumMentions): less dominated by one viral event |
| `goldstein_mean` | double | feature | mean Goldstein score (−10 conflict … +10 cooperation) |
| `verbal_coop_events`, `material_coop_events`, `verbal_conflict_events`, `material_conflict_events` | bigint | feature | events per `QuadClass` 1–4 |
| `material_conflict_mentions` | double | feature | Σ NumMentions of material-conflict events |
| `root_material_conflict_events` | bigint | feature | material-conflict events flagged `IsRootEvent = 1` (less noisy) |
| `conflict_share` | double | feature | `material_conflict_events / n_events` (NULL if no events) |
| `tone_wmean_lag1` | double | feature | last week's `tone_wmean` |
| `tone_change_1w` | double | feature | `tone_wmean − tone_wmean_lag1` |
| `tone_trailing4_wmean` | double | feature | weighted tone over the **previous** 4 weeks (ratio of sums, this week excluded) |
| `tone_drop_vs_trailing4` | double | feature | `tone_wmean − tone_trailing4_wmean`. **Negative = tone dropped**: the research question's signal |
| `material_conflict_lag1` | bigint | feature | last week's material-conflict count |
| `material_conflict_trailing4_mean`, `material_conflict_trailing4_std` | double | feature | mean and sample std of material conflict over the previous 4 weeks |
| `n_events_trailing4_mean` | double | feature | mean weekly events over the previous 4 weeks |
| `weeks_of_history` | bigint | feature / filter | earlier weeks available to the trailing columns (0 in the first week) |
| `label_material_conflict_next_week` | bigint | **target** | next week's `material_conflict_events`. NULL in the last week |
| `label_conflict_share_next_week` | double | **target** | next week's `conflict_share` |

### Rules that keep the model honest

1. **Never use a `label_` column as a feature.** They are `LEAD(...)` values from the following week.
2. **Drop the last week.** Its labels are NULL, because there is no next week inside the range.
3. **The trailing columns are partial at the start.** Where `weeks_of_history < 4` they use fewer weeks, and they are
   NULL when it is 0. Either filter on `weeks_of_history >= 4`, or keep the column as a feature.
4. **Split by `week_start`, not randomly.** At the boundary, drop the last training week: its label is the first
   test week.
5. **Fit any scaling, imputation or thresholds on the training weeks only.** The table is deliberately unscaled.
6. **A week's features are final N days after `week_end`,** once late reports have arrived. A week's features can
   only be known at `week_end + N`, so a real-time system would predict week *t + 1* N days into it. This matters
   for the write-up, not for the offline split.
7. **The "sharp rise" label is Member 4's definition.** The table supplies what a definition needs:
   - next week's count and share (`label_` columns)
   - this week's level
   - the dyad's own trailing mean and std

   For example, `label_material_conflict_next_week > material_conflict_trailing4_mean + 2 × material_conflict_trailing4_std`,
   with a minimum count. Whatever rule is chosen, write it down in the ML section.
8. **Don't add the actor-network centrality as a feature.** It is computed over the whole period, which would leak
   later weeks into earlier rows.

## Daily rolling index (Member 5, HBase)

One row per dyad per day that had at least one event. Only days whose full 7-day window lies inside the complete days
are included (from `first_full_window_day`). Days without events have no row. The rolling columns still cover
"the 7 calendar days ending on this date", because the window is a `RANGE` frame over days, not rows.

| Column | Type | Meaning |
| --- | --- | --- |
| `row_key` | string | `dyad#yyyyMMdd` of `event_date`, e.g. `CHN_USA#20261015` |
| `dyad`, `country_a`, `country_b` | string | the pair |
| `event_date` | date | the day |
| `n_events` | bigint | events on this day |
| `material_conflict_events` | bigint | material-conflict events on this day |
| `tone_wmean_day` | double | mentions-weighted tone of this day alone |
| `tone_rolling7_wmean` | double | **the index**: mentions-weighted tone over this day and the 6 before it |
| `events_rolling7` | bigint | events in the 7-day window |
| `mentions_rolling7` | double | Σ NumMentions in the window |
| `material_conflict_rolling7` | bigint | material-conflict events in the window |
| `active_days_rolling7` | bigint | days in the window with events (1–7) |

**Row-key notes for HBase.**
- The key sorts by dyad, then date. All rows of one dyad are therefore contiguous, and a time range for a dyad is a
  single prefix scan: start `CHN_USA#20261001`, stop `CHN_USA#20261032`.
- The date part is fixed-width `yyyyMMdd`, so string order equals date order.
- The dyad codes use `_` and the date is joined with `#`, so neither separator can appear inside the other part.
- If writes ever hot-spot on one region, a hash prefix on the key is the HBase version of the salting in notebook 05.
  At the sample's size this shouldn't be needed.
- Suggested layout (Member 5's call): one column family (for example `d`), one qualifier per column above.

The sample CSV has `[TBD: daily rows in sample run]` rows. The daily table for the full year is written on Databricks
but not exported, because the local HBase only holds the October sample.

## Agreements still open

| With | Question | Default if no answer |
| --- | --- | --- |
| Member 2 | Is the cleaned table called `events_clean` (as now in `00_config`)? Does it keep `file_year`/`file_month` and the original column names? | 03 uses its own fallback cleaning over `events_bronze` and says so in a banner |
| Member 2 | Do you apply the same `WSB`/`GZS` → `PSE`, `BAG` → `IRQ` remap and recency rule? | 03 applies them anyway; both are idempotent |
| Member 4 | Your "sharp rise" definition and train/test cutoff (training must cover at least the first half of the weeks) | The table supplies the inputs; the label stays yours |
| Member 4 | Is CSV in the group drive OK, or do you want Parquet? | CSV |
| Member 5 | Is `dyad#yyyyMMdd` with `CHN_USA`-style dyads the key you want? | As above |

Questions: message Member 3 in the group chat.
