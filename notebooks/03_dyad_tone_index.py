# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Dyad tone index and weekly features (Member 3)
# MAGIC
# MAGIC **Research question:** does news tone lead conflict? For every pair of states (a *dyad*) this notebook builds a
# MAGIC rolling 7-day news-tone index weighted by `NumMentions`, weekly counts of material conflict, and one row per dyad
# MAGIC per week of features for the model (Member 4). The question becomes: does a drop in a dyad's tone in week *t*
# MAGIC predict a rise in its material conflict in week *t + 1*?
# MAGIC
# MAGIC | Step | What happens | Output |
# MAGIC | --- | --- | --- |
# MAGIC | 1 | Choose the events source (Member 2's cleaned table, or a cast-and-clean fallback over `events_bronze`) | |
# MAGIC | 2 | Size the shuffle partitions from the input | |
# MAGIC | 3 | Load the CAMEO lookups from the repo and broadcast them | |
# MAGIC | 4 | Type the columns with `try_cast`, flag every cleaning and dyad rule, report the funnel | `m3_cleaning_funnel_*` |
# MAGIC | 5 | Choose the recency window N from the data; find the complete days and weeks | `m3_lag_distribution_*` |
# MAGIC | 6 | Keep clean dyadic events on complete days | `m3_dyad_events_*` |
# MAGIC | 7 | Aggregate to dyad-day sums | `m3_dyad_day_*` |
# MAGIC | 8 | Rolling 7-day mentions-weighted tone (Spark SQL window, `RANGE` frame) | `dyad_daily_index_*` (Member 5, HBase) |
# MAGIC | 9 | Dense dyad x week calendar and weekly features | `dyad_weekly_features_*` (Member 4) |
# MAGIC | 10 | Checks, run parameters, CSV export | `m3_run_params_*`, CSV files in the volume |
# MAGIC
# MAGIC **Time axis.** Days and weeks use `SQLDATE`, the date the event happened, as the team agreed. `DATEADDED`, the date
# MAGIC of the file that reported it, is used for only two things: the recency filter `0 <= DATEADDED - SQLDATE <= N`,
# MAGIC and partition pruning (the table is partitioned by `file_year`/`file_month`, which come from `DATEADDED`).
# MAGIC
# MAGIC **Sample and full runs write to different tables** (suffix `_sample` or `_full`), so a sample-mode Run All can never
# MAGIC overwrite the full-data results.
# MAGIC
# MAGIC The logic lives in `dyad_common.py` next to this notebook, so `tests/test_dyads_local.py` can check the same code
# MAGIC locally on hand-made rows. This notebook explains each step and runs it.

# COMMAND ----------

# Declared here, not only in 00_config: a widget created inside a %run notebook does not appear in this notebook's
# parameter bar. Every top-level notebook in the repo starts with this cell.
dbutils.widgets.dropdown("sample_mode", "true", ["true", "false"], "SAMPLE_MODE")

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: imports, widgets, output table names
# MAGIC
# MAGIC Two extra widgets, both safe to leave at their defaults:
# MAGIC - `events_source`: `auto` uses Member 2's cleaned table if it exists, otherwise the fallback. The other two values force one source.
# MAGIC - `recency_days`: `auto` picks N from the data (step 5). A number overrides it, for example to reuse the N chosen on full data.
# MAGIC
# MAGIC Two fixed settings control which dyads go into the weekly feature table (step 9):
# MAGIC - `MIN_EVENTS_PER_WEEK = 5`: a dyad needs on average at least 5 events a week. A tone measured on one or two
# MAGIC   articles is mostly noise.
# MAGIC - `SELECTION_FRACTION = 0.5`: that average is measured on the **first half** of the weeks only, so the test
# MAGIC   period never decides which dyads are in the data.

# COMMAND ----------

import datetime as dt
import pandas as pd
from pyspark.sql import functions as F
import dyad_common as dc

dbutils.widgets.dropdown("events_source", "auto", ["auto", "member2_clean", "bronze_fallback"], "Events source")
dbutils.widgets.dropdown("recency_days", "auto", ["auto"] + [str(n) for n in range(1, 15)], "Recency window N (days)")

MIN_EVENTS_PER_WEEK = 5
SELECTION_FRACTION = 0.5

# Separate names per mode: Member 1's sample-mode Run All must not overwrite the full-data tables.
SUFFIX = "_sample" if SAMPLE_MODE else "_full"
T = lambda name: f"{CATALOG}.{SCHEMA}.{name}{SUFFIX}"
T_FUNNEL, T_LAGS = T("m3_cleaning_funnel"), T("m3_lag_distribution")
T_EVENTS, T_DYAD_DAY = T("m3_dyad_events"), T("m3_dyad_day")
T_DAILY, T_WEEKLY, T_PARAMS = T("dyad_daily_index"), T("dyad_weekly_features"), T("m3_run_params")
EXPORT_DIR = f"{VOLUME_ROOT}/exports/member3"

def save(df, table):
    """Overwrite a result table. overwriteSchema lets a re-run with changed columns replace the old table cleanly."""
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)

print(f"SAMPLE_MODE={SAMPLE_MODE}  files {RANGE_START}..{RANGE_END}  outputs end in '{SUFFIX}'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Events source: Member 2's cleaned table, or the fallback
# MAGIC
# MAGIC Member 2 owns the typed, cleaned events table. Until it exists, this notebook reads `events_bronze`, where every
# MAGIC column is still a string, and applies the same rules itself. Everything downstream reads from a single variable,
# MAGIC `EVENTS_SOURCE`. The name of Member 2's table, `EVENTS_CLEAN`, comes from `00_config`, so it is defined in one
# MAGIC place for every notebook. As soon as that table exists in the workspace, `auto` picks it up with no code change.
# MAGIC
# MAGIC The choice is printed in a banner, because results produced from the fallback must never be mistaken for results
# MAGIC produced from Member 2's table.
# MAGIC
# MAGIC The rules in step 4 are applied to **either** source. They are idempotent (a row that is already clean passes them
# MAGIC unchanged), so the results don't depend on exactly which rules Member 2's table already applied.
# MAGIC
# MAGIC **Partition pruning.** The filter is written on `file_year`/`file_month` only, so Delta skips other months'
# MAGIC files using its transaction log. Filtering on `SQLDATE` would scan every partition. The plan line printed below
# MAGIC must show the months in `PartitionFilters`.

# COMMAND ----------

# EVENTS_CLEAN and EVENTS_BRONZE come from 00_config (via %run above). The same helper picks the source in 03, 04 and 05.
src = dc.resolve_events_source(spark, dbutils.widgets.get("events_source"), EVENTS_CLEAN, EVENTS_BRONZE,
                               RANGE_START, RANGE_END)
EVENTS_SOURCE, use_member2, PART_PREDICATE = src["source"], src["use_member2"], src["predicate"]
source_in_range = src["df_in_range"]
print(dc.source_banner(src))

scan_lines = dc.plan_lines(dc.explain_text(source_in_range, "formatted"), "PartitionFilters")
print("\n".join(scan_lines) if scan_lines else "WARNING: no PartitionFilters line in the plan; check pruning")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Partition tuning: shuffle partitions sized from the data
# MAGIC
# MAGIC `spark.sql.shuffle.partitions` sets how many tasks each shuffle (groupBy, join, window) starts with. The default is 200.
# MAGIC That suits a few GB per shuffle, but the sample month is about 3.2 million rows, so 200 partitions would mean
# MAGIC 200 tiny tasks whose start-up time outweighs their work.
# MAGIC
# MAGIC **Rule:** one partition per about 500,000 input rows, rounded up to a multiple of 8, between 8 and 200.
# MAGIC - The shuffled rows are narrow: about 15 typed columns, roughly 100 bytes each. 500,000 of them is about 50 MB per
# MAGIC   partition, inside the usual 16–128 MB guideline.
# MAGIC - A multiple of 8 divides evenly across typical core counts.
# MAGIC - The rule gives **8 partitions for the sample month** (3.2 M rows) and **80 for the full year** (37.2 M rows).
# MAGIC
# MAGIC Row counts on Delta come from file statistics, so the count below is cheap.
# MAGIC
# MAGIC **Serverless may block the setting.** Free Edition serverless only allows a short list of Spark configs, so the
# MAGIC `spark.conf.set` is wrapped in `try`. If it's blocked, adaptive query execution (AQE) still merges small shuffle
# MAGIC partitions at run time. Either way, the cell prints what is actually in effect.
# MAGIC
# MAGIC The window functions get their own, explicit partition count in step 8, which doesn't depend on this setting.

# COMMAND ----------

rows_in_range = source_in_range.count()
SHUFFLE_PARTITIONS = dc.shuffle_partitions_for(rows_in_range)

def conf_get(key):
    try:
        return spark.conf.get(key)
    except Exception as exc:
        return f"<not readable: {type(exc).__name__}>"

try:
    spark.conf.set("spark.sql.shuffle.partitions", str(SHUFFLE_PARTITIONS))
    shuffle_status = "set"
except Exception as exc:
    shuffle_status = f"blocked by the platform ({type(exc).__name__}); AQE coalescing applies instead"

print(f"rows in range: {rows_in_range:,}  ->  shuffle partitions {SHUFFLE_PARTITIONS} ({shuffle_status})")
print(f"in effect: spark.sql.shuffle.partitions={conf_get('spark.sql.shuffle.partitions')}  "
      f"spark.sql.adaptive.enabled={conf_get('spark.sql.adaptive.enabled')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · CAMEO lookups from the repo, broadcast to every task
# MAGIC
# MAGIC The lookups are files in `lookups/` (see its README for the source URLs, fetch date and checksums). They are not a table
# MAGIC in one member's workspace, so this notebook runs the same way in every account.
# MAGIC - **States:** 261 CAMEO country codes, minus 25 region codes (`EUR`, `MEA`, ...) and minus 3 sub-state codes,
# MAGIC   which leaves 233 states.
# MAGIC - **Remap (cleaning rule shared with Member 2):** `WSB` (West Bank) and `GZS` (Gaza Strip) become `PSE`, and `BAG`
# MAGIC   (Baghdad) becomes `IRQ`. The remap runs before the same-country rule, so a `WSB`–`PSE` event becomes `PSE`–`PSE`
# MAGIC   and is dropped as domestic.
# MAGIC - **Event codes stay strings.** `010` and `10` are different CAMEO codes.
# MAGIC
# MAGIC The state and event-code tables are a few KB, so they are **broadcast**: every task gets a full copy and joins its
# MAGIC slice of events locally, with no shuffle of the event stream. The hint is explicit (`F.broadcast`) so the choice
# MAGIC doesn't depend on Spark's size estimate, and the plan in step 4 shows a `BroadcastHashJoin`.

# COMMAND ----------

lk = dc.load_lookups()
eventcodes_df = spark.createDataFrame(sorted(lk["eventcodes"].items()), "code STRING, description STRING")
countries_df = spark.createDataFrame(sorted(lk["countries"].items()), "code STRING, name STRING")
print(f"lookups from {lk['dir']}")
print(f"  {len(lk['countries'])} CAMEO country codes, {len(lk['states'])} states, {len(lk['nonstate'])} region codes "
      f"excluded, remap {lk['remap']}, {len(lk['eventcodes'])} event codes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Typing, cleaning and dyad rules, with a funnel
# MAGIC
# MAGIC **Typing.** Numbers use `try_cast` and dates use a `yyyyMMdd` check plus `try_to_timestamp`. Serverless runs with ANSI
# MAGIC mode on, where a plain `CAST` of one malformed value would raise and fail the whole run. With `try_cast` the value
# MAGIC becomes NULL, and the row is counted under `invalid_measure` or `invalid_date`. Country and event codes are trimmed
# MAGIC strings, with `''` treated as NULL.
# MAGIC
# MAGIC **Rules, in order.** Each row gets the first rule it fails, so the funnel adds up exactly to the input:
# MAGIC
# MAGIC | Reason | Rule |
# MAGIC | --- | --- |
# MAGIC | `missing_event_id` | no `GLOBALEVENTID` |
# MAGIC | `duplicate_event_id` | a later copy of an ID already seen. The earliest report (smallest `DATEADDED`) is kept, so the choice is deterministic |
# MAGIC | `invalid_date` | `SQLDATE` or `DATEADDED` not a real date |
# MAGIC | `missing_country` | either actor has no country code, so there is no dyad. `Actor*Geo_CountryCode` is **not** used as a fallback: it is a 2-letter FIPS code, not CAMEO |
# MAGIC | `region_code` | either actor is a region (`EUR`, `MEA`, ...), not a state |
# MAGIC | `unknown_code` | a code that isn't in the CAMEO country list. Listed below, not guessed |
# MAGIC | `same_country` | both actors are the same state (after the remap): a domestic event, not a dyad |
# MAGIC | `invalid_measure` | `AvgTone` or `QuadClass` missing, or `NumMentions` missing or below 1 (no weight) |
# MAGIC | `eligible` | passes every rule |
# MAGIC
# MAGIC The rules are applied as a `reason` column rather than as filters, so one aggregation reports how many rows each
# MAGIC rule removes. The recency filter comes next, in step 5, because its N is chosen from the lags of the eligible rows.

# COMMAND ----------

typed = dc.typed_in_range(src)
flagged = dc.flag_events(typed, lk, spark)

join_lines = dc.plan_lines(dc.explain_text(flagged, "formatted"), "BroadcastHashJoin")
print("broadcast joins in the plan:" if join_lines else "WARNING: no BroadcastHashJoin found in the plan")
for ln in join_lines[:4]:
    print("  ", ln)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cleaning funnel and code report
# MAGIC
# MAGIC The first table counts rows per rule. It is saved, so the report can quote it without re-running. The second table
# MAGIC counts **actor values**, two per event, for every code that was remapped, excluded as a region, or not found. It
# MAGIC shows how many values the `WSB`/`GZS`/`BAG` remap rewrote, and which unknown codes (if any) are worth adding to
# MAGIC the lookup.

# COMMAND ----------

lag_or_null = F.when(F.col("reason") == "eligible", dc.lag_bucket_col())
stats = flagged.groupBy("reason", lag_or_null.alias("lag_bucket")).count().collect()

by_reason = {r: 0 for r in dc.REASONS}
lag_counts = {}
for r in stats:
    by_reason[r["reason"]] += r["count"]
    if r["reason"] == "eligible":
        lag_counts[r["lag_bucket"]] = lag_counts.get(r["lag_bucket"], 0) + r["count"]
total_input = sum(by_reason.values())
assert total_input == rows_in_range or PART_PREDICATE is None, "funnel must add up to the input rows"

funnel_pd = pd.DataFrame([(i + 1, r, by_reason[r], by_reason[r] / total_input) for i, r in enumerate(dc.REASONS)],
                         columns=["step", "reason", "rows", "share_of_input"])
save(spark.createDataFrame(funnel_pd), T_FUNNEL)
display(funnel_pd)

code_pd = dc.code_side_stats(flagged, lk).orderBy("status", F.desc("count")).toPandas()
for status, label in [("remapped", "remapped sub-state codes"), ("region_excluded", "region codes excluded"),
                      ("unknown_code", "codes not in the CAMEO country list (top 20)")]:
    part = code_pd[code_pd["status"] == status].head(20)
    print(f"{label}: {int(code_pd.loc[code_pd['status'] == status, 'count'].sum()):,} actor values")
    if len(part):
        print(part[["raw_code", "mapped_code", "count"]].to_string(index=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Recency window N, complete days and complete weeks
# MAGIC
# MAGIC Every monthly batch of files contains rows about events long before the article: the oldest `SQLDATE` in each
# MAGIC month is about 10 years before the file date (handoff note, gotcha 2). Those mentions are about the past, not about
# MAGIC the week they were published in, so only rows with `0 <= DATEADDED - SQLDATE <= N` are kept.
# MAGIC
# MAGIC **How N is chosen.** N is the smallest value from 1 to 14 for which one more day (lag N + 1) would add less than
# MAGIC 0.5 % of the eligible rows. Same-day and next-day reports are always kept. Past that point, each extra day adds
# MAGIC almost no events but delays the last complete day by one more day. The table below shows the distribution so the
# MAGIC choice can be checked by eye. Use the N from the **full-data** run in the report; set the `recency_days` widget to
# MAGIC force it.
# MAGIC
# MAGIC **Complete days.** The loaded files cover `DATEADDED` from `RANGE_START` to `RANGE_END`. An event dated *d* can be
# MAGIC reported on days *d* to *d + N*, so day *d* is complete only if:
# MAGIC - *d* is on or after `RANGE_START` (earlier days only have their late reports), and
# MAGIC - *d + N* is on or before `RANGE_END` (later days are still missing reports from files that aren't loaded).
# MAGIC
# MAGIC **Complete weeks** are Monday-to-Sunday weeks that lie entirely inside the complete days, so the partial weeks at
# MAGIC both ends are dropped. Weeks are keyed by their Monday date, never by week-of-year, which wraps at New Year.
# MAGIC
# MAGIC **Timing for the model.** With this axis, week *t*'s features are final N days after the week ends, when its
# MAGIC late reports have arrived. Member 4 should treat a week's features as known at `week_end + N`.

# COMMAND ----------

eligible_total = by_reason["eligible"]
auto_n = dc.choose_recency_days(lag_counts, eligible_total)
widget_n = dbutils.widgets.get("recency_days")
RECENCY_DAYS = auto_n if widget_n == "auto" else int(widget_n)
kept_rows = sum(lag_counts.get(k, 0) for k in range(0, RECENCY_DAYS + 1))

FIRST_DAY, LAST_DAY = dc.complete_days(RANGE_START, RANGE_END, RECENCY_DAYS)
WEEK_STARTS = dc.complete_week_starts(RANGE_START, RANGE_END, RECENCY_DAYS)
FIRST_FULL_WINDOW_DAY = FIRST_DAY + dt.timedelta(days=6)
assert WEEK_STARTS, "no complete week inside the range"

lag_rows, cum = [], 0
for b in sorted(lag_counts, key=lambda x: (x is None, x)):
    cum += lag_counts[b] if b is not None and b >= 0 else 0
    label = {-1: "< 0", 31: "> 30"}.get(b, str(b))
    lag_rows.append((label, b, lag_counts[b], lag_counts[b] / eligible_total,
                     cum / eligible_total if b is not None and 0 <= b <= 30 else None,
                     b is not None and 0 <= b <= RECENCY_DAYS))
lags_pd = pd.DataFrame(lag_rows, columns=["lag_days", "lag_bucket", "rows", "share", "cumulative_share", "kept"])
save(spark.createDataFrame(lags_pd), T_LAGS)
display(lags_pd)

print(f"recency window N = {RECENCY_DAYS} days ({'chosen from the data' if widget_n == 'auto' else 'set by widget'}; "
      f"rule's choice = {auto_n})")
print(f"kept {kept_rows:,} of {eligible_total:,} eligible rows ({kept_rows / eligible_total:.1%}); "
      f"{kept_rows / total_input:.1%} of all input rows")
print(f"complete days {FIRST_DAY}..{LAST_DAY}; {len(WEEK_STARTS)} complete weeks "
      f"{WEEK_STARTS[0]}..{WEEK_STARTS[-1] + dt.timedelta(days=6)}; daily index from {FIRST_FULL_WINDOW_DAY}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · Clean dyadic events
# MAGIC
# MAGIC Eligible rows inside the recency window, on complete days. Each event gets:
# MAGIC - its **undirected dyad**: the two state codes sorted, keyed like `CHN_USA`. A USA→CHN event and a CHN→USA event
# MAGIC   describe the same relationship, and keeping direction would halve the data per dyad.
# MAGIC - its **direction**, kept in `actor1_state`/`actor2_state` for the actor network (notebook 04).
# MAGIC - a readable **event root description**, from a broadcast join to the CAMEO event codes.
# MAGIC
# MAGIC The result is written to Delta, so the later steps and notebooks 04 and 05 read this table instead of
# MAGIC recomputing the cleaning. Serverless doesn't support `.cache()`, so writing a table is how a result is reused.

# COMMAND ----------

save(dc.eligible_events(flagged, eventcodes_df, RECENCY_DAYS, FIRST_DAY, LAST_DAY), T_EVENTS)
events_df = spark.table(T_EVENTS)
n_clean = events_df.count()
print(f"{T_EVENTS}: {n_clean:,} events ({n_clean / total_input:.1%} of the input rows), "
      f"{events_df.select('dyad').distinct().count():,} dyads")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 · Dyad-day sums (first step of the two-step aggregation)
# MAGIC
# MAGIC One row per dyad per day that had events. The row holds **sums**, not averages: event count, Σ mentions,
# MAGIC Σ(tone × mentions), Σ tone, Σ(tone × log(1 + mentions)), Σ log(1 + mentions), Goldstein, counts per `QuadClass`,
# MAGIC and material-conflict counts.
# MAGIC
# MAGIC Sums can be added up over any window (7 days, a week, 4 weeks) and divided at the end. That makes every window's
# MAGIC tone a true mentions-weighted mean over all its events:
# MAGIC
# MAGIC $$\text{tone}_{w} = \frac{\sum_{e \in w} \text{AvgTone}_e \cdot \text{NumMentions}_e}{\sum_{e \in w} \text{NumMentions}_e}$$
# MAGIC
# MAGIC Averaging daily averages instead would give a day with one article the same weight as a day with a thousand.
# MAGIC
# MAGIC `NumMentions` has a heavy tail: one viral event can dominate a dyad's week. So the table also carries the sums for
# MAGIC two alternatives:
# MAGIC - a **log-weighted** tone, with weight log(1 + mentions)
# MAGIC - the **unweighted** mean
# MAGIC
# MAGIC The model can compare the three.
# MAGIC
# MAGIC Material conflict is `QuadClass = 4`, which covers CAMEO root codes 15–20 (force posture, coercion, assault, fight,
# MAGIC mass violence).

# COMMAND ----------

save(dc.build_dyad_day(events_df), T_DYAD_DAY)
dyad_day_df = spark.table(T_DYAD_DAY)
dyad_day_rows = dyad_day_df.count()
print(f"{T_DYAD_DAY}: {dyad_day_rows:,} dyad-days")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8 · Rolling 7-day tone per dyad (Spark SQL window function)
# MAGIC
# MAGIC **Frame: `RANGE BETWEEN 6 PRECEDING AND CURRENT ROW` over the day number.** This means "the 7 calendar days ending
# MAGIC today", however many of those days had events. A `ROWS` frame would take the last 7 *active* days, which for a
# MAGIC quiet dyad can stretch over several weeks. Days with no events need no filler rows: the `RANGE` frame measures
# MAGIC distance in days, not rows. The local test checks this on a dyad with a 6-day gap.
# MAGIC
# MAGIC **Ratio of window sums:** `SUM(sum_tone_x_mentions) OVER w / SUM(sum_mentions) OVER w`, the formula from step 7.
# MAGIC
# MAGIC **Repartition by dyad before the window.** A window `PARTITION BY dyad` needs all of a dyad's rows in one task.
# MAGIC Repartitioning by `dyad` explicitly:
# MAGIC 1. sets the partition count for this step, sized from the dyad-day row count with the same rule as step 2. The
# MAGIC    window input is much smaller than the raw events.
# MAGIC 2. lets Spark use that one exchange for the window instead of adding another. The plan lines printed below should
# MAGIC    show a single `hashpartitioning(dyad, …)` exchange feeding the `Window`.
# MAGIC
# MAGIC The rows-per-partition summary shows how evenly the dyads spread. One dyad is never split across partitions, so the
# MAGIC biggest dyad sets a floor on the largest partition. Notebook 05 studies that skew.
# MAGIC
# MAGIC Days before `FIRST_FULL_WINDOW_DAY` (the first complete day + 6) are computed but not saved: their window would
# MAGIC reach back before the first complete day.
# MAGIC
# MAGIC **Output for Member 5 (HBase):** `dyad_daily_index_*`, one row per dyad per active day. Its `row_key` is
# MAGIC `dyad#yyyyMMdd`, for example `CHN_USA#20261015`.

# COMMAND ----------

WINDOW_PARTITIONS = dc.shuffle_partitions_for(dyad_day_rows)
sizes = dc.partition_sizes(dyad_day_df.repartition(WINDOW_PARTITIONS, "dyad"))
print(f"window partitions: {WINDOW_PARTITIONS} (from {dyad_day_rows:,} dyad-days); rows per partition: "
      f"{dc.size_summary(sizes)}")

print(dc.ROLLING_SQL.format(view="m3_dyad_day_v"))
daily = dc.rolling_daily_index(spark, dyad_day_df, WINDOW_PARTITIONS, FIRST_FULL_WINDOW_DAY)
for ln in dc.plan_lines(dc.explain_text(daily, "simple"), "Exchange", "Window"):
    print("  ", ln[:160])

save(daily, T_DAILY)
daily_df = spark.table(T_DAILY)
print(f"{T_DAILY}: {daily_df.count():,} dyad-days with a full 7-day window")

# COMMAND ----------

# MAGIC %md
# MAGIC Example: the most-reported dyad's daily rolling tone. `tone_wmean_day` is that day's own weighted tone, and
# MAGIC `tone_rolling7_wmean` is the 7-day index.

# COMMAND ----------

top_dyad = (events_df.groupBy("dyad").count().orderBy(F.desc("count")).first() or {"dyad": None})["dyad"]
display(daily_df.where(F.col("dyad") == top_dyad).orderBy("event_date")
        .select("row_key", "event_date", "n_events", "tone_wmean_day", "tone_rolling7_wmean", "events_rolling7",
                "material_conflict_rolling7", "active_days_rolling7"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9 · Weekly feature table (Member 4)
# MAGIC
# MAGIC **Week:** Monday to Sunday by `SQLDATE`, keyed by `week_start`. Only the complete weeks from step 5 are used.
# MAGIC
# MAGIC **Which dyads:** dyads averaging at least `MIN_EVENTS_PER_WEEK` events a week over the **first half** of the weeks.
# MAGIC Judging activity over the whole period would let the test weeks decide what is in the data (selection leakage).
# MAGIC Member 4's training period must cover at least the first half of the weeks for this to hold.
# MAGIC
# MAGIC **Dense calendar:** every selected dyad × every complete week.
# MAGIC - In a week with no events, counts are 0 ("nothing reported" is a real observation) and tone is NULL (there is no
# MAGIC   tone to measure).
# MAGIC - Without the calendar, `LAG` would skip empty weeks and compare a week with one a month earlier, the same mistake
# MAGIC   a `ROWS` frame makes for days.
# MAGIC
# MAGIC **Features. All windows look backward only:**
# MAGIC
# MAGIC | Column | Meaning |
# MAGIC | --- | --- |
# MAGIC | `tone_wmean`, `tone_logw_mean`, `tone_mean` | the week's tone: mentions-weighted, log-weighted, unweighted |
# MAGIC | `tone_wmean_lag1`, `tone_change_1w` | last week's tone, and the change since then |
# MAGIC | `tone_trailing4_wmean` | weighted tone over the previous 4 weeks (ratio of sums), not including this week |
# MAGIC | `tone_drop_vs_trailing4` | `tone_wmean − tone_trailing4_wmean`. **Negative = tone dropped**, the signal in the research question |
# MAGIC | `material_conflict_events`, `conflict_share`, `material_conflict_lag1` | this week's conflict level, last week's level |
# MAGIC | `material_conflict_trailing4_mean/_std`, `n_events_trailing4_mean` | the dyad's own recent baseline, for defining a "sharp rise" |
# MAGIC | `weeks_of_history` | how many earlier weeks the trailing columns could use (fewer than 4 at the start) |
# MAGIC | `label_material_conflict_next_week`, `label_conflict_share_next_week` | **targets, not features** (`LEAD`). NULL in the last week |
# MAGIC
# MAGIC The `label_` columns hold next week's raw values. Member 4 turns them into the "sharp rise" label, so the exact
# MAGIC definition stays with the model. Any scaling must be fitted on the training weeks only. `row_key` is `dyad#yyyyMMdd`
# MAGIC of `week_start`.

# COMMAND ----------

wsum = dc.weekly_sums(dyad_day_df, WEEK_STARTS)
selected = dc.select_dyads(wsum, WEEK_STARTS, SELECTION_FRACTION, MIN_EVENTS_PER_WEEK)
n_selected = selected.count()
sel_weeks = dc.selection_weeks(WEEK_STARTS, SELECTION_FRACTION)
print(f"{n_selected:,} dyads with >= {MIN_EVENTS_PER_WEEK} events/week on average over the first {len(sel_weeks)} "
      f"of {len(WEEK_STARTS)} weeks ({sel_weeks[0]}..{sel_weeks[-1] + dt.timedelta(days=6)})")

dense = dc.dense_weekly(spark, wsum, selected, WEEK_STARTS)
save(dc.weekly_features(spark, dense, WINDOW_PARTITIONS), T_WEEKLY)
weekly_df = spark.table(T_WEEKLY)
display(weekly_df.where(F.col("dyad") == top_dyad).orderBy("week_start"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10 · Checks
# MAGIC
# MAGIC These checks fail the run (rather than print a warning) if any output breaks its contract:
# MAGIC - The weekly table has exactly *dyads × weeks* rows, and every `row_key` is unique (in both output tables).
# MAGIC - There is no tone in a week without events.
# MAGIC - Labels are NULL only in the last week.
# MAGIC - **Conservation:** the selected dyads' events in the weekly table add up to the same total as the dyad-day table
# MAGIC   over the same weeks. Nothing is lost or double-counted by the calendar join.

# COMMAND ----------

last_week = WEEK_STARTS[-1]
chk = weekly_df.agg(
    F.count(F.lit(1)).alias("rows"),
    F.countDistinct("row_key").alias("keys"),
    F.sum(F.when((F.col("n_events") == 0) & F.col("tone_wmean").isNotNull(), 1).otherwise(0)).alias("tone_without_events"),
    F.sum(F.when((F.col("week_start") == F.lit(last_week)) & F.col("label_material_conflict_next_week").isNotNull(), 1)
          .otherwise(0)).alias("label_in_last_week"),
    F.sum(F.when((F.col("week_start") < F.lit(last_week)) & F.col("label_material_conflict_next_week").isNull(), 1)
          .otherwise(0)).alias("missing_labels"),
    F.sum("n_events").alias("weekly_events")).first()
day_events = (dyad_day_df.join(selected.select("dyad"), "dyad")
              .where(F.col("event_date").between(F.lit(WEEK_STARTS[0]), F.lit(last_week + dt.timedelta(days=6))))
              .agg(F.sum("n_events")).first()[0] or 0)
daily_chk = daily_df.agg(F.count(F.lit(1)).alias("rows"), F.countDistinct("row_key").alias("keys")).first()

assert chk["rows"] == n_selected * len(WEEK_STARTS), (chk["rows"], n_selected, len(WEEK_STARTS))
assert chk["keys"] == chk["rows"], "weekly row_key must be unique"
assert chk["tone_without_events"] == 0 and chk["label_in_last_week"] == 0 and chk["missing_labels"] == 0
assert chk["weekly_events"] == day_events, (chk["weekly_events"], day_events)
assert daily_chk["keys"] == daily_chk["rows"], "daily row_key must be unique"
print(f"checks passed: weekly {chk['rows']:,} rows = {n_selected:,} dyads x {len(WEEK_STARTS)} weeks; "
      f"{chk['weekly_events']:,} events conserved; daily {daily_chk['rows']:,} unique row keys")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11 · Run parameters and export
# MAGIC
# MAGIC The parameters of this run go into a one-row table. Notebooks 04 and 05 read the same N and complete days from it,
# MAGIC and the report quotes it.
# MAGIC
# MAGIC Each member has their own Free Edition workspace, so Member 4 can't read this workspace's tables. The weekly feature
# MAGIC table is therefore also written as a single CSV file to the volume, to download from **Catalog → workspace → gdelt →
# MAGIC raw → exports/member3**. In sample mode the daily index is exported too, for Member 5's HBase load on the local
# MAGIC cluster, which holds the same October sample.

# COMMAND ----------

params = {
    "run_ts": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"), "sample_mode": SAMPLE_MODE,
    "events_source": EVENTS_SOURCE, "used_fallback": not use_member2,
    "range_start": RANGE_START, "range_end": RANGE_END, "rows_in_range": rows_in_range,
    "recency_days": RECENCY_DAYS, "recency_days_rule": auto_n,
    "eligible_rows": eligible_total, "kept_rows_in_window": kept_rows, "clean_events": n_clean,
    "first_day": str(FIRST_DAY), "last_day": str(LAST_DAY), "first_full_window_day": str(FIRST_FULL_WINDOW_DAY),
    "first_week": str(WEEK_STARTS[0]), "last_week": str(last_week), "n_weeks": len(WEEK_STARTS),
    "selected_dyads": n_selected, "min_events_per_week": MIN_EVENTS_PER_WEEK, "selection_fraction": SELECTION_FRACTION,
    "shuffle_partitions": SHUFFLE_PARTITIONS, "shuffle_partitions_status": shuffle_status,
    "window_partitions": WINDOW_PARTITIONS,
}
save(spark.createDataFrame(pd.DataFrame([params])), T_PARAMS)

os.makedirs(EXPORT_DIR, exist_ok=True)
weekly_csv = f"{EXPORT_DIR}/dyad_weekly_features{SUFFIX}.csv"
weekly_df.orderBy("dyad", "week_start").toPandas().to_csv(weekly_csv, index=False)
print(f"exported {weekly_csv}")
if SAMPLE_MODE:
    daily_csv = f"{EXPORT_DIR}/dyad_daily_index{SUFFIX}.csv"
    daily_df.orderBy("dyad", "event_date").toPandas().to_csv(daily_csv, index=False)
    print(f"exported {daily_csv}")
display(spark.table(T_PARAMS))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12 · First look
# MAGIC
# MAGIC These tables are descriptive only and don't test the research question; that is the model's job (Member 4).
# MAGIC - The most-reported dyads, with country names from the lookup.
# MAGIC - The most common material-conflict event types, named through the broadcast-joined CAMEO event codes.

# COMMAND ----------

names = F.broadcast(countries_df)
display(events_df.groupBy("dyad", "country_a", "country_b")
        .agg(F.count(F.lit(1)).alias("events"),
             (F.sum(F.col("avg_tone") * F.col("num_mentions")) / F.sum("num_mentions")).alias("tone_wmean"),
             F.count(F.when(F.col("quad_class") == 4, 1)).alias("material_conflict_events"))
        .join(names.select(F.col("code").alias("country_a"), F.col("name").alias("name_a")), "country_a", "left")
        .join(names.select(F.col("code").alias("country_b"), F.col("name").alias("name_b")), "country_b", "left")
        .select("dyad", "name_a", "name_b", "events", "tone_wmean", "material_conflict_events")
        .orderBy(F.desc("events")).limit(20))

display(events_df.where("quad_class = 4").groupBy("event_root_code", "event_root_desc").count()
        .orderBy(F.desc("count")))
