# Handoff: typed schema, cleaning, Pig and MapReduce (Member 2)

Last updated: 2026-11-26. Builds on `docs/handoff-platform-ingestion.md` (Member 1, Nick) and matches
Member 3's `member3` branch (`notebooks/dyad_common.py`, `lookups/`).

This note says what Member 2's part produces, how Members 3, 4 and 5 can use it, and what is still
waiting for the official runs. **Every number below comes from local test runs on the 2-day sample
(2026-10-01 and 2026-10-02, 228,199 rows) inside the team image on a laptop. They are not official
results.** The official runs are the `02_clean_events` notebook on Databricks and the cluster jobs on
Nick's PC.

## TL;DR

- **Table:** `workspace.gdelt.events_clean` (`EVENTS_CLEAN` in `00_config`), written by `notebooks/02_clean_events.py`.
  Delta, partitioned by **`file_year`, `file_month`** (the file date = `DATEADDED`, the same as bronze).
- **Same column names as bronze** (the official GDELT names), typed. Member 3's notebooks read it with no code change.
- **Same rules as Member 3's fallback**, in the same order, with the same `lookups/` files: a dyad table of state pairs.
  On the 2-day sample 36,346 of 228,199 rows (15.93%) survive. The big drop is `missing_country` (76.39%):
  most GDELT events have no country for one of the two actors.
- **No recency rule in the table.** Old events (`SQLDATE` up to ~10 years before `DATEADDED`) are still there.
  Filter `0 <= datediff(DATEADDED, SQLDATE) <= N` yourself; Member 3 chooses N from the data.
- **Audit:** `workspace.gdelt.clean_audit`, rows affected by each rule per file month (the A&B data-quality audit).
- **Codes stay STRING** (`EventCode` `"010"`, not 10). `GLOBALEVENTID` is a STRING too. Dates are DATE.
- Run the notebook after `01_load_gdelt`. It replaces only the months it processes (sample mode: October 2026).

## `events_clean`

| Columns | Type |
| --- | --- |
| The 58 GDELT 1.0 columns, official names and order (`gc.GDELT_V1_COLUMNS`) | `gc.TYPED_COLUMN_TYPES`: `SQLDATE`, `DATEADDED` DATE; `MonthYear`, `Year`, `IsRootEvent`, `QuadClass`, `NumMentions`, `NumSources`, `NumArticles`, `*Geo_Type` INT; `FractionDate`, `GoldsteinScale`, `AvgTone`, `*Geo_Lat`, `*Geo_Long` DOUBLE; everything else STRING, including `GLOBALEVENTID` and the event codes |
| `source_file` | STRING (e.g. `20261001.export.CSV`) |
| `ingest_ts` | TIMESTAMP (from bronze) |
| `file_year`, `file_month` | INT, partition columns |

These are the types `dyad_common.typed_events()` produces. Strings are trimmed with `''` turned into NULL, and
`GLOBALEVENTID`, the two actor country codes and the three event codes are also upper-cased, as Member 3 does.
Every cast is a `try_cast`, so a bad value becomes NULL and never fails Run All under ANSI mode.

Guarantees (checked by the notebook's sanity cell on every run): one row per `GLOBALEVENTID`; both actors are
CAMEO states and different after the remap; `AvgTone`, `QuadClass` present and `NumMentions >= 1`; every
coordinate pair is valid or both NULL. **The actor codes are the original ones** (`WSB`, not `PSE`): apply the
remap in `lookups/cameo_code_remap.txt` when you group by state. **Coordinates can be NULL** (54 of the 36,346 clean
sample rows have no ActionGeo location), so filter them out before mapping.

Always filter on the partition columns, so Delta reads only those months:
`spark.table("workspace.gdelt.events_clean").where("file_year = 2026 AND file_month = 10")`.
For weekly or daily series, group by `SQLDATE` (the event date).

## Cleaning rules

The same rules, order and names as `dyad_common.flag_events` (`REASONS`). Each row is tagged with the **first**
rule it fails, so the audit adds up: `rows_in = rows_clean + sum(rows dropped)`.

| # | Rule | Drops a row when | 2-day sample |
| --- | --- | --- | ---: |
| 1 | `missing_event_id` | no `GLOBALEVENTID` | 0 |
| 2 | `duplicate_event_id` | a later copy of an ID: the earliest `DATEADDED` is kept, then most `NumMentions`, then smallest `source_file`, `SOURCEURL`, row hash | 0 |
| 3 | `invalid_date` | `SQLDATE` or `DATEADDED` missing or not a real date | 0 |
| 4 | `missing_country` | `Actor1CountryCode` or `Actor2CountryCode` missing | 174,323 (76.391%) |
| 5 | `region_code` | either actor is one of the 25 regions in `lookups/cameo_nonstate_codes.txt` | 3,761 (1.648%) |
| 6 | `unknown_code` | an actor code is not in `lookups/CAMEO.country.txt` | 95 (0.042%) |
| 7 | `same_country` | both actors are the same state after the remap `WSB`, `GZS` → `PSE`, `BAG` → `IRQ` | 13,674 (5.992%) |
| 8 | `invalid_measure` | `AvgTone` or `QuadClass` missing, or `NumMentions` missing or below 1 | 0 |
| – | `invalid_coordinates` | **not a drop**: a lat/long pair that doesn't parse, is out of range, is exactly (0, 0) or is half empty becomes NULL; the row is kept | 2 rows (0.001%) |

Not applied, on purpose: a recency rule. Member 3 picks N from the lag distribution of these rows, so dropping old
events here would change their N.

## Contract with Member 3

`tests/test_clean_member3_compat.py` runs Member 3's own `dyad_common.py` against this cleaning. On the 2-day sample:
the two rule sets agree on all 38 edge-case rows; the per-rule counts are identical; Member 3's rules mark all 36,346
rows of the clean table `eligible`; the clean IDs equal the IDs their fallback keeps; and `eligible_events` gives the
same 35,450 rows from either source. Section 8 of `02_clean_events` repeats the main checks on Databricks whenever
`dyad_common.py` is in the checkout, and prints `CONTRACT CHECK PASSED` or a warning.

`lookups/` and the `EVENTS_CLEAN` line in `00_config.py` are byte-identical to the `member3` branch, so merging the
two branches doesn't conflict on them.

## `clean_audit`

Columns: `file_year`, `file_month`, `rule_order`, `rule`, `action` (`drop` or `set_null`), `rows_in`, `rows_clean`,
`rows_affected`, `pct` (percent of `rows_in`), `audited_at`. One row per (month, rule), 9 rows per month.
Per year: `gc.audit_by_year(spark.table("workspace.gdelt.clean_audit"))`.

## CAMEO lookups

The CAMEO lookups live in the repo's `lookups/` folder (Member 3's README there has the sources and checksums).
`gc.load_cameo_lookups()` returns them as Python dicts and sets (the same classification as
`dyad_common.load_lookups()`). For a DataFrame, `gc.read_lookup_tsv` reads a file with plain Python (serverless
compute can't `spark.read` workspace files):

```python
from pyspark.sql import functions as F
import gdelt_common as gc

lk = gc.load_cameo_lookups()
codes = gc.read_lookup_tsv(spark, f"{lk['dir']}/CAMEO.eventcodes.txt")   # CAMEOEVENTCODE, EVENTDESCRIPTION
events = spark.table("workspace.gdelt.events_clean").where("file_year = 2026 AND file_month = 10")
labelled = events.join(F.broadcast(codes), events.EventCode == codes.CAMEOEVENTCODE, "left")

# Pig's country centroids (hadoop/lookups/, no header):
centroids = gc.read_lookup_tsv(spark, f"{gc.repo_path('hadoop', 'lookups')}/country_centroids.tsv",
                               columns=["cameo_code", "country_name", "lat", "lon"])
```

Section 7 of `02_clean_events` runs the event-code join. Tested locally (`tests/test_clean_local.py` part (d), and
the notebook test); not run on Databricks yet.

## Member 5: GDELT 2.0 15-minute files (streaming)

GDELT 2.0 export files have 61 columns (`gc.GDELT_V2_COLUMNS`): the 58 of 1.0 plus one ADM2 code per
geography, and `DATEADDED` is `YYYYMMDDHHMMSS`. `gc.v2_to_shared` selects the 58 shared columns **by
name** and adds `dateadded_ts` (the 15-minute slot), `source_file`, `ingest_ts`, `file_year`,
`file_month`, so `gc.to_typed` and the cleaning rules work unchanged:

```python
lk = gc.load_cameo_lookups()
raw = spark.readStream.options(**gc.V2_CSV_OPTIONS).schema(gc.v2_raw_schema()).csv(folder)
shared = gc.v2_to_shared(raw)                         # bronze shape, still a streaming DataFrame

def clean_batch(batch_df, batch_id):                  # each micro-batch is a normal DataFrame
    gc.clean_rows(gc.tag_drop_rule(gc.to_typed(batch_df), lk)).write.mode("append").saveAsTable(target_table)

(shared.writeStream.foreachBatch(clean_batch).option("checkpointLocation", checkpoint)
       .trigger(availableNow=True).start())
```

The cleaning rules must run inside `foreachBatch`: the duplicate rule uses a window over
`GLOBALEVENTID`, and Spark rejects non-time windows on a streaming DataFrame
(`gc.tag_drop_rule(gc.to_typed(shared), lk)` directly on the stream fails). Duplicates are then removed
within each micro-batch, not across batches. `tests/test_clean_local.py` part (c) runs the reader
lines with `readStream` (trigger `availableNow`) on two 15-minute files. The `foreachBatch` pattern
above was also run locally on those two files (564 clean rows, the same as the batch reader gives);
`target_table` and `checkpoint` are yours to choose.

## Member 5: MR-vs-Spark benchmark timings

`hadoop/summary/bench_mr_vs_spark.sh` (run on Nick's cluster, inside `namenode`) writes
`hadoop/output/benchmark/mr_vs_spark.csv`: one row per engine per run, with columns `timestamp_utc,
engine, run, wall_seconds, input_rows, valid_events, output_rows, settings, input`
(details in `hadoop/summary/README.md`). Same input (October 2026 in HDFS), same definition
(events per `Actor1CountryCode` per `SQLDATE`), 9 parallel tasks and 3 reduce tasks for both engines,
an untimed warm-up read first, and alternating order. **Not run yet on the 3-worker cluster**, so there
are no official timings yet.

**Git note:** `.gitignore` has `*.CSV`, which on macOS and Windows (`core.ignorecase=true`) also matches
`.csv`. A `!hadoop/output/benchmark/*.csv` line keeps `mr_vs_spark.csv` committable.

## Hadoop jobs (Nick's cluster)

| Job | Command (inside `namenode`) | Output |
| --- | --- | --- |
| Pig conflict filter | `bash /opt/cp422/pig/run_pig.sh` | HDFS `/gdelt/pig/conflict`; log, summary and small TSVs in `hadoop/output/pig/` |
| MapReduce summary | `bash /opt/cp422/summary/run_summary_mr.sh <tag> 3` | HDFS `/gdelt/summary/mr_<tag>`; log in `hadoop/output/summary/` |
| Benchmark | `bash /opt/cp422/summary/bench_mr_vs_spark.sh 3` | `hadoop/output/benchmark/` |

Before the official Pig run, rebuild `hadoop/lookups/country_centroids.tsv` from the full October month
(command in `hadoop/lookups/README.md`). The committed file comes from the 2-day sample only, so a few
real countries (for example BIH, ROM, SVN) have no centroid yet. PSE (Palestine) has one through a
documented override, so the Israel-Palestine dyads are kept.

## Local tests (no cluster, Docker only)

| Test | What it covers |
| --- | --- |
| `tests/test_clean_local.py` | `gdelt_common` typing and cleaning functions, the GDELT 2.0 reader (batch and stream), the lookup reader |
| `tests/test_clean_member3_compat.py` | Member 3's `dyad_common.py` against this cleaning: same rule per row, same counts, same downstream events (needs `dyad_common.py`) |
| `tests/test_clean_notebook_local.py` | the `02_clean_events` notebook file run cell by cell twice (Delta, ANSI on), in sample or full mode, including the Member 3 contract check when `dyad_common.py` is importable |
| `hadoop/summary/test_summary_local.sh` | MR shell pipe, Hadoop Streaming in local mode, Spark local, output comparison, skip-rule edge cases |
| `hadoop/pig/run_pig.sh local` + the `awk` checks in `hadoop/pig/README.md` | the Pig script in local mode |

The run commands are in each file's header.
