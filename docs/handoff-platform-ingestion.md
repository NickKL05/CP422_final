# Handoff: platform and ingestion (Member 1, Nick)

Last updated: 2026-11-22. Repo: https://github.com/NickKL05/CP422_final (branch `main`)

<!-- AI citation: drafted with Claude Code (Claude Opus 5.5, Anthropic) as an internal team note.
     This is not report text and must not be copied into the report. -->

The local Hadoop cluster and the Databricks load are both working, and their row counts match. This note covers
what you can build on, how to use it, and the gotchas that affect your parts.

## TL;DR

- **Table:** `workspace.gdelt.events_bronze`, 37,225,966 rows, Nov 2025 – Oct 2026, all 365 daily files, 1.76 GiB Delta.
- **Partition columns are `file_year` and `file_month`, not `year` and `month`.**
- **Every column is a STRING.** Typing and cleaning are Member 2's step.
- **Every notebook must start with the `sample_mode` widget cell and then `%run ./00_config`** (details below).
- **`SQLDATE` goes back about 10 years** in every month. Filter or group by event date carefully (details below).
- Local Hadoop cluster (HDFS, YARN, HBase, Pig, Spark) runs on **Nick's PC**. Book time with him.

## Open decisions: now settled

| Decision | Outcome |
| --- | --- |
| Can Databricks Free Edition download GDELT directly? | **Yes.** The Parquet fallback (`tools/gdelt_to_parquet.py`) exists but isn't needed. |
| Whose machine hosts the local cluster? | **Nick's PC** (12 cores and about 18 GB given to Docker). |
| HBase or MongoDB? | **HBase.** It runs on the cluster (standalone, data on HDFS) and passed a create/put/get test in the HBase shell. No need to switch. |

## Databricks: what exists

| Object | What it is |
| --- | --- |
| `workspace.gdelt.events_bronze` | Raw GDELT 1.0 daily events: 58 STRING columns (official codebook names), plus `source_file`, `ingest_ts`, `file_year`, `file_month` |
| `workspace.gdelt.load_log` | One row per loaded month: files, rows, bytes, download and write seconds |
| `/Volumes/workspace/gdelt/raw` | Landing volume. It's empty after each load, because CSVs are deleted once they're in Delta. |

Row counts per month (file date):

| Month | Rows | Files |
| --- | ---: | ---: |
| 2025-11 | 3,218,988 | 30 |
| 2025-12 | 3,004,321 | 31 |
| 2026-01 | 3,309,934 | 31 |
| 2026-02 | 3,079,386 | 28 |
| 2026-03 | 3,508,491 | 31 |
| 2026-04 | 3,080,608 | 30 |
| 2026-05 | 3,101,213 | 31 |
| 2026-06 | 2,891,095 | 30 |
| 2026-07 | 3,001,613 | 31 |
| 2026-08 | 2,809,109 | 31 |
| 2026-09 | 3,025,268 | 30 |
| 2026-10 | 3,195,940 | 31 |
| **Total** | **37,225,966** | **365** |

October 2026 (the sample month) has 3,195,940 rows on Databricks, exactly the same as the MapReduce ingestion on the local cluster.

### Each of us has our own Free Edition account

The tables above are in **Nick's** workspace. In your own account:

1. **Workspace → Create → Git folder**, and paste the repo URL above.
2. Open `notebooks/01_load_gdelt` and **Run All** with the defaults (`sample_mode = true`, `run_full_load = false`).
   This loads October 2026 into your own `events_bronze`, and you can develop against that.
3. Only run the full year (`run_full_load = true`) if you really need it, because it uses your quota. Finish all full-data runs by **Nov 27**.

To pick up later changes: click the `main` branch name on the Git folder, then **Pull**.

## How to start a notebook

Every top-level notebook needs these as its **first two code cells**. Copy them from `01_load_gdelt`:

```python
# cell 1: must be in YOUR notebook. A widget created inside a %run notebook doesn't show in your parameters bar.
dbutils.widgets.dropdown("sample_mode", "true", ["true", "false"], "SAMPLE_MODE")
```

```python
# cell 2 (a cell on its own)
%run ./00_config
```

After that you have the following variables:

| Variable | Value |
| --- | --- |
| `SAMPLE_MODE` | `True` by default |
| `RANGE_START`, `RANGE_END` | `2026-10-01..2026-10-31` in sample mode, `2025-11-01..2026-10-31` otherwise |
| `EVENTS_BRONZE` | `"workspace.gdelt.events_bronze"` |
| `CATALOG`, `SCHEMA`, `VOLUME_ROOT` | Unity Catalog locations |

`import gdelt_common as gc` gives you `gc.GDELT_V1_COLUMNS`, `gc.raw_string_schema()` and `gc.PART_YEAR` / `gc.PART_MONTH`.

**Use `SAMPLE_MODE` to choose which data you read.** It never triggers a download; only `run_full_load` does.

## Gotchas that affect your work

1. **`file_year` and `file_month` are the file's date (`DATEADDED`), not the event date.** They can't be called
   `year` and `month`, because GDELT already has a `Year` column and Spark column names are case-insensitive. A column
   called `year` would overwrite it, and CREATE TABLE would fail.
2. **`SQLDATE` (when the event happened) goes back about 10 years.** In every month, `min_sqldate` is almost exactly
   10 years before the file date (for example, Nov 2025 files contain events dated 2015-11-04), because articles mention
   past events. `max_sqldate` is always the last day of the file month, so there are no future dates. **Weekly windows
   should group by `SQLDATE` and drop events far older than `DATEADDED`.** To see how many rows that affects:

   ```sql
   SELECT datediff(to_date(DATEADDED,'yyyyMMdd'), to_date(SQLDATE,'yyyyMMdd')) DIV 30 AS months_old,
          COUNT(*) AS events
   FROM workspace.gdelt.events_bronze GROUP BY 1 ORDER BY 1
   ```
3. **Everything is a string**, including `AvgTone`, `GoldsteinScale`, `NumMentions`, `QuadClass` and the lat/long
   columns. Cast them before doing any maths.
4. **The 15-minute update files (GDELT 2.0) are NOT loaded.** They have 61 columns, not 58, so they need their own schema (Member 2) and their own landing folder (Member 5).
5. **Partition pruning works on `file_year` and `file_month` only.** Filtering on `SQLDATE` still scans every partition,
   although Delta file statistics help.

## Local Hadoop cluster

Everything is in `hadoop/`; read `hadoop/README.md` first. It's one Docker image with the role picked by the container command:

| Container | Runs | UI on the host |
| --- | --- | --- |
| `namenode` | NameNode, YARN ResourceManager, JobHistory; client for `hadoop jar`, `pig`, `spark-submit` | http://localhost:9870, http://localhost:8088, http://localhost:19888 |
| `worker1`–`worker3` | DataNode + NodeManager each (2 GB and 4 vcores of YARN each, 12 vcores in total) | none |
| `hbase` | HBase standalone, data on HDFS; Thrift server | http://localhost:16010, Thrift `localhost:9090` |

Settings: block size 32 MB, replication 2, dead-node timeout about 90 s, YARN `DominantResourceCalculator` (vcores are enforced).
Versions: Hadoop 3.3.6, Pig 0.17.0, HBase 2.5.10, Spark 3.5.3, Java 8.

**The cluster runs on Nick's PC.** Message him to book a slot, then send him your script, or sit with him while it runs.

Data in HDFS: the October 2026 sample, at `/gdelt/raw/events/year=2026/month=10/YYYYMMDD.export.CSV`
(31 files, 1.2 GB, 53 blocks). It's tab-separated with no header, in the same 58-column order as `gc.GDELT_V1_COLUMNS`.

Anything under `hadoop/` in the repo appears at `/opt/cp422` inside `namenode` and `hbase`. Anything written to
`/opt/cp422/output` ends up in `hadoop/output/`, which the Databricks notebook displays. That's how the Hadoop parts get into the code deliverable.

### Results so far (captured in `hadoop/output/`)

- **MapReduce ingestion:** 31/31 files, 3,195,940 rows, 48 s, split 10/10/11 files across the 3 workers.
- **Block placement:** 53 blocks × 2 replicas, spread 34/33/33% across the DataNodes.
- **Data locality:** 52 of 53 map tasks data-local, 1 rack-local.
- **Fault tolerance (A&B):** a worker's DataNode was stopped mid-job. The NameNode declared it dead at 95 s, re-replicated all affected blocks by 169 s, and the job finished at 229 s with the correct 3,195,940 rows.

## Notes for each member

**Member 2 (Pig, MR summary, schemas, cleaning)**
- Pig: `docker exec -it namenode pig -x mapreduce /opt/cp422/<your script>.pig`. Pig 0.17 on Hadoop 3 hasn't been tested yet, so try it early.
- Input for cleaning is `events_bronze`, all strings. Your typed schema and cleaning rules produce the next table.
- Candidate cleaning rule: event-date recency (gotcha 2).
- The MR vs Spark benchmark has to run on the same machine and sample, which means Nick's PC.

**Member 3 (dyads, rolling windows, network)**
- Group by `SQLDATE` for weekly windows and apply a recency filter (gotcha 2).
- Cast `AvgTone` and `NumMentions` first (gotcha 3).

**Member 4 (ML)**
- The time-based split should use event dates. Keep in mind that a file from week *t* can contain events from much earlier weeks.

**Member 5 (streaming, HBase, benchmarks)**
- HBase works from the shell: `docker exec -it hbase hbase shell` (create/put/get tested). The Thrift server runs on
  `localhost:9090` for Python clients such as happybase, but **no client has connected through it yet**, so test that early.
- Spark on YARN: `docker exec -it namenode spark-submit --num-executors N --executor-cores C job.py`. The Spark jars are
  pre-uploaded to HDFS, so timings don't include the jar upload. **No Spark job has been run on YARN yet**, so do a small
  test run before the scaling study.
- The scaling study can use up to 12 vcores across the 3 workers. Each worker allows at most 4 cores per executor, so for 8 cores use, for example, 2 executors × 4 cores, or 8 × 1.
- Databricks load timings per month are in `load_log` (`download_seconds`, `write_seconds`) if you want them for benchmarks.

## Dates (from the division-of-work doc)

- **Nov 26:** code freeze for new features; first drafts of report sections due (Members 1–4)
- **Nov 27:** last day for full-data runs; full-data notebook check
- **Nov 28, 12 PM:** all code handed to Nick, after the clean sample-mode Run All and Member 2's AI-citation audit
- **Nov 29, 6 PM:** code submitted

Questions: message Nick in the group chat.
