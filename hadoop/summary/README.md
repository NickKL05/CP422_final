# MapReduce summary job and the MR-vs-Spark benchmark (Member 2)

Events per country per day, computed twice over the same raw GDELT files: once as a Hadoop
Streaming MapReduce job (Python mapper, combiner and reducer) and once in PySpark. Both run on the
local cluster, and `bench_mr_vs_spark.sh` times them against each other for Member 5.

## Definition (identical in both engines)

**Input:** raw GDELT 1.0 daily event files (`YYYYMMDD.export.CSV`, 58 tab-separated fields, no
header), as ingested into HDFS at `/gdelt/raw/events/year=*/month=*/*.CSV`.

**Line rules.** A line is skipped if it fails any rule below. It is counted under the **first**
rule it fails, in this order, under the same counter name in both engines:

| Order | Counter | A line is skipped when |
| ---: | --- | --- |
| 1 | `SkippedFieldCount` | it does not split into exactly 58 fields on `\t` (empty trailing fields count) |
| 2 | `SkippedEmptyActor1Country` | `Actor1CountryCode` (field 8, 1-based) is the empty string |
| 3 | `SkippedBadSqldate` | `SQLDATE` (field 2) is not exactly 8 ASCII digits |
| 4 | `SkippedBadNumMentions` | `NumMentions` (field 32) is not 1–9 ASCII digits |
| 5 | `SkippedBadAvgTone` | `AvgTone` (field 35) is not `[+-]` + a decimal number (1–15 integer digits, or `.digits`), with an optional exponent `e[+-]NN` (1–2 digits) |

The other counters are `InputLines` (all lines read) and `ValidEvents` (lines that pass every rule),
so `InputLines = ValidEvents + all Skipped*`.

The number rules are regular expressions, not `float()` or `CAST`, because Python and Spark
disagree on edge cases such as `nan`, `inf`, `1_000` or padded values. The e-notation is needed:
GDELT writes tones very close to zero as, for example, `1.44699562933207e-05`. The digit limits keep
every value finite.

**Key:** (`SQLDATE`, `Actor1CountryCode`). The day is the event date `SQLDATE`, not the file date.
`Actor1CountryCode` matches the dyads used later; `ActionGeo_CountryCode` uses FIPS codes instead.

**Output:** one tab-separated line per key, with no header:

| Column | Meaning |
| --- | --- |
| `SQLDATE` | event date, `YYYYMMDD` |
| `Actor1CountryCode` | CAMEO country code of actor 1 |
| `events` | number of valid events (lines) with this key |
| `sum_mentions` | sum of `NumMentions` |
| `avg_tone` | mean `AvgTone` (sum of AvgTone / events), printed with 6 decimal places |

This job summarises the **raw** files, so it applies no recency rule. Old `SQLDATE`s (up to about 10
years before the file date, see the platform handoff note) appear as their own keys, with small
counts. The cleaning rules (duplicate IDs, same-country pairs, coordinates, recency) belong to the
cleaned Databricks tables, not to this benchmark job.

## Files

| File | What it does |
| --- | --- |
| `summary_mapper.py` | Applies the line rules and emits `SQLDATE\|country  1  NumMentions  AvgTone` per valid event; reports the `GDELT Summary` counters |
| `summary_combiner.py` | Partial sums (count, sum_mentions, sum_tone) per key inside each map task; same format in and out |
| `summary_reducer.py` | Adds up the partial sums and writes the final line, taking the average only at the end |
| `run_summary_mr.sh` | Runs the Streaming job and prints the counters, the wall seconds and a `RESULT` line |
| `summary_spark.py` | The same computation in PySpark (DataFrame API, explicit schema, ANSI mode on) |
| `compare_outputs.py` | Checks two outputs are identical: same keys, exact counts and sums, `avg_tone` within 1e-6 |
| `bench_mr_vs_spark.sh` | Benchmark: N runs of each engine on the cluster, writes the CSV for Member 5 |
| `test_summary_local.sh` | Local checks inside the Docker image, with no cluster |

### Why a combiner

Each daily file has about 100,000 events, but only a few hundred distinct (day, country) keys.
Without a combiner, every event is spilled, sorted, sent over the network and merged by a reducer.
With the combiner, each map task sends at most one record per key it saw. The job log shows this as
`Combine input records` vs `Combine output records`.

The combiner has to be safe to run zero, one or several times, so it only passes on partial
(count, sum, sum) values, which can be added in any grouping. It never passes on an average, because
an average of averages is wrong. The reducer divides at the end. `sum_tone` is passed with Python's
`repr()`, which round-trips a float exactly.

## Running on the cluster (Nick's PC, inside `namenode`)

```bash
# MapReduce: output to HDFS /gdelt/summary/mr_<tag>, log to output/summary/summary_mr_<tag>.log
docker exec namenode bash /opt/cp422/summary/run_summary_mr.sh try1 3
#   env: INPUT=<glob>  OUT=<hdfs dir>  MAP_LIMIT=<max concurrent maps, 0 = none>  LOG_DIR=<dir>

# Spark on YARN: the same input and the same definition
docker exec namenode spark-submit --num-executors 3 --executor-cores 3 \
  --executor-memory 512m --conf spark.executor.memoryOverhead=384m \
  --conf spark.sql.adaptive.coalescePartitions.enabled=false \
  /opt/cp422/summary/summary_spark.py '/gdelt/raw/events/year=*/month=*/*.CSV' /gdelt/summary/spark_try1 \
  --shuffle-partitions 3

# Check they agree
docker exec namenode bash -c 'hdfs dfs -getmerge /gdelt/summary/mr_try1 /tmp/mr.tsv &&
  hdfs dfs -getmerge /gdelt/summary/spark_try1 /tmp/sp.tsv &&
  python3 /opt/cp422/summary/compare_outputs.py /tmp/mr.tsv /tmp/sp.tsv'

# The benchmark (about 3 x (MR + Spark) runs; nothing else should be running on the cluster)
docker exec namenode bash /opt/cp422/summary/bench_mr_vs_spark.sh 3
```

Do one `summary_spark.py` run by hand before the benchmark. These commands have run end to end
on a test cluster with one worker (MR and Spark outputs IDENTICAL), but not yet on Nick's 3-worker
cluster.

Always pass the executor sizes shown above. With only the `spark-defaults.conf` values (768 MB
heap + 384 MB overhead), each executor needs a 1280 MB container (YARN rounds up to multiples of
256 MB). The worker that holds the 1 GB application master has only 1024 MB left, so that worker
can't take an executor. On the one-worker test cluster, the job never got an executor at all
("Initial job has not accepted any resources").

## Benchmark setup

Same machine (the local cluster), same input (the October 2026 sample in HDFS, 31 files), and the
same definition. Before the first timed run, an untimed warm-up reads the whole input once
(`hdfs dfs -cat`), so every timed run of either engine starts with the HDFS blocks in the OS page
cache. Each run then times both engines, MR first on odd runs and Spark first on even runs
(MR, Spark / Spark, MR / MR, Spark), so neither engine always goes first and a slow moment on the
host affects both. Every run is kept; the summary file reports medians over these warm-cache runs.

| Setting | MapReduce | Spark on YARN |
| --- | --- | --- |
| Concurrent tasks | at most 9 maps at once (`mapreduce.job.running.map.limit=9`) | 3 executors × 3 cores = 9 |
| Memory per task container | 512 MB (`mapred-site.xml`) | 512 MB heap + 384 MB overhead per executor (a 1 GB YARN container) |
| Reduce side | 3 reducers | 3 shuffle partitions (`spark.sql.shuffle.partitions`), with adaptive partition coalescing off (`spark.sql.adaptive.coalescePartitions.enabled=false`) |
| Client / driver | `hadoop jar` in `namenode` | `spark-submit` client mode, driver in `namenode` |
| Timed | around `hadoop jar` | around `spark-submit` |

Why coalescing is off: Spark 3.5 has adaptive query execution on by default, and after the map stage
it merges small shuffle partitions, so Spark could run fewer reduce tasks (and write fewer files)
than the 3 MR reducers. In the local test with 2 shuffle partitions and coalescing on, Spark wrote
1 file while MR wrote 2. The benchmark turns coalescing off (the rest of AQE stays on) and records
`aqe_coalesce=false` in the settings column.

Why 3 × 3 and not 3 × 4: each worker gives YARN 4 vcores and 2048 MB. The Spark application master
needs 1 vcore and a 1 GB container, so the worker it lands on has only 3 vcores left. A 4-core
executor would not fit there, and Spark would get only 2 executors. With 3 cores per executor, all
3 fit. MR maps are capped at the same 9.

Both timings are end to end: they include each engine's own start-up (YARN application master,
containers, JVMs). `summary_spark.py` also prints `job_seconds` (read, aggregate and write, with the
session already running) in its log, for reference.

### Output files (`hadoop/output/benchmark/`, which is `/opt/cp422/output/benchmark` in the container)

`mr_vs_spark.csv` has one row per engine per run. This is the file Member 5 uses.

| Column | Meaning |
| --- | --- |
| `timestamp_utc` | when the run finished (ISO 8601, UTC) |
| `engine` | `mapreduce` or `spark` |
| `run` | 1..N |
| `wall_seconds` | wall-clock seconds around `hadoop jar` / `spark-submit`, 2 dp |
| `input_rows` | lines read. MR: `Map input records`; Spark: `InputLines` from `DataFrame.observe` in the same job |
| `valid_events` | lines that passed every rule (`ValidEvents`) |
| `output_rows` | output keys. MR: `Reduce output records`; Spark: an observed count of the written rows |
| `settings` | `;`-separated `name=value` engine settings (see the table above; Spark rows include `aqe_coalesce=false`). Spark rows also have `executors_registered`, the number of executors YARN actually started. If it is below 3, the run was not like-for-like, and the script prints a warning and notes it in the summary file |
| `input` | input glob |

`input_rows`, `valid_events` and `output_rows` should be the same in every row. If they aren't, the
runs didn't process the same data.

**Committing the CSV:** the repo's `.gitignore` has `*.CSV` (for raw GDELT downloads), and with
`core.ignorecase=true` (the default on macOS and Windows) git also ignores `mr_vs_spark.csv`. Until
`.gitignore` gets a negation such as `!hadoop/output/**/*.csv`, add the file explicitly with
`git add -f hadoop/output/benchmark/mr_vs_spark.csv`. Check with
`git check-ignore -v --no-index hadoop/output/benchmark/mr_vs_spark.csv` (no output = not ignored).

Also written: `mr_vs_spark_summary.txt` (settings, median/min/max per engine),
`compare_mr_vs_spark.txt` (the output comparison for the last run) and `logs/` (every run's full log).

## Local test (no cluster)

`test_summary_local.sh` runs inside the team image, on any GDELT 1.0 daily files:

1. a shell pipe: `cat | mapper | sort | combiner | sort | reducer`;
2. the real Streaming job via `run_summary_mr.sh` in Hadoop local mode (`LOCAL_TEST=1`: `-D
   mapreduce.framework.name=local -fs file:///`);
3. `summary_spark.py` with `--master local[2]` (ANSI on, adaptive partition coalescing off as in the
   benchmark, so Spark writes as many files as MR);
4. `compare_outputs.py` on 2 vs 3, and on 1 vs 2;
5. an independent `awk` count of events for the 3 biggest keys, checked against all three outputs;
6. 20 synthetic lines built to fail each skip rule (57 and 59 fields, an empty line, empty country,
   a 7-digit date, `3.0` mentions, `nan` / ` 1.5` / `1e999` tones, plus a valid `1.5e-05`). The
   mapper's and Spark's counters must both equal the expected numbers.

```bash
docker run --rm --entrypoint bash -v "$PWD":/repo \
  -v /path/to/sample/events:/data/raw/events/year=2026/month=10:ro -v /path/to/scratch:/sp \
  cp422/hadoop-stack:1.0 /repo/hadoop/summary/test_summary_local.sh \
  '/data/raw/events/year=*/month=*/*.CSV' /sp/test_outputs/summary
```

Local test output is only a correctness check. It is not a benchmark result and doesn't go in
`hadoop/output/`.
