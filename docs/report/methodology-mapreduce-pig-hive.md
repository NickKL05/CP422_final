<!-- Draft v2 (Member 2), 2026-11-26. [TBD: ...] marks a number from the official runs on the 3-worker cluster
     (hadoop/output/pig, hadoop/output/summary, hadoop/output/benchmark). All other figures come from test runs of this
     code on the 2-day extract (2026-10-01 and 2026-10-02, 228,199 rows), in Hadoop local mode and on a one-worker copy
     of the cluster; their timings are not reported as results. -->

### Methodology – MapReduce, Pig & Hive

#### 3.1.1 Role of the Hadoop components

The local Hadoop cluster (Hadoop 3.3.6, Pig 0.17.0, Spark 3.5.3; one NameNode/ResourceManager and three DataNode/NodeManager workers) holds the October 2026 sample in HDFS: `/gdelt/raw/events/year=2026/month=10/`, 31 daily files, 3,195,940 rows. Both jobs read only the GDELT 1.0 daily files [1]; the GDELT 2.0 15-minute files [2] are handled on Databricks. Two batch jobs run on this data:

| Job | Engine | Code | Output |
| --- | --- | --- | --- |
| Conflict filter with country coordinates | Apache Pig (MapReduce mode) | `hadoop/pig/conflict_filter.pig`, `run_pig.sh` | International conflict events with a centroid for each actor country, plus four summary tables |
| Events per country per day | Hadoop Streaming MapReduce (Python mapper, combiner, reducer), and the same calculation in PySpark | `hadoop/summary/` | One row per (event date, actor-1 country) |

The second job is also the workload for the MapReduce-versus-Spark benchmark (Section 3.1.5). Both jobs write a log and small summary files to `hadoop/output/`, which the Databricks notebook `02_clean_events` displays.

#### 3.1.2 Pig dataflow: conflict filter with a replicated join

##### 3.1.2.1 Dataflow

```
raw daily files (58 cols, all chararray)
  └─ FILTER  QuadClass ∈ {3, 4}  AND  Actor1CountryCode, Actor2CountryCode non-empty and different
       └─ FOREACH  keep 9 fields; cast QuadClass, NumMentions → int, GoldsteinScale, AvgTone → double
            └─ JOIN … BY a1_code LEFT OUTER, centroids BY cameo_code USING 'replicated'
                 └─ JOIN … BY a2_code LEFT OUTER, centroids BY cameo_code USING 'replicated'
                      └─ SPLIT  both centroids found → events/      otherwise → unmatched
```

1. **Load.** The files are loaded with an explicit 58-field schema in codebook order, with every field as `chararray`. Pig therefore never guesses a type, and a malformed number cannot fail the load.
2. **Filter.** QuadClass 3 (verbal conflict) and 4 (material conflict) [3] are kept when both actor countries are present and different, which makes the event an international dyad. The test compares strings, so a non-numeric QuadClass simply does not match.
3. **Project and cast.** Only the nine fields needed later are kept. Only these are cast. Pig turns a failed cast into a null rather than an error.
4. **Two replicated joins.** Each actor's CAMEO country code is joined to `hadoop/lookups/country_centroids.tsv` (188 rows: code, name, latitude, longitude). A fragment-replicate join loads the small relation into memory in every map task and joins it with the large relation on the map side, so the event data is not shuffled for the join [7]. The large relation must come first in the `JOIN` statement, and the replicated relation must fit in memory (Pig's default limit is 1 GB) [7]. The lookup is a few kilobytes. Both joins are `LEFT OUTER`, so events whose country has no centroid are kept long enough to be counted and are then split off.
5. **Store.** The main output is `events/` (13 columns: event ID, `SQLDATE`, both country codes, QuadClass, root event code, Goldstein score, mentions, tone, and the two coordinate pairs). There are four summaries: row counts at each step, events per QuadClass before and after the join, the top 20 directed dyads, and the country codes that had no centroid.

The script applies no recency rule. That rule belongs to the dyad analysis on Databricks, which chooses the window from the data (Dataset Profile, Sections 2.4 and 2.6), so `sqldate` is passed on unchanged.

##### 3.1.2.2 The centroid lookup

GDELT does not publish a coordinate for each CAMEO country code. `hadoop/lookups/build_country_centroids.py` derives one from the events. When an actor's geocode is of type 1 (a whole country), GDELT gives that country's centroid and its FIPS code [1]. The builder counts the (CAMEO code, FIPS code) pairs, and for each CAMEO code it chooses the most frequent FIPS candidate **whose country name matches the CAMEO name**. It does not simply take the most frequent candidate. On the extract, the plain rule maps GEO (Georgia) to Russia, HKG and NMR (North America) to the United States, and MMR (Myanmar) to Malaysia. The name test also excludes regions and cities, which have no matching FIPS country, without a hand-written exclusion list. Two checks follow:

- A centroid must lie within 3,000 km of at least one sub-national geocode in the same FIPS country. This check removes Mauritius, whose GDELT country centroid lies about 8,500 km away in the Northern Mariana Islands.
- The build fails unless ten reference countries lie within 500 km of their known centres. The largest offsets are India (304 km) and the United Kingdom (131 km).

Built from the 2-day extract, the lookup gives a centroid to 188 of the 262 CAMEO codes (including PSE, see Section 3.1.2.3). A rebuild inside the cluster image produced a byte-identical file.

##### 3.1.2.3 Results on the 2-day extract

| Step | Rows |
| --- | ---: |
| Input | 228,199 |
| Conflict events (QuadClass 3 or 4, two different countries) | 9,995 (4.38% of input) |
|   of which QuadClass 3 / QuadClass 4 | 4,830 / 5,165 |
| Joined (both countries have a centroid) | 9,296 (93.01% of conflict events) |
|   of which QuadClass 3 / QuadClass 4 | 4,417 / 4,879 |
| Unmatched (at least one country without a centroid) | 699 |

An independent `awk` computation on the raw files gave the same counts, the same top 20 dyads, the same 15 unmatched codes, and identical values in all 13 columns of the 9,296 joined rows. The FIPS country list has no Palestine entry [5], so PSE (Occupied Palestinian Territory) cannot receive a centroid under the name rule; the builder therefore derives it from GDELT's own country-level geocode for the West Bank (FIPS `WE`), which keeps the 411 Israel–Palestine conflict rows of the extract in the output. In MapReduce mode the script ran as 15 YARN jobs, because each summary adds its own GROUP and ORDER jobs, and its output was identical to the local-mode run, row for row and for every summary table (that comparison was made with the earlier lookup, before the PSE entry was added).

Results on the full October sample on the 3-worker cluster: **[TBD: Pig counts and wall time from hadoop/output/pig/, official 3-worker run]**

#### 3.1.3 MapReduce summary job: events per country per day

##### 3.1.3.1 Definition

The input is the raw GDELT 1.0 daily lines in HDFS. The key is (`SQLDATE`, `Actor1CountryCode`): the event date, not the file date, and the CAMEO country of actor 1, the same coding as the dyads. The `ActionGeo_CountryCode` field uses FIPS codes instead [1]. For each key the job outputs `events` (count), `sum_mentions` (sum of `NumMentions`) and `avg_tone` (mean `AvgTone`).

A line is skipped under the first rule it fails, and each skip is recorded in a Hadoop counter. The rules, in order:

| Order | Counter | Skip condition |
| ---: | --- | --- |
| 1 | `SkippedFieldCount` | not exactly 58 tab-separated fields |
| 2 | `SkippedEmptyActor1Country` | empty `Actor1CountryCode` |
| 3 | `SkippedBadSqldate` | `SQLDATE` is not 8 digits |
| 4 | `SkippedBadNumMentions` | `NumMentions` is not 1–9 digits |
| 5 | `SkippedBadAvgTone` | `AvgTone` is not a decimal number (optional exponent allowed) |

The numeric rules are regular expressions, not `float()` or `CAST`. Python and Spark accept different edge cases (for example `nan`, `inf`, `1_000`, padded values), and a regular expression gives both engines exactly the same definition. The exponent is needed because GDELT writes tones close to zero in e-notation, for example `1.44699562933207e-05` in the 2026-10-01 file.

##### 3.1.3.2 Mapper, combiner and reducer

| Stage | Input | Output |
| --- | --- | --- |
| Mapper (`summary_mapper.py`) | one raw line | `SQLDATE\|country ⇥ 1 ⇥ NumMentions ⇥ AvgTone` |
| Combiner (`summary_combiner.py`) | sorted partial records of one map task | one `key ⇥ count ⇥ sum_mentions ⇥ sum_tone` per key |
| Reducer (`summary_reducer.py`) | all partial records of a key | `SQLDATE ⇥ country ⇥ events ⇥ sum_mentions ⇥ avg_tone` |

**Why a combiner.** A daily file holds about 100,000 events but only a few hundred distinct (day, country) keys. Without a combiner, every event is spilled, sorted, copied across the network and merged by a reducer. With one, each map task sends at most one record per key. Hadoop may run a combiner zero, one or several times on any subset of a key's records [6]. The mapper therefore already emits partial sums, and the combiner only adds them up, which is associative and commutative. The average is taken only in the reducer, because an average of averages would be wrong. Partial tone sums are passed with Python's `repr()`, which round-trips a float exactly.

On the 2-day extract:

| Counter | Value |
| --- | ---: |
| Map input records | 228,199 |
| `SkippedEmptyActor1Country` (other skip counters: 0) | 102,032 |
| Map output records (= valid events) | 126,167 |
| Combine input → output records | 126,167 → 1,228 (99.03% fewer shuffled records, about 103 events per record) |
| Reduce output records (keys) | 765 |

The job ran with 3 reducers on a one-worker copy of the cluster.

##### 3.1.3.3 Equivalent Spark job and correctness check

`summary_spark.py` computes the same definition with the DataFrame API. It reads each line as one string column under an explicit schema, splits it on tabs with limit −1 (so trailing empty fields are kept, as in Python), applies the same regular expressions in the same order, and aggregates by key. It uses `try_element_at` and `try_cast` so that it runs with ANSI mode on [9], and it collects the same counters in the same pass with `DataFrame.observe`. `compare_outputs.py` checks that two outputs have the same keys, exact counts and sums, and `avg_tone` within 10⁻⁶.

On the 2-day extract, Hadoop Streaming on YARN, Spark on YARN, Hadoop local mode, Spark `local[2]` and a shell pipeline (`mapper | sort | combiner | sort | reducer`) all produced the same 765 keys, 126,167 events and 1,419,421 mentions. The comparison reported the outputs as identical, with a maximum `avg_tone` difference of 0, and the sorted MapReduce and Spark outputs have the same MD5 checksum. Twenty synthetic lines, built to fail each skip rule, produced the expected counter values in both engines.

#### 3.1.4 HDFS layout and data locality

The cluster settings that matter for these jobs are a 32 MB block size, replication factor 2, and YARN containers of 512 MB (MapReduce tasks) on workers that offer 2,048 MB and 4 vcores each. The October sample measured during ingestion:

| Measure | Value |
| --- | --- |
| Files, blocks | 31 files, 53 blocks (1.71 blocks per file), every block with 2 replicas |
| Replicas per DataNode | 36 / 35 / 35 (34% / 33% / 33%) |
| Map tasks of a row-count job over the same blocks (one map per block) | 53 launched: 52 data-local, 1 rack-local |
| Rows counted | 3,195,940, the same as the ingestion manifest and the Databricks table |
| Fault tolerance | One DataNode stopped during a job: declared dead after 95 s, all affected blocks re-replicated by 169 s, job finished at 229 s with the correct 3,195,940 rows |

The Pig filter and the summary job read the same blocks through the standard input format, so the scheduler places their map tasks on the nodes that hold the blocks. In the one-worker test, all 4 map tasks of the summary job were data-local, which is trivially true with a single DataNode. Locality on the 3-worker cluster: **[TBD: Data-local / Rack-local map tasks from the summary MR log, official 3-worker run]**. The replicated join adds no shuffle for the join itself. The summary job's shuffle is reduced by the combiner. Network traffic in both jobs is therefore limited to the small per-key records.

#### 3.1.5 MapReduce versus Spark benchmark design

**Question.** For the same aggregation on the same data and hardware, how does end-to-end time on Hadoop MapReduce compare with Spark on YARN?

**Controls.** `hadoop/summary/bench_mr_vs_spark.sh`:

| Setting | MapReduce | Spark on YARN |
| --- | --- | --- |
| Machine and data | Same 3-worker cluster, same HDFS input (31 October files, 3,195,940 rows) | same |
| Definition | `summary_mapper/combiner/reducer.py` | `summary_spark.py` (same rules, outputs checked identical) |
| Concurrent tasks | at most 9 maps (`mapreduce.job.running.map.limit=9`) | 3 executors × 3 cores = 9 |
| Memory | 512 MB per task container | 512 MB heap + 384 MB overhead per executor (a 1 GB container) |
| Reduce side | 3 reducers | 3 shuffle partitions |
| Timed interval | around `hadoop jar` | around `spark-submit` (client mode) |

The 3 × 3 executor layout follows from the container sizes. Each worker offers 4 vcores and 2,048 MB, and the Spark application master takes 1 vcore and 1 GB on one worker. A 4-core executor would therefore not fit on that worker, and Spark would get only 2 executors. Executor memory is set explicitly. With the cluster's default executor size (768 MB + 384 MB, rounded up by YARN to a 1,280 MB container), no executor fits beside the application master on its worker. During testing, a job with the default sizes never received an executor.

**Procedure.** Three runs per engine, alternating MapReduce and Spark so that a slow period on the host affects both. All runs are kept, including the first (cold page cache), and medians are reported. For every run the script records input rows, valid events and output rows, which must be the same in all runs. It also records `executors_registered` for Spark, and prints a warning if fewer than 3 executors started, since such a run is not a like-for-like comparison. The output of the last pair of runs is compared with `compare_outputs.py`. The results go to `hadoop/output/benchmark/mr_vs_spark.csv`, which is the input to the Performance Analysis section.

**Measured quantity.** Wall-clock time including each engine's start-up (YARN application master, containers, JVMs), because that is the cost a user pays per job. Spark also reports `job_seconds` (read, aggregate and write with the session already running), as a secondary figure.

| Engine | Median wall s | Min | Max | Runs |
| --- | ---: | ---: | ---: | ---: |
| MapReduce | [TBD] | [TBD] | [TBD] | 3 |
| Spark on YARN | [TBD] | [TBD] | [TBD] | 3 |

**[TBD: benchmark results from hadoop/output/benchmark/mr_vs_spark_summary.txt, official 3-worker run]**

#### 3.1.6 Hive: Spark SQL tables on Databricks

The cluster image contains Hadoop, Pig, HBase and Spark, but not Hive, and Hive was not added after the code freeze. Hive's functions in a Hadoop stack are a metastore of table definitions, SQL over files in distributed storage, and partitioned tables that queries can prune [10]. Spark SQL on Databricks provides each of these:

| Hive concept | Equivalent in this project |
| --- | --- |
| Metastore (database, table, schema) | Unity Catalog: `workspace.gdelt.*` |
| External or managed table over HDFS files | Managed Delta tables `events_bronze`, `events_clean`, `clean_audit` |
| `PARTITIONED BY` with partition pruning | `PARTITIONED BY (file_year, file_month)`; filters on these columns prune partitions |
| HiveQL `CREATE TABLE`, `INSERT OVERWRITE … PARTITION` | Spark SQL `CREATE TABLE IF NOT EXISTS` and Delta `replaceWhere` on the processed months |
| Schema on read | Explicit `STRING` schema at load (bronze), then a typed schema in `to_typed()` |
| HiveQL queries compiled to MapReduce or Tez jobs | Spark SQL queries executed by Spark on serverless compute |

Delta adds ACID writes, so a re-run of a month replaces that month atomically, and the cleaning notebook was checked to be idempotent on re-run. The HDFS-side work that Hive would also cover, batch transformation and aggregation over raw files, is done by the Pig and MapReduce jobs in Sections 3.1.2 and 3.1.3.

#### 3.1.7 Limitations

- **GDELT measures coverage, not events.** An event row is a machine-coded report extracted from news articles. Its count depends on how much the media write about a country on a given day, and on the number of sources GDELT monitors, which changes over time [4]. A rise in "events per country per day" can therefore reflect more coverage rather than more conflict. Coverage fields such as `NumMentions` show the same effect. GDELT publishes daily total-event counts for normalisation [8]. Analyses in later sections should use shares or normalised counts rather than raw counts. In addition, `AvgTone` is the tone of the articles, not of the event, so tone and coverage are measured from the same text.
- **Automated coding error.** Actor and event codes come from automated extraction. Country codes are often missing: on the 2-day extract, `Actor1CountryCode` is empty for 44.71% of rows and at least one actor country for 76.39%. Locations can also be wrong; the centroid builder found Myanmar actors geocoded to Malaysia more often than to Myanmar.
- **Lookup coverage.** The centroid lookup was built from two days of data, so 74 of 262 CAMEO codes have no centroid, including regions and several real countries without country-level geocodes in those days (for example BIH, ROM and SVN). Conflict events involving these codes are excluded from the Pig `events/` output, though they are counted in the unmatched summary. Rebuilding the lookup from more days would reduce this.
- **Raw versus cleaned definitions.** The MapReduce summary and the Pig filter run on raw HDFS files and apply no recency or de-duplication rule. Their counts therefore differ from those derived from `events_clean` on Databricks. This is deliberate for the benchmark, which needs a fixed raw input for both engines.
- **First-fail audit.** Each dropped row is counted under the first rule it fails, so the audit shows how many rows each rule removes *in this order*, not how many rows violate each rule. For example, a row whose actors are a region and nothing at all is counted under `missing_country`, not `region_code`.
- **Benchmark scope.** The benchmark runs on Docker containers on one physical host, on about 1.2 GB of data. It compares engine overheads at small scale and is not evidence of behaviour on a multi-machine cluster. Pig in MapReduce mode was not benchmarked; its 15 sequential jobs are dominated by per-job start-up at this data size.

#### Sources cited in this section

_Numbered for this section only; Member 5 merges them into the report's References._

[1] GDELT Project, *GDELT 1.0 Event Database: Data Format Codebook*. http://data.gdeltproject.org/documentation/GDELT-Data_Format_Codebook.pdf

[2] GDELT Project, *GDELT 2.0 Event Database Codebook* (V2.0). http://data.gdeltproject.org/documentation/GDELT-Event_Codebook-V2.0.pdf

[3] P. A. Schrodt, *CAMEO: Conflict and Mediation Event Observations, Event and Actor Codebook*, version 1.1b3, 2012. http://data.gdeltproject.org/documentation/CAMEO.Manual.1.1b3.pdf

[4] K. Leetaru and P. A. Schrodt, "GDELT: Global Data on Events, Location and Tone, 1979–2012," *ISA Annual Convention*, 2013.

[5] GDELT Project, lookup tables: https://www.gdeltproject.org/data/lookups/CAMEO.country.txt, https://www.gdeltproject.org/data/lookups/FIPS.country.txt

[6] Apache Hadoop 3.3.6, "Hadoop Streaming." https://hadoop.apache.org/docs/r3.3.6/hadoop-streaming/HadoopStreaming.html

[7] Apache Pig 0.17.0, "Performance and Efficiency: Replicated Joins." https://pig.apache.org/docs/r0.17.0/perf.html

[8] GDELT Project, daily event totals for normalisation. http://data.gdeltproject.org/normfiles/daily.csv

[9] Apache Spark, "ANSI Compliance," Spark SQL reference. https://spark.apache.org/docs/latest/sql-ref-ansi-compliance.html

[10] Apache Hive, "Hive Documentation." https://hive.apache.org/
