<!-- Draft v1 (Member 3), 2026-11-26. [TBD: ...] marks a number that comes from the full-data Databricks run.
     Sources for each number: m3_run_params_full, m3_cleaning_funnel_full, m3_lag_distribution_full,
     actor_centrality_full, m3_skew_results_full, and the printed output of notebooks 03–05. -->

### Methodology – Spark

The Spark stage turns the raw event stream into dyad-level sentiment and conflict measures. It runs on Databricks Free
Edition (serverless compute) over Delta tables in Unity Catalog, in three notebooks:

| Notebook | Purpose |
| --- | --- |
| `03_dyad_tone_index` | cleaning, dyads, the rolling tone index and the weekly features |
| `04_actor_network` | the country-to-country network and its centrality rankings |
| `05_skew_salting_ab` | the data-skew experiment |

A flag (`SAMPLE_MODE`) switches every notebook between the October 2026 sample and the full year. The two modes write
to separately named tables, so a sample run can never overwrite full-data results. The transformation logic lives in
one Python module (`dyad_common.py`). The notebooks call it, and a local test suite runs the same functions on
hand-constructed data whose correct results are known (Section 3.2.9).

#### 3.2.1 Input and typing

The input is the events table produced by the cleaning stage (Section [TBD: MapReduce, Pig & Hive section]). If that
table is not present in a workspace, the notebook falls back to the raw table (`events_bronze`, all 58 GDELT columns
stored as strings) and applies the same rules itself. It prints which source was used, so results from the two paths
cannot be confused. The full-data run used [TBD: events source].

The raw table is partitioned by the year and month of the file date (`DATEADDED`). Every read filters on these
partition columns, so Delta skips the files of other months using its transaction log alone. The physical plan
confirms the pruning: [TBD: PartitionFilters line from the plan].

Only the 13 columns the analysis needs are read and typed:
- **Numbers** use `try_cast`. Serverless Spark runs in ANSI mode, where an ordinary cast of one malformed value raises
  an error and aborts the job. `try_cast` returns NULL instead, and the row is then removed by a validity rule and
  counted.
- **Dates** use the same principle: a check for an eight-digit value followed by `try_to_timestamp`.
- **CAMEO event codes stay strings.** `010` and `10` are different codes, and numeric typing would destroy the leading
  zeros.

#### 3.2.2 Cleaning and dyad construction

The cleaning and dyad rules are applied as a single classification. Each row receives the first rule it fails, so the
removed rows add up exactly to the input and one aggregation reports the effect of every rule (Table [TBD]).

| Rule | Rows removed (full run) |
| --- | ---: |
| Missing event identifier | [TBD] |
| Duplicate event identifier (earliest report kept) | [TBD] |
| Invalid event or file date | [TBD] |
| Either actor without a country code | [TBD] |
| Either actor coded as a region rather than a state | [TBD] |
| Country code not in the CAMEO list | [TBD] |
| Both actors in the same state (domestic event) | [TBD] |
| Missing tone, quad class or mentions | [TBD] |
| **Eligible dyadic events** | [TBD] |

**Lookups.** Country names, the list of states and the CAMEO event descriptions come from GDELT's published lookup
files, which are kept in the repository with their checksums. Each lookup holds a few hundred rows. They are joined
to the event stream as **broadcast joins**: every task receives a full copy of the lookup and joins its slice of
events locally, without shuffling the event stream. The joins are hinted explicitly, and the physical plan shows
`BroadcastHashJoin` operators.

**Code mapping.** Before the rules are applied, the sub-state codes for the West Bank and Gaza Strip are mapped to
Palestine and Baghdad to Iraq. This rule is shared with the cleaning stage. It rewrote [TBD: remapped actor values]
actor values in the full run.

**Dyads.** Each eligible event is assigned to an unordered dyad by sorting its two state codes (key `CHN_USA` for both
USA→CHN and CHN→USA events). The original direction is kept for the network analysis.

#### 3.2.3 Recency filter and complete periods

GDELT files contain many references to past events. The oldest event date in every month of data is about ten years
before the file date. An event row is therefore kept only if 0 ≤ `DATEADDED` − `SQLDATE` ≤ *N*.

**Choosing *N*.** *N* is taken from the observed distribution of reporting lags. It is the smallest value between 1
and 14 for which extending the window by one more day would add less than 0.5 % of the eligible rows. This rule
selected *N* = [TBD: N] days. The filter keeps [TBD: kept share]% of eligible rows; the rest are mostly references to
events more than [TBD] days old (Figure [TBD: lag distribution chart from m3_lag_distribution_full]).

**Complete periods.** Because events are dated by `SQLDATE` but loaded by file date, the first and last days of the
range are incomplete. Day *d* is complete only if:
- *d* is not earlier than the first file date, and
- *d* + *N* is not later than the last file date.

Only complete days are used, and only Monday-to-Sunday weeks lying entirely within them. That gives [TBD: n_weeks]
weeks from [TBD: first_week] to [TBD: last_week], and [TBD: clean_events] events in [TBD: number of dyads] dyads.

#### 3.2.4 Two-step aggregation and the rolling tone index

**Step 1: dyad-day sums.** Events are first aggregated into one row per dyad per day. The row holds sums, not
averages: event count, Σ NumMentions, Σ AvgTone·NumMentions, counts per quad class, and material-conflict counts.
Sums can be re-aggregated over any window and divided at the end. The tone of any window *W* is then an exact
mentions-weighted mean over all its events:

$$\text{tone}(d, W) = \frac{\sum_{e \in W} \text{AvgTone}_e \cdot \text{NumMentions}_e}{\sum_{e \in W} \text{NumMentions}_e}$$

This weights a day with one report and a day with a thousand reports by their actual volume. An average of daily
averages would weight them equally. Because `NumMentions` is heavy-tailed, a log-weighted variant (weight
log(1 + NumMentions)) and the unweighted mean are also computed, so the model can test how sensitive results are to
the weighting.

**Step 2: rolling window.** The rolling index is computed with a Spark SQL window function:

```sql
SUM(sum_tone_x_mentions) OVER w / NULLIF(SUM(sum_mentions) OVER w, 0)
WINDOW w AS (PARTITION BY dyad ORDER BY day_num RANGE BETWEEN 6 PRECEDING AND CURRENT ROW)
```

The frame is a `RANGE` frame over the day number, so it always covers the seven *calendar* days ending on the current
day, however many of them had events. A `ROWS` frame would cover the last seven *active* days, which for a quiet
dyad can span several weeks. The local tests check this on a dyad with a six-day gap.

The result is a table with one row per dyad per active day, keyed `dyad#yyyyMMdd`. It feeds the HBase serving layer
(Section [TBD: HBase section]) and has [TBD: daily rows] rows in the full run.

#### 3.2.5 Weekly feature table and leakage control

Weekly sums are built from the same dyad-day table and placed on a **dense calendar**: every selected dyad × every
complete week, with zero counts for weeks without events. Without the dense calendar, the `LAG` function would compare
a week with the previous *active* week rather than the previous calendar week.

**Features.** All are computed with backward-looking window functions only:
- this week's weighted, log-weighted and unweighted tone;
- last week's tone and the one-week change;
- the weighted tone of the previous four weeks, and this week's deviation from it (`tone_drop_vs_trailing4`, the
  signal in the research question);
- counts per quad class, the conflict share, and the trailing four-week mean and standard deviation of material
  conflict.

**Targets.** Next week's material-conflict count and share are added with `LEAD` and named with a `label_` prefix,
so they cannot be mistaken for features.

**Leakage controls:**
1. no window uses future rows except the labelled targets;
2. dyads are selected using only the first half of the weeks, so the test period does not influence which dyads are
   studied;
3. no scaling is fitted in this stage;
4. network centrality, which uses the whole period, is not used as a feature.

The table has [TBD: selected_dyads] dyads × [TBD: n_weeks] weeks = [TBD: rows] rows.

#### 3.2.6 Partition tuning

Two choices control parallelism in this stage.

**Shuffle partitions.** Spark's default of 200 shuffle partitions suits shuffles of several gigabytes, but would
create many near-empty tasks here. The number is instead derived from the input size:
- one partition per 500,000 input rows (about 50 MB of the narrow typed rows, inside the usual 16–128 MB range);
- rounded up to a multiple of eight;
- bounded between 8 and 200.

That gives 8 partitions for the sample month and [TBD: shuffle_partitions; expected 80] for the full year. On
serverless compute the setting [TBD: was applied / was blocked by the platform]. Adaptive query execution then merges
any remaining small partitions at run time.

**Repartitioning by dyad before the window functions.** A window partitioned by dyad needs all rows of a dyad in the
same task. The dyad-day table is explicitly hash-partitioned by dyad, into a partition count sized from its own row
count ([TBD: window_partitions]). Spark reuses that partitioning for the window, so the physical plan contains exactly
one `hashpartitioning(dyad)` exchange in front of the `Window` operator and no second shuffle. This is verified in
the local test suite, and the plan is printed in the notebook.

The resulting partitions held [TBD: max] rows at most against a median of [TBD: median] (max/median [TBD]). The
largest dyad sets a floor on the largest partition, because one dyad is never split.

#### 3.2.7 Actor network

A directed graph is built with one node per state. Each edge goes from the initiating state (Actor 1) to the target
(Actor 2), weighted by the number of events. There are two graphs: all events, and material conflict only. The edge
list is aggregated in Spark SQL from the clean events ([TBD: edges] edges among [TBD: states] states for all events).

For each state the following are computed:
- **degree:** the number of distinct partners (in, out, total);
- **strength:** the number of events (in, out, total);
- **normalised strength,** which corrects for media prominence, in three forms:
  - the state's share of all graph activity;
  - events per partner;
  - for the conflict graph, the share of the state's own events that are material conflict;
- **weighted PageRank** with damping 0.85.

Rankings use `RANK()` window functions within each graph.

**PageRank without GraphFrames.** The GraphFrames library requires a JAR, which Free Edition cannot install, so
PageRank is implemented directly. The graph is small (at most 233 nodes and a few thousand edges), so the power
iteration runs on the driver over the edge list that Spark aggregated. Running it as distributed Spark jobs would
spend more time on job scheduling than on computation. The same update is also implemented as Spark DataFrame
operations (one join and one aggregation per step). Its result after three steps agrees with the driver
implementation to within [TBD: max difference; < 1e-9 asserted], which shows the distributed formulation is
equivalent. Iteration converged after [TBD] steps.

The most central states by PageRank in the all-events graph were [TBD: top five]. In the conflict graph they were
[TBD: top five]. Ranking by conflict share, among states with at least two events per day, gives [TBD: top five].
Because centrality is computed over the whole year, it is descriptive and is not used as a model feature.

#### 3.2.8 Above and beyond: data skew and salting

**Problem.** GDELT is dominated by a few countries: [TBD: top key] alone is the Actor-1 country of [TBD: share]% of
events with a country code. In any shuffle keyed by country, all of that key's rows go to a single task, and the
stage waits for it. With 64 partitions, a balanced shuffle would give each partition 1.6 % of the rows.

**Remedy.** Salting splits each hot key into several sub-keys:
- A key is hot if it holds more than one partition's fair share of rows (1/64).
- It receives ⌈share × 64⌉ salt buckets ([TBD: hot keys and their buckets]).
- Each row's salt is a hash of its event identifier modulo the key's bucket count. A hash rather than a random number
  keeps the assignment reproducible when a task is retried.
- **Aggregations** become two-stage: aggregate per (key, salt), then combine the few partial rows per key.
- **Joins** copy the small side once per salt bucket of its key, and join on (key, salt).

**Experimental design on serverless compute.** Free Edition blocks most Spark configuration changes, and adaptive query
execution (AQE) cannot be relied on to be off. The experiment therefore avoids configuration entirely:
- every shuffle uses an explicit `repartition(64, …)`, which AQE does not merge;
- a `shuffle_merge` hint forces a sort-merge join where Spark would otherwise broadcast the small table;
- partition sizes are measured with `spark_partition_id()`, because neither the RDD API nor the Spark UI is
  available;
- timings run each version once as a warm-up, then [TBD: runs] times alternating with the other version, executing
  the full plan into a no-op sink, and report the median;
- every salted result is asserted equal to its unsalted counterpart.

AQE's built-in skew-join handling only splits partitions larger than [TBD: threshold; 256 MB by default] and five
times the median. The partitions in this experiment are far below that threshold, so AQE should not hide the effect.

| Experiment | Before (key) | After (key + salt) |
| --- | --- | --- |
| A · largest partition (rows) | [TBD] | [TBD] |
| A · max / median partition size | [TBD] | [TBD] |
| A · share of rows in the largest task | [TBD] | [TBD] |
| B · aggregation, median wall time (s) | [TBD] | [TBD] |
| C · sort-merge join, median wall time (s) | [TBD] | [TBD] |

**Result.** [TBD: copy the generated conclusion from m3_skew_results_full.] At this data size, the expected outcome is
the following, and it is reported as measured:
- Salting evens out the partitions substantially.
- Wall time changes little, or rises, because the largest partition is still processed in a fraction of a second, so
  the fixed cost of the extra shuffle (or of the copied small side) outweighs the gain.

The partition-level evidence is the finding. A speed-up would appear once the hot partition is large enough to
dominate the stage, for example on a multi-year history.

For comparison, partitioning the clean events by dyad instead of by country already reduces the largest partition's
share to [TBD]% (Experiment A, context row). This is why the main pipeline, which partitions by dyad, does not need
salting.

#### 3.2.9 Validation

Correctness is checked at three levels:
- **Unit tests.** `tests/test_dyads_local.py` runs the shared functions on hand-constructed data with ANSI mode on.
  It checks the lookups, the cleaning rules, dyad symmetry, the `RANGE` frame across missing days, the ratio-of-sums
  weighting, the dense weekly calendar, the single exchange before the window, PageRank against a closed-form
  solution, and the exactness of salted aggregations and joins.
- **Contract checks in the notebook.** Each run asserts that the weekly table has exactly dyads × weeks rows, that
  row keys are unique, that no week without events has a tone, that labels are missing only in the final week, and
  that the event totals in the weekly table equal those in the dyad-day table (no rows lost or duplicated).
- **Reconciliation with the ingestion stage.** The October 2026 sample has the same row count on Databricks and on
  the local Hadoop cluster (3,195,940; Section [TBD: architecture section]).

#### 3.2.10 Limitations of the method

- **PageRank on the driver.** PageRank is computed on the driver because the graph is small. A much larger graph
  would need the distributed version, which is implemented but only run for three verification steps.
- **Noisy timings.** Serverless compute is shared and its size is fixed, so the skew timings carry run-to-run noise
  and cannot be repeated on a controlled cluster size.
- **The time axis and when features are known.** Using the event date with an *N*-day reporting window means a week's
  features are only final *N* days after the week ends. A real-time system would act on slightly incomplete data.
- **Measurement.** All measures inherit the limitation discussed in the Problem Formulation: they describe news
  coverage of events, not the events themselves.
