# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Data skew and salting, before and after (Member 3, above and beyond)
# MAGIC
# MAGIC GDELT is dominated by a few countries. When rows are shuffled by actor country, every row with the same country
# MAGIC goes to the same partition, so one task gets the United States while the others get a few small countries. The
# MAGIC whole stage then waits for that one task. **Salting** splits each hot key into several sub-keys (`key`, `salt`),
# MAGIC so its rows spread over several partitions, and then combines the pieces in a second, much smaller step.
# MAGIC
# MAGIC This notebook measures that, honestly, on Databricks Free Edition:
# MAGIC
# MAGIC | Experiment | Before (unsalted) | After (salted) | Evidence |
# MAGIC | --- | --- | --- | --- |
# MAGIC | A · partition sizes | `repartition(P, key)` | `repartition(P, key, salt)` | rows per partition: max, median, max/median, largest share |
# MAGIC | B · aggregation | shuffle by key, aggregate | shuffle by (key, salt), partial aggregate, then combine per key | identical results (asserted), plans, wall time |
# MAGIC | C · join | sort-merge join on key (forced by a hint) | join on (key, salt), with the small side copied once per salt | identical results (asserted), plans, wall time |
# MAGIC
# MAGIC **Why these choices make the comparison real on serverless:**
# MAGIC - **No configs need changing.** Free Edition blocks many Spark configs and adaptive query execution (AQE) may not
# MAGIC   be switchable. Instead of disabling broadcast joins, a `shuffle_merge` **join hint** forces the sort-merge join.
# MAGIC   Instead of relying on `spark.sql.shuffle.partitions`, every shuffle here uses an explicit `repartition(P, ...)`,
# MAGIC   which AQE doesn't merge.
# MAGIC - **Partition sizes come from `spark_partition_id()`,** not the RDD API (which serverless doesn't have) and not the
# MAGIC   Spark UI (which may be missing).
# MAGIC - **Timings** use the `noop` sink: it runs the whole plan without collecting or storing anything. Each version runs
# MAGIC   once untimed, then several times alternating with the other. The median is reported.
# MAGIC - **Results are compared** between the salted and unsalted versions, so salting is shown not to change any number.
# MAGIC
# MAGIC Notebook 03 uses a separate partition-tuning step (shuffle partitions, repartition by dyad). This notebook is only
# MAGIC about skew.

# COMMAND ----------

# Declared here, not only in 00_config: a widget created inside a %run notebook does not appear in this notebook's
# parameter bar. Every top-level notebook in the repo starts with this cell.
dbutils.widgets.dropdown("sample_mode", "true", ["true", "false"], "SAMPLE_MODE")

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC - `DEMO_PARTITIONS = 64`: every shuffle in the demo uses 64 partitions. With about 250 country codes, a perfectly
# MAGIC   balanced shuffle would put about 1/64 (1.6 %) of rows in each partition, so one hot key stands out clearly.
# MAGIC - `timing_runs` widget: timed runs per version, after one untimed warm-up each. 3 is enough for a median and keeps
# MAGIC   the sample-mode Run All short.
# MAGIC
# MAGIC The events source is chosen with the same helper and widget as notebook 03.

# COMMAND ----------

import pandas as pd
from pyspark.sql import functions as F
import dyad_common as dc

dbutils.widgets.dropdown("events_source", "auto", ["auto", "member2_clean", "bronze_fallback"], "Events source")
dbutils.widgets.dropdown("timing_runs", "3", ["1", "2", "3", "4", "5"], "Timed runs per version")
TIMING_RUNS = int(dbutils.widgets.get("timing_runs"))
DEMO_PARTITIONS = 64

SUFFIX = "_sample" if SAMPLE_MODE else "_full"
T = lambda name: f"{CATALOG}.{SCHEMA}.{name}{SUFFIX}"
T_SKEW_INPUT, T_SKEW_RESULTS = T("m3_skew_input"), T("m3_skew_results")
T_EVENTS = T("m3_dyad_events")   # notebook 03's clean events, used only for the dyad comparison in experiment A

def save(df, table):
    """Overwrite a result table. overwriteSchema lets a re-run with changed columns replace the old table cleanly."""
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)

def conf_get(key):
    try:
        return spark.conf.get(key)
    except Exception as exc:
        return f"<not readable: {type(exc).__name__}>"

src = dc.resolve_events_source(spark, dbutils.widgets.get("events_source"), EVENTS_CLEAN, EVENTS_BRONZE,
                               RANGE_START, RANGE_END)
print(dc.source_banner(src))
for k in ["spark.sql.adaptive.enabled", "spark.sql.adaptive.skewJoin.enabled",
          "spark.sql.adaptive.skewJoin.skewedPartitionFactor",
          "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes"]:
    print(f"{k} = {conf_get(k)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Demo input: one narrow row per event with an actor-1 country
# MAGIC
# MAGIC The skewed key is `Actor1CountryCode`, the country of the actor who starts the event. All events in range with a
# MAGIC country code and valid numbers are kept, typed with the same `try_cast` helper as notebook 03. Rows without a
# MAGIC country are left out: their NULL key would form a second hot partition, which is a different problem (fixed by
# MAGIC filtering, not salting).
# MAGIC
# MAGIC The four columns are written to a Delta table once. Every timed run then reads the same narrow input, and the
# MAGIC timings measure the shuffle rather than re-parsing the raw strings.

# COMMAND ----------

skew_input = (dc.typed_in_range(src)
              .where("actor1_country IS NOT NULL AND event_id IS NOT NULL AND num_mentions IS NOT NULL "
                     "AND avg_tone IS NOT NULL")
              .select(F.col("actor1_country").alias("key"), "event_id", "num_mentions", "avg_tone"))
save(skew_input, T_SKEW_INPUT)
inp = spark.table(T_SKEW_INPUT)
n_rows = inp.count()
print(f"{T_SKEW_INPUT}: {n_rows:,} events")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · How skewed is the key, and which keys get salted
# MAGIC
# MAGIC A key is **hot** if it holds more than one partition's fair share of rows (1/64). Each hot key gets
# MAGIC `ceil(share × 64)` salt buckets, enough to cut each piece down to about one fair share. For example, a key holding
# MAGIC 20 % of rows gets 13 buckets. All other keys keep a single bucket (`salt = 0`), so salting adds no work for them.
# MAGIC
# MAGIC The salt is `pmod(xxhash64(event_id), buckets)`. It is a hash, not `rand()`, so a re-run or a retried task sends
# MAGIC every row to the same bucket.

# COMMAND ----------

key_counts = {r["key"]: r["count"] for r in inp.groupBy("key").count().collect()}
salts = dc.hot_key_salts(key_counts, DEMO_PARTITIONS)
top = sorted(key_counts.items(), key=lambda kv: -kv[1])[:10]
display(pd.DataFrame([(k, c, c / n_rows, salts.get(k, 1)) for k, c in top],
                     columns=["key", "rows", "share", "salt_buckets"]))
print(f"{len(key_counts)} distinct keys; fair share per partition = 1/{DEMO_PARTITIONS} = {1 / DEMO_PARTITIONS:.2%}; "
      f"hot keys salted: {salts}")
salted_inp = dc.add_salt(inp, spark, salts)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Experiment A: rows per partition, before and after
# MAGIC
# MAGIC The same rows are shuffled into 64 partitions twice: by `key` (before), and by (`key`, `salt`) (after). The
# MAGIC partitions are then counted with `spark_partition_id()`.
# MAGIC - **max/median** says how much longer the slowest task has to work than a typical one.
# MAGIC - **largest share** is the fraction of all rows that one task processes.
# MAGIC - **non-empty partitions** counts partitions that received any rows. With about 250 keys hashed into 64
# MAGIC   partitions, some stay empty before salting; salting spreads the hot keys over more of them.
# MAGIC
# MAGIC For context, the last row partitions notebook 03's clean events by **dyad**, the key its window functions use.
# MAGIC Spreading a country over all its dyads already breaks up most of the country skew, which is why 03 doesn't need
# MAGIC salting.

# COMMAND ----------

before_sizes = dc.partition_sizes(inp.repartition(DEMO_PARTITIONS, "key"))
after_sizes = dc.partition_sizes(salted_inp.repartition(DEMO_PARTITIONS, "key", "salt"))
summary_rows = [("before: key", dc.size_summary(before_sizes)), ("after: key + salt", dc.size_summary(after_sizes))]
if spark.catalog.tableExists(T_EVENTS):
    summary_rows.append(("context: 03 events by dyad",
                         dc.size_summary(dc.partition_sizes(spark.table(T_EVENTS).repartition(DEMO_PARTITIONS, "dyad")))))
part_pd = pd.DataFrame([{"partitioning": name, **s} for name, s in summary_rows])
display(part_pd)
print("largest 5 partitions before:", before_sizes[:5])
print("largest 5 partitions after: ", after_sizes[:5])
A_BEFORE, A_AFTER = dc.size_summary(before_sizes), dc.size_summary(after_sizes)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Experiment B: aggregation per country
# MAGIC
# MAGIC Per-country event count, Σ mentions and Σ(tone × mentions):
# MAGIC - **Before:** shuffle by `key`, then aggregate. Because the data is already partitioned by `key`, Spark adds no
# MAGIC   second shuffle, and the task holding the hot key does all of its aggregation alone.
# MAGIC - **After:** shuffle by (`key`, `salt`), aggregate per (`key`, `salt`), then combine the at most a few dozen
# MAGIC   partial rows per key in a second, tiny shuffle.
# MAGIC
# MAGIC Both versions must return identical numbers. The plan lines show the extra small exchange salting adds.

# COMMAND ----------

def agg_unsalted():
    return (inp.repartition(DEMO_PARTITIONS, "key").groupBy("key")
            .agg(F.count(F.lit(1)).alias("events"), F.sum("num_mentions").alias("mentions"),
                 F.sum(F.col("avg_tone") * F.col("num_mentions")).alias("tone_x_mentions")))

def agg_salted():
    partial = (salted_inp.repartition(DEMO_PARTITIONS, "key", "salt").groupBy("key", "salt")
               .agg(F.count(F.lit(1)).alias("events"), F.sum("num_mentions").alias("mentions"),
                    F.sum(F.col("avg_tone") * F.col("num_mentions")).alias("tone_x_mentions")))
    return partial.groupBy("key").agg(F.sum("events").alias("events"), F.sum("mentions").alias("mentions"),
                                      F.sum("tone_x_mentions").alias("tone_x_mentions"))

u = {r["key"]: r for r in agg_unsalted().collect()}
s = {r["key"]: r for r in agg_salted().collect()}
assert u.keys() == s.keys()
for k in u:
    assert u[k]["events"] == s[k]["events"] and u[k]["mentions"] == s[k]["mentions"], k
    assert abs(u[k]["tone_x_mentions"] - s[k]["tone_x_mentions"]) <= 1e-6 * max(1.0, abs(u[k]["tone_x_mentions"])), k
print(f"experiment B: salted and unsalted results identical for all {len(u)} keys")
for name, fn in [("before", agg_unsalted), ("after", agg_salted)]:
    print(f"{name}:")
    for ln in dc.plan_lines(dc.explain_text(fn(), "simple"), "Exchange", "HashAggregate"):
        print("   ", ln[:150])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Experiment C: sort-merge join on the hot key
# MAGIC
# MAGIC Each event is joined to its country's average tone, and its deviation from that average is computed.
# MAGIC - The country table is tiny, so Spark would normally broadcast it, and a broadcast join has no shuffle and so no
# MAGIC   skew. The `shuffle_merge` hint forces a **sort-merge join**: the shuffle-based join used when both sides are large.
# MAGIC - **Before:** both sides are partitioned by `key` into 64 partitions.
# MAGIC - **After:** the events are partitioned by (`key`, `salt`), and the country table is copied once per salt bucket
# MAGIC   of its key, so every (`key`, `salt`) piece still finds its match.
# MAGIC - Row count and Σ deviation must match exactly.
# MAGIC
# MAGIC **About AQE.** AQE's automatic skew-join handling only splits a partition that is more than
# MAGIC `skewedPartitionFactor` × the median **and** larger than `skewedPartitionThresholdInBytes` (256 MB by default;
# MAGIC printed in the setup cell). The demo partitions are far smaller than 256 MB, so AQE's skew handling shouldn't kick
# MAGIC in, and the "before" join should run with its skew intact. On Databricks the final adaptive plan isn't visible
# MAGIC through `explain()`, so the partition sizes in experiment A are the direct evidence.

# COMMAND ----------

key_tone = inp.groupBy("key").agg((F.sum(F.col("avg_tone") * F.col("num_mentions")) / F.sum("num_mentions"))
                                  .alias("key_tone"))
salted_key_tone = dc.explode_salts(key_tone, spark, salts)

def join_unsalted():
    return (inp.repartition(DEMO_PARTITIONS, "key")
            .join(key_tone.repartition(DEMO_PARTITIONS, "key").hint("shuffle_merge"), "key")
            .select("key", (F.col("avg_tone") - F.col("key_tone")).alias("tone_dev")))

def join_salted():
    return (salted_inp.repartition(DEMO_PARTITIONS, "key", "salt")
            .join(salted_key_tone.repartition(DEMO_PARTITIONS, "key", "salt").hint("shuffle_merge"), ["key", "salt"])
            .select("key", (F.col("avg_tone") - F.col("key_tone")).alias("tone_dev")))

cu = join_unsalted().agg(F.count(F.lit(1)).alias("n"), F.sum("tone_dev").alias("dev")).first()
cs = join_salted().agg(F.count(F.lit(1)).alias("n"), F.sum("tone_dev").alias("dev")).first()
assert cu["n"] == cs["n"] == n_rows, (cu["n"], cs["n"], n_rows)
assert abs(cu["dev"] - cs["dev"]) <= 1e-6 * max(1.0, abs(cu["dev"])), (cu["dev"], cs["dev"])
print(f"experiment C: both joins return {cu['n']:,} rows and the same total deviation ({cu['dev']:.6f})")
for name, fn in [("before", join_unsalted), ("after", join_salted)]:
    plan = dc.explain_text(fn(), "simple")
    print(f"{name}: sort-merge join in plan = {'SortMergeJoin' in plan}")
    for ln in dc.plan_lines(plan, "Exchange", "Join"):
        print("   ", ln[:150])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · Timings
# MAGIC
# MAGIC Each version runs once untimed (a warm-up), then `TIMING_RUNS` times, alternating with the other version.
# MAGIC Wall-clock seconds are measured around a `noop` write, which runs every stage of the plan and stores nothing.
# MAGIC Serverless compute is shared and its size is fixed, so single timings are noisy. Look at the median, and treat
# MAGIC differences under about 10 % as noise.

# COMMAND ----------

sink = {}
def timed(fn):
    return lambda: sink.setdefault("mode", dc.run_noop(fn()))

t_agg = dc.time_alternating({"before": timed(agg_unsalted), "after": timed(agg_salted)}, runs=TIMING_RUNS)
t_join = dc.time_alternating({"before": timed(join_unsalted), "after": timed(join_salted)}, runs=TIMING_RUNS)
print(f"execution via: {sink.get('mode')}")
for exp, t in [("B aggregation", t_agg), ("C join", t_join)]:
    print(f"{exp}: before {[round(x, 2) for x in t['before']]} s, after {[round(x, 2) for x in t['after']]} s")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 · Results and conclusion
# MAGIC
# MAGIC The table is saved for the report. The conclusion is generated from the measured numbers, never written in
# MAGIC advance:
# MAGIC - Salting gets credit for a speed-up only if the median time drops by at least 10 %.
# MAGIC - Within ±10 % it says the time barely changed; more than 10 % slower, it says salting made it slower.
# MAGIC   In both cases it explains why that is expected at this data size, and that the evened-out partitions are the finding.

# COMMAND ----------

def verdict(exp, before_s, after_s):
    change = (after_s - before_s) / before_s if before_s else 0.0
    balance = (f"Salting cut the largest partition from {A_BEFORE['max']:,} to {A_AFTER['max']:,} rows "
               f"(max/median {A_BEFORE['max_over_median']} -> {A_AFTER['max_over_median']}; largest task's share "
               f"{A_BEFORE['largest_share']:.1%} -> {A_AFTER['largest_share']:.1%}).")
    why = ("At this data size even the largest partition is processed quickly, so the fixed cost of salting (an extra "
           "small shuffle, or copies of the small side) outweighs the gain, and timings on shared serverless compute "
           "vary from run to run. The partition-level evening-out is the finding; a speed-up would only appear once "
           "the hot partition is large enough to dominate the stage.")
    if change <= -0.10:
        timing = f"The {exp} also got faster: median {before_s:.2f} s -> {after_s:.2f} s ({change:+.0%})."
    elif change < 0.10:
        timing = f"The {exp}'s wall time barely changed (median {before_s:.2f} s -> {after_s:.2f} s, {change:+.0%}). {why}"
    else:
        timing = f"The {exp} got slower with salting (median {before_s:.2f} s -> {after_s:.2f} s, {change:+.0%}). {why}"
    return f"{balance} {timing}"

result_rows = []
for exp, t in [("B aggregation", t_agg), ("C join", t_join)]:
    mb, ma = dc.median(t["before"]), dc.median(t["after"])
    for variant, summ, med, runs in [("before (key)", A_BEFORE, mb, t["before"]),
                                     ("after (key + salt)", A_AFTER, ma, t["after"])]:
        result_rows.append({"experiment": exp, "variant": variant, "rows": n_rows, "nonempty_partitions": summ["nonempty_partitions"],
                            "max_rows": summ["max"], "median_rows": float(summ["median"]),
                            "max_over_median": summ["max_over_median"], "largest_share": summ["largest_share"],
                            "median_seconds": med, "run_seconds": ", ".join(f"{x:.3f}" for x in runs),
                            "hot_keys": str(dict(sorted(salts.items(), key=lambda kv: -kv[1]))), "execution": sink.get("mode"),
                            "conclusion": verdict(exp.split(" ", 1)[1], mb, ma)})
results_pd = pd.DataFrame(result_rows)
save(spark.createDataFrame(results_pd), T_SKEW_RESULTS)
display(results_pd.drop(columns=["conclusion"]))
for exp in ["B aggregation", "C join"]:
    print(f"{exp}: {results_pd.loc[results_pd['experiment'] == exp, 'conclusion'].iloc[0]}\n")
