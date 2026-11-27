#!/usr/bin/env python3
"""Events per country per day in PySpark: the same computation as the MapReduce job.

    spark-submit summary_spark.py <input path or glob> <output dir> [--shuffle-partitions N]

On the cluster (YARN, from the namenode container). Always pass the executor sizes: with only the
spark-defaults.conf values (768m + 384m) an executor doesn't fit next to the 1 GB AM (see README.md):
    spark-submit --num-executors 3 --executor-cores 3 \
        --executor-memory 512m --conf spark.executor.memoryOverhead=384m \
        /opt/cp422/summary/summary_spark.py \
        '/gdelt/raw/events/year=*/month=*/*.CSV' /gdelt/summary/spark_adhoc --shuffle-partitions 3
Locally, without a cluster. The image's core-site.xml makes hdfs://namenode:9000 the default file
system, so local paths need file:// (or --conf spark.hadoop.fs.defaultFS=file:///), and the event
log is switched off because spark-defaults.conf points it at HDFS:
    spark-submit --master 'local[2]' --conf spark.eventLog.enabled=false \
        summary_spark.py file:///path/to/IN file:///path/to/OUT

The definition (skip rules, key, output columns) is in README.md and matches summary_mapper.py
and summary_reducer.py rule for rule:
  * Lines are read as plain text and split on tabs with limit -1, exactly like Python's
    str.split("\\t"). (Spark's CSV reader would silently pad short lines and drop extra fields,
    so it can't tell a 57- or 59-field line apart from a good one.)
  * Each skipped line is counted under the first rule it fails, in the mapper's order.
  * Numbers are checked with the same regular expressions as the mapper. \\A and \\z anchor the
    whole string in Java regex. (A plain $ would also match before a trailing line terminator.)
    Values are then converted with try_cast, so the job also runs with ANSI mode on, where a
    plain cast of a bad value raises an error.
  * Output is one tab-separated text line per key: SQLDATE, Actor1CountryCode, events,
    sum_mentions, avg_tone (6 dp), the same as the MR reducer writes.

Row counts come from DataFrame.observe(), which collects them while the single write job runs,
so the job doesn't make a second pass over the input (that would distort the benchmark).
The last line printed is a machine-readable RESULT line that bench_mr_vs_spark.sh parses.
"""
import argparse
import time

from pyspark.sql import Observation, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

N_FIELDS = 58
# 0-based positions in the 58-field GDELT 1.0 line (codebook fields 2, 8, 32 and 35).
SQLDATE, ACTOR1_COUNTRY, NUM_MENTIONS, AVG_TONE = 1, 7, 31, 34

# Explicit schema for the text source: one STRING column per line, nothing inferred.
# (The full 58-column string schema is gdelt_common.raw_string_schema() on Databricks. This job
# runs on the cluster, outside the notebooks folder, and only needs the four fields above.)
LINE_SCHEMA = StructType([StructField("value", StringType(), True)])

# Same patterns as summary_mapper.py, anchored to the whole string.
SQLDATE_RE = r"\A[0-9]{8}\z"
MENTIONS_RE = r"\A[0-9]{1,9}\z"
TONE_RE = r"\A[+-]?(?:[0-9]{1,15}(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]{1,2})?\z"

COUNTERS = ["InputLines", "SkippedFieldCount", "SkippedEmptyActor1Country", "SkippedBadSqldate",
            "SkippedBadNumMentions", "SkippedBadAvgTone", "ValidEvents"]


def summarise(lines, input_obs, output_obs):
    """lines: DataFrame with one STRING column `value`. Returns the per-key output lines."""
    # split() with limit -1 keeps trailing empty fields, like Python's str.split("\t").
    fields = lines.select(F.split(F.col("value"), "\t", -1).alias("f"))
    # try_element_at (1-based) returns NULL past the end of a short line. With ANSI on, a plain
    # f[i] on a short array raises an error instead, which would fail the whole job.
    parsed = fields.select(
        F.size("f").alias("n_fields"),
        *[F.expr(f"try_element_at(f, {i + 1})").alias(name)
          for i, name in [(SQLDATE, "sqldate"), (ACTOR1_COUNTRY, "country"),
                          (NUM_MENTIONS, "num_mentions"), (AVG_TONE, "avg_tone")]],
    )
    # The first failing rule, in the mapper's order. Short lines are caught by the first rule,
    # so the NULLs from try_element_at never reach the other rules.
    reason = (F.when(F.col("n_fields") != N_FIELDS, F.lit("SkippedFieldCount"))
              .when(F.col("country") == "", F.lit("SkippedEmptyActor1Country"))
              .when(~F.col("sqldate").rlike(SQLDATE_RE), F.lit("SkippedBadSqldate"))
              .when(~F.col("num_mentions").rlike(MENTIONS_RE), F.lit("SkippedBadNumMentions"))
              .when(~F.col("avg_tone").rlike(TONE_RE), F.lit("SkippedBadAvgTone")))
    parsed = parsed.withColumn("reason", reason)

    counted = parsed.observe(
        input_obs,
        F.count(F.lit(1)).alias("InputLines"),
        *[F.sum(F.when(F.col("reason") == name, 1).otherwise(0)).alias(name)
          for name in COUNTERS[1:-1]],
        F.sum(F.when(F.col("reason").isNull(), 1).otherwise(0)).alias("ValidEvents"),
    )

    valid = counted.where(F.col("reason").isNull())
    summary = (valid.groupBy("sqldate", "country")
               .agg(F.count(F.lit(1)).alias("events"),
                    F.sum(F.expr("try_cast(num_mentions AS BIGINT)")).alias("sum_mentions"),
                    F.sum(F.expr("try_cast(avg_tone AS DOUBLE)")).alias("sum_tone")))
    out = summary.select(F.concat_ws(
        "\t", "sqldate", "country",
        F.col("events").cast("string"), F.col("sum_mentions").cast("string"),
        F.format_string("%.6f", F.col("sum_tone") / F.col("events"))).alias("value"))
    return out.observe(output_obs, F.count(F.lit(1)).alias("OutputRows"))


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("input", help="input file, directory or glob of raw GDELT 1.0 daily files")
    ap.add_argument("output", help="output directory (overwritten)")
    ap.add_argument("--shuffle-partitions", type=int, default=None,
                    help="spark.sql.shuffle.partitions (the benchmark sets it to the MR reducer count)")
    # builder.appName() overrides spark-submit's --name, so the name is a script option instead.
    ap.add_argument("--app-name", default="gdelt-summary-spark",
                    help="application name shown in YARN (the benchmark gives each run its own)")
    args = ap.parse_args()

    spark = SparkSession.builder.appName(args.app_name).getOrCreate()
    # Same setting as Databricks serverless: a bad cast must not slip through as NULL silently.
    spark.conf.set("spark.sql.ansi.enabled", "true")
    if args.shuffle_partitions:
        spark.conf.set("spark.sql.shuffle.partitions", str(args.shuffle_partitions))

    input_obs, output_obs = Observation("summary_input"), Observation("summary_output")
    t0 = time.time()
    lines = spark.read.schema(LINE_SCHEMA).text(args.input)
    summarise(lines, input_obs, output_obs).write.mode("overwrite").text(args.output)
    job_seconds = time.time() - t0

    stats = input_obs.get
    out_rows = output_obs.get["OutputRows"]
    print("== GDELT Summary counters (Spark)")
    for name in COUNTERS:
        print(f"  {name}={stats[name]}")
    print(f"  OutputRows={out_rows}")
    print(f"job_seconds (read + aggregate + write, session already up): {job_seconds:.2f}")
    print(f"RESULT engine=spark job_seconds={job_seconds:.2f} input_rows={stats['InputLines']} "
          f"valid_events={stats['ValidEvents']} output_rows={out_rows} "
          f"shuffle_partitions={spark.conf.get('spark.sql.shuffle.partitions')} "
          f"ansi={spark.conf.get('spark.sql.ansi.enabled')}")
    spark.stop()


if __name__ == "__main__":
    main()
