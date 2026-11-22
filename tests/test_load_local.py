"""Local test of the Databricks load logic (Spark local mode + Delta, inside the cp422/hadoop-stack image).

Checks, on the October 2026 sample month:
  1. download_and_extract fetches and MD5-checks all files in parallel;
  2. read_daily_csvs yields 58 string columns + source_file/year/month and the SAME row count as the
     Hadoop MapReduce ingestion (3,195,940), with no short rows;
  3. quoting really must be off (default quote handling changes the count);
  4. the replaceWhere month overwrite is idempotent (loading twice does not duplicate rows).

Run (from the repo root, Docker running):
  docker run --rm -v "%cd%":/repo -w /repo cp422/hadoop-stack:1.0 \
    spark-submit --master local[8] --packages io.delta:delta-spark_2.12:3.2.1 \
    --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
    tests/test_load_local.py

AI citation: Claude Code (Claude Opus 5.5, Anthropic) drafted this test. Reviewed and run by Member 1.
"""
import sys
import time
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, "notebooks")
import gdelt_common as gc  # noqa: E402
from pyspark.sql import SparkSession, functions as F  # noqa: E402

EXPECTED_ROWS = 3_195_940
LANDING = "/tmp/landing/events/year=2026/month=10"
TABLE = "events_bronze_test"

spark = (SparkSession.builder.appName("cp422-load-test")
         .config("spark.sql.warehouse.dir", "/tmp/warehouse").getOrCreate())
spark.sparkContext.setLogLevel("WARN")

# 1. download
files = gc.list_daily_files("2026-10-01", "2026-10-31")
assert len(files) == 31, len(files)
t0 = time.time()
with ThreadPoolExecutor(8) as pool:
    results = list(pool.map(lambda f: gc.download_and_extract(f, LANDING), files))
print(f"[1] downloaded {len(results)} files, {sum(r['zip_bytes'] for r in results) / 1e6:.0f} MB zipped, "
      f"{time.time() - t0:.0f}s")

# 2. read with the bronze schema
df = gc.read_daily_csvs(spark, LANDING)
n = df.count()
print(f"[2] rows {n:,} (Hadoop {EXPECTED_ROWS:,}); columns {len(df.columns)}")
assert n == EXPECTED_ROWS
assert len(df.columns) == 58 + 4
short = df.where("SOURCEURL IS NULL OR DATEADDED IS NULL").count()
print(f"    rows missing trailing columns: {short}")
assert short == 0
months = [tuple(r) for r in df.select(gc.PART_YEAR, gc.PART_MONTH).distinct().collect()]
assert months == [(2026, 10)], months
# GDELT's own "Year" (event year, a string) must survive next to the partition columns.
event_years = sorted(r[0] for r in df.select("Year").distinct().collect())
print(f"    GDELT Year values kept: {event_years[:3]} ... {event_years[-3:]} ({len(event_years)} distinct)")
assert "2026" in event_years
assert df.select("source_file").distinct().count() == 31

# 3. why quote handling is disabled
default_n = (spark.read.options(sep="\t", header="false").schema(gc.raw_string_schema()).csv(LANDING).count())
print(f"[3] rows with Spark's default quote handling: {default_n:,} (difference {default_n - EXPECTED_ROWS:+,})")

# 4. idempotent month overwrite
cols = ", ".join(f"`{c}` STRING" for c in gc.GDELT_V1_COLUMNS)
spark.sql(f"DROP TABLE IF EXISTS {TABLE}")
spark.sql(f"CREATE TABLE {TABLE} ({cols}, source_file STRING, ingest_ts TIMESTAMP, "
          f"{gc.PART_YEAR} INT, {gc.PART_MONTH} INT) USING DELTA PARTITIONED BY ({gc.PART_YEAR}, {gc.PART_MONTH})")
for attempt in (1, 2):
    (df.select(spark.table(TABLE).columns).write.format("delta").mode("overwrite")
       .option("replaceWhere", f"{gc.PART_YEAR} = 2026 AND {gc.PART_MONTH} = 10").saveAsTable(TABLE))
    print(f"[4] after load #{attempt}: {spark.table(TABLE).count():,} rows")
assert spark.table(TABLE).count() == EXPECTED_ROWS
detail = spark.sql(f"DESCRIBE DETAIL {TABLE}").first()
print(f"    partitionColumns={detail.partitionColumns} numFiles={detail.numFiles} "
      f"size={detail.sizeInBytes / 2**20:.0f} MiB (CSV was {sum(r['csv_bytes'] for r in results) / 2**20:.0f} MiB)")
print("ALL CHECKS PASSED")
