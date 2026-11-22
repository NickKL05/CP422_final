# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Load GDELT into Delta (Member 1)
# MAGIC
# MAGIC Loads GDELT 1.0 daily event files into **`workspace.gdelt.events_bronze`**, a Delta table **partitioned by year and month**
# MAGIC of the file date (`DATEADDED`), stored as `file_year` and `file_month`. They can't be called `year` and `month`, because GDELT already has
# MAGIC a `Year` column and Spark column names are case-insensitive. Every GDELT column is kept as a string, and typing and cleaning happen in the next step.
# MAGIC
# MAGIC The load works one month at a time, and every step can be re-run safely:
# MAGIC 1. List the month's files from GDELT's `md5sums` index.
# MAGIC 2. Download and unzip them in parallel into the landing folder of the Unity Catalog volume (MD5-checked, retried).
# MAGIC 3. Read them with an explicit schema and overwrite just that month's partition (`replaceWhere`), so a re-run never duplicates rows.
# MAGIC 4. Log the month in `load_log`, then delete the landing CSVs to stay inside the Free Edition storage quota.
# MAGIC
# MAGIC **Run All only loads the sample month.** `run_full_load = true` is the only setting that starts a full-history download.
# MAGIC `SAMPLE_MODE` does not change what gets loaded; it only controls which range the analysis notebooks read.
# MAGIC If the workspace blocks outbound downloads, use the fallback `tools/gdelt_to_parquet.py` (see the last section).

# COMMAND ----------

# AI citation: Claude Code (Claude Opus 5.5, Anthropic) drafted this cell. Reviewed and tested by Member 1.
# Declared here, not only in 00_config: widgets created inside a %run notebook don't show in this
# notebook's parameter bar. Every top-level notebook should start with this cell.
dbutils.widgets.dropdown("sample_mode", "true", ["true", "false"], "SAMPLE_MODE")

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# AI citation: Claude Code (Claude Opus 5.5, Anthropic) drafted this cell (widgets, UC objects, load-log DDL).
# Reviewed and tested by Member 1.
import time, datetime as dt
from concurrent.futures import ThreadPoolExecutor, as_completed
from pyspark.sql import functions as F
import gdelt_common as gc

dbutils.widgets.dropdown("run_full_load", "false", ["false", "true"], "Run full-history load")
dbutils.widgets.dropdown("source", "download", ["download", "parquet_upload"], "Source")
dbutils.widgets.dropdown("force_reload", "false", ["false", "true"], "Reload months already loaded")
RUN_FULL_LOAD = dbutils.widgets.get("run_full_load") == "true"
SOURCE = dbutils.widgets.get("source")
FORCE_RELOAD = dbutils.widgets.get("force_reload") == "true"
DOWNLOAD_THREADS = 8

LANDING_CSV = f"{VOLUME_ROOT}/landing/events"            # temporary: unzipped CSVs for the month being loaded
LANDING_PARQUET = f"{VOLUME_ROOT}/landing_parquet/events" # fallback: Parquet uploaded from a laptop

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")

cols_ddl = ",\n  ".join(f"`{c}` STRING" for c in gc.GDELT_V1_COLUMNS)
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {EVENTS_BRONZE} (
  {cols_ddl},
  source_file STRING,
  ingest_ts TIMESTAMP,
  {gc.PART_YEAR} INT,
  {gc.PART_MONTH} INT
) USING DELTA
PARTITIONED BY ({gc.PART_YEAR}, {gc.PART_MONTH})
COMMENT 'GDELT 1.0 daily events, raw strings. Partitioned by file date (DATEADDED). Loaded by 01_load_gdelt.'
""")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {LOAD_LOG} (
  year INT, month INT, files INT, rows BIGINT, zip_bytes BIGINT, csv_bytes BIGINT,
  download_seconds DOUBLE, write_seconds DOUBLE, source STRING, loaded_at TIMESTAMP
) USING DELTA
""")
# Only run_full_load can start a full-history download. SAMPLE_MODE=false must not, because a
# member switching to full analysis should never trigger a year of downloads by accident.
range_start, range_end = (FULL_START, FULL_END) if RUN_FULL_LOAD else (SAMPLE_START, SAMPLE_END)
print(f"source={SOURCE}  range={range_start}..{range_end}  force_reload={FORCE_RELOAD}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Connectivity check (open decision: can Free Edition compute download GDELT directly?)

# COMMAND ----------

# AI citation: Claude Code (Claude Opus 5.5, Anthropic) drafted this cell. Reviewed and tested by Member 1.
files = []
if SOURCE == "download":
    try:
        files = gc.list_daily_files(range_start, range_end)
        print(f"OK: direct download works. {len(files)} daily files listed "
              f"({sum(f['zip_bytes'] for f in files) / 1e6:.0f} MB zipped) for {range_start}..{range_end}")
    except Exception as exc:
        raise RuntimeError(
            "This compute cannot reach data.gdeltproject.org (" + repr(exc)[:200] + "). "
            "Use the fallback: run tools/gdelt_to_parquet.py on a laptop, upload its output folder to "
            f"{LANDING_PARQUET}, then set the 'source' widget to parquet_upload."
        ) from exc

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load one month at a time

# COMMAND ----------

# AI citation: Claude Code (Claude Opus 5.5, Anthropic) drafted these load functions (parallel download,
# replaceWhere partition overwrite, load log). Reviewed and tested by Member 1.

def loaded_months():
    return {(r.year, r.month): r.files for r in
            spark.sql(f"SELECT year, month, max_by(files, loaded_at) AS files FROM {LOAD_LOG} GROUP BY year, month").collect()}


def month_filter(year, month):
    return f"{gc.PART_YEAR} = {year} AND {gc.PART_MONTH} = {month}"


def write_month(df, year, month):
    """Overwrite exactly one month partition, so re-running a month replaces it rather than appending."""
    t0 = time.time()
    (df.select(spark.table(EVENTS_BRONZE).columns)
       .write.format("delta").mode("overwrite")
       .option("replaceWhere", month_filter(year, month))
       .saveAsTable(EVENTS_BRONZE))
    return time.time() - t0


def log_month(year, month, files, rows, zip_bytes, csv_bytes, dl_s, wr_s, source):
    spark.createDataFrame(
        [(year, month, files, rows, zip_bytes, csv_bytes, float(dl_s), float(wr_s), source, dt.datetime.utcnow())],
        spark.table(LOAD_LOG).schema).write.mode("append").saveAsTable(LOAD_LOG)


def load_month_download(year, month, month_files):
    landing = f"{LANDING_CSV}/year={year}/month={month:02d}"
    t0 = time.time()
    results = []
    with ThreadPoolExecutor(DOWNLOAD_THREADS) as pool:
        futures = [pool.submit(gc.download_and_extract, f, landing) for f in month_files]
        for fut in as_completed(futures):
            results.append(fut.result())  # re-raises a download that failed after its retries
    dl_s = time.time() - t0

    df = gc.read_daily_csvs(spark, landing)
    wr_s = write_month(df, year, month)
    rows = spark.table(EVENTS_BRONZE).where(month_filter(year, month)).count()
    log_month(year, month, len(month_files), rows, sum(r["zip_bytes"] for r in results),
              sum(r["csv_bytes"] for r in results), dl_s, wr_s, "download")
    dbutils.fs.rm(landing, recurse=True)  # free volume storage; the Delta copy is about 5x smaller
    return rows, dl_s, wr_s


def load_month_parquet(year, month):
    path = f"{LANDING_PARQUET}/year={year}/month={month:02d}"
    df = (spark.read.parquet(path)
          .withColumn("ingest_ts", F.current_timestamp())
          .withColumn(gc.PART_YEAR, F.lit(year)).withColumn(gc.PART_MONTH, F.lit(month)))
    n_files = df.select("source_file").distinct().count()
    wr_s = write_month(df, year, month)
    rows = spark.table(EVENTS_BRONZE).where(month_filter(year, month)).count()
    log_month(year, month, n_files, rows, 0, 0, 0.0, wr_s, "parquet_upload")
    return rows, 0.0, wr_s

# COMMAND ----------

# AI citation: Claude Code (Claude Opus 5.5, Anthropic) drafted this driver loop. Reviewed and tested by Member 1.
done = loaded_months()
summary = []

if SOURCE == "download":
    plan = gc.group_by_month(files)
else:
    # Months present in the uploaded Parquet folder that fall inside the selected range
    lo, hi = dt.date.fromisoformat(range_start), dt.date.fromisoformat(range_end)
    plan = {}
    for y_dir in dbutils.fs.ls(LANDING_PARQUET):
        for m_dir in dbutils.fs.ls(y_dir.path):
            y, m = int(y_dir.name.strip("/").split("=")[1]), int(m_dir.name.strip("/").split("=")[1])
            if lo.replace(day=1) <= dt.date(y, m, 1) <= hi:
                plan[(y, m)] = None

for (year, month), month_files in sorted(plan.items()):
    expected = len(month_files) if month_files is not None else None
    if not FORCE_RELOAD and (year, month) in done and (expected is None or done[(year, month)] >= expected):
        print(f"{year}-{month:02d}: already loaded ({done[(year, month)]} files), skipping")
        continue
    if SOURCE == "download":
        rows, dl_s, wr_s = load_month_download(year, month, month_files)
    else:
        rows, dl_s, wr_s = load_month_parquet(year, month)
    summary.append((year, month, rows, round(dl_s, 1), round(wr_s, 1)))
    print(f"{year}-{month:02d}: {rows:,} rows  download {dl_s:.0f}s  write {wr_s:.0f}s")

print(f"loaded {len(summary)} month(s) this run")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify the table

# COMMAND ----------

# AI citation: Claude Code (Claude Opus 5.5, Anthropic) drafted these checks. Reviewed and tested by Member 1.
per_month = spark.sql(f"""
  SELECT {gc.PART_YEAR}, {gc.PART_MONTH}, COUNT(*) AS rows, COUNT(DISTINCT source_file) AS files,
         MIN(SQLDATE) AS min_sqldate, MAX(SQLDATE) AS max_sqldate
  FROM {EVENTS_BRONZE} GROUP BY ALL ORDER BY 1, 2""")
display(per_month)

detail = spark.sql(f"DESCRIBE DETAIL {EVENTS_BRONZE}").select("partitionColumns", "numFiles", "sizeInBytes").first()
total_rows = spark.table(EVENTS_BRONZE).count()
print(f"events_bronze: {total_rows:,} rows, {detail.numFiles} files, {detail.sizeInBytes / 2**30:.2f} GiB, "
      f"partitioned by {detail.partitionColumns}")

# Cross-check against the local Hadoop cluster's MapReduce ingestion of the same month.
oct_rows = spark.table(EVENTS_BRONZE).where(month_filter(2026, 10)).count()
print(f"2026-10 rows: Databricks {oct_rows:,} vs Hadoop {HADOOP_SAMPLE_ROWS:,} -> "
      f"{'MATCH' if oct_rows == HADOOP_SAMPLE_ROWS else 'MISMATCH'}")
assert oct_rows == HADOOP_SAMPLE_ROWS, "sample month row count differs from the Hadoop ingestion"

# Every row must have exactly 58 fields: a short row means a parsing problem (e.g. a quote character).
bad = spark.table(EVENTS_BRONZE).where("SOURCEURL IS NULL AND DATEADDED IS NULL").count()
print(f"rows with missing trailing columns: {bad:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Local Hadoop cluster output (captured, not executed here)
# MAGIC The MapReduce ingestion, `hdfs fsck` block placement and the fault-tolerance demo run on the local Docker cluster
# MAGIC (`hadoop/`). This cell only displays the output files those runs saved into the repo.

# COMMAND ----------

# AI citation: Claude Code (Claude Opus 5.5, Anthropic) drafted this cell. Reviewed and tested by Member 1.
import glob

repo_hadoop_out = None
for p in sys.path:
    cand = os.path.normpath(os.path.join(p, "..", "hadoop", "output"))
    if os.path.isdir(cand):
        repo_hadoop_out = cand
        break

if repo_hadoop_out is None:
    print("hadoop/output not found next to this notebook (open the notebooks from the Git folder to see it).")
else:
    wanted = (sorted(glob.glob(f"{repo_hadoop_out}/ingest_summary_*.txt"))[-1:]
              + [f"{repo_hadoop_out}/block_placement_summary.txt", f"{repo_hadoop_out}/locality_counters.txt",
                 f"{repo_hadoop_out}/fault_tolerance/ft_02_timeline.txt", f"{repo_hadoop_out}/fault_tolerance/ft_03_after.txt"])
    for path in wanted:
        if os.path.exists(path):
            print(f"===== {os.path.relpath(path, repo_hadoop_out)} =====")
            print(open(path).read())
