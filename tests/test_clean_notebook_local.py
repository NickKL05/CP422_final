"""Local end-to-end test of the 02_clean_events notebook itself (Spark local mode + Delta, ANSI ON).

tests/test_clean_local.py tests the functions in gdelt_common.py. This test runs the NOTEBOOK FILE,
cell by cell, the way Run All would, so it also covers what only the notebook does: the CREATE TABLE
DDL against the to_typed() types, the replaceWhere writes, the audit tables and the sanity asserts.

How it works:
  * events_bronze is built from the 2-day GDELT 1.0 sample with the same layout as 01_load_gdelt
    (58 STRING columns + source_file, ingest_ts, file_year, file_month; Delta, partitioned).
  * notebooks/02_clean_events.py is split on "# COMMAND ----------" and each code cell is executed.
    %md cells are skipped; "%run ./00_config" executes notebooks/00_config.py, then points CATALOG and
    the table names at the local spark_catalog (no Unity Catalog here). dbutils and display are stubs.
  * The notebook runs twice. Between the runs, rows are added to two months the run must not touch
    (or, in full mode, must replace):
      2020-01: outside both the sample and the full range, so it must survive in both modes;
      2026-09: outside the sample range (kept in sample mode), inside the full range (replaced in
               full mode, and there is no September bronze data, so it must disappear).
    The second run must give the same events_clean / clean_audit rows for the processed months.
  * Section 8 runs Member 3's dyad_common.py against events_clean if it can be imported. To include it,
    put Member 3's notebooks folder on PYTHONPATH (e.g. -e PYTHONPATH=/m3/notebooks); otherwise it is skipped.
    The test fails if that section prints "CONTRACT CHECK FAILED".
  * The GDELT 2.0 demo cell downloads one 15-minute file from data.gdeltproject.org. Without network
    it prints "GDELT 2.0 demo skipped" and the notebook carries on, as on Databricks.

Sample folder layout: /sample/events/YYYYMMDD.export.CSV (GDELT 1.0 daily, as downloaded and unzipped).

Run (from the repo root, Docker running; no cluster needed). SAMPLE_MODE=true is the default;
SAMPLE_MODE=false runs the notebook in full mode (on the same 2-day bronze):
  docker run --rm -v "$PWD":/repo -v <folder with the sample>:/sample:ro -w /repo [-e SAMPLE_MODE=false] \
    cp422/hadoop-stack:1.0 \
    spark-submit --master local[4] --driver-memory 2g --packages io.delta:delta-spark_2.12:3.2.1 \
    --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
    --conf spark.hadoop.fs.defaultFS=file:/// --conf spark.eventLog.enabled=false \
    tests/test_clean_notebook_local.py [/sample]
"""
import contextlib
import io
import os
import shutil
import sys
import tempfile
import types

sys.path.insert(0, "notebooks")
import gdelt_common as gc  # noqa: E402
from pyspark.sql import SparkSession, functions as F  # noqa: E402

SAMPLE = sys.argv[1] if len(sys.argv) > 1 else "/sample"
SAMPLE_MODE = os.environ.get("SAMPLE_MODE", "true")
assert SAMPLE_MODE in ("true", "false"), SAMPLE_MODE
NOTEBOOK = "notebooks/02_clean_events.py"
SCHEMA = "gdelt"
WORK = tempfile.mkdtemp(prefix="cp422_nb_")
OUTPUT = []                                   # everything the notebook cells printed

spark = (SparkSession.builder.appName("cp422-clean-notebook-test")
         .config("spark.sql.ansi.enabled", "true")            # as on Databricks serverless
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.shuffle.partitions", "8")
         .config("spark.sql.warehouse.dir", f"{WORK}/warehouse").getOrCreate())
spark.sparkContext.setLogLevel("WARN")
assert spark.conf.get("spark.sql.ansi.enabled") == "true"


# --- stubs for the Databricks-only objects the notebook uses ---
class _Widgets:
    def dropdown(self, *args, **kwargs):
        pass

    def get(self, name):
        assert name == "sample_mode", name
        return SAMPLE_MODE


class _FS:
    def rm(self, path, recurse=False):
        shutil.rmtree(path, ignore_errors=True)


dbutils = types.SimpleNamespace(widgets=_Widgets(), fs=_FS(), notebook=None)


def display(df):
    if hasattr(df, "show"):
        df.show(40, truncate=60)
    else:
        print(df)


# --- events_bronze from the sample, same layout as 01_load_gdelt ---
spark.sql(f"CREATE DATABASE IF NOT EXISTS {SCHEMA}")
cols = ", ".join(f"`{c}` STRING" for c in gc.GDELT_V1_COLUMNS)
spark.sql(f"CREATE TABLE {SCHEMA}.events_bronze ({cols}, source_file STRING, ingest_ts TIMESTAMP, "
          f"{gc.PART_YEAR} INT, {gc.PART_MONTH} INT) USING DELTA PARTITIONED BY ({gc.PART_YEAR}, {gc.PART_MONTH})")
gc.read_daily_csvs(spark, f"{SAMPLE}/events").write.format("delta").mode("append").saveAsTable(f"{SCHEMA}.events_bronze")
bronze_rows = spark.table(f"{SCHEMA}.events_bronze").count()
print(f"events_bronze: {bronze_rows:,} rows")


def run_notebook():
    """Execute every code cell of the notebook in one shared namespace, like Run All. Prints are captured
    so the contract-check verdict can be asserted."""
    cells = open(NOTEBOOK).read().split("# COMMAND ----------")
    g = {"spark": spark, "dbutils": dbutils, "display": display, "__name__": "__notebook__"}
    for i, cell in enumerate(cells):
        lines = [ln for ln in cell.splitlines() if not ln.startswith("# Databricks notebook source")]
        if any(ln.strip() == "# MAGIC %run ./00_config" for ln in lines):
            exec(compile(open("notebooks/00_config.py").read(), "00_config", "exec"), g)
            g["CATALOG"] = "spark_catalog"                       # local catalog instead of Unity Catalog
            g["EVENTS_BRONZE"] = f"spark_catalog.{SCHEMA}.events_bronze"
            g["EVENTS_CLEAN"] = f"spark_catalog.{SCHEMA}.events_clean"
            g["VOLUME_ROOT"] = f"{WORK}/Volumes/raw"
            print(f"--- cell {i}: %run ./00_config (local catalog)")
            continue
        if all(ln.startswith("# MAGIC") or not ln.strip() for ln in lines):
            continue                                              # markdown cell
        print(f"--- cell {i}")
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            exec(compile("\n".join(lines), f"{NOTEBOOK} cell {i}", "exec"), g)
        sys.stdout.write(buf.getvalue())
        OUTPUT.append(buf.getvalue())
    return g


def snapshot(table, pred):
    """Sorted rows of the processed months, without the timestamp columns that change on every run."""
    df = spark.table(table).where(pred)
    keep = [c for c in df.columns if c not in ("ingest_ts", "audited_at")]
    return sorted(tuple(r) for r in df.select(keep).collect())


EVENTS_CLEAN, CLEAN_AUDIT = f"{SCHEMA}.events_clean", f"{SCHEMA}.clean_audit"

print(f"===== run 1 (SAMPLE_MODE={SAMPLE_MODE})")
g1 = run_notebook()
pred = g1["MONTHS_PRED"]
assert g1["SAMPLE_MODE"] == (SAMPLE_MODE == "true")
clean1, audit1 = snapshot(EVENTS_CLEAN, pred), snapshot(CLEAN_AUDIT, pred)
assert len(clean1) == g1["tot"].rows_clean > 0
assert len(audit1) == len(gc.CLEANING_RULES) + 1      # one file month (2026-10): 8 drop rules + coordinates

# Table schemas: the DDL in the notebook and the to_typed() types agree.
ec_types = {f.name: f.dataType for f in spark.table(EVENTS_CLEAN).schema.fields}
for f in gc.typed_schema().fields:
    assert ec_types[f.name] == f.dataType, (f.name, ec_types[f.name], f.dataType)
assert spark.sql(f"DESCRIBE DETAIL {EVENTS_CLEAN}").first().partitionColumns == [gc.PART_YEAR, gc.PART_MONTH]

# Rows in two other months (see the docstring), copied from October with only the partition changed.
for table, n in ((EVENTS_CLEAN, 7), (CLEAN_AUDIT, 5)):
    for year, month in ((2020, 1), (2026, 9)):
        (spark.table(table).where(pred).limit(n)
         .withColumn(gc.PART_YEAR, F.lit(year)).withColumn(gc.PART_MONTH, F.lit(month))
         .write.format("delta").mode("append").saveAsTable(table))

print(f"===== run 2 (SAMPLE_MODE={SAMPLE_MODE}), re-run on the same bronze")
run_notebook()
clean2, audit2 = snapshot(EVENTS_CLEAN, pred), snapshot(CLEAN_AUDIT, pred)
assert clean2 == clean1, "re-run changed events_clean for the processed months"
assert audit2 == audit1, "re-run changed clean_audit for the processed months"


def month_rows(table, year, month):
    return spark.table(table).where(f"{gc.PART_YEAR} = {year} AND {gc.PART_MONTH} = {month}").count()


old = {(t, ym): month_rows(t, *ym) for t in (EVENTS_CLEAN, CLEAN_AUDIT) for ym in ((2020, 1), (2026, 9))}
print("rows in the seeded months after run 2:", {f"{t.split('.')[1]} {y}-{m:02d}": n for (t, (y, m)), n in old.items()})
assert old[(EVENTS_CLEAN, (2020, 1))] == 7 and old[(CLEAN_AUDIT, (2020, 1))] == 5   # outside every range: kept
if SAMPLE_MODE == "true":
    assert old[(EVENTS_CLEAN, (2026, 9))] == 7 and old[(CLEAN_AUDIT, (2026, 9))] == 5   # outside October: kept
else:
    assert old[(EVENTS_CLEAN, (2026, 9))] == 0 and old[(CLEAN_AUDIT, (2026, 9))] == 0   # inside the full range: replaced
print(f"events_clean {len(clean1):,} rows for {pred}; audit {len(audit1)} rows")

printed = "".join(OUTPUT)
assert "CONTRACT CHECK FAILED" not in printed, "Member 3 contract check failed"
contract = ("passed" if "CONTRACT CHECK PASSED" in printed else
            "skipped (dyad_common.py not importable)" if "contract check skipped" in printed else "NOT RUN")
assert contract != "NOT RUN", "the contract-check cell printed neither a pass nor a skip"
print(f"Member 3 contract check: {contract}")

spark.stop()
shutil.rmtree(WORK, ignore_errors=True)
print(f"NOTEBOOK TEST PASSED (SAMPLE_MODE={SAMPLE_MODE}: Run All twice, re-run identical, other months handled)")
