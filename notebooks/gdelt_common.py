"""Plain-Python helpers shared by the Databricks notebooks and the local tools/tests.

Kept free of Databricks-only APIs (dbutils, display) so the same code can be unit-tested with a
local Spark and imported from a notebook (Databricks Git folders put the repo on sys.path).

AI citation: Claude Code (Claude Opus 5.5, Anthropic) drafted this module (GDELT 1.0 column list,
index parsing, download/unzip helper, Spark reader options). Reviewed and tested by Nick Kunde-Lenny.
"""
import datetime as dt
import hashlib
import os
import re
import time
import urllib.request
import zipfile
from collections import OrderedDict

GDELT_EVENTS_BASE = "https://data.gdeltproject.org/events/"
DAILY_NAME_RE = re.compile(r"^(\d{8})\.export\.CSV\.zip$")

# --- GDELT 1.0 daily event file: 58 tab-separated columns, no header (GDELT 1.0 codebook order) ---
GDELT_V1_COLUMNS = [
    "GLOBALEVENTID", "SQLDATE", "MonthYear", "Year", "FractionDate",
    "Actor1Code", "Actor1Name", "Actor1CountryCode", "Actor1KnownGroupCode", "Actor1EthnicCode",
    "Actor1Religion1Code", "Actor1Religion2Code", "Actor1Type1Code", "Actor1Type2Code", "Actor1Type3Code",
    "Actor2Code", "Actor2Name", "Actor2CountryCode", "Actor2KnownGroupCode", "Actor2EthnicCode",
    "Actor2Religion1Code", "Actor2Religion2Code", "Actor2Type1Code", "Actor2Type2Code", "Actor2Type3Code",
    "IsRootEvent", "EventCode", "EventBaseCode", "EventRootCode", "QuadClass",
    "GoldsteinScale", "NumMentions", "NumSources", "NumArticles", "AvgTone",
    "Actor1Geo_Type", "Actor1Geo_FullName", "Actor1Geo_CountryCode", "Actor1Geo_ADM1Code",
    "Actor1Geo_Lat", "Actor1Geo_Long", "Actor1Geo_FeatureID",
    "Actor2Geo_Type", "Actor2Geo_FullName", "Actor2Geo_CountryCode", "Actor2Geo_ADM1Code",
    "Actor2Geo_Lat", "Actor2Geo_Long", "Actor2Geo_FeatureID",
    "ActionGeo_Type", "ActionGeo_FullName", "ActionGeo_CountryCode", "ActionGeo_ADM1Code",
    "ActionGeo_Lat", "ActionGeo_Long", "ActionGeo_FeatureID",
    "DATEADDED", "SOURCEURL",
]
assert len(GDELT_V1_COLUMNS) == 58

# Spark CSV options for GDELT: tab-separated, no header, and quoting OFF. GDELT does not quote fields,
# so a field that happened to start with '"' would make Spark's default parser glue lines together.
# On the October 2026 sample both settings give the same count (tests/test_load_local.py, check 3),
# so this is a precaution for the full history. An empty string disables quoting in Spark 3.
GDELT_CSV_OPTIONS = {"sep": "\t", "header": "false", "quote": "", "escape": "", "mode": "PERMISSIVE"}


def raw_string_schema():
    """Bronze schema: every GDELT column kept as STRING, exactly as published.
    Typing and cleaning happen in the cleaning step (Member 2), so a bad value never drops a row here."""
    from pyspark.sql.types import StructType, StructField, StringType
    return StructType([StructField(c, StringType(), True) for c in GDELT_V1_COLUMNS])


# --- GDELT index files ---------------------------------------------------------------------------
def _fetch_index(name, timeout=60):
    with urllib.request.urlopen(GDELT_EVENTS_BASE + name, timeout=timeout) as resp:
        text = resp.read().decode("utf-8", "replace")
    out = {}
    for line in text.splitlines():
        parts = line.split()
        if len(parts) == 2:
            out[parts[1]] = parts[0]
    return out


def list_daily_files(start, end):
    """Daily export files with file date in [start, end] (ISO strings or dates).
    Returns a list of dicts sorted by date: {date, name, url, md5, zip_bytes}."""
    start = dt.date.fromisoformat(str(start))
    end = dt.date.fromisoformat(str(end))
    md5s = _fetch_index("md5sums")
    sizes = _fetch_index("filesizes")
    files = []
    for name, md5 in md5s.items():
        m = DAILY_NAME_RE.match(name)
        if not m:
            continue
        day = dt.datetime.strptime(m.group(1), "%Y%m%d").date()
        if start <= day <= end:
            files.append({"date": day, "name": name, "url": GDELT_EVENTS_BASE + name,
                          "md5": md5, "zip_bytes": int(sizes.get(name, 0))})
    return sorted(files, key=lambda f: f["date"])


def group_by_month(files):
    """{(year, month): [file, ...]} in date order."""
    months = OrderedDict()
    for f in files:
        months.setdefault((f["date"].year, f["date"].month), []).append(f)
    return months


# --- download + unzip ----------------------------------------------------------------------------
def download_and_extract(file_info, dest_dir, tmp_dir="/tmp/gdelt_dl", attempts=3):
    """Download one daily ZIP (MD5-checked, retried) and write the CSV into dest_dir.
    Returns {name, csv_path, zip_bytes, csv_bytes, seconds}. Skips the download if the CSV is already there."""
    os.makedirs(dest_dir, exist_ok=True)
    os.makedirs(tmp_dir, exist_ok=True)
    csv_name = file_info["name"][:-4]  # strip ".zip"
    csv_path = os.path.join(dest_dir, csv_name)
    if os.path.exists(csv_path) and os.path.getsize(csv_path) > 0:
        return {"name": csv_name, "csv_path": csv_path, "zip_bytes": 0,
                "csv_bytes": os.path.getsize(csv_path), "seconds": 0.0, "skipped": True}

    t0 = time.time()
    zip_path = os.path.join(tmp_dir, file_info["name"])
    for attempt in range(1, attempts + 1):
        try:
            digest, zip_bytes = hashlib.md5(), 0
            with urllib.request.urlopen(file_info["url"], timeout=120) as resp, open(zip_path, "wb") as out:
                while True:
                    chunk = resp.read(1 << 20)
                    if not chunk:
                        break
                    out.write(chunk)
                    digest.update(chunk)
                    zip_bytes += len(chunk)
            if file_info.get("md5") and digest.hexdigest() != file_info["md5"]:
                raise IOError(f"md5 mismatch for {file_info['name']}")
            break
        except Exception:
            if attempt == attempts:
                raise
            time.sleep(5 * attempt)

    # Extract to a temp name and rename, so a half-written CSV never looks complete to a re-run.
    part_path = csv_path + ".part"
    with zipfile.ZipFile(zip_path) as zf, zf.open(zf.namelist()[0]) as src, open(part_path, "wb") as dst:
        while True:
            chunk = src.read(1 << 20)
            if not chunk:
                break
            dst.write(chunk)
    os.replace(part_path, csv_path)
    os.remove(zip_path)
    return {"name": csv_name, "csv_path": csv_path, "zip_bytes": zip_bytes,
            "csv_bytes": os.path.getsize(csv_path), "seconds": round(time.time() - t0, 1), "skipped": False}


# --- Spark: CSV landing folder -> bronze DataFrame -------------------------------------------------
# Partition columns. NOT "year"/"month": GDELT already has a "Year" column, and Spark column names
# are case-insensitive, so "year" would overwrite it (and CREATE TABLE would reject the duplicate).
PART_YEAR, PART_MONTH = "file_year", "file_month"


def read_daily_csvs(spark, path):
    """Read GDELT daily CSVs into the bronze shape: 58 string columns plus source_file, file_year, file_month, ingest_ts.
    file_year/file_month come from the FILE date (= DATEADDED), the same partitioning as HDFS on the local cluster."""
    from pyspark.sql import functions as F
    df = (spark.read.options(**GDELT_CSV_OPTIONS).schema(raw_string_schema()).csv(path)
          # _metadata.file_name, not input_file_name(): the latter is blocked on Unity Catalog / serverless.
          .withColumn("source_file", F.col("_metadata.file_name")))
    file_date = F.regexp_extract("source_file", r"^(\d{8})", 1)
    return (df.withColumn("ingest_ts", F.current_timestamp())
              .withColumn(PART_YEAR, F.substring(file_date, 1, 4).cast("int"))
              .withColumn(PART_MONTH, F.substring(file_date, 5, 2).cast("int")))
