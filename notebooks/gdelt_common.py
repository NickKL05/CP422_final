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


# =================================================================================================
# Member 2: GDELT 2.0 schema, typed schema and cleaning rules
# Pure DataFrame functions (no dbutils, no RDDs, no cache), so the same code runs on Databricks
# serverless (ANSI mode on) and in tests/test_clean_local.py.
# =================================================================================================

# --- GDELT 2.0 15-minute event file: 61 tab-separated columns, no header (GDELT 2.0 codebook order) ---
# Same fields as 1.0 plus one ADM2 code per geography, each placed right after the matching ADM1 code.
# DATEADDED is YYYYMMDDHHMMSS (UTC) here, instead of YYYYMMDD in the daily files.
GDELT_V2_COLUMNS = [
    "GLOBALEVENTID", "SQLDATE", "MonthYear", "Year", "FractionDate",
    "Actor1Code", "Actor1Name", "Actor1CountryCode", "Actor1KnownGroupCode", "Actor1EthnicCode",
    "Actor1Religion1Code", "Actor1Religion2Code", "Actor1Type1Code", "Actor1Type2Code", "Actor1Type3Code",
    "Actor2Code", "Actor2Name", "Actor2CountryCode", "Actor2KnownGroupCode", "Actor2EthnicCode",
    "Actor2Religion1Code", "Actor2Religion2Code", "Actor2Type1Code", "Actor2Type2Code", "Actor2Type3Code",
    "IsRootEvent", "EventCode", "EventBaseCode", "EventRootCode", "QuadClass",
    "GoldsteinScale", "NumMentions", "NumSources", "NumArticles", "AvgTone",
    "Actor1Geo_Type", "Actor1Geo_FullName", "Actor1Geo_CountryCode", "Actor1Geo_ADM1Code", "Actor1Geo_ADM2Code",
    "Actor1Geo_Lat", "Actor1Geo_Long", "Actor1Geo_FeatureID",
    "Actor2Geo_Type", "Actor2Geo_FullName", "Actor2Geo_CountryCode", "Actor2Geo_ADM1Code", "Actor2Geo_ADM2Code",
    "Actor2Geo_Lat", "Actor2Geo_Long", "Actor2Geo_FeatureID",
    "ActionGeo_Type", "ActionGeo_FullName", "ActionGeo_CountryCode", "ActionGeo_ADM1Code", "ActionGeo_ADM2Code",
    "ActionGeo_Lat", "ActionGeo_Long", "ActionGeo_FeatureID",
    "DATEADDED", "SOURCEURL",
]
assert len(GDELT_V2_COLUMNS) == 61

# Columns present in both versions, matched BY NAME (in 1.0 order). The 2.0 reader selects these by
# name, so the 3 extra ADM2 columns can never shift a value into the wrong field.
_V2_SET = set(GDELT_V2_COLUMNS)
SHARED_COLUMNS = [c for c in GDELT_V1_COLUMNS if c in _V2_SET]
assert len(SHARED_COLUMNS) == 58 and set(GDELT_V2_COLUMNS) - set(SHARED_COLUMNS) == {
    "Actor1Geo_ADM2Code", "Actor2Geo_ADM2Code", "ActionGeo_ADM2Code"}

# Same parsing rules as the daily files (tab-separated, no header, quoting off). Exposed on its own so
# Member 5's streaming job can use it: spark.readStream.options(**V2_CSV_OPTIONS).schema(v2_raw_schema())
V2_CSV_OPTIONS = dict(GDELT_CSV_OPTIONS)


def v2_raw_schema():
    """GDELT 2.0 raw schema: all 61 columns as STRING, exactly as published."""
    from pyspark.sql.types import StructType, StructField, StringType
    return StructType([StructField(c, StringType(), True) for c in GDELT_V2_COLUMNS])


def _try_cast_sql(col_name, sql_type):
    """SQL text for try_cast. try_cast returns NULL instead of raising, so a bad value never fails the
    job when ANSI mode is on (the default on Databricks serverless). Column.try_cast only exists in
    PySpark 4, so expressions are written as SQL text."""
    return f"try_cast(`{col_name}` AS {sql_type})"


def _digits_to_date_sql(col_name):
    """SQL text: 'YYYYMMDD' or 'YYYYMMDDHHMMSS' -> DATE; anything else (empty, wrong length, 20251301) -> NULL.
    Rebuilt as 'YYYY-MM-DD' and try_cast, not to_date(): to_date raises on bad input under ANSI mode,
    and a cast does not depend on the session's legacy time-parser policy."""
    d = f"regexp_extract(`{col_name}`, '^([0-9]{{8}})([0-9]{{6}})?$', 1)"
    return (f"CASE WHEN {d} = '' THEN CAST(NULL AS DATE) ELSE try_cast(concat_ws('-', substr({d}, 1, 4), "
            f"substr({d}, 5, 2), substr({d}, 7, 2)) AS DATE) END")


def _digits_to_timestamp_sql(col_name):
    """SQL text: 'YYYYMMDDHHMMSS' -> TIMESTAMP (GDELT 2.0 DATEADDED, UTC), else NULL.
    Interpreted in the session time zone; Databricks defaults to UTC, which matches GDELT."""
    d = f"regexp_extract(`{col_name}`, '^([0-9]{{14}})$', 1)"
    return (f"CASE WHEN {d} = '' THEN CAST(NULL AS TIMESTAMP) ELSE try_cast(concat(substr({d}, 1, 4), '-', "
            f"substr({d}, 5, 2), '-', substr({d}, 7, 2), ' ', substr({d}, 9, 2), ':', substr({d}, 11, 2), ':', "
            f"substr({d}, 13, 2)) AS TIMESTAMP) END")


def v2_to_shared(df, with_source_file=True):
    """Turn a raw 61-column GDELT 2.0 DataFrame (batch OR streaming) into the bronze shape:
    the 58 shared columns selected BY NAME (all STRING, DATEADDED still YYYYMMDDHHMMSS), plus
      dateadded_ts  TIMESTAMP  full DATEADDED (the 15-minute slot, UTC)
      source_file   STRING     file name from _metadata (only if with_source_file)
      ingest_ts     TIMESTAMP
      file_year, file_month INT  from the file name (fallback: DATEADDED), same meaning as in events_bronze.
    to_typed() then works on it unchanged."""
    from pyspark.sql import functions as F
    out = df.select(*[F.col(c) for c in SHARED_COLUMNS],
                    *([F.col("_metadata.file_name").alias("source_file")] if with_source_file else []))
    out = out.withColumn("dateadded_ts", F.expr(_digits_to_timestamp_sql("DATEADDED")))
    from_added = "regexp_extract(`DATEADDED`, '^([0-9]{8})', 1)"
    if with_source_file:
        file_date = f"coalesce(nullif(regexp_extract(`source_file`, '^([0-9]{{8}})', 1), ''), {from_added})"
    else:
        file_date = from_added
    out = out.withColumn("_file_date", F.expr(file_date))
    return (out.withColumn("ingest_ts", F.current_timestamp())
               .withColumn(PART_YEAR, F.expr("try_cast(substr(_file_date, 1, 4) AS INT)"))
               .withColumn(PART_MONTH, F.expr("try_cast(substr(_file_date, 5, 2) AS INT)"))
               .drop("_file_date"))


def read_15min_csvs(spark, path):
    """Read GDELT 2.0 15-minute export CSVs (61 columns) into the bronze shape (see v2_to_shared).
    For streaming, use the same pieces: v2_to_shared(spark.readStream.options(**V2_CSV_OPTIONS)
    .schema(v2_raw_schema()).csv(folder))."""
    return v2_to_shared(spark.read.options(**V2_CSV_OPTIONS).schema(v2_raw_schema()).csv(path))


# --- Typed schema (silver) ----------------------------------------------------------------------
# Contract with Member 3 (notebooks/dyad_common.py on the member3 branch): events_clean keeps the ORIGINAL
# GDELT column names, because dyad_common.resolve_events_source() looks them up by name, and it uses the
# same types as dyad_common.typed_events(): GLOBALEVENTID and every CAMEO code are STRING (UPPER(TRIM()),
# '' -> NULL), SQLDATE/DATEADDED are DATE, QuadClass/NumMentions/IsRootEvent are INT, GoldsteinScale/AvgTone
# are DOUBLE. Codes stay strings on purpose: '010' and '10' are different CAMEO event codes, and FeatureIDs
# can be letters. Dates become DATE, counts INT, scores and coordinates DOUBLE.
_TYPED_OVERRIDES = {
    "SQLDATE": "DATE", "MonthYear": "INT", "Year": "INT", "FractionDate": "DOUBLE",
    "IsRootEvent": "INT", "QuadClass": "INT",
    "GoldsteinScale": "DOUBLE", "NumMentions": "INT", "NumSources": "INT", "NumArticles": "INT", "AvgTone": "DOUBLE",
    "Actor1Geo_Type": "INT", "Actor1Geo_Lat": "DOUBLE", "Actor1Geo_Long": "DOUBLE",
    "Actor2Geo_Type": "INT", "Actor2Geo_Lat": "DOUBLE", "Actor2Geo_Long": "DOUBLE",
    "ActionGeo_Type": "INT", "ActionGeo_Lat": "DOUBLE", "ActionGeo_Long": "DOUBLE",
    "DATEADDED": "DATE",
}
TYPED_COLUMN_TYPES = [(c, _TYPED_OVERRIDES.get(c, "STRING")) for c in GDELT_V1_COLUMNS]

# Upper-cased as well as trimmed, exactly like dyad_common._code(), so Member 3's typing is a no-op on them.
UPPER_CODE_COLUMNS = ("GLOBALEVENTID", "Actor1CountryCode", "Actor2CountryCode",
                      "EventCode", "EventBaseCode", "EventRootCode")

# The three latitude/longitude pairs. Invalid coordinates are set to NULL, not dropped (see to_typed).
GEO_PAIRS = [("Actor1Geo_Lat", "Actor1Geo_Long"), ("Actor2Geo_Lat", "Actor2Geo_Long"),
             ("ActionGeo_Lat", "ActionGeo_Long")]
LAT_MIN, LAT_MAX = -90.0, 90.0
LON_MIN, LON_MAX = -180.0, 180.0

# Helper column added by to_typed(): TRUE if any coordinate value in the row was set to NULL because it was
# invalid. Used only by the audit; clean_rows() drops it.
COORDS_NULLED_COL = "_coords_nulled"


def typed_schema():
    """The explicit typed StructType of the 58 GDELT columns after to_typed() (for documentation and DDL)."""
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
    spark_types = {"STRING": StringType(), "INT": IntegerType(), "DOUBLE": DoubleType(), "DATE": DateType()}
    return StructType([StructField(c, spark_types[t], True) for c, t in TYPED_COLUMN_TYPES])


def _geo_pair_sql(lat, lon):
    """SQL texts (lat, lon, nulled) for one coordinate pair.

    A pair is kept only if BOTH values parse as numbers, latitude is in [-90, 90], longitude is in
    [-180, 180], and it is not exactly (0, 0), GDELT's placeholder in the Gulf of Guinea. Otherwise BOTH
    sides become NULL: half a pair (GDELT's ActionGeo_Type 0 rows sometimes carry a stray longitude) is no
    location at all. NaN and Infinity fail the range test, because NaN compares greater than every number.
    `nulled` is TRUE when the raw pair had any value and the typed pair ended up NULL."""
    la = f"try_cast(nullif(trim(`{lat}`), '') AS DOUBLE)"
    lo = f"try_cast(nullif(trim(`{lon}`), '') AS DOUBLE)"
    ok = (f"coalesce({la} BETWEEN {LAT_MIN} AND {LAT_MAX} AND {lo} BETWEEN {LON_MIN} AND {LON_MAX} "
          f"AND NOT ({la} = 0 AND {lo} = 0), false)")
    any_raw = f"(nullif(trim(`{lat}`), '') IS NOT NULL OR nullif(trim(`{lon}`), '') IS NOT NULL)"
    return f"CASE WHEN {ok} THEN {la} END", f"CASE WHEN {ok} THEN {lo} END", f"({any_raw} AND NOT {ok})"


def to_typed(df):
    """Cast the STRING bronze columns to their proper types without ever raising (ANSI-safe).
    Works on events_bronze (GDELT 1.0) and on read_15min_csvs() output (GDELT 2.0 shared columns):
    DATEADDED may be YYYYMMDD or YYYYMMDDHHMMSS; both become a DATE.
    Blank strings become NULL. Unparseable numbers or dates become NULL (the cleaning rules then decide
    what to drop). Invalid coordinate pairs become NULL (see _geo_pair_sql), and COORDS_NULLED_COL records
    that for the audit. Columns that are not GDELT fields (source_file, file_year, ...) pass through."""
    from pyspark.sql import functions as F
    geo = {}
    nulled = []
    for lat, lon in GEO_PAIRS:
        la, lo, nl = _geo_pair_sql(lat, lon)
        geo[lat], geo[lon] = la, lo
        nulled.append(nl)
    exprs = []
    for c, t in TYPED_COLUMN_TYPES:
        if c in geo:
            e = geo[c]
        elif t == "STRING":
            e = f"nullif(upper(trim(`{c}`)), '')" if c in UPPER_CODE_COLUMNS else f"nullif(trim(`{c}`), '')"
        elif t == "DATE":
            e = _digits_to_date_sql(c)
        else:
            e = f"try_cast(nullif(trim(`{c}`), '') AS {t})"
        exprs.append(F.expr(e).alias(c))
    extras = [F.col(c) for c in df.columns if c not in set(GDELT_V1_COLUMNS)]
    return df.select(*exprs, *extras, F.expr(" OR ".join(nulled)).alias(COORDS_NULLED_COL))


# --- CAMEO lookups (repo folder lookups/, shared with Member 3) -----------------------------------
# The same four files Member 3's dyad_common.load_lookups() reads, so both of us classify codes the same way:
# the official CAMEO country list, the 25 region codes that are not states, and the sub-state remap
# (WSB and GZS -> PSE, BAG -> IRQ). See lookups/README.md for sources and checksums.
CAMEO_LOOKUP_FILES = {"countries": "CAMEO.country.txt", "eventcodes": "CAMEO.eventcodes.txt",
                      "nonstate": "cameo_nonstate_codes.txt", "remap": "cameo_code_remap.txt"}


def find_lookups_dir(search_paths=None):
    """The repo's lookups/ folder: a sibling of notebooks/ (Databricks Git folders put notebooks/ on sys.path)."""
    import sys
    for p in (search_paths if search_paths is not None else sys.path):
        for cand in (os.path.join(p or ".", "..", "lookups"), os.path.join(p or ".", "lookups")):
            cand = os.path.normpath(cand)
            if os.path.isfile(os.path.join(cand, CAMEO_LOOKUP_FILES["countries"])):
                return cand
    raise FileNotFoundError("lookups/ not found next to the notebooks; open the notebooks from the repo's Git folder")


def _read_lookup_rows(path):
    """Rows of a tab-separated lookup with a header row. The GDELT files use CRLF, so '\\r' is stripped."""
    rows = []
    with open(path, encoding="utf-8", errors="replace") as fh:
        for i, line in enumerate(fh):
            line = line.rstrip("\r\n")
            if i == 0 or not line.strip():
                continue
            rows.append([part.strip() for part in line.split("\t")])
    return rows


def load_cameo_lookups(lookups_dir=None):
    """The lookups as plain dicts/sets. states = CAMEO country codes that are neither a region nor a remapped
    sub-state code (233 with the committed files). Every remap target must itself be a state."""
    d = lookups_dir or find_lookups_dir()
    read = {k: {r[0]: r[1] for r in _read_lookup_rows(os.path.join(d, f))} for k, f in CAMEO_LOOKUP_FILES.items()}
    remap, nonstate = read["remap"], read["nonstate"]
    states = {c for c in read["countries"] if c not in nonstate and c not in remap}
    bad_targets = sorted(t for t in remap.values() if t not in states)
    if bad_targets:
        raise ValueError(f"remap targets that are not states: {bad_targets}")
    return {"dir": d, "countries": read["countries"], "eventcodes": read["eventcodes"], "nonstate": nonstate,
            "remap": remap, "states": states}


# --- Cleaning rules -----------------------------------------------------------------------------
# The same rules, in the same order and with the same names, as Member 3's fallback
# (dyad_common.flag_events / REASONS), so Member 3's notebooks give identical results whether they read
# events_clean or clean events_bronze themselves. Each row is tagged with the FIRST rule it fails, so every
# dropped row is counted exactly once and the audit adds up: rows_in = clean + sum(rows dropped).
CLEANING_RULES = ["missing_event_id", "duplicate_event_id", "invalid_date", "missing_country", "region_code",
                  "unknown_code", "same_country", "invalid_measure"]
COORDS_RULE = "invalid_coordinates"   # not a drop rule: the values are set to NULL and the row is kept
DROP_RULE_COL = "drop_rule"           # NULL = clean row

# Not applied here, on purpose (agreed with Member 3): the recency window 0 <= DATEADDED - SQLDATE <= N.
# Member 3 chooses N from the lag distribution of exactly these clean rows, so events_clean must still
# contain the old events. The lag is cheap to compute: datediff(DATEADDED, SQLDATE).


def _remapped(col_name, remap):
    from pyspark.sql import functions as F
    if not remap:
        return F.col(col_name)
    m = F.create_map(*[F.lit(x) for kv in sorted(remap.items()) for x in kv])
    return F.coalesce(m[F.col(col_name)], F.col(col_name))


def tag_drop_rule(typed_df, lookups):
    """Add DROP_RULE_COL: the first cleaning rule the row fails, or NULL if it passes all of them.
    Input: the output of to_typed(), and load_cameo_lookups().

    1. missing_event_id    no GLOBALEVENTID.
    2. duplicate_event_id  a later copy of an ID already seen. The earliest report is kept (smallest
                           DATEADDED, then most NumMentions, as in Member 3's fallback), then smallest
                           source_file, SOURCEURL and a row hash, so the choice is fully deterministic.
    3. invalid_date        SQLDATE or DATEADDED missing or not a real date.
    4. missing_country     Actor1CountryCode or Actor2CountryCode missing: no dyad.
    5. region_code         either actor is a region (EUR, MEA, ...), not a state.
    6. unknown_code        either actor code is not in the CAMEO country list.
    7. same_country        both actors are the same state AFTER the sub-state remap (WSB/GZS -> PSE,
                           BAG -> IRQ), so a WSB-PSE event is domestic.
    8. invalid_measure     AvgTone or QuadClass missing, or NumMentions missing or below 1.
    Steps 4-7 compare the REMAPPED codes; the table itself keeps the original codes (Member 3 remaps again,
    which is idempotent)."""
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    gdelt_cols = [c for c, _ in TYPED_COLUMN_TYPES]
    order = [F.col("DATEADDED").asc_nulls_last(), F.col("NumMentions").desc_nulls_last()]
    if "dateadded_ts" in typed_df.columns:          # GDELT 2.0: the 15-minute slot breaks ties within a day
        order.append(F.col("dateadded_ts").asc_nulls_last())
    if "source_file" in typed_df.columns:
        order.append(F.col("source_file").asc_nulls_last())
    order += [F.col("SOURCEURL").asc_nulls_last(), F.xxhash64(*gdelt_cols).asc()]
    rn = F.row_number().over(Window.partitionBy("GLOBALEVENTID").orderBy(*order))

    regions, states = sorted(lookups["nonstate"]), sorted(lookups["states"])
    a, b = _remapped("Actor1CountryCode", lookups["remap"]), _remapped("Actor2CountryCode", lookups["remap"])
    rule = (F.when(F.col("GLOBALEVENTID").isNull(), F.lit("missing_event_id"))
             .when(rn > 1, F.lit("duplicate_event_id"))
             .when(F.col("SQLDATE").isNull() | F.col("DATEADDED").isNull(), F.lit("invalid_date"))
             .when(a.isNull() | b.isNull(), F.lit("missing_country"))
             .when(a.isin(regions) | b.isin(regions), F.lit("region_code"))
             .when(~a.isin(states) | ~b.isin(states), F.lit("unknown_code"))
             .when(a == b, F.lit("same_country"))
             .when(F.col("AvgTone").isNull() | F.col("QuadClass").isNull() | F.col("NumMentions").isNull()
                   | (F.col("NumMentions") < 1), F.lit("invalid_measure"))
             .otherwise(F.lit(None).cast("string")))
    return typed_df.withColumn(DROP_RULE_COL, rule)


def clean_rows(tagged_df):
    """Rows that pass every rule, without the helper columns."""
    from pyspark.sql import functions as F
    return tagged_df.where(F.col(DROP_RULE_COL).isNull()).drop(DROP_RULE_COL, COORDS_NULLED_COL)


def cleaning_audit(tagged_df):
    """Data-quality audit in ONE aggregation pass: per file month, rows in, and the rows each rule affects.
    Returns (file_year, file_month, rule_order, rule, action, rows_in, rows_clean, rows_affected, pct):
      * one row per drop rule (action 'drop'): rows_affected = rows dropped by that rule (first failing rule);
      * one row for invalid_coordinates (action 'set_null'): clean rows kept with a coordinate pair set to NULL.
    pct is rows_affected as a percentage of rows_in. rows_in = rows_clean + sum(rows_affected of the drops)."""
    from pyspark.sql import functions as F
    rule = F.col(DROP_RULE_COL)
    aggs = [F.count(F.lit(1)).alias("rows_in"), F.count(F.when(rule.isNull(), 1)).alias("rows_clean")]
    aggs += [F.count(F.when(rule == r, 1)).alias(f"d_{i}") for i, r in enumerate(CLEANING_RULES)]
    aggs.append(F.count(F.when(rule.isNull() & F.col(COORDS_NULLED_COL), 1)).alias("d_coords"))
    per_month = tagged_df.groupBy(PART_YEAR, PART_MONTH).agg(*aggs)
    # Unpivot the small aggregated result (one column per rule -> one row per rule); no second pass over the data.
    entries = [f"{i + 1}, '{r}', 'drop', d_{i}" for i, r in enumerate(CLEANING_RULES)]
    entries.append(f"{len(CLEANING_RULES) + 1}, '{COORDS_RULE}', 'set_null', d_coords")
    long = per_month.select(PART_YEAR, PART_MONTH, "rows_in", "rows_clean",
                            F.expr(f"stack({len(entries)}, {', '.join(entries)}) AS (rule_order, rule, action, rows_affected)"))
    return (long.withColumn("pct", F.round(F.col("rows_affected") * 100.0 / F.col("rows_in"), 3))
                .select(PART_YEAR, PART_MONTH, F.col("rule_order").cast("int").alias("rule_order"), "rule", "action",
                        F.col("rows_in").cast("long").alias("rows_in"),
                        F.col("rows_clean").cast("long").alias("rows_clean"),
                        F.col("rows_affected").cast("long").alias("rows_affected"), "pct"))


def audit_by_year(audit_df):
    """Roll the monthly audit up to one row per (file_year, rule)."""
    from pyspark.sql import functions as F
    return (audit_df.groupBy(PART_YEAR, "rule_order", "rule", "action")
            .agg(F.sum("rows_in").alias("rows_in"), F.sum("rows_clean").alias("rows_clean"),
                 F.sum("rows_affected").alias("rows_affected"))
            .withColumn("pct", F.round(F.col("rows_affected") * 100.0 / F.col("rows_in"), 3))
            .orderBy(PART_YEAR, "rule_order"))


def month_range_predicate(start, end):
    """SQL predicate on the partition columns for every month touched by [start, end] (ISO dates),
    e.g. '(file_year = 2025 AND file_month IN (11, 12)) OR (file_year = 2026 AND file_month IN (1, 2))'.
    Written as plain equality/IN on file_year/file_month so Delta prunes partitions and replaceWhere
    only replaces those months."""
    start = dt.date.fromisoformat(str(start))
    end = dt.date.fromisoformat(str(end))
    assert start <= end, (start, end)
    by_year = OrderedDict()
    y, m = start.year, start.month
    while (y, m) <= (end.year, end.month):
        by_year.setdefault(y, []).append(m)
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return " OR ".join(f"({PART_YEAR} = {yr} AND {PART_MONTH} IN ({', '.join(map(str, ms))}))"
                       for yr, ms in by_year.items())


# --- Lookup files in the repo ----------------------------------------------------------------------
def repo_path(*parts):
    """Absolute path of a repo folder or file (e.g. repo_path("hadoop", "lookups")), found next to the
    notebooks folder on sys.path (Databricks Git folders put the notebook's folder there; 00_config
    adds it too). Returns None if the notebooks were imported without the rest of the repo."""
    import sys
    for p in sys.path:
        cand = os.path.normpath(os.path.join(p or ".", "..", *parts))
        if os.path.exists(cand):
            return cand
    return None


def read_lookup_tsv(spark, path, columns=None):
    """Small tab-separated lookup (e.g. lookups/CAMEO.eventcodes.txt) -> DataFrame, all STRING.
    Read with plain Python on the driver, then spark.createDataFrame, because serverless compute has no
    spark.read access to workspace files. The first line is the header unless `columns` is given
    (country_centroids.tsv has no header). CRLF or LF line endings. Codes stay strings ("010" != "10").
    Meant for files of a few thousand rows; join the result with F.broadcast()."""
    from pyspark.sql.types import StructType, StructField, StringType
    with open(path, encoding="utf-8", errors="replace") as fh:
        rows = [line.rstrip("\r\n").split("\t") for line in fh if line.strip()]
    if columns is None:
        columns, rows = rows[0], rows[1:]
    bad = [r for r in rows if len(r) != len(columns)]
    assert not bad, f"{path}: {len(bad)} rows do not have {len(columns)} fields, e.g. {bad[0]}"
    schema = StructType([StructField(c, StringType(), True) for c in columns])
    return spark.createDataFrame(rows, schema)
