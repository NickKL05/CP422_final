# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Typed schema and cleaning (Member 2)
# MAGIC
# MAGIC Reads **`workspace.gdelt.events_bronze`** (58 STRING columns, loaded by `01_load_gdelt`) for the selected file-date range,
# MAGIC casts every column to its proper type, applies the cleaning rules and writes:
# MAGIC
# MAGIC | Table | What it is |
# MAGIC | --- | --- |
# MAGIC | `workspace.gdelt.events_clean` (`EVENTS_CLEAN` in `00_config`) | Typed, cleaned events with the **original GDELT column names**. Delta, partitioned by `file_year`, `file_month` (file date = `DATEADDED`, same as bronze) |
# MAGIC | `workspace.gdelt.clean_audit` | Data-quality audit: rows in and rows affected by each rule, per file month |
# MAGIC
# MAGIC **Contract with Member 3.** `03_dyad_tone_index`, `04_actor_network` and `05_skew_salting_ab` read `EVENTS_CLEAN` when it exists
# MAGIC (`dyad_common.resolve_events_source`). They look columns up by their original GDELT names and re-apply their own typing and rules,
# MAGIC so this table uses the same types as `dyad_common.typed_events()` and the same rules, in the same order, with the same lookups
# MAGIC (`lookups/`). Member 3's notebooks then switch over with no code change and give the same results. Section 7 checks this live.
# MAGIC
# MAGIC Both tables are written with `replaceWhere` on the processed months only, so a sample-mode run (October 2026)
# MAGIC replaces October and never wipes full-year data written by an earlier full run. All the logic lives in
# MAGIC `gdelt_common.py` (`to_typed`, `tag_drop_rule`, `clean_rows`, `cleaning_audit`) and is tested locally by `tests/test_clean_local.py`
# MAGIC with ANSI mode on, the same setting as serverless compute. `tests/test_clean_notebook_local.py` runs this notebook itself, twice, on local Delta tables.

# COMMAND ----------

# Declared here, not only in 00_config: widgets created inside a %run notebook don't show in this
# notebook's parameter bar. Every top-level notebook starts with this cell.
dbutils.widgets.dropdown("sample_mode", "true", ["true", "false"], "SAMPLE_MODE")

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

import time
from pyspark.sql import functions as F
import gdelt_common as gc

# EVENTS_CLEAN comes from 00_config (Member 3's notebooks read the same name). The audit table is only ours.
CLEAN_AUDIT = f"{CATALOG}.{SCHEMA}.clean_audit"     # rows affected by each cleaning rule per file month (Member 2)

# CAMEO lookups from the repo's lookups/ folder: the same files Member 3's dyad_common.load_lookups() reads.
LK = gc.load_cameo_lookups()

# Months touched by RANGE_START..RANGE_END, written on the PARTITION columns so Delta prunes partitions
# when reading bronze, and replaceWhere replaces exactly these months.
MONTHS_PRED = gc.month_range_predicate(RANGE_START, RANGE_END)
print(f"range {RANGE_START}..{RANGE_END}\nmonths predicate: {MONTHS_PRED}")
print(f"rules (in order): {gc.CLEANING_RULES}, then {gc.COORDS_RULE} (set to NULL, row kept)")
print(f"lookups from {LK['dir']}: {len(LK['countries'])} CAMEO country codes, {len(LK['states'])} states, "
      f"{len(LK['nonstate'])} region codes, remap {LK['remap']}")

if not spark.catalog.tableExists(EVENTS_BRONZE):
    raise RuntimeError(f"{EVENTS_BRONZE} does not exist yet: Run All of 01_load_gdelt first (it loads the sample month).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Explicit schemas
# MAGIC
# MAGIC * **GDELT 1.0 daily files** (`YYYYMMDD.export.CSV`): 58 tab-separated columns, no header. Bronze keeps them as STRING (`gc.raw_string_schema()`).
# MAGIC * **GDELT 2.0 15-minute files** (`YYYYMMDDHHMMSS.export.CSV`): 61 columns (`gc.v2_raw_schema()`). The 3 extra columns are
# MAGIC   `Actor1Geo_ADM2Code`, `Actor2Geo_ADM2Code`, `ActionGeo_ADM2Code`, each right after the matching `*_ADM1Code`, and `DATEADDED` is
# MAGIC   `YYYYMMDDHHMMSS` instead of `YYYYMMDD`. `gc.read_15min_csvs()` selects the 58 shared columns **by name**, so the extra columns can't shift values.
# MAGIC * **Typed schema** (`gc.typed_schema()`): dates as DATE, counts as INT, scores and coordinates as DOUBLE. CAMEO codes stay STRING because
# MAGIC   they have leading zeros (`061`). Every cast is a `try_cast`, so a bad value becomes NULL instead of failing the job under ANSI mode.
# MAGIC   The cleaning rules then decide what to drop.

# COMMAND ----------

typed_types = dict(gc.TYPED_COLUMN_TYPES)
v1_pos = {c: i + 1 for i, c in enumerate(gc.GDELT_V1_COLUMNS)}
schema_rows = [(i + 1, c, v1_pos.get(c), typed_types.get(c, "STRING (2.0 only, not kept)"))
               for i, c in enumerate(gc.GDELT_V2_COLUMNS)]
display(spark.createDataFrame(schema_rows, "v2_position INT, column STRING, v1_position INT, typed_type STRING"))
print(f"GDELT 1.0: {len(gc.GDELT_V1_COLUMNS)} columns, GDELT 2.0: {len(gc.GDELT_V2_COLUMNS)} columns, shared by name: {len(gc.SHARED_COLUMNS)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Cleaning rules
# MAGIC
# MAGIC The same rules, order and names as Member 3's fallback (`dyad_common.flag_events`). Each row is tagged with the **first** rule
# MAGIC it fails, so a dropped row is counted once and the audit adds up (`rows_in = clean + rows dropped`).
# MAGIC
# MAGIC | # | Rule | Drops a row when | Why |
# MAGIC | --- | --- | --- | --- |
# MAGIC | 1 | `missing_event_id` | no `GLOBALEVENTID` | An event without an ID can't be de-duplicated or joined |
# MAGIC | 2 | `duplicate_event_id` | a later copy of an ID already kept | The same event must not be counted twice. The **earliest** report is kept (smallest `DATEADDED`, then most `NumMentions`, as Member 3 does), then smallest `source_file`, `SOURCEURL` and a row hash, so re-runs give identical tables |
# MAGIC | 3 | `invalid_date` | `SQLDATE` or `DATEADDED` missing or not a real date | Weekly windows need a real event date |
# MAGIC | 4 | `missing_country` | `Actor1CountryCode` or `Actor2CountryCode` missing | The question is about conflict *between states*: no second country, no dyad. `Actor*Geo_CountryCode` is not a fallback (FIPS codes, not CAMEO) |
# MAGIC | 5 | `region_code` | either actor is one of the 25 regions in `lookups/cameo_nonstate_codes.txt` (`EUR`, `MEA`, ...) | A region is not a state |
# MAGIC | 6 | `unknown_code` | an actor code is not in `lookups/CAMEO.country.txt` | Reported, not guessed |
# MAGIC | 7 | `same_country` | both actors are the same state **after** the remap `WSB`, `GZS` → `PSE` and `BAG` → `IRQ` (`lookups/cameo_code_remap.txt`) | Domestic events are not dyads. A `WSB`–`PSE` event becomes `PSE`–`PSE` and is dropped |
# MAGIC | 8 | `invalid_measure` | `AvgTone` or `QuadClass` missing, or `NumMentions` missing or below 1 | Tone and the mentions weight are the inputs of the index |
# MAGIC
# MAGIC **Invalid coordinates are set to NULL, not dropped** (`invalid_coordinates`, counted in the audit with action `set_null`). For each of the
# MAGIC three geographies, a latitude/longitude pair is kept only if both parse, latitude is in [-90, 90], longitude in [-180, 180], and it
# MAGIC isn't exactly (0, 0), a placeholder in the Gulf of Guinea. Otherwise both values become NULL. Dropping the row would remove valid tone
# MAGIC and conflict data that Member 3 uses, and the dyad analysis doesn't use coordinates.
# MAGIC
# MAGIC **No recency rule here.** Articles mention events from up to ~10 years earlier. Member 3 keeps reports with `0 <= DATEADDED - SQLDATE <= N`
# MAGIC and chooses N from the lag distribution of these clean rows, so the old events stay in this table. The table also keeps the
# MAGIC **original** actor codes (`WSB`, not `PSE`); Member 3's remap is idempotent.
# MAGIC
# MAGIC Duplicates are found within the processed range (one month in sample mode, the whole range in a full run).

# COMMAND ----------

bronze = spark.table(EVENTS_BRONZE).where(MONTHS_PRED)
tagged = gc.tag_drop_rule(gc.to_typed(bronze), LK)
clean = gc.clean_rows(tagged)

if bronze.limit(1).count() == 0:
    raise RuntimeError(f"{EVENTS_BRONZE} has no rows for {RANGE_START}..{RANGE_END}: Run All of 01_load_gdelt first.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Write `events_clean` and `clean_audit` (only the processed months are replaced)

# COMMAND ----------

typed_ddl = ",\n  ".join(f"`{c}` {t}" for c, t in gc.TYPED_COLUMN_TYPES)
CLEAN_COLUMNS = list(gc.TYPED_COLUMN_TYPES) + [("source_file", "STRING"), ("ingest_ts", "TIMESTAMP"),
                                               (gc.PART_YEAR, "INT"), (gc.PART_MONTH, "INT")]
AUDIT_COLUMNS = [(gc.PART_YEAR, "INT"), (gc.PART_MONTH, "INT"), ("rule_order", "INT"), ("rule", "STRING"),
                 ("action", "STRING"), ("rows_in", "BIGINT"), ("rows_clean", "BIGINT"), ("rows_affected", "BIGINT"),
                 ("pct", "DOUBLE"), ("audited_at", "TIMESTAMP")]


def drop_if_layout_changed(table, expected):
    """An existing table written by an older version of this notebook (different columns or types) is dropped and
    recreated below, rather than failing the write. Its rows came from the old rules, so they must not be mixed in."""
    if not spark.catalog.tableExists(table):
        return
    have = [(f.name.lower(), f.dataType.simpleString().upper()) for f in spark.table(table).schema.fields]
    if have != [(c.lower(), t) for c, t in expected]:
        print(f"note: {table} has an older layout; dropping it so it is recreated with the current one")
        spark.sql(f"DROP TABLE {table}")


drop_if_layout_changed(EVENTS_CLEAN, CLEAN_COLUMNS)
drop_if_layout_changed(CLEAN_AUDIT, AUDIT_COLUMNS)
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {EVENTS_CLEAN} (
  {typed_ddl},
  source_file STRING,
  ingest_ts TIMESTAMP,
  {gc.PART_YEAR} INT,
  {gc.PART_MONTH} INT
) USING DELTA
PARTITIONED BY ({gc.PART_YEAR}, {gc.PART_MONTH})
COMMENT 'GDELT 1.0 events, typed and cleaned (rules: {", ".join(gc.CLEANING_RULES)}; invalid coordinates set to NULL). Original GDELT column names. Partitioned by file date. Written by 02_clean_events.'
""")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CLEAN_AUDIT} (
  {", ".join(f"{c} {t}" for c, t in AUDIT_COLUMNS)}
) USING DELTA
COMMENT 'Rows affected by each cleaning rule per file month: dropped (first failing rule only) or coordinates set to NULL. Written by 02_clean_events.'
""")

t0 = time.time()
(clean.select(spark.table(EVENTS_CLEAN).columns)
      .write.format("delta").mode("overwrite")
      .option("replaceWhere", MONTHS_PRED)
      .saveAsTable(EVENTS_CLEAN))
clean_s = time.time() - t0

# The audit recomputes the tagging (cache/persist is not available on serverless). That is safe because
# the dedup order is deterministic, so both passes tag every row the same way.
t0 = time.time()
(gc.cleaning_audit(tagged)
   .withColumn("audited_at", F.current_timestamp())
   .select(spark.table(CLEAN_AUDIT).columns)
   .write.format("delta").mode("overwrite")
   .option("replaceWhere", MONTHS_PRED)
   .saveAsTable(CLEAN_AUDIT))
audit_s = time.time() - t0
print(f"wrote {EVENTS_CLEAN} in {clean_s:.0f}s and {CLEAN_AUDIT} in {audit_s:.0f}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Data-quality audit (A&B): rows affected by each rule

# COMMAND ----------

audit = spark.table(CLEAN_AUDIT).where(MONTHS_PRED)

# Whole processed range, per rule
months = audit.select(gc.PART_YEAR, gc.PART_MONTH, "rows_in", "rows_clean").distinct()
tot = months.agg(F.sum("rows_in").alias("rows_in"), F.sum("rows_clean").alias("rows_clean")).first()
per_rule = (audit.groupBy("rule_order", "rule", "action").agg(F.sum("rows_affected").alias("rows_affected"))
            .withColumn("pct_of_rows_in", F.round(F.col("rows_affected") * 100.0 / F.lit(tot.rows_in), 3))
            .orderBy("rule_order"))
print(f"{RANGE_START}..{RANGE_END}: rows_in {tot.rows_in:,}  clean {tot.rows_clean:,} "
      f"({tot.rows_clean * 100.0 / tot.rows_in:.2f}%)")
display(per_rule)

# COMMAND ----------

# Per year
display(gc.audit_by_year(audit))

# COMMAND ----------

# Per month (one column per rule, easier to read than the long table)
display(audit.groupBy(gc.PART_YEAR, gc.PART_MONTH, "rows_in", "rows_clean")
             .pivot("rule", gc.CLEANING_RULES + [gc.COORDS_RULE]).agg(F.first("rows_affected"))
             .orderBy(gc.PART_YEAR, gc.PART_MONTH))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Sanity checks

# COMMAND ----------

ec = spark.table(EVENTS_CLEAN).where(MONTHS_PRED)
remap = F.create_map(*[F.lit(x) for kv in sorted(LK["remap"].items()) for x in kv])
a1 = F.coalesce(remap[F.col("Actor1CountryCode")], F.col("Actor1CountryCode"))
a2 = F.coalesce(remap[F.col("Actor2CountryCode")], F.col("Actor2CountryCode"))
states = sorted(LK["states"])
lat_lon = [(F.col(la), F.col(lo)) for la, lo in gc.GEO_PAIRS]
chk = ec.agg(
    F.count(F.lit(1)).alias("rows"),
    F.countDistinct("GLOBALEVENTID").alias("ids"),
    F.count(F.when(F.col("GLOBALEVENTID").isNull(), 1)).alias("null_id"),
    F.count(F.when(F.col("SQLDATE").isNull() | F.col("DATEADDED").isNull(), 1)).alias("null_date"),
    F.count(F.when(~a1.isin(states) | ~a2.isin(states) | a1.isNull() | a2.isNull(), 1)).alias("not_states"),
    F.count(F.when(a1 == a2, 1)).alias("same_country"),
    F.count(F.when(F.col("AvgTone").isNull() | F.col("QuadClass").isNull() | F.col("NumMentions").isNull()
                   | (F.col("NumMentions") < 1), 1)).alias("bad_measure"),
    # greatest() of the per-pair flags = "any pair is bad" (a NULL pair gives NULL, which greatest() skips)
    F.count(F.when(F.greatest(*[(la.isNull() != lo.isNull()) | ~la.between(-90, 90) | ~lo.between(-180, 180)
                                | ((la == 0) & (lo == 0)) for la, lo in lat_lon]), 1)).alias("bad_coords"),
    F.count(F.when(F.datediff("DATEADDED", "SQLDATE") > 30, 1)).alias("older_30d"),
    F.min("SQLDATE").alias("min_sqldate"), F.max("SQLDATE").alias("max_sqldate")).first()
dropped_total = audit.where("action = 'drop'").agg(F.sum("rows_affected")).first()[0]
bronze_rows = bronze.count()
code_types = {c: t for c, t in ec.dtypes if c in ("GLOBALEVENTID", "EventCode", "EventBaseCode", "EventRootCode")}

print(f"bronze rows {bronze_rows:,} | audit rows_in {tot.rows_in:,} | clean {chk.rows:,} + dropped {dropped_total:,} = {chk.rows + dropped_total:,}")
print(f"distinct GLOBALEVENTID {chk.ids:,} | missing id {chk.null_id} | missing date {chk.null_date} | actor not a state {chk.not_states} | "
      f"same country after remap {chk.same_country} | invalid measure {chk.bad_measure} | invalid coordinates left {chk.bad_coords}")
print(f"SQLDATE {chk.min_sqldate}..{chk.max_sqldate}; {chk.older_30d:,} kept rows are reports more than 30 days after the event "
      f"(kept on purpose: Member 3's recency window removes them). Code column types: {code_types}")
assert tot.rows_in == bronze_rows, "audit rows_in differs from the bronze row count"
assert tot.rows_in == chk.rows + dropped_total, "rows_in != clean + dropped"
assert tot.rows_clean == chk.rows, "events_clean row count differs from the audit"
assert chk.ids == chk.rows and chk.null_id == 0, "missing or duplicate GLOBALEVENTID in events_clean"
assert chk.null_date == 0 and chk.not_states == 0 and chk.same_country == 0 and chk.bad_measure == 0
assert chk.bad_coords == 0, "an invalid coordinate pair is left in events_clean"
assert set(code_types.values()) == {"string"}, code_types
print("all sanity checks passed")

# COMMAND ----------

display(ec.select("GLOBALEVENTID", "SQLDATE", "DATEADDED", "Actor1CountryCode", "Actor2CountryCode", "EventCode",
                  "QuadClass", "GoldsteinScale", "AvgTone", "NumMentions", "ActionGeo_Lat", "ActionGeo_Long").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Demo: one GDELT 2.0 15-minute file with the 61-column schema
# MAGIC Downloads one small 15-minute export (about 1 MB) into the volume, reads it with `gc.read_15min_csvs()` and runs the same
# MAGIC typing and cleaning on it. GDELT 2.0 is not part of `events_clean` (Member 5's streaming job reuses the reader). If the download fails,
# MAGIC this cell prints a message and the notebook carries on.

# COMMAND ----------

V2_DEMO_FILE = "20261001000000.export.CSV.zip"
V2_DEMO_DIR = f"{VOLUME_ROOT}/landing/v2_demo"
try:
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")   # normally created by 01_load_gdelt
except Exception as exc:
    print(f"note: could not create the volume ({repr(exc)[:120]}); trying the download anyway")
try:
    info = gc.download_and_extract({"name": V2_DEMO_FILE, "url": "https://data.gdeltproject.org/gdeltv2/" + V2_DEMO_FILE,
                                    "md5": None}, V2_DEMO_DIR, attempts=2)
    v2 = gc.read_15min_csvs(spark, V2_DEMO_DIR)
    v2_typed = gc.to_typed(v2)
    v2_audit = gc.cleaning_audit(gc.tag_drop_rule(v2_typed, LK)).orderBy("rule_order")
    # Data check of the 61-column layout: read with one EXTRA column. A row with fewer than 61 fields has an
    # empty SOURCEURL (the last column); a row with more fills `extra`. Both counts should be 0.
    wide = spark.read.options(**gc.V2_CSV_OPTIONS).schema(gc.v2_raw_schema().add("extra", "string")).csv(V2_DEMO_DIR)
    not_61 = wide.where("SOURCEURL IS NULL OR extra IS NOT NULL").count()
    print(f"{info['name']}: {v2.count():,} rows, rows without exactly {len(gc.GDELT_V2_COLUMNS)} fields: {not_61} "
          f"-> {len(gc.SHARED_COLUMNS)} shared columns + {[c for c in v2.columns if c not in gc.SHARED_COLUMNS]}")
    display(v2.select("GLOBALEVENTID", "SQLDATE", "DATEADDED", "dateadded_ts", "Actor1CountryCode", "Actor2CountryCode",
                      "EventCode", "ActionGeo_Lat", "ActionGeo_Long", "SOURCEURL", "source_file").limit(5))
    display(v2_audit.select("rule", "action", "rows_in", "rows_affected", "pct"))
except Exception as exc:
    print(f"GDELT 2.0 demo skipped: could not download or read {V2_DEMO_FILE} ({repr(exc)[:200]}).")
finally:
    try:
        dbutils.fs.rm(V2_DEMO_DIR, recurse=True)   # keep the volume empty, like 01_load_gdelt does
    except Exception:
        pass

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. CAMEO event-code join
# MAGIC The lookups are files in the repo's `lookups/` folder (shared with Member 3; see its README for sources and checksums).
# MAGIC `gc.read_lookup_tsv()` reads one with plain Python and `spark.createDataFrame` (serverless has no `spark.read` access to
# MAGIC workspace files). Codes stay STRING, because `01` and `010` are different codes. The lookup is a few KB, so it is broadcast.

# COMMAND ----------

cameo_codes = gc.read_lookup_tsv(spark, f"{LK['dir']}/CAMEO.eventcodes.txt")
print(f"{cameo_codes.count():,} CAMEO event codes from {LK['dir']}/CAMEO.eventcodes.txt")
display(ec.groupBy("EventCode").agg(F.count(F.lit(1)).alias("events"))
          .join(F.broadcast(cameo_codes), F.col("EventCode") == F.col("CAMEOEVENTCODE"), "left")
          .select("EventCode", "EVENTDESCRIPTION", "events")
          .orderBy(F.desc("events"), "EventCode").limit(15))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Contract check with Member 3's code
# MAGIC Runs Member 3's own functions (`dyad_common.py`) on this table, the same calls `03_dyad_tone_index` makes:
# MAGIC 1. `resolve_events_source(..., "member2_clean", ...)` must find every column it needs, with the event codes as strings;
# MAGIC 2. `flag_events` must mark **every** row of `events_clean` as `eligible` (nothing left for Member 3's rules to remove);
# MAGIC 3. Member 3's fallback over `events_bronze` must keep exactly the same `GLOBALEVENTID`s as this table.
# MAGIC
# MAGIC If `dyad_common.py` is not in this checkout yet, the check is skipped. A failed check prints a warning instead of stopping
# MAGIC Run All; it means the two cleaning steps have drifted apart and Member 2 and Member 3 need to talk.

# COMMAND ----------

try:
    import dyad_common as dc
except ImportError:
    dc = None

if dc is None:
    print("dyad_common.py (Member 3) is not in this checkout yet: contract check skipped.")
else:
    m3_lk = dc.load_lookups()
    src = dc.resolve_events_source(spark, "member2_clean", EVENTS_CLEAN, EVENTS_BRONZE, RANGE_START, RANGE_END)
    print(dc.source_banner(src))
    m3_on_clean = dc.flag_events(dc.typed_in_range(src), m3_lk, spark)
    reasons = {r["reason"]: r["count"] for r in m3_on_clean.groupBy("reason").count().collect()}
    fb = dc.resolve_events_source(spark, "bronze_fallback", EVENTS_CLEAN, EVENTS_BRONZE, RANGE_START, RANGE_END)
    m3_ids = (dc.flag_events(dc.typed_in_range(fb), m3_lk, spark)
                .where("reason = 'eligible'").select(F.col("event_id").alias("id")))
    my_ids = ec.select(F.col("GLOBALEVENTID").alias("id"))
    only_m3, only_m2 = m3_ids.subtract(my_ids).count(), my_ids.subtract(m3_ids).count()
    print(f"Member 3's rules on events_clean: {reasons}")
    print(f"IDs kept by Member 3's fallback but not here: {only_m3:,}; kept here but not by the fallback: {only_m2:,}")
    ok = not src["warnings"] and set(reasons) <= {"eligible"} and only_m3 == 0 and only_m2 == 0
    print("CONTRACT CHECK PASSED: Member 3's notebooks get the same events from events_clean as from their fallback"
          if ok else "!" * 100 + "\nWARNING: CONTRACT CHECK FAILED, the two cleaning steps differ (details above)\n" + "!" * 100)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Member 2's local Hadoop output (captured, not executed here)
# MAGIC The Pig conflict filter, the MapReduce events-per-country-per-day job and the MapReduce-vs-Spark benchmark run on the local
# MAGIC Docker cluster (`hadoop/pig/`, `hadoop/summary/`). This cell only displays the files those runs saved under `hadoop/output/`.

# COMMAND ----------

import glob

repo_hadoop_out = None
for p in sys.path:
    cand = os.path.normpath(os.path.join(p, "..", "hadoop", "output"))
    if os.path.isdir(cand):
        repo_hadoop_out = cand
        break

MAX_LINES = 40
if repo_hadoop_out is None:
    print("hadoop/output not found next to this notebook (open the notebooks from the Git folder to see it).")
else:
    for sub in ("pig", "summary", "benchmark"):
        paths = sorted(p for p in glob.glob(f"{repo_hadoop_out}/{sub}/**/*", recursive=True) if os.path.isfile(p))
        if not paths:
            print(f"(no captured output in hadoop/output/{sub}/ yet)")
            continue
        for path in paths:
            try:
                with open(path, errors="replace") as fh:
                    lines = fh.readlines()
                print(f"===== {os.path.relpath(path, repo_hadoop_out)} ({len(lines):,} lines) =====")
                print("".join(lines[:MAX_LINES]) + (f"... ({len(lines) - MAX_LINES:,} more lines)\n" if len(lines) > MAX_LINES else ""))
            except Exception as exc:
                print(f"could not read {path}: {exc!r}")
