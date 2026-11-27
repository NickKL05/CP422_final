"""Local test of the typed schema, GDELT 2.0 reader and cleaning rules (Spark local mode, ANSI ON, no Delta).

Checks:
  (a) synthetic rows that each break exactly one rule (plus clean rows): the tagged rule, the clean
      output, the kept duplicate, the coordinates set to NULL and the audit counts are exactly as expected,
      and ANSI mode never raises on bad values. The rules are Member 3's (dyad_common.flag_events), with the
      CAMEO lookups from lookups/; tests/test_clean_member3_compat.py runs Member 3's code itself;
  (b) the real 2-day GDELT 1.0 sample (2026-10-01, 2026-10-02) through read_daily_csvs -> to_typed ->
      cleaning: rows_in == clean + sum(dropped), no duplicate IDs, every actor a state, no same-country
      pair after the remap, no invalid measure or coordinate pair in the clean rows; prints the per-rule counts;
  (c) the two GDELT 2.0 15-minute files through read_15min_csvs: 61 raw fields, the 58 shared columns
      by name, DATEADDED parsed to a timestamp and a date, and the same reader pieces work with readStream;
  (d) read_lookup_tsv on lookups/ (CAMEO event codes stay strings) and a broadcast join to the clean
      events, plus the Pig centroid lookup in hadoop/lookups/.

Sample folder layout (GDELT CSVs as downloaded and unzipped, mounted read-only at /sample):
  /sample/events/YYYYMMDD.export.CSV            (GDELT 1.0 daily)
  /sample/v2_15min/YYYYMMDDHHMMSS.export.CSV    (GDELT 2.0 15-minute)

Run (from the repo root, Docker running; no cluster needed). The image's spark-defaults.conf points at
YARN and HDFS on "namenode", so a standalone container overrides the master, the default file system and
the event log:
  docker run --rm -v "$PWD":/repo -v <folder with the sample>:/sample:ro -w /repo cp422/hadoop-stack:1.0 \
    spark-submit --master local[4] --driver-memory 2g \
    --conf spark.hadoop.fs.defaultFS=file:/// --conf spark.eventLog.enabled=false \
    --conf spark.sql.ansi.enabled=true \
    tests/test_clean_local.py [/sample]
"""
import datetime as dt
import shutil
import sys
import tempfile

sys.path.insert(0, "notebooks")
import gdelt_common as gc  # noqa: E402
from pyspark.sql import SparkSession, functions as F  # noqa: E402

SAMPLE = sys.argv[1] if len(sys.argv) > 1 else "/sample"
EXPECTED_DAILY_ROWS = 118_572 + 109_627          # wc -l of the two daily files
EXPECTED_V2_ROWS = 2_666 + 1_928                  # wc -l of the two 15-minute files

spark = (SparkSession.builder.appName("cp422-clean-test")
         .config("spark.sql.ansi.enabled", "true")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.shuffle.partitions", "8").getOrCreate())
spark.sparkContext.setLogLevel("WARN")
assert spark.conf.get("spark.sql.ansi.enabled") == "true"

# ------------------------------------------------------------------------------------------------
# (a) synthetic rows
# ------------------------------------------------------------------------------------------------
LK = gc.load_cameo_lookups()
assert len(LK["states"]) == 233 and LK["remap"] == {"WSB": "PSE", "GZS": "PSE", "BAG": "IRQ"}, LK["remap"]
assert len(LK["nonstate"]) == 25 and "EUR" in LK["nonstate"]

BASE = {c: None for c in gc.GDELT_V1_COLUMNS}
BASE.update({"SQLDATE": "20261010", "MonthYear": "202610", "Year": "2026", "FractionDate": "2026.7699",
             "Actor1Code": "USA", "Actor1CountryCode": "USA", "Actor2Code": "CHN", "Actor2CountryCode": "CHN",
             "IsRootEvent": "1", "EventCode": "010", "EventBaseCode": "010", "EventRootCode": "01", "QuadClass": "4",
             "GoldsteinScale": "-10", "NumMentions": "5", "NumSources": "1", "NumArticles": "5", "AvgTone": "-3.5",
             "ActionGeo_Type": "1", "ActionGeo_Lat": "35.0", "ActionGeo_Long": "105.0",
             "DATEADDED": "20261012", "SOURCEURL": "https://example.org/a"})


def row(event_id, expected, source_file="20261012.export.CSV", **overrides):
    r = dict(BASE, GLOBALEVENTID=event_id, **overrides)
    return [r[c] for c in gc.GDELT_V1_COLUMNS] + [source_file, 2026, 10, expected]


synthetic = [
    # clean rows
    row("1", None),
    row(" 2 ", None, ActionGeo_Lat=None, ActionGeo_Long=None),              # ID is trimmed; missing coordinates kept
    row("3", None, ActionGeo_Lat="", ActionGeo_Long=" "),
    row("4", None, SQLDATE="20160101"),                                     # 10 years old: NOT dropped (no recency rule)
    row("5", None, SQLDATE="20261013"),                                     # after DATEADDED: kept (Member 3 filters lags)
    row("abc", None),                                                       # IDs are strings, like Member 3's fallback
    row("6", None, Actor1CountryCode="WSB", Actor2CountryCode="ISR"),       # WSB stays WSB in the table, a PSE-ISR dyad
    row("7", None, Actor1CountryCode="usa ", Actor2CountryCode=" chn"),    # codes are upper-cased and trimmed
    # invalid coordinates: the row is KEPT, the pair becomes NULL
    row("40", None, ActionGeo_Lat="91", ActionGeo_Long="10"),
    row("41", None, ActionGeo_Lat="10", ActionGeo_Long="-180.5"),
    row("42", None, ActionGeo_Lat="0", ActionGeo_Long="0"),
    row("43", None, ActionGeo_Lat="abc", ActionGeo_Long="10"),
    row("44", None, ActionGeo_Lat="10", ActionGeo_Long=None),               # half a pair
    row("45", None, ActionGeo_Lat="NaN", ActionGeo_Long="10"),
    row("46", None, ActionGeo_Type="0", ActionGeo_Lat="", ActionGeo_Long="42"),   # the GDELT "GG" quirk
    row("47", None, Actor1Geo_Lat="100", Actor1Geo_Long="5"),               # Actor1Geo pair, ActionGeo fine
    # missing_event_id
    row(None, "missing_event_id"),
    row("  ", "missing_event_id"),
    # duplicate_event_id: the EARLIEST DATEADDED is kept, then most NumMentions, then smallest source_file
    row("10", "duplicate_event_id", source_file="20261012.export.CSV"),
    row("10", None, source_file="20261013.export.CSV", DATEADDED="20261011"),
    row("10", "duplicate_event_id", source_file="20261014.export.CSV", NumMentions="99"),
    row("11", "duplicate_event_id", source_file="20261012.export.CSV", NumMentions="5"),
    row("11", None, source_file="20261013.export.CSV", NumMentions="9"),
    row("12", None, source_file="20261012.export.CSV"),
    row("12", "duplicate_event_id", source_file="20261013.export.CSV"),
    # invalid_date
    row("50", "invalid_date", SQLDATE="20261301"),                          # invalid month
    row("51", "invalid_date", SQLDATE="2026101"),                           # wrong length
    row("52", "invalid_date", DATEADDED=None),
    # missing_country
    row("20", "missing_country", Actor1CountryCode=None),
    row("21", "missing_country", Actor2CountryCode="  "),
    # region_code
    row("22", "region_code", Actor2CountryCode="EUR"),
    row("23", "region_code", Actor1CountryCode="afr"),                      # lower case, still a region
    # unknown_code
    row("24", "unknown_code", Actor2CountryCode="XYZ"),
    # same_country, after the remap
    row("30", "same_country", Actor2CountryCode="USA"),
    row("31", "same_country", Actor1CountryCode="WSB", Actor2CountryCode="PSE"),
    row("32", "same_country", Actor1CountryCode="GZS", Actor2CountryCode="WSB"),
    row("33", "same_country", Actor1CountryCode="BAG", Actor2CountryCode="IRQ"),
    # invalid_measure
    row("60", "invalid_measure", AvgTone="x"),
    row("61", "invalid_measure", NumMentions="0"),
    row("62", "invalid_measure", QuadClass=None),
    # rule ORDER: a row breaking several rules is tagged with the first one only
    row("80", "invalid_date", SQLDATE="bad", Actor1CountryCode=None),
    row("81", "missing_country", Actor1CountryCode=None, Actor2CountryCode="EUR"),
    row("82", "region_code", Actor1CountryCode="EUR", Actor2CountryCode="EUR"),
    row("83", "same_country", Actor2CountryCode="USA", AvgTone="x"),
    # a second month, for the per-month audit
    row("70", None, source_file="20261101.export.CSV", SQLDATE="20261030", DATEADDED="20261101"),
    row("71", "same_country", source_file="20261101.export.CSV", SQLDATE="20261030",
        DATEADDED="20261101", Actor1CountryCode="FRA", Actor2CountryCode="FRA"),
]
# Fix the partition columns of the November rows.
synthetic = [r[:-3] + ([2026, 11] if r[-4].startswith("202611") else [2026, 10]) + [r[-1]] for r in synthetic]
COORDS_NULLED_IDS = {"40", "41", "42", "43", "44", "45", "46", "47"}

schema = (gc.raw_string_schema().add("source_file", "string").add(gc.PART_YEAR, "integer")
          .add(gc.PART_MONTH, "integer").add("expected", "string"))
syn_raw = spark.createDataFrame(synthetic, schema)
syn_tagged = gc.tag_drop_rule(gc.to_typed(syn_raw), LK)

got = [(r["GLOBALEVENTID"], r["source_file"], r["expected"], r[gc.DROP_RULE_COL])
       for r in syn_tagged.select("GLOBALEVENTID", "source_file", "expected", gc.DROP_RULE_COL).collect()]
mismatches = [g for g in got if g[2] != g[3]]
print(f"[a] synthetic rows: {len(synthetic)}, tag mismatches: {len(mismatches)}")
for m in sorted(mismatches, key=str):
    print("    MISMATCH id/source_file/expected/got:", m)
assert not mismatches

clean = gc.clean_rows(syn_tagged)
assert gc.DROP_RULE_COL not in clean.columns and gc.COORDS_NULLED_COL not in clean.columns
clean_ids = sorted(r[0] for r in clean.select("GLOBALEVENTID").collect())
expected_clean = sorted({"1", "2", "3", "4", "5", "ABC", "6", "7", "10", "11", "12", "70"} | COORDS_NULLED_IDS)
print(f"    clean ids: {clean_ids}")
assert clean_ids == expected_clean, clean_ids
# The kept duplicates: earliest DATEADDED (10), most mentions (11), smallest source_file (12).
kept = {r.GLOBALEVENTID: (r.source_file, r.NumMentions) for r in clean.where("GLOBALEVENTID IN ('10','11','12')").collect()}
assert kept == {"10": ("20261013.export.CSV", 5), "11": ("20261013.export.CSV", 9), "12": ("20261012.export.CSV", 5)}, kept

# Typed columns have the documented types; codes are strings with their leading zeros.
typed_fields = {f.name: f.dataType for f in gc.typed_schema().fields}
for f in clean.schema.fields:
    if f.name in typed_fields:
        assert f.dataType == typed_fields[f.name], (f.name, f.dataType)
r1 = clean.where("GLOBALEVENTID = '1'").first()
assert r1.SQLDATE == dt.date(2026, 10, 10) and r1.DATEADDED == dt.date(2026, 10, 12)
assert r1.QuadClass == 4 and r1.AvgTone == -3.5 and r1.NumMentions == 5
assert (r1.EventCode, r1.EventBaseCode, r1.EventRootCode) == ("010", "010", "01")
assert (r1.ActionGeo_Lat, r1.ActionGeo_Long) == (35.0, 105.0)
assert clean.where("GLOBALEVENTID = '6'").first().Actor1CountryCode == "WSB"          # original code kept
assert clean.where("GLOBALEVENTID = '7'").select("Actor1CountryCode", "Actor2CountryCode").first() == ("USA", "CHN")
for r in clean.where(F.col("GLOBALEVENTID").isin(sorted(COORDS_NULLED_IDS - {"47"}))).collect():
    assert r.ActionGeo_Lat is None and r.ActionGeo_Long is None, r
r47 = clean.where("GLOBALEVENTID = '47'").first()
assert r47.Actor1Geo_Lat is None and r47.Actor1Geo_Long is None and r47.ActionGeo_Lat == 35.0

audit = {(r.file_month, r.rule): r for r in gc.cleaning_audit(syn_tagged).collect()}
assert set(audit) == {(m, r) for m in (10, 11) for r in gc.CLEANING_RULES + [gc.COORDS_RULE]}, sorted(audit)
for (m, rule), r in audit.items():
    if rule == gc.COORDS_RULE:
        want = len(COORDS_NULLED_IDS) if m == 10 else 0
        assert r.action == "set_null"
    else:
        want = sum(1 for s_ in synthetic if s_[-1] == rule and s_[-2] == m)
        assert r.action == "drop"
    assert r.rows_affected == want, (m, rule, r.rows_affected, want)
oct_rows = sum(1 for s_ in synthetic if s_[-2] == 10)
oct_clean = sum(1 for s_ in synthetic if s_[-2] == 10 and s_[-1] is None)
assert (audit[(10, "missing_event_id")].rows_in, audit[(10, "missing_event_id")].rows_clean) == (oct_rows, oct_clean)
assert oct_rows == oct_clean + sum(r.rows_affected for (m, _), r in audit.items() if m == 10 and r.action == "drop")
assert audit[(10, "same_country")].pct == round(5 * 100.0 / oct_rows, 3)
yearly = gc.audit_by_year(gc.cleaning_audit(syn_tagged)).collect()
assert len(yearly) == len(gc.CLEANING_RULES) + 1 and yearly[0].rows_in == len(synthetic)
assert sum(r.rows_affected for r in yearly if r.action == "drop") == sum(1 for s_ in synthetic if s_[-1])
print("    audit counts per month and per year match")

# ------------------------------------------------------------------------------------------------
# (b) the real 2-day GDELT 1.0 sample
# ------------------------------------------------------------------------------------------------
bronze = gc.read_daily_csvs(spark, f"{SAMPLE}/events")
tagged = gc.tag_drop_rule(gc.to_typed(bronze), LK)
audit_rows = gc.cleaning_audit(tagged).orderBy(gc.PART_YEAR, gc.PART_MONTH, "rule_order").collect()
rows_in = audit_rows[0].rows_in
rows_clean = audit_rows[0].rows_clean
print(f"[b] real sample: rows_in {rows_in:,} (expected {EXPECTED_DAILY_ROWS:,}), clean {rows_clean:,}")
assert len({(r.file_year, r.file_month) for r in audit_rows}) == 1      # both files are October 2026
assert rows_in == EXPECTED_DAILY_ROWS
for r in audit_rows:
    print(f"    {r.rule_order} {r.rule:<20} {r.action:<8} {r.rows_affected:>8,}  ({r.pct:6.3f}% of rows_in)")
dropped = sum(r.rows_affected for r in audit_rows if r.action == "drop")
assert rows_in == rows_clean + dropped, (rows_in, rows_clean, dropped)

clean_real = gc.clean_rows(tagged)
remap = F.create_map(*[F.lit(x) for kv in sorted(LK["remap"].items()) for x in kv])
ra1 = F.coalesce(remap[F.col("Actor1CountryCode")], F.col("Actor1CountryCode"))
ra2 = F.coalesce(remap[F.col("Actor2CountryCode")], F.col("Actor2CountryCode"))
states = sorted(LK["states"])
bad_pair = F.greatest(*[(F.col(la).isNull() != F.col(lo).isNull()) | ~F.col(la).between(-90, 90)
                        | ~F.col(lo).between(-180, 180) | ((F.col(la) == 0) & (F.col(lo) == 0))
                        for la, lo in gc.GEO_PAIRS])
stats = clean_real.agg(
    F.count(F.lit(1)).alias("n"), F.countDistinct("GLOBALEVENTID").alias("ids"),
    F.count(F.when(~ra1.isin(states) | ~ra2.isin(states) | ra1.isNull() | ra2.isNull(), 1)).alias("not_state"),
    F.count(F.when(ra1 == ra2, 1)).alias("same_c"),
    F.count(F.when(F.col("AvgTone").isNull() | F.col("QuadClass").isNull() | F.col("NumMentions").isNull()
                   | (F.col("NumMentions") < 1), 1)).alias("bad_measure"),
    F.count(F.when(bad_pair, 1)).alias("bad_geo"),
    F.count(F.when(F.col("ActionGeo_Lat").isNull(), 1)).alias("null_geo"),
    F.count(F.when(F.datediff("DATEADDED", "SQLDATE") > 30, 1)).alias("older_30d"),
    F.min("SQLDATE").alias("min_d"), F.max("SQLDATE").alias("max_d")).first()
print(f"    clean rows {stats.n:,}, distinct ids {stats.ids:,}, actor not a state {stats.not_state}, same country {stats.same_c}, "
      f"invalid measure {stats.bad_measure}, invalid coordinate pairs {stats.bad_geo}, NULL ActionGeo {stats.null_geo:,}, "
      f"reports > 30 days after the event (kept) {stats.older_30d:,}, SQLDATE {stats.min_d}..{stats.max_d}")
assert stats.n == rows_clean and stats.ids == stats.n
assert stats.not_state == 0 and stats.same_c == 0 and stats.bad_measure == 0 and stats.bad_geo == 0

# ------------------------------------------------------------------------------------------------
# (c) GDELT 2.0 15-minute files
# ------------------------------------------------------------------------------------------------
v2_path = f"{SAMPLE}/v2_15min"
raw_v2 = spark.read.options(**gc.V2_CSV_OPTIONS).schema(gc.v2_raw_schema()).csv(v2_path)
assert len(raw_v2.columns) == 61
# Every row really has 61 fields: the last column is always filled and, read with one EXTRA column,
# that extra column is always empty.
wide = spark.read.options(**gc.V2_CSV_OPTIONS).schema(gc.v2_raw_schema().add("extra", "string")).csv(v2_path)
short_or_long = wide.where("SOURCEURL IS NULL OR extra IS NOT NULL").count()
v2 = gc.read_15min_csvs(spark, v2_path)
n_v2 = v2.count()
print(f"[c] 15-minute rows {n_v2:,} (expected {EXPECTED_V2_ROWS:,}), rows without exactly 61 fields: {short_or_long}")
assert n_v2 == EXPECTED_V2_ROWS and short_or_long == 0
assert v2.columns[:58] == gc.SHARED_COLUMNS
assert set(v2.columns[58:]) == {"source_file", "dateadded_ts", "ingest_ts", gc.PART_YEAR, gc.PART_MONTH}
# DATEADDED lands in the right column: 14 digits that equal the file name's slot; SOURCEURL is a URL.
chk = v2.agg(
    F.count(F.when(~F.col("DATEADDED").rlike(r"^\d{14}$"), 1)).alias("bad_added"),
    F.count(F.when(F.col("dateadded_ts").isNull(), 1)).alias("null_ts"),
    F.count(F.when(F.date_format("dateadded_ts", "yyyyMMddHHmmss") != F.regexp_extract("source_file", r"^(\d{14})", 1), 1)).alias("ts_vs_file"),
    F.count(F.when(~F.col("SOURCEURL").rlike("^https?://"), 1)).alias("bad_url"),
    F.count(F.when(F.expr("try_cast(ActionGeo_Lat AS DOUBLE)").isNull() & F.col("ActionGeo_Lat").isNotNull(), 1)).alias("bad_lat"),
    F.countDistinct("source_file").alias("files")).first()
print(f"    DATEADDED not 14 digits: {chk.bad_added}, unparsed dateadded_ts: {chk.null_ts}, "
      f"slot != file name: {chk.ts_vs_file}, SOURCEURL not a URL: {chk.bad_url}, non-numeric ActionGeo_Lat: {chk.bad_lat}, "
      f"files: {chk.files}")
assert chk.bad_added == 0 and chk.null_ts == 0 and chk.ts_vs_file == 0 and chk.bad_url == 0 and chk.bad_lat == 0
assert chk.files == 2
print("    slots:", [str(r[0]) for r in v2.select("dateadded_ts").distinct().orderBy("dateadded_ts").collect()])
assert [tuple(r) for r in v2.select(gc.PART_YEAR, gc.PART_MONTH).distinct().collect()] == [(2026, 10)]

v2_typed = gc.to_typed(v2)
assert v2_typed.where("DATEADDED IS NULL").count() == 0
v2_dates = [r[0] for r in v2_typed.select("DATEADDED").distinct().collect()]
print(f"    typed DATEADDED values: {v2_dates}")
v2_audit = gc.cleaning_audit(gc.tag_drop_rule(v2_typed, LK)).collect()
assert v2_audit[0].rows_in == v2_audit[0].rows_clean + sum(r.rows_affected for r in v2_audit if r.action == "drop")
print("    v2 rules:", {r.rule: r.rows_affected for r in sorted(v2_audit, key=lambda r: r.rule_order)},
      f"clean {v2_audit[0].rows_clean:,}")

# Streaming: the same schema/options/v2_to_shared pieces work on spark.readStream (Member 5).
ckpt = tempfile.mkdtemp(prefix="cp422_ckpt_")
try:
    stream = gc.v2_to_shared(spark.readStream.options(**gc.V2_CSV_OPTIONS).schema(gc.v2_raw_schema()).csv(v2_path))
    q = (stream.writeStream.format("memory").queryName("v2_stream_test").option("checkpointLocation", ckpt)
         .trigger(availableNow=True).start())
    q.awaitTermination(300)
    n_stream = spark.table("v2_stream_test").count()
    n_stream_files = spark.table("v2_stream_test").select("source_file").distinct().count()
    print(f"    readStream: {n_stream:,} rows from {n_stream_files} files")
    assert n_stream == EXPECTED_V2_ROWS and n_stream_files == 2
finally:
    shutil.rmtree(ckpt, ignore_errors=True)

# ------------------------------------------------------------------------------------------------
# (d) lookup files via read_lookup_tsv (the serverless-safe reader): CAMEO codes from lookups/, the Pig
#     centroid table from hadoop/lookups/
# ------------------------------------------------------------------------------------------------
assert LK["dir"].endswith("lookups") and not LK["dir"].endswith("hadoop/lookups"), LK["dir"]
codes = gc.read_lookup_tsv(spark, f"{LK['dir']}/CAMEO.eventcodes.txt")
centroids = gc.read_lookup_tsv(spark, f"{gc.repo_path('hadoop', 'lookups')}/country_centroids.tsv",
                               columns=["cameo_code", "country_name", "lat", "lon"])
n_codes, n_cent = codes.count(), centroids.count()
assert codes.columns == ["CAMEOEVENTCODE", "EVENTDESCRIPTION"]
assert codes.where("CAMEOEVENTCODE = '010'").count() == 1 and codes.where("CAMEOEVENTCODE = '01'").count() == 1
joined = (clean_real.groupBy("EventCode").count()
          .join(F.broadcast(codes), F.col("EventCode") == F.col("CAMEOEVENTCODE"), "left"))
unmatched = joined.where("CAMEOEVENTCODE IS NULL").agg(F.coalesce(F.sum("count"), F.lit(0))).first()[0]
print(f"[d] lookups: {n_codes} CAMEO event codes, {n_cent} centroids; clean events whose EventCode "
      f"is not in CAMEO.eventcodes.txt: {unmatched}")
assert n_cent > 0 and centroids.where("cameo_code = 'USA'").count() == 1

print("ALL CHECKS PASSED")
