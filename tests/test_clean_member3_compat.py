"""Contract test: Member 3's notebooks get the same events from events_clean as from their own fallback.

Member 3's 03/04/05 notebooks read EVENTS_CLEAN when it exists (dyad_common.resolve_events_source), look up
the original GDELT column names, and run their own typing and rules again (typed_events -> flag_events). If the
table is missing a column, or Member 2's rules and Member 3's rules disagree, their results would change
silently. This test runs Member 3's real dyad_common.py (it is not copied into this test) and checks:
  (a) edge-case rows: for every row, Member 2's rule (tag_drop_rule) equals Member 3's reason (flag_events),
      including duplicates (which copy is kept), the WSB/GZS/BAG remap, regions, unknown codes, bad numbers
      and dates under ANSI mode, lower-case and padded codes;
  (b) the real 2-day sample: the per-rule counts are identical; the clean table has every column Member 3
      needs, with the event codes as strings; Member 3's rules mark EVERY row of the clean table eligible; and
      the clean GLOBALEVENTIDs equal the IDs Member 3's fallback keeps;
  (c) downstream: dyad_common.eligible_events (recency window, dyad keys, event-root join) gives exactly the
      same rows from the clean table as from bronze, so every later table (daily index, weekly features,
      network) is unchanged.

Needs Member 3's notebooks/dyad_common.py: after the member3 branch is merged it is in notebooks/; before that,
put a checkout of their notebooks folder on PYTHONPATH. Run (from the repo root, Docker running):
  docker run --rm -v "$PWD":/repo -v <folder with the sample>:/sample:ro [-v <member3 checkout>:/m3:ro \
    -e PYTHONPATH=/m3/notebooks] -w /repo cp422/hadoop-stack:1.0 \
    spark-submit --master local[4] --driver-memory 2g \
    --conf spark.hadoop.fs.defaultFS=file:/// --conf spark.eventLog.enabled=false \
    --conf spark.sql.ansi.enabled=true \
    tests/test_clean_member3_compat.py [/sample]
"""
import datetime as dt
import sys

sys.path.insert(0, "notebooks")
import gdelt_common as gc  # noqa: E402

try:
    import dyad_common as dc  # noqa: E402
except ImportError:
    sys.exit("FAILED: Member 3's dyad_common.py is not importable. Merge the member3 branch, or put their "
             "notebooks folder on PYTHONPATH (see this file's docstring).")
from pyspark.sql import SparkSession, functions as F  # noqa: E402

SAMPLE = sys.argv[1] if len(sys.argv) > 1 else "/sample"

spark = (SparkSession.builder.appName("cp422-clean-member3-compat")
         .config("spark.sql.ansi.enabled", "true")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.shuffle.partitions", "8").getOrCreate())
spark.sparkContext.setLogLevel("WARN")
assert spark.conf.get("spark.sql.ansi.enabled") == "true"
print(f"dyad_common from {dc.__file__}")

LK = gc.load_cameo_lookups()
M3_LK = dc.load_lookups()
assert (LK["states"], LK["nonstate"], LK["remap"]) == (set(M3_LK["states"]), M3_LK["nonstate"], M3_LK["remap"])
print(f"lookups agree: {len(LK['states'])} states, {len(LK['nonstate'])} regions, remap {LK['remap']}")


def m2_reason(tagged):
    return F.coalesce(F.col(gc.DROP_RULE_COL), F.lit("eligible"))


# ------------------------------------------------------------------------------------------------
# (a) edge-case rows, compared row by row
# ------------------------------------------------------------------------------------------------
BASE = {c: None for c in gc.GDELT_V1_COLUMNS}
BASE.update({"SQLDATE": "20261010", "Actor1CountryCode": "USA", "Actor2CountryCode": "CHN", "IsRootEvent": "1",
             "EventCode": "010", "EventBaseCode": "010", "EventRootCode": "01", "QuadClass": "1",
             "GoldsteinScale": "0", "NumMentions": "5", "AvgTone": "-1.5", "DATEADDED": "20261012"})
cases = [
    {}, {"GLOBALEVENTID": None}, {"GLOBALEVENTID": "  "}, {"GLOBALEVENTID": " abc "},
    {"SQLDATE": "20261301"}, {"SQLDATE": "2026101"}, {"DATEADDED": ""}, {"SQLDATE": "20160101"},
    {"SQLDATE": "20261020"},
    {"Actor1CountryCode": None}, {"Actor2CountryCode": " "}, {"Actor1CountryCode": "usa", "Actor2CountryCode": " chn"},
    {"Actor2CountryCode": "EUR"}, {"Actor1CountryCode": "afr"}, {"Actor2CountryCode": "XYZ"},
    {"Actor2CountryCode": "USA"}, {"Actor1CountryCode": "WSB", "Actor2CountryCode": "PSE"},
    {"Actor1CountryCode": "GZS", "Actor2CountryCode": "WSB"}, {"Actor1CountryCode": "BAG", "Actor2CountryCode": "IRQ"},
    {"Actor1CountryCode": "WSB", "Actor2CountryCode": "ISR"}, {"Actor1CountryCode": "BAG", "Actor2CountryCode": "USA"},
    {"AvgTone": "x"}, {"AvgTone": ""}, {"NumMentions": "0"}, {"NumMentions": "-3"}, {"NumMentions": "2.5"},
    {"QuadClass": None}, {"QuadClass": "four"}, {"GoldsteinScale": "bad"},
    {"SQLDATE": "bad", "Actor1CountryCode": None}, {"Actor1CountryCode": None, "Actor2CountryCode": "EUR"},
    {"Actor1CountryCode": "EUR", "Actor2CountryCode": "EUR"}, {"Actor2CountryCode": "USA", "AvgTone": "x"},
    {"ActionGeo_Lat": "0", "ActionGeo_Long": "0"}, {"ActionGeo_Lat": "95", "ActionGeo_Long": "10"},
]
# Every row gets a distinct GoldsteinScale, used only as the row key for the comparison (typed_events keeps it as
# `goldstein`, while event ids can be missing or repeated). One case sets it to "bad" on purpose: NULL on both sides.
rows = []
for i, over in enumerate(cases):
    r = dict(BASE)
    r.update(GLOBALEVENTID=str(1000 + i), GoldsteinScale=str(i))
    r.update(over)
    rows.append([r[c] for c in gc.GDELT_V1_COLUMNS] + [f"case{i:03d}", 2026, 10])
# Duplicates: three copies of one ID that differ in DATEADDED and NumMentions (no exact ties, where either
# implementation may keep any copy). Member 3 keeps the earliest DATEADDED, then the most mentions.
for k, (added, mentions) in enumerate([("20261012", "5"), ("20261011", "3"), ("20261011", "8")]):
    r = dict(BASE, GLOBALEVENTID="2000", DATEADDED=added, NumMentions=mentions, GoldsteinScale=str(500 + k))
    rows.append([r[c] for c in gc.GDELT_V1_COLUMNS] + [f"dup{k}", 2026, 10])
schema = (gc.raw_string_schema().add("source_file", "string").add(gc.PART_YEAR, "integer")
          .add(gc.PART_MONTH, "integer"))
edge = spark.createDataFrame(rows, schema)

mine = (gc.tag_drop_rule(gc.to_typed(edge), LK)
        .select(F.col("GoldsteinScale").alias("k2"), m2_reason(None).alias("m2"), "source_file"))
theirs = dc.flag_events(dc.typed_events(edge), M3_LK, spark).select(F.col("goldstein").alias("k3"),
                                                                     F.col("reason").alias("m3"))
cmp = mine.join(theirs, F.col("k2").eqNullSafe(F.col("k3")), "full_outer").collect()
diff = [r for r in cmp if r.m2 != r.m3]
print(f"[a] {len(rows)} edge-case rows; rows where the two rule sets disagree: {len(diff)}")
for r in sorted(diff, key=str):
    print("    DIFF", r)
assert len(cmp) == len(rows) and not diff
print("    reasons:", sorted({(r.source_file, r.m2) for r in cmp if r.m2 != "eligible"}))

# ------------------------------------------------------------------------------------------------
# (b) the real 2-day sample
# ------------------------------------------------------------------------------------------------
bronze = gc.read_daily_csvs(spark, f"{SAMPLE}/events")
tagged = gc.tag_drop_rule(gc.to_typed(bronze), LK)
clean = gc.clean_rows(tagged)
n_bronze, n_clean = bronze.count(), clean.count()

m2_counts = {r["m2"]: r["count"] for r in tagged.groupBy(m2_reason(tagged).alias("m2")).count().collect()}
fb_flagged = dc.flag_events(dc.typed_events(bronze), M3_LK, spark)
m3_counts = {r["reason"]: r["count"] for r in fb_flagged.groupBy("reason").count().collect()}
print(f"[b] bronze {n_bronze:,} rows, events_clean {n_clean:,} rows")
for reason in dc.REASONS:
    print(f"    {reason:<20} Member 2 {m2_counts.get(reason, 0):>8,}   Member 3 fallback {m3_counts.get(reason, 0):>8,}")
assert m2_counts == m3_counts, (m2_counts, m3_counts)

# The table has what resolve_events_source() needs, with the event codes still strings.
lower = {c.lower(): c for c in clean.columns}
missing = [c for c in dc.SOURCE_COLUMNS if c.lower() not in lower]
dtypes = dict(clean.dtypes)
assert not missing, missing
assert {"file_year", "file_month"} <= set(lower)
assert all(dtypes[c] == "string" for c in ["EventCode", "EventBaseCode", "EventRootCode", "GLOBALEVENTID"])
assert dtypes["SQLDATE"] == dtypes["DATEADDED"] == "date"

# Member 3's rules find nothing left to remove in the clean table.
on_clean = dc.flag_events(dc.typed_events(clean), M3_LK, spark)
on_clean_counts = {r["reason"]: r["count"] for r in on_clean.groupBy("reason").count().collect()}
print(f"    Member 3's rules on events_clean: {on_clean_counts}")
assert on_clean_counts == {"eligible": n_clean}, on_clean_counts

fb_ids = fb_flagged.where("reason = 'eligible'").select(F.col("event_id").alias("id"))
my_ids = clean.select(F.col("GLOBALEVENTID").alias("id"))
only_m3, only_m2 = fb_ids.subtract(my_ids).count(), my_ids.subtract(fb_ids).count()
print(f"    IDs only in Member 3's fallback: {only_m3}; only in events_clean: {only_m2}")
assert only_m3 == 0 and only_m2 == 0

# ------------------------------------------------------------------------------------------------
# (c) downstream: the clean dyadic events Member 3 builds everything from
# ------------------------------------------------------------------------------------------------
lags = {r["lag_bucket"]: r["count"] for r in fb_flagged.where("reason = 'eligible'")
        .groupBy(dc.lag_bucket_col().alias("lag_bucket")).count().collect()}
n_days = dc.choose_recency_days(lags, sum(lags.values()))
eventcodes_df = spark.createDataFrame(sorted(M3_LK["eventcodes"].items()), "code STRING, description STRING")
# A wide day window, so the comparison covers as many rows as possible (the 2-day sample has no complete week).
first_day, last_day = dt.date(2026, 9, 1), dt.date(2026, 10, 2)
from_bronze = dc.eligible_events(fb_flagged, eventcodes_df, n_days, first_day, last_day)
from_clean = dc.eligible_events(on_clean, eventcodes_df, n_days, first_day, last_day)
cols = sorted(from_bronze.columns)
assert cols == sorted(from_clean.columns)
a_rows = sorted(tuple(r) for r in from_bronze.select(cols).collect())
b_rows = sorted(tuple(r) for r in from_clean.select(cols).collect())
print(f"[c] recency N = {n_days}; eligible_events rows from bronze {len(a_rows):,}, from events_clean {len(b_rows):,}; "
      f"columns {cols}")
assert len(a_rows) > 0 and a_rows == b_rows, "eligible_events differs between the fallback and events_clean"

spark.stop()
print("MEMBER 3 CONTRACT TEST PASSED")
