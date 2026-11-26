"""Local check of Member 3's dyad logic (notebooks/dyad_common.py) on tiny synthetic data.

It runs the same functions the 03_dyad_tone_index notebook calls, on hand-made rows whose correct answers can be
worked out on paper:
  1. the lookups load as documented (233 states, sub-state codes remapped, regions excluded);
  2. the partition predicate, week starts, complete weeks and the recency-N rule;
  3. cleaning: bad numbers and dates become invalid rows instead of crashing (ANSI mode on), duplicates, missing and
     region codes, same-country pairs, and the WSB/GZS -> PSE, BAG -> IRQ remap;
  4. dyads are undirected (USA->CHN and CHN->USA share one key);
  5. the rolling 7-day tone uses a RANGE frame (a gap of empty days is not bridged) and is a mentions-weighted ratio of
     sums, not an average of daily averages;
  6. the weekly table is a dense calendar: empty weeks exist with zero counts and NULL tone, LAG looks at the previous
     calendar week, LEAD gives next week's label;
  7. repartitioning by dyad before the windows leaves exactly one shuffle (no second exchange for the window);
  8. PageRank (notebook 04) matches hand-solvable graphs and its Spark version matches the driver version, and salting
     (notebook 05) assigns the right buckets, keeps aggregates and join results exact, and shrinks the largest partition.

Needs only PySpark (no Delta, no Databricks). From the repo root:
  pip install pyspark
  python tests/test_dyads_local.py
If several Python versions are installed, point Spark's workers at the same interpreter first, otherwise they fail with
PYTHON_VERSION_MISMATCH:  PYSPARK_PYTHON=$(which python) python tests/test_dyads_local.py
"""
import datetime as dt
import math
import sys

sys.path.insert(0, "notebooks")
import dyad_common as dc  # noqa: E402
from pyspark.sql import SparkSession, functions as F  # noqa: E402

D = dt.date
spark = (SparkSession.builder.master("local[2]").appName("cp422-dyads-test")
         .config("spark.sql.shuffle.partitions", "4")
         .config("spark.sql.session.timeZone", "UTC")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
print(f"Spark {spark.version}, ANSI mode = {spark.conf.get('spark.sql.ansi.enabled')}")


def close(a, b, tol=1e-9):
    return a is not None and b is not None and abs(a - b) < tol


# 1. lookups ---------------------------------------------------------------------------------------------------
lk = dc.load_lookups(dc.find_lookups_dir(["notebooks"]))
assert len(lk["countries"]) == 261, len(lk["countries"])
assert len(lk["eventcodes"]) == 310, len(lk["eventcodes"])
assert len(lk["nonstate"]) == 25 and "EUR" in lk["nonstate"] and "WSB" not in lk["nonstate"]
assert lk["remap"] == {"WSB": "PSE", "GZS": "PSE", "BAG": "IRQ"}, lk["remap"]
assert len(lk["states"]) == 233 and "WSB" not in lk["states"] and "PSE" in lk["states"]
assert lk["eventcodes"]["010"] != lk["eventcodes"].get("10", "missing"), "event codes must stay strings"
assert "\r" not in "".join(lk["countries"].values()), "CRLF must be stripped"
print(f"[1] lookups: {len(lk['countries'])} codes, {len(lk['states'])} states, {len(lk['nonstate'])} regions, "
      f"remap {lk['remap']}")

# 2. pure-Python date rules --------------------------------------------------------------------------------------
assert dc.month_range_predicate("2026-10-01", "2026-10-31") == "(file_year = 2026 AND file_month BETWEEN 10 AND 10)"
p = dc.month_range_predicate("2025-11-01", "2026-10-31")
assert p == "((file_year = 2025 AND file_month >= 11) OR (file_year = 2026 AND file_month <= 10))", p
assert dc.week_start(D(2026, 10, 1)) == D(2026, 9, 28)          # Thursday -> Monday
assert dc.week_start(D(2026, 10, 4)) == D(2026, 9, 28)          # Sunday belongs to the week before
assert dc.week_start(D(2026, 10, 5)) == D(2026, 10, 5)          # Monday is its own start
# Sample month with N = 2: complete days Oct 1..29, so full Monday weeks are Oct 5, 12, 19 (Oct 26 week needs Nov 1).
assert dc.complete_week_starts("2026-10-01", "2026-10-31", 2) == [D(2026, 10, 5), D(2026, 10, 12), D(2026, 10, 19)]
full = dc.complete_week_starts("2025-11-01", "2026-10-31", 2)
assert full[0] == D(2025, 11, 3) and full[-1] == D(2026, 10, 19) and len(full) == 51, (full[0], full[-1], len(full))
# Recency rule: lag 0..3 carry most rows, lag 4 adds 0.2% (< 0.5%), so N = 3.
lags = {0: 500, 1: 300, 2: 100, 3: 40, 4: 2, 5: 1, 31: 57}
assert dc.choose_recency_days(lags, sum(lags.values())) == 3
assert dc.choose_recency_days({0: 10, 1: 10}, 20) == 1
assert dc.shuffle_partitions_for(3_195_940) == 8 and dc.shuffle_partitions_for(37_225_966) == 80
assert dc.selection_weeks(full) == full[:26] and dc.selection_weeks([D(2026, 10, 5)]) == [D(2026, 10, 5)]
print(f"[2] predicates, weeks ({len(full)} full-mode weeks {full[0]}..{full[-1]}), recency rule, partition sizing OK")

# 3 + 4. typing, cleaning, dyad rules ----------------------------------------------------------------------------
# Bronze-shaped rows: every column a STRING, as in events_bronze.
COLS = ["GLOBALEVENTID", "SQLDATE", "DATEADDED", "Actor1CountryCode", "Actor2CountryCode", "EventCode",
        "EventBaseCode", "EventRootCode", "QuadClass", "GoldsteinScale", "NumMentions", "AvgTone", "IsRootEvent"]


def ev(eid, sqldate, added, a1, a2, tone, mentions, quad="1", code="010", gold="1.0", root="1"):
    return (eid, sqldate, added, a1, a2, code, code[:3], code[:2], quad, gold, mentions, tone, root)


raw_rows = [
    ev("1", "20261005", "20261005", "USA", "CHN", "2.0", "1"),     # eligible, USA->CHN
    ev("2", "20261005", "20261006", "CHN", "USA", "-4.0", "3"),    # eligible, CHN->USA: same dyad, lag 1
    ev("2", "20261005", "20261007", "CHN", "USA", "-4.0", "3"),    # duplicate id, later report -> dropped
    ev("3", "20261005", "20261005", "WSB", "ISR", "-1.0", "2"),    # WSB -> PSE: dyad ISR_PSE
    ev("4", "20261005", "20261005", "GZS", "PSE", "-1.0", "2"),    # GZS -> PSE: PSE-PSE -> same_country
    ev("5", "20261005", "20261005", "BAG", "USA", "0.5", "1"),     # BAG -> IRQ: dyad IRQ_USA
    ev("6", "20261005", "20261005", "EUR", "USA", "1.0", "1"),     # region -> excluded
    ev("7", "20261005", "20261005", None, "USA", "1.0", "1"),      # missing country
    ev("8", "20261005", "20261005", "XYZ", "USA", "1.0", "1"),     # unknown code
    ev("9", "20261005", "20261005", "FRA", "FRA", "1.0", "1"),     # same country
    ev("10", "20261005", "20261005", "FRA", "DEU", "abc", "1"),    # malformed tone -> invalid_measure, no crash
    ev("11", "20261399", "20261005", "FRA", "DEU", "1.0", "1"),    # malformed date -> invalid_date, no crash
    ev("12", "20160105", "20261005", "FRA", "DEU", "1.0", "1"),    # 10-year-old event: eligible, removed by recency
    ev(None, "20261005", "20261005", "FRA", "DEU", "1.0", "1"),    # missing id
    ev("13", "20261005", "20261005", "FRA", "DEU", "1.0", "0"),    # zero mentions -> invalid_measure
]
raw = spark.createDataFrame(raw_rows, ", ".join(f"{c} STRING" for c in COLS))
flagged = dc.flag_events(dc.typed_events(raw), lk, spark)
reasons = {r["event_id"] + ("" if r["dup_rank"] == 1 else "dup") if r["event_id"] else "null": r["reason"]
           for r in flagged.collect()}
expected = {"1": "eligible", "2": "eligible", "2dup": "duplicate_event_id", "3": "eligible", "4": "same_country",
            "5": "eligible", "6": "region_code", "7": "missing_country", "8": "unknown_code", "9": "same_country",
            "10": "invalid_measure", "11": "invalid_date", "12": "eligible", "null": "missing_event_id",
            "13": "invalid_measure"}
assert reasons == expected, {k: (reasons.get(k), v) for k, v in expected.items() if reasons.get(k) != v}
dropped = flagged.where("reason = 'duplicate_event_id'").first()
assert dropped["added_date"] == D(2026, 10, 7), "the later report of a duplicated id is the one dropped"
assert set(dc.REASONS) >= set(reasons.values())

eventcodes = spark.createDataFrame(sorted(lk["eventcodes"].items()), "code STRING, description STRING")
clean = dc.eligible_events(flagged, eventcodes, recency_days=2, first_day=D(2026, 10, 1), last_day=D(2026, 10, 29))
dyads = {r["event_id"]: r["dyad"] for r in clean.collect()}
assert dyads == {"1": "CHN_USA", "2": "CHN_USA", "3": "ISR_PSE", "5": "IRQ_USA"}, dyads
roots = {r["event_root_desc"] for r in clean.collect()}
assert roots == {lk["eventcodes"]["01"]}, roots
code_stats = {(r["status"], r["raw_code"]): r["count"] for r in dc.code_side_stats(flagged, lk).collect()}
assert code_stats[("remapped", "WSB")] == 1 and code_stats[("remapped", "GZS")] == 1
assert code_stats[("remapped", "BAG")] == 1 and code_stats[("region_excluded", "EUR")] == 1
assert code_stats[("unknown_code", "XYZ")] == 1
print("[3] cleaning flags OK: try_cast/try_to_timestamp turn bad values into invalid rows; remap, regions, "
      "unknown codes, duplicates (earliest report kept), same-country")
print("[4] undirected dyads OK: USA->CHN and CHN->USA both map to CHN_USA")

# 5. rolling window: RANGE frame across missing days, ratio of sums ------------------------------------------------
# Dyad AAA_BBB (synthetic, built directly as clean events):
#   Oct 1: tone 10, 1 mention;  Oct 2: tone -2, 3 mentions;  Oct 9: tone 4, 1 mention   (Oct 3..8 empty)
#   Oct 2 window (Sep 26..Oct 2) = (10*1 + -2*3) / (1+3) = 1.0
#        (the average of daily averages would be 4.0; the plain average of events would also be 4.0)
#   Oct 9 window (Oct 3..Oct 9) = 4.0 only. A ROWS frame of 7 rows would wrongly include Oct 1 and Oct 2.
#   Oct 8 boundary check, dyad CCC_DDD: events on Oct 2 and Oct 8 -> Oct 8 window (Oct 2..8) includes both.
def clean_ev(eid, dyad, day, tone, mentions, quad=1):
    a, b = dyad.split("_")
    return (eid, dyad, a, b, a, b, day, day, 0, "010", "010", "01", "x", quad, 1.0, mentions, tone, 1)


clean_schema = ("event_id STRING, dyad STRING, country_a STRING, country_b STRING, actor1_state STRING, "
                "actor2_state STRING, event_date DATE, added_date DATE, lag_days INT, event_code STRING, "
                "event_base_code STRING, event_root_code STRING, event_root_desc STRING, quad_class INT, "
                "goldstein DOUBLE, num_mentions INT, avg_tone DOUBLE, is_root INT")
synthetic = spark.createDataFrame([
    clean_ev("a1", "AAA_BBB", D(2026, 10, 1), 10.0, 1),
    clean_ev("a2", "AAA_BBB", D(2026, 10, 2), -2.0, 3, quad=4),
    clean_ev("a3", "AAA_BBB", D(2026, 10, 9), 4.0, 1),
    clean_ev("c1", "CCC_DDD", D(2026, 10, 2), 1.0, 1),
    clean_ev("c2", "CCC_DDD", D(2026, 10, 8), 3.0, 1),
], clean_schema)
dyad_day = dc.build_dyad_day(synthetic)
daily = dc.rolling_daily_index(spark, dyad_day, partitions=4, first_full_day=D(2026, 9, 1))
roll = {(r["dyad"], r["event_date"]): r for r in daily.collect()}
r2, r9 = roll[("AAA_BBB", D(2026, 10, 2))], roll[("AAA_BBB", D(2026, 10, 9))]
assert close(r2["tone_rolling7_wmean"], 1.0), r2["tone_rolling7_wmean"]
assert close(r9["tone_rolling7_wmean"], 4.0), r9["tone_rolling7_wmean"]
assert r9["events_rolling7"] == 1 and r9["active_days_rolling7"] == 1, "RANGE frame must not bridge the 6-day gap"
assert r2["material_conflict_rolling7"] == 1 and r9["material_conflict_rolling7"] == 0
r8 = roll[("CCC_DDD", D(2026, 10, 8))]
assert r8["events_rolling7"] == 2 and close(r8["tone_rolling7_wmean"], 2.0), "6 PRECEDING must include day d-6"
assert r9["row_key"] == "AAA_BBB#20261009"
# first_full_day trims rows whose window reaches before the first complete day.
trimmed = dc.rolling_daily_index(spark, dyad_day, partitions=4, first_full_day=D(2026, 10, 7))
assert {r["event_date"] for r in trimmed.collect()} == {D(2026, 10, 8), D(2026, 10, 9)}
print("[5] rolling 7-day OK: Oct 2 = 1.0 (ratio of sums, not 4.0), Oct 9 = 4.0 (gap not bridged), d-6 included")

# 6. weekly dense calendar, LAG, LEAD -----------------------------------------------------------------------------
# Dyad AAA_BBB has events in week Oct 5 and week Oct 19 only; week Oct 12 must exist with zero counts.
weeks = [D(2026, 10, 5), D(2026, 10, 12), D(2026, 10, 19)]
wk_events = spark.createDataFrame([
    clean_ev("w1", "AAA_BBB", D(2026, 10, 5), 2.0, 1, quad=4),
    clean_ev("w2", "AAA_BBB", D(2026, 10, 11), 4.0, 3),            # Sunday: still the Oct 5 week
    clean_ev("w3", "AAA_BBB", D(2026, 10, 19), -6.0, 2, quad=4),
    clean_ev("w4", "AAA_BBB", D(2026, 10, 20), -6.0, 2, quad=4),
    clean_ev("x1", "EEE_FFF", D(2026, 10, 5), 1.0, 1),             # too quiet: not selected
    clean_ev("x2", "EEE_FFF", D(2026, 10, 26), 1.0, 1),            # outside the complete weeks: ignored
], clean_schema)
wsum = dc.weekly_sums(dc.build_dyad_day(wk_events), weeks)
assert wsum.where("week_start = DATE'2026-10-26'").count() == 0
sel = dc.select_dyads(wsum, weeks, fraction=0.5, min_events_per_week=1)
# Selection weeks = first ceil(3 * 0.5) = 2 weeks; AAA_BBB has 2 events (>= 1 * 2), EEE_FFF has 1 (< 2).
assert [r["dyad"] for r in sel.collect()] == ["AAA_BBB"]
dense = dc.dense_weekly(spark, wsum, sel, weeks)
feats = {r["week_start"]: r for r in dc.weekly_features(spark, dense, partitions=4).collect()}
assert len(feats) == 3, "dense calendar must hold every week, including the empty one"
w1, w2, w3 = feats[weeks[0]], feats[weeks[1]], feats[weeks[2]]
assert w1["n_events"] == 2 and close(w1["tone_wmean"], (2 * 1 + 4 * 3) / 4) and close(w1["tone_mean"], 3.0)
assert w2["n_events"] == 0 and w2["material_conflict_events"] == 0 and w2["tone_wmean"] is None
assert w3["tone_wmean_lag1"] is None, "LAG must see the empty previous calendar week, not skip back to Oct 5"
assert close(w2["tone_wmean_lag1"], 3.5) and w2["tone_change_1w"] is None
assert close(w3["tone_trailing4_wmean"], 3.5), "trailing tone = ratio of sums over the previous weeks"
assert close(w3["tone_drop_vs_trailing4"], -6.0 - 3.5)
assert w1["label_material_conflict_next_week"] == 0 and w2["label_material_conflict_next_week"] == 2
assert w3["label_material_conflict_next_week"] is None, "last week has no next week inside the range"
assert w1["weeks_of_history"] == 0 and w3["weeks_of_history"] == 2
assert close(w3["conflict_share"], 1.0) and close(w1["conflict_share"], 0.5)
assert close(w3["material_conflict_trailing4_mean"], 0.5)
assert close(w3["material_conflict_trailing4_std"], math.sqrt(0.5))
assert w3["row_key"] == "AAA_BBB#20261019" and w3["week_end"] == D(2026, 10, 25)
print("[6] weekly OK: empty week kept with 0 counts / NULL tone, LAG uses the calendar week, LEAD label, "
      "trailing stats backward-only")

# 7. one exchange for the window -----------------------------------------------------------------------------------
# AQE off for this check only, so the static plan is what we inspect. "simple" mode keeps each Exchange and its
# partitioning on one line. Expected: the groupBy's exchange (by all four grouping keys) and our repartition by dyad
# alone, and NO further exchange in front of the Window, because hashpartitioning(dyad) already satisfies it.
import re  # noqa: E402
spark.conf.set("spark.sql.adaptive.enabled", "false")
plan = dc.explain_text(dc.rolling_daily_index(spark, dyad_day, partitions=4, first_full_day=D(2026, 9, 1)), "simple")
exchanges = [ln.strip() for ln in plan.splitlines() if "Exchange" in ln]
print("    exchanges in the rolling-window plan:")
for ln in exchanges:
    print("     ", ln[:110])
dyad_only = [ln for ln in exchanges if re.search(r"hashpartitioning\(dyad#\d+, 4\)", ln)]
assert len(dyad_only) == 1, "expected exactly one Exchange hashpartitioning(dyad, 4)"
assert len(exchanges) == 2, "expected 2 exchanges in total: the dyad-day groupBy and the repartition by dyad"
assert "Window" in plan
spark.conf.set("spark.sql.adaptive.enabled", "true")
fplan = dc.explain_text(flagged, "simple")
assert "BroadcastHashJoin" in fplan, "the state lookup must be broadcast-joined"
print("[7] plans OK: one hashpartitioning(dyad) exchange feeds the window; state lookup is a BroadcastHashJoin")

# 8. PageRank and salting (notebooks 04 and 05) -------------------------------------------------------------------
# A directed 3-cycle with equal weights is perfectly symmetric, so every node must get exactly 1/3.
cyc = [("A", "B", 1.0), ("B", "C", 1.0), ("C", "A", 1.0)]
r_cyc, _, _ = dc.pagerank(cyc)
assert all(close(v, 1 / 3) for v in r_cyc.values()), r_cyc
# A splits its weight 1:3 between B and C, B points to C, C has no out-edges (dangling). C must rank highest, ranks
# must sum to 1, and the Spark DataFrame version must match the driver version step for step.
g = [("A", "B", 1.0), ("A", "C", 3.0), ("B", "C", 1.0)]
r_g, iters, delta = dc.pagerank(g)
assert close(sum(r_g.values()), 1.0) and r_g["C"] > r_g["B"] > 0 and delta < 1e-12, (r_g, delta)
# Closed form for this graph (d = 0.85, N = 3, teleport t = (1 - d) / N = 0.05, dangling C spreads C/3 to everyone):
#   A = t + d*C/3            (A only receives teleport and C's dangling share)
#   B = A + d*A/4            (B also gets a quarter of A's rank)
#   C = 1 - A - B            (ranks sum to 1)
# Both A and B are functions of C, so C is the root of A(C) + B(C) + C - 1, found by bisection.
d_, t_ = 0.85, 0.05
a = lambda c: t_ + d_ * c / 3
b = lambda c: a(c) + d_ * a(c) / 4
lo, hi = 0.0, 1.0
for _ in range(200):
    mid = (lo + hi) / 2
    if a(mid) + b(mid) + mid - 1 > 0:
        hi = mid
    else:
        lo = mid
c_solved = (lo + hi) / 2
assert close(r_g["C"], c_solved, 1e-9) and close(r_g["A"], a(c_solved), 1e-9) and close(r_g["B"], b(c_solved), 1e-9)
g_df = spark.createDataFrame(g, "src STRING, dst STRING, weight DOUBLE")
spark_steps = dc.pagerank_spark_steps(spark, g_df, steps=3)
driver_steps, _, _ = dc.pagerank(g, max_iter=3, tol=0.0)
assert all(close(spark_steps[k], driver_steps[k], 1e-12) for k in driver_steps), (spark_steps, driver_steps)

# Salting: USA holds 60 % and CHN 15 % of 1,000 rows; with 8 partitions the fair share is 12.5 %, so
# USA gets ceil(0.6 * 8) = 5 buckets, CHN gets ceil(0.15 * 8) = 2, and the rest stay unsalted.
counts = {"USA": 600, "CHN": 150, "RUS": 100, "FRA": 50, "DEU": 50, "GBR": 50}
salts = dc.hot_key_salts(counts, 8)
assert salts == {"USA": 5, "CHN": 2}, salts
rows = [(k, f"{k}-{i}", 1, 1.0) for k, n in counts.items() for i in range(n)]
sk = spark.createDataFrame(rows, "key STRING, event_id STRING, num_mentions INT, avg_tone DOUBLE")
salted = dc.add_salt(sk, spark, salts)
by_key = {r["key"]: sorted(r["s"]) for r in salted.groupBy("key").agg(F.collect_set("salt").alias("s")).collect()}
assert by_key["USA"] == [0, 1, 2, 3, 4] and by_key["CHN"] == [0, 1] and by_key["RUS"] == [0], by_key
two_stage = {r["key"]: r["n"] for r in salted.groupBy("key", "salt").agg(F.count(F.lit(1)).alias("n"))
             .groupBy("key").agg(F.sum("n").alias("n")).collect()}
assert two_stage == counts, two_stage
small = spark.createDataFrame([(k, 0.0) for k in counts], "key STRING, key_tone DOUBLE")
exploded = {r["key"]: r["n"] for r in dc.explode_salts(small, spark, salts).groupBy("key").count()
            .withColumnRenamed("count", "n").collect()}
assert exploded == {"USA": 5, "CHN": 2, "RUS": 1, "FRA": 1, "DEU": 1, "GBR": 1}, exploded
assert salted.join(dc.explode_salts(small, spark, salts), ["key", "salt"]).count() == 1000
before = dc.partition_sizes(sk.repartition(8, "key"))
after = dc.partition_sizes(salted.repartition(8, "key", "salt"))
assert before[0] >= 600 and after[0] < before[0] and sum(after) == 1000, (before, after)
assert dc.median([3, 1, 2]) == 2 and dc.median([4, 1, 2, 3]) == 2.5
print(f"[8] PageRank OK (cycle = 1/3 each; dangling graph matches closed form; Spark = driver after 3 steps); "
      f"salting OK (buckets {salts}, two-stage counts exact, largest partition {before[0]} -> {after[0]} rows)")

print("ALL CHECKS PASSED")
spark.stop()
