"""Helpers for Member 3's dyad notebooks (03_dyad_tone_index, 04_actor_network, 05_skew_salting_ab).

Kept free of Databricks-only APIs (dbutils, display) for the same reason as gdelt_common.py: the notebooks import it
from their Git folder, and tests/test_dyads_local.py runs the exact same functions on a local Spark with synthetic
rows. The notebook explains each step in markdown; this module holds the code that has to be identical in both places.

Conventions used throughout:
  * A dyad is an UNORDERED pair of states, keyed "AAA_BBB" with the two CAMEO codes sorted, so USA->CHN and
    CHN->USA events land in the same dyad "CHN_USA".
  * The time axis is SQLDATE (the event date). DATEADDED (the file date) is only used for the recency filter
    0 <= DATEADDED - SQLDATE <= N and, through file_year/file_month, for partition pruning.
  * Weeks start on Monday and are keyed by their start date (never by week-of-year, which wraps at New Year).

AI citation: Claude Code (Claude Opus 5.5, Anthropic) drafted this module (lookup loading, typing and cleaning rules, dyad-day and rolling-window SQL,
weekly features, PageRank, salting and timing helpers). Reviewed and tested by Neel Patel.
"""
import contextlib
import datetime as dt
import io
import math
import os
import sys

# --- lookups ------------------------------------------------------------------------------------------------
LOOKUP_FILES = {
    "countries": "CAMEO.country.txt",
    "eventcodes": "CAMEO.eventcodes.txt",
    "nonstate": "cameo_nonstate_codes.txt",
    "remap": "cameo_code_remap.txt",
}


def find_lookups_dir(search_paths=None):
    """Locate the repo's lookups/ folder. Databricks Git folders put notebooks/ on sys.path, so lookups/ is its sibling."""
    for p in (search_paths if search_paths is not None else sys.path):
        for cand in (os.path.join(p, "..", "lookups"), os.path.join(p, "lookups")):
            cand = os.path.normpath(cand)
            if os.path.isfile(os.path.join(cand, LOOKUP_FILES["countries"])):
                return cand
    raise FileNotFoundError("lookups/ not found next to the notebooks; open the notebooks from the repo's Git folder")


def read_tsv(path):
    """Rows of a tab-separated lookup file, header skipped. The GDELT files use CRLF, so '\\r' is stripped."""
    rows = []
    with open(path, encoding="utf-8", errors="replace") as f:
        for i, line in enumerate(f):
            line = line.rstrip("\r\n")
            if i == 0 or not line.strip():
                continue
            rows.append([part.strip() for part in line.split("\t")])
    return rows


def load_lookups(lookups_dir=None):
    """All lookup tables as plain dicts.

    states = every CAMEO country code that is neither a region (excluded) nor a sub-state code (remapped).
    Remapped codes are not states themselves: after the remap they have become their target state's code.
    """
    d = lookups_dir or find_lookups_dir()
    countries = {r[0]: r[1] for r in read_tsv(os.path.join(d, LOOKUP_FILES["countries"]))}
    eventcodes = {r[0]: r[1] for r in read_tsv(os.path.join(d, LOOKUP_FILES["eventcodes"]))}
    nonstate = {r[0]: r[1] for r in read_tsv(os.path.join(d, LOOKUP_FILES["nonstate"]))}
    remap = {r[0]: r[1] for r in read_tsv(os.path.join(d, LOOKUP_FILES["remap"]))}
    states = {c: n for c, n in countries.items() if c not in nonstate and c not in remap}
    # Every remap target must itself be a state, otherwise a remapped row would silently fall out later.
    bad_targets = sorted(t for t in remap.values() if t not in states)
    if bad_targets:
        raise ValueError(f"remap targets that are not states: {bad_targets}")
    return {"dir": d, "countries": countries, "eventcodes": eventcodes, "nonstate": nonstate,
            "remap": remap, "states": states}


def dyad_key(a, b):
    """Plain-Python version of the dyad key, used in tests and for readable output."""
    lo, hi = sorted((a, b))
    return f"{lo}_{hi}"


# --- dates, partitions and windows ------------------------------------------------------------------------
def _d(x):
    return x if isinstance(x, dt.date) else dt.date.fromisoformat(str(x))


def month_range_predicate(start, end, year_col="file_year", month_col="file_month"):
    """SQL predicate on the partition columns covering the months of [start, end].

    Written with plain comparisons on the partition columns only, so Delta can prune partitions from its log
    without reading any data files. A filter on SQLDATE would scan every partition (handoff gotcha 5).
    """
    s, e = _d(start), _d(end)
    if s.year == e.year:
        return f"({year_col} = {s.year} AND {month_col} BETWEEN {s.month} AND {e.month})"
    parts = [f"({year_col} = {s.year} AND {month_col} >= {s.month})"]
    if e.year - s.year > 1:
        parts.append(f"({year_col} > {s.year} AND {year_col} < {e.year})")
    parts.append(f"({year_col} = {e.year} AND {month_col} <= {e.month})")
    return "(" + " OR ".join(parts) + ")"


def week_start(day):
    """Monday of the week containing day (the same rule as the SQL expression WEEK_START_SQL)."""
    day = _d(day)
    return day - dt.timedelta(days=day.weekday())


def complete_days(range_start, range_end, recency_days):
    """First and last SQLDATE whose events are fully present in the loaded files.

    Files cover DATEADDED in [range_start, range_end]. With the recency filter, an event dated d can be reported
    in files dated d .. d+N. So d is complete only if d >= range_start (earlier days only have their late reports)
    and d + N <= range_end (later days are still missing reports from files that are not loaded).
    """
    return _d(range_start), _d(range_end) - dt.timedelta(days=recency_days)


def complete_week_starts(range_start, range_end, recency_days):
    """Monday starts of the weeks lying entirely inside the complete days. Partial weeks at both ends are dropped."""
    first, last = complete_days(range_start, range_end, recency_days)
    w = week_start(first)
    if w < first:
        w += dt.timedelta(days=7)
    weeks = []
    while w + dt.timedelta(days=6) <= last:
        weeks.append(w)
        w += dt.timedelta(days=7)
    return weeks


def choose_recency_days(lag_counts, total, max_days=14, min_next_day_share=0.005):
    """Pick N for the recency filter 0 <= DATEADDED - SQLDATE <= N from the observed lag distribution.

    lag_counts maps lag in days -> rows, total is all eligible rows (every lag, including very old events).
    N is the smallest value from 1 to max_days for which widening the window by one more day would add less than
    min_next_day_share of all rows: past that point each extra day buys almost no events but makes the most recent
    days (and so the last week) wait longer to be complete. Same-day and next-day reports are always kept.
    """
    if total <= 0:
        return max_days
    for n in range(1, max_days + 1):
        if lag_counts.get(n + 1, 0) / total < min_next_day_share:
            return n
    return max_days


def shuffle_partitions_for(rows, target_rows_per_partition=500_000, lo=8, hi=200, multiple=8):
    """Shuffle partition count sized from the input row count.

    About 500k narrow rows (the ~15 typed columns this notebook keeps, roughly 100 bytes each) is about 50 MB per
    partition, inside the usual 16-128 MB guideline: big enough that task start-up does not dominate, small enough
    to stay well away from spilling. Rounded up to a multiple of 8 so it divides evenly over typical core counts.
    """
    n = max(lo, min(hi, math.ceil(max(rows, 1) / target_rows_per_partition)))
    return int(math.ceil(n / multiple) * multiple)


# --- Spark helpers --------------------------------------------------------------------------------------------
def explain_text(df, mode="formatted"):
    """The physical plan as a string. df.explain() prints, in classic PySpark and in Spark Connect (serverless) alike,
    so capturing stdout is the one way that works on both."""
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        df.explain(mode=mode)
    return buf.getvalue()


def plan_lines(plan, *needles):
    """Plan lines containing any of the needles (for printing just the relevant part of a long plan)."""
    return [ln.strip() for ln in plan.splitlines() if any(n in ln for n in needles)]


def partition_sizes(df):
    """Rows in each partition of df, largest first. Uses spark_partition_id() rather than df.rdd, because the RDD API
    does not exist on serverless (Spark Connect)."""
    from pyspark.sql import functions as F
    rows = df.groupBy(F.spark_partition_id().alias("pid")).count().collect()
    return sorted((r["count"] for r in rows), reverse=True)


def size_summary(sizes):
    """max / median / share-of-largest summary of a partition_sizes() list. spark_partition_id() only sees partitions
    that hold rows, so the count is of NON-EMPTY partitions and the median is over those."""
    if not sizes:
        return {"nonempty_partitions": 0, "max": 0, "median": 0, "max_over_median": None, "largest_share": None}
    s = sorted(sizes)
    med = s[len(s) // 2] if len(s) % 2 else (s[len(s) // 2 - 1] + s[len(s) // 2]) / 2
    total = sum(s)
    return {"nonempty_partitions": len(s), "max": s[-1], "median": med,
            "max_over_median": round(s[-1] / med, 2) if med else None,
            "largest_share": round(s[-1] / total, 4) if total else None}


# --- typing and cleaning --------------------------------------------------------------------------------------
def _code(c):
    """String code column: trimmed, upper-cased, '' turned into NULL. CAST(... AS STRING) first, so this works whether
    the source column is still a string (bronze) or was already typed (Member 2's table)."""
    return f"NULLIF(UPPER(TRIM(CAST(`{c}` AS STRING))), '')"


def _num(c, sql_type):
    """try_cast, not CAST: with ANSI mode on (the serverless default) a single malformed value would make CAST raise
    and fail the whole run. try_cast turns it into NULL, and the row is then counted as invalid_measure."""
    return f"try_cast(TRIM(CAST(`{c}` AS STRING)) AS {sql_type})"


def _date(df, c):
    """yyyyMMdd string -> DATE. Already-typed DATE/TIMESTAMP columns pass through. The RLIKE guard plus
    try_to_timestamp returns NULL for anything malformed (e.g. 20261399) instead of raising under ANSI mode."""
    from pyspark.sql import functions as F
    dtype = dict(df.dtypes)[c]
    if dtype == "date":
        return F.col(c)
    if dtype.startswith("timestamp"):
        return F.to_date(F.col(c))
    s = f"TRIM(CAST(`{c}` AS STRING))"
    return F.expr(f"CASE WHEN {s} RLIKE '^[0-9]{{8}}$' THEN to_date(try_to_timestamp({s}, 'yyyyMMdd')) END")


TYPED_COLUMNS = ["event_id", "event_date", "added_date", "actor1_country", "actor2_country", "event_code",
                 "event_base_code", "event_root_code", "quad_class", "goldstein", "num_mentions", "avg_tone",
                 "is_root"]
# Carried along only to break ties between duplicate GLOBALEVENTIDs (see flag_events), when the source has them.
TIEBREAK_COLUMNS = {"source_file": "source_file", "SOURCEURL": "source_url"}


def typed_events(df):
    """Project and type the columns this analysis needs. Event codes stay strings: '010' and '10' are different codes.
    source_file and SOURCEURL are kept (as source_file, source_url) when present, for the duplicate tie-breakers."""
    from pyspark.sql import functions as F
    cols = [
        F.expr(_code("GLOBALEVENTID")).alias("event_id"),
        _date(df, "SQLDATE").alias("event_date"),
        _date(df, "DATEADDED").alias("added_date"),
        F.expr(_code("Actor1CountryCode")).alias("actor1_country"),
        F.expr(_code("Actor2CountryCode")).alias("actor2_country"),
        F.expr(_code("EventCode")).alias("event_code"),
        F.expr(_code("EventBaseCode")).alias("event_base_code"),
        F.expr(_code("EventRootCode")).alias("event_root_code"),
        F.expr(_num("QuadClass", "INT")).alias("quad_class"),
        F.expr(_num("GoldsteinScale", "DOUBLE")).alias("goldstein"),
        F.expr(_num("NumMentions", "INT")).alias("num_mentions"),
        F.expr(_num("AvgTone", "DOUBLE")).alias("avg_tone"),
        F.expr(_num("IsRootEvent", "INT")).alias("is_root"),
    ]
    present = {c.lower(): c for c in df.columns}
    cols += [F.expr(f"NULLIF(TRIM(CAST(`{present[src.lower()]}` AS STRING)), '')").alias(dst)
             for src, dst in TIEBREAK_COLUMNS.items() if src.lower() in present]
    return df.select(*cols)


# Rule order matters: each row gets the FIRST rule it fails, so the funnel adds up to the input row count.
REASONS = ["missing_event_id", "duplicate_event_id", "invalid_date", "missing_country", "region_code",
           "unknown_code", "same_country", "invalid_measure", "eligible"]


def flag_events(typed, lookups, spark):
    """Apply the cleaning and dyad rules as FLAGS (a `reason` column) instead of filters, so a single aggregation can
    report how many rows every rule removes. Filtering on reason == 'eligible' gives the clean dyadic events.

    Steps: dedupe on event_id -> remap sub-state codes -> broadcast-join the state lookup for both actors ->
    classify. lag_days = DATEADDED - SQLDATE is computed for the recency filter applied later.
    """
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    remap, regions = lookups["remap"], sorted(lookups["nonstate"])

    def remapped(c):
        if not remap:
            return F.col(c)
        m = F.create_map(*[F.lit(x) for kv in sorted(remap.items()) for x in kv])
        return F.coalesce(m[F.col(c)], F.col(c))

    # Keep the first report of a duplicated GLOBALEVENTID: earliest DATEADDED, then most mentions, then smallest
    # source_file and SOURCEURL (the same order as Member 2's cleaning step), then a hash of the event's fields, so the
    # choice is deterministic. dropDuplicates() would keep an arbitrary copy.
    order = [F.col("added_date").asc_nulls_last(), F.col("num_mentions").desc_nulls_last()]
    order += [F.col(c).asc_nulls_last() for c in ("source_file", "source_url") if c in typed.columns]
    order.append(F.xxhash64(*TYPED_COLUMNS).asc())
    first_report = Window.partitionBy("event_id").orderBy(*order)
    df = (typed.withColumn("dup_rank", F.row_number().over(first_report))
               .withColumn("a", remapped("actor1_country"))
               .withColumn("b", remapped("actor2_country")))

    # Explicit broadcast: the state list is 233 rows, so every executor gets a full copy and the event stream is joined
    # without being shuffled. Spark would probably pick this by itself; the hint makes it certain and visible in explain().
    states = spark.createDataFrame(sorted(lookups["states"].items()), "code STRING, name STRING")
    df = (df.join(F.broadcast(states.select(F.col("code").alias("a"), F.col("name").alias("a_name"))), "a", "left")
            .join(F.broadcast(states.select(F.col("code").alias("b"), F.col("name").alias("b_name"))), "b", "left"))

    reason = (F.when(F.col("event_id").isNull(), "missing_event_id")
               .when(F.col("dup_rank") > 1, "duplicate_event_id")
               .when(F.col("event_date").isNull() | F.col("added_date").isNull(), "invalid_date")
               .when(F.col("a").isNull() | F.col("b").isNull(), "missing_country")
               .when(F.col("a").isin(regions) | F.col("b").isin(regions), "region_code")
               .when(F.col("a_name").isNull() | F.col("b_name").isNull(), "unknown_code")
               .when(F.col("a") == F.col("b"), "same_country")
               .when(F.col("avg_tone").isNull() | F.col("num_mentions").isNull() | (F.col("num_mentions") < 1)
                     | F.col("quad_class").isNull(), "invalid_measure")
               .otherwise("eligible"))
    return (df.withColumn("reason", reason)
              .withColumn("lag_days", F.datediff("added_date", "event_date")))


def lag_bucket_col():
    """Lag in days clamped for reporting: -1 means negative (event dated after its file), 31 means more than 30 days."""
    from pyspark.sql import functions as F
    lag = F.col("lag_days")
    return F.when(lag < 0, -1).when(lag > 30, 31).otherwise(lag)


def code_side_stats(flagged, lookups):
    """Actor-code values that were remapped, excluded as regions, or unknown, counted per raw code.
    Counted per actor value (an event has two), on rows that survived the event_id checks."""
    from pyspark.sql import functions as F
    base = flagged.where("event_id IS NOT NULL AND dup_rank = 1")
    sides = (base.select(F.col("actor1_country").alias("raw_code"), F.col("a").alias("mapped_code"),
                         F.col("a_name").isNotNull().alias("is_state"))
             .unionByName(base.select(F.col("actor2_country").alias("raw_code"), F.col("b").alias("mapped_code"),
                                      F.col("b_name").isNotNull().alias("is_state"))))
    status = (F.when(F.col("raw_code").isin(sorted(lookups["remap"])), "remapped")
               .when(F.col("raw_code").isin(sorted(lookups["nonstate"])), "region_excluded")
               .when(~F.col("is_state") & F.col("raw_code").isNotNull(), "unknown_code"))
    return (sides.withColumn("status", status).where("status IS NOT NULL")
                 .groupBy("status", "raw_code", "mapped_code").count())


def eligible_events(flagged, eventcodes_df, recency_days, first_day, last_day):
    """Clean dyadic events kept for the analysis: eligible rows, inside the recency window, on complete days.

    country_a/country_b are the two states in sorted order, which makes the dyad undirected. actor1/actor2 keep the
    direction for the actor network. The event root description comes from a broadcast join to the CAMEO event codes.
    """
    from pyspark.sql import functions as F
    roots = F.broadcast(eventcodes_df.select(F.col("code").alias("event_root_code"),
                                             F.col("description").alias("event_root_desc")))
    return (flagged
            .where((F.col("reason") == "eligible")
                   & F.col("lag_days").between(0, recency_days)
                   & F.col("event_date").between(F.lit(first_day), F.lit(last_day)))
            .withColumn("country_a", F.least("a", "b"))
            .withColumn("country_b", F.greatest("a", "b"))
            .withColumn("dyad", F.concat_ws("_", "country_a", "country_b"))
            .join(roots, "event_root_code", "left")
            .select("event_id", "dyad", "country_a", "country_b",
                    F.col("a").alias("actor1_state"), F.col("b").alias("actor2_state"),
                    "event_date", "added_date", "lag_days", "event_code", "event_base_code", "event_root_code",
                    "event_root_desc", "quad_class", "goldstein", "num_mentions", "avg_tone", "is_root"))


# --- dyad-day, rolling index, weekly features -------------------------------------------------------------------
def build_dyad_day(events):
    """Step 1 of the two-step aggregation: events -> one row per dyad per day, holding SUMS rather than averages.

    Sums can be added up again over any window (7 days, a week, 4 weeks) and divided at the end, which is what makes
    the window tone a true mentions-weighted mean. Averaging daily averages would give a quiet day the same weight as a
    busy one.
    """
    from pyspark.sql import functions as F
    m = F.col("num_mentions").cast("double")
    logm = F.log1p(m)
    q = F.col("quad_class")
    return (events.groupBy("dyad", "country_a", "country_b", "event_date")
            .agg(F.count(F.lit(1)).alias("n_events"),
                 F.sum(m).alias("sum_mentions"),
                 F.sum(F.col("avg_tone") * m).alias("sum_tone_x_mentions"),
                 F.sum("avg_tone").alias("sum_tone"),
                 F.sum(F.col("avg_tone") * logm).alias("sum_tone_x_logm"),
                 F.sum(logm).alias("sum_logm"),
                 F.sum("goldstein").alias("sum_goldstein"),
                 F.count(F.when(F.col("goldstein").isNotNull(), 1)).alias("n_goldstein"),
                 F.count(F.when(q == 1, 1)).alias("verbal_coop_events"),
                 F.count(F.when(q == 2, 1)).alias("material_coop_events"),
                 F.count(F.when(q == 3, 1)).alias("verbal_conflict_events"),
                 F.count(F.when(q == 4, 1)).alias("material_conflict_events"),
                 F.sum(F.when(q == 4, m).otherwise(0.0)).alias("material_conflict_mentions"),
                 F.count(F.when((q == 4) & (F.col("is_root") == 1), 1)).alias("root_material_conflict_events"))
            .withColumn("day_num", F.datediff("event_date", F.lit("1970-01-01"))))


# RANGE frame on the day number: "the 7 calendar days ending today", however many of those days had events.
# A ROWS frame would take the last 7 ACTIVE days, which for a quiet dyad can stretch over weeks.
# The weighted tone is a ratio of window sums: sum(tone * mentions) / sum(mentions) over all events in the 7 days.
ROLLING_SQL = """
SELECT
  concat(dyad, '#', date_format(event_date, 'yyyyMMdd'))                     AS row_key,
  dyad, country_a, country_b, event_date,
  n_events,
  material_conflict_events,
  sum_tone_x_mentions / NULLIF(sum_mentions, 0)                               AS tone_wmean_day,
  SUM(sum_tone_x_mentions) OVER w / NULLIF(SUM(sum_mentions) OVER w, 0)       AS tone_rolling7_wmean,
  SUM(n_events) OVER w                                                        AS events_rolling7,
  SUM(sum_mentions) OVER w                                                    AS mentions_rolling7,
  SUM(material_conflict_events) OVER w                                        AS material_conflict_rolling7,
  COUNT(*) OVER w                                                             AS active_days_rolling7
FROM {view}
WINDOW w AS (PARTITION BY dyad ORDER BY day_num RANGE BETWEEN 6 PRECEDING AND CURRENT ROW)
"""


def rolling_daily_index(spark, dyad_day, partitions, first_full_day, view="m3_dyad_day_v"):
    """Rolling 7-day mentions-weighted tone for every dyad-day that has events.

    The input is repartitioned by dyad first, with an explicit partition count: the window needs all rows of one dyad
    in one task anyway, and doing it explicitly lets Spark reuse this one exchange for the window instead of planning
    its own (checked in the notebook and the test with explain()).
    Days before first_full_day are computed (they feed later windows) but not returned, because their 7-day window
    would reach back before the first complete day.
    """
    from pyspark.sql import functions as F
    dyad_day.repartition(partitions, "dyad").createOrReplaceTempView(view)
    return spark.sql(ROLLING_SQL.format(view=view)).where(F.col("event_date") >= F.lit(first_full_day))


# Monday of the event's week. dayofweek() is 1 = Sunday ... 7 = Saturday, so (dayofweek + 5) % 7 is days since Monday.
# Pure date arithmetic, so the result cannot shift with the session time zone the way date_trunc's timestamp could.
WEEK_START_SQL = "date_sub(event_date, (dayofweek(event_date) + 5) % 7)"

WEEKLY_SUM_COLUMNS = ["n_events", "sum_mentions", "sum_tone_x_mentions", "sum_tone", "sum_tone_x_logm", "sum_logm",
                      "sum_goldstein", "n_goldstein", "verbal_coop_events", "material_coop_events",
                      "verbal_conflict_events", "material_conflict_events", "material_conflict_mentions",
                      "root_material_conflict_events"]


def weekly_sums(dyad_day, week_starts):
    """Dyad-day sums rolled up to Monday-start weeks, keeping only the complete weeks."""
    from pyspark.sql import functions as F
    return (dyad_day.withColumn("week_start", F.expr(WEEK_START_SQL))
            .where(F.col("week_start").between(F.lit(week_starts[0]), F.lit(week_starts[-1])))
            .groupBy("dyad", "country_a", "country_b", "week_start")
            .agg(*[F.sum(c).alias(c) for c in WEEKLY_SUM_COLUMNS],
                 F.countDistinct("event_date").alias("active_days")))


def selection_weeks(week_starts, fraction=0.5):
    """The earliest weeks used to decide which dyads enter the feature table (at least one week)."""
    return week_starts[:max(1, math.ceil(len(week_starts) * fraction))]


def select_dyads(weekly, week_starts, fraction=0.5, min_events_per_week=5):
    """Dyads active enough to model, judged ONLY on the earliest `fraction` of weeks.

    Choosing on the whole period would use the test weeks to decide what is in the data (selection leakage). With
    fraction=0.5 the choice only uses data that sits inside any training period covering at least the first half.
    """
    from pyspark.sql import functions as F
    sel = selection_weeks(week_starts, fraction)
    return (weekly.where(F.col("week_start").between(F.lit(sel[0]), F.lit(sel[-1])))
            .groupBy("dyad", "country_a", "country_b")
            .agg(F.sum("n_events").alias("selection_events"))
            .where(F.col("selection_events") >= min_events_per_week * len(sel)))


def dense_weekly(spark, weekly, dyads, week_starts):
    """Every selected dyad x every complete week, with zero counts for weeks without events.

    Without this calendar, LAG(...) would silently skip empty weeks and compare a week with one from a month ago,
    the same bug a ROWS frame causes for days. Tone stays NULL in an empty week (there is no tone to measure);
    counts become 0, because "no events reported" is a real observation.
    """
    from pyspark.sql import functions as F
    weeks = spark.createDataFrame([(w,) for w in week_starts], "week_start DATE")
    grid = dyads.select("dyad", "country_a", "country_b").crossJoin(F.broadcast(weeks))
    joined = grid.join(weekly.drop("country_a", "country_b"), ["dyad", "week_start"], "left")
    return joined.fillna(0, subset=WEEKLY_SUM_COLUMNS + ["active_days"])


# Every window looks BACKWARD only (LAG, or frames ending at 1 PRECEDING), so a week's features never contain
# information from later weeks. The only forward-looking columns are the label_ ones (LEAD), named so nobody uses them
# as features. All windows run on the dense calendar, so ROWS frames count calendar weeks.
WEEKLY_FEATURES_SQL = """
WITH base AS (
  SELECT *,
    sum_tone_x_mentions / NULLIF(sum_mentions, 0)           AS tone_wmean,
    sum_tone / NULLIF(n_events, 0)                          AS tone_mean,
    sum_tone_x_logm / NULLIF(sum_logm, 0)                   AS tone_logw_mean,
    sum_goldstein / NULLIF(n_goldstein, 0)                  AS goldstein_mean,
    material_conflict_events / NULLIF(n_events, 0)          AS conflict_share
  FROM {view}
)
SELECT
  concat(dyad, '#', date_format(week_start, 'yyyyMMdd'))   AS row_key,
  dyad, country_a, country_b, week_start,
  date_add(week_start, 6)                                  AS week_end,
  n_events, active_days, sum_mentions,
  tone_wmean, tone_mean, tone_logw_mean, goldstein_mean,
  verbal_coop_events, material_coop_events, verbal_conflict_events,
  material_conflict_events, material_conflict_mentions, root_material_conflict_events, conflict_share,

  LAG(tone_wmean, 1) OVER (PARTITION BY dyad ORDER BY week_start)                                   AS tone_wmean_lag1,
  tone_wmean - LAG(tone_wmean, 1) OVER (PARTITION BY dyad ORDER BY week_start)                      AS tone_change_1w,
  SUM(sum_tone_x_mentions) OVER (PARTITION BY dyad ORDER BY week_start ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING)
    / NULLIF(SUM(sum_mentions) OVER (PARTITION BY dyad ORDER BY week_start ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING), 0)
                                                                                                   AS tone_trailing4_wmean,
  LAG(material_conflict_events, 1) OVER (PARTITION BY dyad ORDER BY week_start)                     AS material_conflict_lag1,
  AVG(material_conflict_events) OVER (PARTITION BY dyad ORDER BY week_start ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING)
                                                                                                   AS material_conflict_trailing4_mean,
  STDDEV_SAMP(material_conflict_events) OVER (PARTITION BY dyad ORDER BY week_start ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING)
                                                                                                   AS material_conflict_trailing4_std,
  AVG(n_events) OVER (PARTITION BY dyad ORDER BY week_start ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING)
                                                                                                   AS n_events_trailing4_mean,
  COUNT(*) OVER (PARTITION BY dyad ORDER BY week_start ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
                                                                                                   AS weeks_of_history,

  LEAD(material_conflict_events, 1) OVER (PARTITION BY dyad ORDER BY week_start)                    AS label_material_conflict_next_week,
  LEAD(conflict_share, 1) OVER (PARTITION BY dyad ORDER BY week_start)                              AS label_conflict_share_next_week
FROM base
"""


def weekly_features(spark, dense, partitions, view="m3_dyad_week_v"):
    """Weekly feature table for Member 4. Repartitioned by dyad first for the same reason as the daily window."""
    from pyspark.sql import functions as F
    dense.repartition(partitions, "dyad").createOrReplaceTempView(view)
    out = spark.sql(WEEKLY_FEATURES_SQL.format(view=view))
    # tone_drop_vs_trailing4 is the "drop in tone" signal of the research question: negative = tone fell this week
    # relative to the dyad's own previous four weeks.
    return out.withColumn("tone_drop_vs_trailing4", F.col("tone_wmean") - F.col("tone_trailing4_wmean"))


# --- events source (shared by notebooks 03, 04 and 05) ----------------------------------------------------------
SOURCE_COLUMNS = ["GLOBALEVENTID", "SQLDATE", "DATEADDED", "Actor1CountryCode", "Actor2CountryCode", "EventCode",
                  "EventBaseCode", "EventRootCode", "QuadClass", "GoldsteinScale", "NumMentions", "AvgTone",
                  "IsRootEvent"]


def file_months(spark, table, range_start, range_end):
    """The (year, month) file months a table holds inside [range_start, range_end].

    Uses file_year/file_month when the table has them: selecting only partition columns reads no data columns, so this
    is cheap. Otherwise the file month is derived from DATEADDED.
    """
    from pyspark.sql import functions as F
    df = spark.table(table)
    lower = {c.lower(): c for c in df.columns}
    if {"file_year", "file_month"} <= set(lower):
        y, m = lower["file_year"], lower["file_month"]
        rows = df.where(month_range_predicate(range_start, range_end, y, m)).select(y, m).distinct().collect()
    else:
        d = _date(df, lower["dateadded"])
        rows = (df.select(d.alias("d")).where(F.col("d").between(F.lit(range_start), F.lit(range_end)))
                  .select(F.year("d"), F.month("d")).distinct().collect())
    return {(int(r[0]), int(r[1])) for r in rows}


def clean_coverage_gaps(spark, events_clean, events_bronze, range_start, range_end):
    """File months that events_bronze has for the range but events_clean does not (sorted). Empty = complete.

    Guards against a partially built cleaned table: e.g. 02_clean_events run only in sample mode (October), then this
    notebook run on the full year, which would otherwise analyse one month and call it the full year.
    """
    if not spark.catalog.tableExists(events_bronze):
        return []
    return sorted(file_months(spark, events_bronze, range_start, range_end)
                  - file_months(spark, events_clean, range_start, range_end))


def resolve_events_source(spark, choice, events_clean, events_bronze, range_start, range_end):
    """Pick the events table and restrict it to the run's file months.

    choice: 'auto' (Member 2's EVENTS_CLEAN if it exists in this workspace, else events_bronze), 'member2_clean' or
    'bronze_fallback'. Returns a dict with the chosen table, the in-range DataFrame and everything the banner prints.
    If the table has file_year/file_month, the range is applied on those partition columns so Delta prunes; otherwise
    typed_in_range() filters on DATEADDED after typing (correct, but without pruning).

    Coverage guard: Member 2's table is only used if it holds every file month that events_bronze holds for the range.
    With 'auto' a gap falls back to events_bronze with a NOTE in the banner; with 'member2_clean' a gap is an error.
    """
    member2_ready = spark.catalog.tableExists(events_clean)
    if choice == "member2_clean" and not member2_ready:
        raise RuntimeError(f"events_source=member2_clean but {events_clean} does not exist in this workspace")
    gaps, coverage_note = [], None
    if member2_ready and choice != "bronze_fallback":
        gaps = clean_coverage_gaps(spark, events_clean, events_bronze, range_start, range_end)
        if gaps:
            months = ", ".join(f"{y}-{m:02d}" for y, m in gaps)
            msg = f"{events_clean} is missing file months {months} that {events_bronze} has for this range"
            if choice == "member2_clean":
                raise RuntimeError(f"{msg}; run 02_clean_events with the same sample_mode first")
            coverage_note = f"{msg}; using the fallback instead (run 02_clean_events with the same sample_mode to fix)"
    use_member2 = choice == "member2_clean" or (choice == "auto" and member2_ready and not gaps)
    source = events_clean if use_member2 else events_bronze

    df = spark.table(source)
    lower = {c.lower(): c for c in df.columns}
    missing = [c for c in SOURCE_COLUMNS if c.lower() not in lower]
    if missing:
        raise RuntimeError(f"{source} is missing columns {missing}; agree the column names with Member 2")
    dtypes = dict(df.dtypes)
    warnings = [f"{c} is not a string in {source}; leading zeros ('010') may already be lost"
                for c in ["EventCode", "EventBaseCode", "EventRootCode"] if dtypes[lower[c.lower()]] != "string"]

    partitioned = {"file_year", "file_month"} <= set(lower)
    predicate = month_range_predicate(range_start, range_end, lower.get("file_year", "file_year"),
                                      lower.get("file_month", "file_month")) if partitioned else None
    return {"source": source, "use_member2": use_member2, "member2_ready": member2_ready,
            "events_clean": events_clean, "predicate": predicate, "warnings": warnings,
            "coverage_gaps": gaps, "coverage_note": coverage_note,
            "df_in_range": df.where(predicate) if predicate else df,
            "range_start": range_start, "range_end": range_end}


def source_banner(info):
    """The loud banner every notebook prints, so fallback results are never mistaken for Member 2's."""
    line = "=" * 100
    how = ("  -> Member 2's cleaned table" if info["use_member2"] else
           f"  -> FALLBACK: cast-and-clean over events_bronze inside the notebook ({info['events_clean']} "
           f"{'exists but was not used' if info['member2_ready'] else 'does not exist yet'})")
    out = [line, f"EVENTS SOURCE: {info['source']}", how,
           f"  partition filter: {info['predicate'] or 'none (source has no file_year/file_month)'}"]
    if info.get("coverage_note"):
        out.append(f"  NOTE: {info['coverage_note']}")
    out += [f"  WARNING: {w}" for w in info["warnings"]]
    return "\n".join(out + [line])


def typed_in_range(info):
    """typed_events() over the chosen source, with the DATEADDED range applied when partition pruning was not possible."""
    from pyspark.sql import functions as F
    typed = typed_events(info["df_in_range"])
    if info["predicate"] is None:
        typed = typed.where(F.col("added_date").between(F.lit(info["range_start"]), F.lit(info["range_end"])))
    return typed


# --- actor network: PageRank without GraphFrames ----------------------------------------------------------------
def pagerank(edges, damping=0.85, max_iter=200, tol=1e-12):
    """Weighted, directed PageRank by power iteration on a small edge list [(src, dst, weight), ...].

    A node passes its rank to its targets in proportion to edge weight. Nodes with no outgoing edges ("dangling")
    spread their rank evenly over all nodes, and every node gets the (1 - damping) / N teleport share, so the ranks
    always sum to 1. Returns (ranks, iterations, last L1 change).
    """
    out_w, nodes = {}, set()
    for s, d, w in edges:
        out_w[s] = out_w.get(s, 0.0) + w
        nodes.update((s, d))
    nodes = sorted(nodes)
    n = len(nodes)
    if n == 0:
        return {}, 0, 0.0
    trans = [(s, d, w / out_w[s]) for s, d, w in edges if out_w[s] > 0]
    ranks = {v: 1.0 / n for v in nodes}
    delta, it = float("inf"), 0
    for it in range(1, max_iter + 1):
        dangling = sum(ranks[v] for v in nodes if out_w.get(v, 0.0) == 0)
        new = {v: (1 - damping) / n + damping * dangling / n for v in nodes}
        for s, d, p in trans:
            new[d] += damping * ranks[s] * p
        delta = sum(abs(new[v] - ranks[v]) for v in nodes)
        ranks = new
        if delta < tol:
            break
    return ranks, it, delta


def pagerank_spark_steps(spark, edges_df, damping=0.85, steps=3):
    """The same PageRank update written as Spark DataFrame operations, run for a fixed number of steps.

    Each step is one join (edge transition probabilities x current ranks) and one groupBy(dst). The rank vector is
    rebuilt from the driver every step, so the query plan does not grow with the number of iterations.
    Used to show that the distributed formulation gives the same numbers as pagerank() on the driver.
    """
    from pyspark.sql import functions as F
    out_w = edges_df.groupBy("src").agg(F.sum("weight").alias("out_weight"))
    trans = edges_df.join(out_w, "src").select("src", "dst", (F.col("weight") / F.col("out_weight")).alias("p"))
    nodes = sorted({r[0] for r in edges_df.select("src").union(edges_df.select("dst")).distinct().collect()})
    has_out = {r[0] for r in out_w.collect()}
    n = len(nodes)
    ranks = {v: 1.0 / n for v in nodes}
    for _ in range(steps):
        r_df = spark.createDataFrame(sorted(ranks.items()), "src STRING, rank DOUBLE")
        contrib = {r["dst"]: r["c"] for r in
                   trans.join(r_df, "src").groupBy("dst").agg(F.sum(F.col("p") * F.col("rank")).alias("c")).collect()}
        dangling = sum(ranks[v] for v in nodes if v not in has_out)
        ranks = {v: (1 - damping) / n + damping * (contrib.get(v, 0.0) + dangling / n) for v in nodes}
    return ranks


# --- skew and salting -------------------------------------------------------------------------------------------
def hot_key_salts(key_counts, partitions):
    """Salt buckets per hot key.

    A key is hot if it holds more rows than one partition's fair share (1 / partitions of all rows). It gets
    ceil(share x partitions) buckets, which cuts each of its pieces to about one fair share. All other keys keep a
    single bucket, so salting adds no extra work for them.
    """
    total = sum(key_counts.values())
    if total == 0:
        return {}
    return {k: int(math.ceil(c / total * partitions)) for k, c in key_counts.items()
            if c / total > 1.0 / partitions and math.ceil(c / total * partitions) > 1}


def add_salt(df, spark, salts, key_col="key", id_col="event_id"):
    """Add a `salt` column: pmod(xxhash64(id), buckets for this key), 0 for keys that are not hot.

    Hashing the event id (instead of rand()) makes the salt deterministic, so re-running a query or re-trying a task
    sends every row to the same bucket.
    """
    from pyspark.sql import functions as F
    if not salts:
        return df.withColumn("salt", F.lit(0))
    sdf = F.broadcast(spark.createDataFrame(sorted(salts.items()), f"{key_col} STRING, n_salt INT"))
    return (df.join(sdf, key_col, "left")
              .withColumn("salt", F.expr(f"pmod(xxhash64({id_col}), coalesce(n_salt, 1))").cast("int"))
              .drop("n_salt"))


def explode_salts(small_df, spark, salts, key_col="key"):
    """The small side of a salted join: each hot key's row repeated once per salt bucket (0 .. n-1), others once."""
    from pyspark.sql import functions as F
    if not salts:
        return small_df.withColumn("salt", F.lit(0))
    sdf = F.broadcast(spark.createDataFrame(sorted(salts.items()), f"{key_col} STRING, n_salt INT"))
    return (small_df.join(sdf, key_col, "left")
            .withColumn("salt", F.explode(F.sequence(F.lit(0), F.coalesce(F.col("n_salt"), F.lit(1)) - 1)))
            .drop("n_salt"))


def run_noop(df):
    """Execute df completely without collecting or storing anything. The "noop" sink runs every stage of the plan;
    count() could let the optimizer skip work. Falls back to count() if the platform rejects the noop format."""
    try:
        df.write.format("noop").mode("overwrite").save()
        return "noop"
    except Exception:
        df.count()
        return "count"


def time_alternating(variants, runs=3, warmup=1):
    """Wall-clock seconds for each variant, run in alternating order (A, B, A, B, ...) after untimed warm-ups.

    Alternating spreads any drift (caches warming up, other load on shared serverless compute) over both variants
    instead of penalising whichever runs first. variants: {name: zero-argument callable}.
    """
    import time
    for _ in range(warmup):
        for fn in variants.values():
            fn()
    times = {name: [] for name in variants}
    for _ in range(runs):
        for name, fn in variants.items():
            t0 = time.perf_counter()
            fn()
            times[name].append(time.perf_counter() - t0)
    return times


def median(xs):
    s = sorted(xs)
    return None if not s else (s[len(s) // 2] if len(s) % 2 else (s[len(s) // 2 - 1] + s[len(s) // 2]) / 2)
