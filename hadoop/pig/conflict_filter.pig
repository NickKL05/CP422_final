/*
 * Conflict filter for GDELT 1.0 daily events (Member 2).
 *
 * Keeps the "material/verbal conflict" events between two different countries and attaches a
 * latitude/longitude to each actor country, so later steps can work with dyads (Actor1 -> Actor2).
 *
 * Parameters (passed with -param by run_pig.sh):
 *   $INPUT   GDELT 1.0 daily export files: 58 tab-separated columns, no header.
 *            HDFS: /gdelt/raw/events/year=*\/month=*\/*.CSV   local: a folder or glob of CSVs
 *   $LOOKUP  ../lookups/country_centroids.tsv (cameo_code, country_name, lat, lon; no header)
 *   $OUTPUT  output folder. Pig writes these subfolders:
 *              events/                 one row per conflict event whose two actor countries both have a centroid
 *              summary_counts/         input_rows, conflict_rows, joined_rows, unmatched_rows
 *              summary_quadclass/      QuadClass, conflict events, joined events
 *              summary_top_dyads/      top 20 (Actor1CountryCode, Actor2CountryCode) pairs by conflict events
 *              summary_unmatched_codes/ actor country codes with no centroid, and how often they occur
 *
 * Dataflow:
 *   raw (58 cols) -> conflict (QuadClass 3/4, two different, non-empty actor countries)
 *     -> replicated LEFT OUTER join on Actor1CountryCode -> replicated LEFT OUTER join on Actor2CountryCode
 *     -> SPLIT: both centroids found -> events/ ; otherwise -> unmatched codes summary
 */

SET job.name 'cp422-pig-conflict-filter';

-- 1. Load every column as chararray in the official GDELT 1.0 codebook order (same list as
--    notebooks/gdelt_common.py GDELT_V1_COLUMNS). Casting happens later, only for the fields we keep,
--    so a malformed number becomes a null instead of failing the job.
raw = LOAD '$INPUT' USING PigStorage('\t') AS (
    GLOBALEVENTID:chararray, SQLDATE:chararray, MonthYear:chararray, Year:chararray, FractionDate:chararray,
    Actor1Code:chararray, Actor1Name:chararray, Actor1CountryCode:chararray, Actor1KnownGroupCode:chararray,
    Actor1EthnicCode:chararray, Actor1Religion1Code:chararray, Actor1Religion2Code:chararray,
    Actor1Type1Code:chararray, Actor1Type2Code:chararray, Actor1Type3Code:chararray,
    Actor2Code:chararray, Actor2Name:chararray, Actor2CountryCode:chararray, Actor2KnownGroupCode:chararray,
    Actor2EthnicCode:chararray, Actor2Religion1Code:chararray, Actor2Religion2Code:chararray,
    Actor2Type1Code:chararray, Actor2Type2Code:chararray, Actor2Type3Code:chararray,
    IsRootEvent:chararray, EventCode:chararray, EventBaseCode:chararray, EventRootCode:chararray, QuadClass:chararray,
    GoldsteinScale:chararray, NumMentions:chararray, NumSources:chararray, NumArticles:chararray, AvgTone:chararray,
    Actor1Geo_Type:chararray, Actor1Geo_FullName:chararray, Actor1Geo_CountryCode:chararray, Actor1Geo_ADM1Code:chararray,
    Actor1Geo_Lat:chararray, Actor1Geo_Long:chararray, Actor1Geo_FeatureID:chararray,
    Actor2Geo_Type:chararray, Actor2Geo_FullName:chararray, Actor2Geo_CountryCode:chararray, Actor2Geo_ADM1Code:chararray,
    Actor2Geo_Lat:chararray, Actor2Geo_Long:chararray, Actor2Geo_FeatureID:chararray,
    ActionGeo_Type:chararray, ActionGeo_FullName:chararray, ActionGeo_CountryCode:chararray, ActionGeo_ADM1Code:chararray,
    ActionGeo_Lat:chararray, ActionGeo_Long:chararray, ActionGeo_FeatureID:chararray,
    DATEADDED:chararray, SOURCEURL:chararray);

-- Centroid lookup (about 190 rows), small enough to ship to every map task.
centroids = LOAD '$LOOKUP' USING PigStorage('\t')
            AS (cameo_code:chararray, country_name:chararray, lat:double, lon:double);

-- 2. Conflict filter. QuadClass 3 = verbal conflict, 4 = material conflict. Both actor countries must be
--    present and different (an international dyad). Empty fields are tested both as null and as ''.
--    The comparison is on the string, so a non-numeric QuadClass is simply not kept.
conflict_raw = FILTER raw BY (QuadClass == '3' OR QuadClass == '4')
                         AND Actor1CountryCode IS NOT NULL AND Actor1CountryCode != ''
                         AND Actor2CountryCode IS NOT NULL AND Actor2CountryCode != ''
                         AND Actor1CountryCode != Actor2CountryCode;

-- Keep only the output fields, with numeric types. SQLDATE stays a yyyyMMdd string, as in bronze.
conflict = FOREACH conflict_raw GENERATE
    GLOBALEVENTID                AS event_id,
    SQLDATE                      AS sqldate,
    Actor1CountryCode            AS a1_code,
    Actor2CountryCode            AS a2_code,
    (int) QuadClass              AS quad_class,
    EventRootCode                AS event_root_code,
    (double) GoldsteinScale      AS goldstein,
    (int) NumMentions            AS num_mentions,
    (double) AvgTone             AS avg_tone;

-- 3. Map-side (replicated) joins: the lookup is held in memory in every map task, so the big relation is
--    never shuffled. LEFT OUTER keeps events with an unknown country, so they can be counted below.
--    Pig requires the replicated relation to be the LAST one in the JOIN.
j1 = JOIN conflict BY a1_code LEFT OUTER, centroids BY cameo_code USING 'replicated';
j1 = FOREACH j1 GENERATE
    conflict::event_id AS event_id, conflict::sqldate AS sqldate,
    conflict::a1_code AS a1_code, conflict::a2_code AS a2_code,
    conflict::quad_class AS quad_class, conflict::event_root_code AS event_root_code,
    conflict::goldstein AS goldstein, conflict::num_mentions AS num_mentions, conflict::avg_tone AS avg_tone,
    centroids::lat AS a1_lat, centroids::lon AS a1_lon;

j2 = JOIN j1 BY a2_code LEFT OUTER, centroids BY cameo_code USING 'replicated';
joined_all = FOREACH j2 GENERATE
    j1::event_id AS event_id, j1::sqldate AS sqldate, j1::a1_code AS a1_code, j1::a2_code AS a2_code,
    j1::quad_class AS quad_class, j1::event_root_code AS event_root_code,
    j1::goldstein AS goldstein, j1::num_mentions AS num_mentions, j1::avg_tone AS avg_tone,
    j1::a1_lat AS a1_lat, j1::a1_lon AS a1_lon,
    centroids::lat AS a2_lat, centroids::lon AS a2_lon;

SPLIT joined_all INTO
    joined    IF a1_lat IS NOT NULL AND a2_lat IS NOT NULL,
    unmatched OTHERWISE;

-- Main output. Column order: event_id, sqldate, a1_code, a2_code, quad_class, event_root_code,
-- goldstein, num_mentions, avg_tone, a1_lat, a1_lon, a2_lat, a2_lon
STORE joined INTO '$OUTPUT/events' USING PigStorage('\t');

-- 4. Summaries (small, for the report and the notebook).

-- 4a. Row counts at each step. COUNT_STAR counts every row, including rows with nulls.
raw_g       = GROUP raw ALL;
conflict_g  = GROUP conflict ALL;
joined_g    = GROUP joined ALL;
unmatched_g = GROUP unmatched ALL;
c_raw       = FOREACH raw_g       GENERATE 1 AS ord, 'input_rows'     AS metric, COUNT_STAR(raw)       AS n;
c_conflict  = FOREACH conflict_g  GENERATE 2 AS ord, 'conflict_rows'  AS metric, COUNT_STAR(conflict)  AS n;
c_joined    = FOREACH joined_g    GENERATE 3 AS ord, 'joined_rows'    AS metric, COUNT_STAR(joined)    AS n;
c_unmatched = FOREACH unmatched_g GENERATE 4 AS ord, 'unmatched_rows' AS metric, COUNT_STAR(unmatched) AS n;
-- (A relation with no rows gives no line here; run_pig.sh reports a missing metric as 0.)
counts = UNION c_raw, c_conflict, c_joined, c_unmatched;
counts = ORDER counts BY ord;
counts = FOREACH counts GENERATE metric, n;
STORE counts INTO '$OUTPUT/summary_counts' USING PigStorage('\t');

-- 4b. Events per QuadClass, before and after the centroid join.
quad = COGROUP conflict BY quad_class, joined BY quad_class;
quad = FOREACH quad GENERATE group AS quad_class, COUNT_STAR(conflict) AS conflict_events,
                             COUNT_STAR(joined) AS joined_events;
quad = ORDER quad BY quad_class;
STORE quad INTO '$OUTPUT/summary_quadclass' USING PigStorage('\t');

-- 4c. Top 20 directed dyads (Actor1 -> Actor2) over all conflict events. Ties are broken by code so the
--     list is the same on every run.
dyad_g = GROUP conflict BY (a1_code, a2_code);
dyads  = FOREACH dyad_g GENERATE FLATTEN(group) AS (a1_code, a2_code), COUNT_STAR(conflict) AS events;
dyads  = ORDER dyads BY events DESC, a1_code ASC, a2_code ASC;
top_dyads = LIMIT dyads 20;
STORE top_dyads INTO '$OUTPUT/summary_top_dyads' USING PigStorage('\t');

-- 4d. Which actor country codes had no centroid, counted once per event side (Actor1 or Actor2).
u1 = FILTER unmatched BY a1_lat IS NULL;
u1 = FOREACH u1 GENERATE a1_code AS code;
u2 = FILTER unmatched BY a2_lat IS NULL;
u2 = FOREACH u2 GENERATE a2_code AS code;
u  = UNION u1, u2;
u_g = GROUP u BY code;
unmatched_codes = FOREACH u_g GENERATE group AS code, COUNT_STAR(u) AS event_sides;
unmatched_codes = ORDER unmatched_codes BY event_sides DESC, code ASC;
STORE unmatched_codes INTO '$OUTPUT/summary_unmatched_codes' USING PigStorage('\t');
