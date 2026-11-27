# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Actor network: which states are most central (Member 3)
# MAGIC
# MAGIC AI citation: Claude Code (Claude Opus 5.5, Anthropic) drafted this notebook (edge list, degree, strength, normalised strength, PageRank, rankings). Reviewed and tested by Neel Patel.
# MAGIC
# MAGIC Builds a directed country-to-country graph from the clean dyadic events of notebook 03, and ranks the most central
# MAGIC states by four measures. There are two graphs: one over **all events**, and one over **material conflict only**
# MAGIC (`QuadClass = 4`).
# MAGIC
# MAGIC | Measure | What it says | How it is computed |
# MAGIC | --- | --- | --- |
# MAGIC | Degree (in / out / total) | how many different states a state interacts with | Spark SQL, `COUNT(DISTINCT partner)` |
# MAGIC | Strength (in / out / total) | how many events a state is involved in | Spark SQL, sum of edge weights |
# MAGIC | Normalised strength | share of all graph activity (`strength_share`), events per partner (`strength_per_partner`), and for the conflict graph the share of a state's own events that are material conflict (`conflict_share_of_activity`) | Spark SQL window over the graph |
# MAGIC | PageRank | how much attention a state receives from states that themselves receive attention | power iteration (below) |
# MAGIC
# MAGIC **Why not GraphFrames:** it needs a JAR, and Free Edition serverless can't install JAR libraries. Nothing here
# MAGIC needs it: degree and strength are plain aggregations, and PageRank is a short loop over an edge list of a few
# MAGIC thousand rows.
# MAGIC
# MAGIC **Descriptive only.** These measures cover the whole run period, so they must **not** be used as model features:
# MAGIC a feature for week *t* would then contain information from later weeks. The model's features are in
# MAGIC `dyad_weekly_features_*` (notebook 03).
# MAGIC
# MAGIC **Run notebook 03 first, in the same mode.** This notebook reads its clean events (`m3_dyad_events_*`) and run
# MAGIC parameters (`m3_run_params_*`), so both notebooks use the same events source, recency window and complete days.

# COMMAND ----------

# Declared here, not only in 00_config: a widget created inside a %run notebook does not appear in this notebook's
# parameter bar. Every top-level notebook in the repo starts with this cell.
dbutils.widgets.dropdown("sample_mode", "true", ["true", "false"], "SAMPLE_MODE")

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and consistency check with notebook 03
# MAGIC
# MAGIC The events source is resolved with the same helper and the same widget as in 03. If it differs from the source
# MAGIC 03 recorded, the notebook stops. That happens, for example, when Member 2's table appeared after 03 ran. Mixing
# MAGIC sources between notebooks would make the report's numbers inconsistent, so re-run 03 first.
# MAGIC
# MAGIC Fixed settings:
# MAGIC - `DAMPING = 0.85`: the standard PageRank damping factor. 15 % of each step's rank is spread evenly over all states.
# MAGIC - `MIN_ACTIVITY_PER_DAY = 2`: the conflict-share ranking only includes states with at least 2 events per complete
# MAGIC   day on average, across all event types. Otherwise a state with 3 events, 2 of them conflict, would top the
# MAGIC   ranking at 67 %.
# MAGIC - `TOP_N = 15`: rows shown in each ranking.

# COMMAND ----------

import datetime as dt
import pandas as pd
from pyspark.sql import functions as F
import dyad_common as dc

dbutils.widgets.dropdown("events_source", "auto", ["auto", "member2_clean", "bronze_fallback"], "Events source")

DAMPING = 0.85
MIN_ACTIVITY_PER_DAY = 2
TOP_N = 15

SUFFIX = "_sample" if SAMPLE_MODE else "_full"
T = lambda name: f"{CATALOG}.{SCHEMA}.{name}{SUFFIX}"
T_EVENTS, T_PARAMS = T("m3_dyad_events"), T("m3_run_params")          # written by notebook 03
T_EDGES, T_CENTRALITY = T("actor_edges"), T("actor_centrality")       # written here

def save(df, table):
    """Overwrite a result table. overwriteSchema lets a re-run with changed columns replace the old table cleanly."""
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)

for t in (T_EVENTS, T_PARAMS):
    if not spark.catalog.tableExists(t):
        raise RuntimeError(f"{t} not found: run notebook 03 first with the same sample_mode")
P = spark.table(T_PARAMS).first().asDict()

src = dc.resolve_events_source(spark, dbutils.widgets.get("events_source"), EVENTS_CLEAN, EVENTS_BRONZE,
                               RANGE_START, RANGE_END)
print(dc.source_banner(src))
if src["source"] != P["events_source"]:
    raise RuntimeError(f"notebook 03 ran on {P['events_source']} but the current source is {src['source']}; "
                       f"re-run notebook 03 first so all notebooks use the same events")

complete_days = (dt.date.fromisoformat(P["last_day"]) - dt.date.fromisoformat(P["first_day"])).days + 1
MIN_ACTIVITY = MIN_ACTIVITY_PER_DAY * complete_days
lk = dc.load_lookups()
countries_df = spark.createDataFrame(sorted(lk["countries"].items()), "code STRING, name STRING")
print(f"using notebook 03's run of {P['run_ts']}: N = {P['recency_days']} days, complete days "
      f"{P['first_day']}..{P['last_day']} ({complete_days} days), {P['clean_events']:,} clean events; "
      f"conflict-share ranking needs >= {MIN_ACTIVITY:,} events")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Edge list (Spark SQL)
# MAGIC
# MAGIC Each edge is a directed pair of states (`src` = Actor1, the initiator; `dst` = Actor2, the target). Its weight is
# MAGIC the number of events. Unlike in notebook 03, direction is kept here, because "who acts on whom" is the point of a
# MAGIC network. Mentions are summed too, for reference. There are no self-loops, because 03 already dropped same-country
# MAGIC events.
# MAGIC
# MAGIC The aggregation runs over millions of events, so Spark does it. The result is a few thousand edges, small enough
# MAGIC for everything that follows.

# COMMAND ----------

EDGES_SQL = """
SELECT 'all' AS graph, actor1_state AS src, actor2_state AS dst, COUNT(*) AS events, SUM(num_mentions) AS mentions
FROM m3_net_events_v
GROUP BY actor1_state, actor2_state
UNION ALL
SELECT 'material_conflict' AS graph, actor1_state, actor2_state, COUNT(*), SUM(num_mentions)
FROM m3_net_events_v
WHERE quad_class = 4
GROUP BY actor1_state, actor2_state
"""
spark.table(T_EVENTS).createOrReplaceTempView("m3_net_events_v")
save(spark.sql(EDGES_SQL), T_EDGES)
edges_df = spark.table(T_EDGES)
display(edges_df.groupBy("graph").agg(F.count(F.lit(1)).alias("edges"), F.sum("events").alias("events"),
                                      F.countDistinct("src").alias("states_with_out_edges"),
                                      F.countDistinct("dst").alias("states_with_in_edges")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Degree, strength and normalised strength (Spark SQL)
# MAGIC
# MAGIC Every edge is counted at both ends: once as an *out* edge for `src`, and once as an *in* edge for `dst`. So:
# MAGIC - a state's **strength** is the number of events it takes part in;
# MAGIC - its **degree** is its number of distinct partners.
# MAGIC
# MAGIC Raw strength mostly measures how much a country is in the news, so the United States will lead almost every raw
# MAGIC ranking. The normalised columns correct for that in different ways:
# MAGIC - `strength_share`: the state's share of all activity in the graph. Sums to 1 per graph.
# MAGIC - `strength_per_partner`: events per distinct partner. Separates a few intense relationships from many light ones.
# MAGIC - `conflict_share_of_activity` (conflict graph only): the state's conflict strength divided by its strength in
# MAGIC   the all-events graph. In other words, what fraction of a state's interactions are material conflict, regardless
# MAGIC   of how much coverage it gets.

# COMMAND ----------

CENTRALITY_SQL = """
WITH ends AS (
  SELECT graph, src AS country, dst AS partner, events, 'out' AS dir FROM m3_net_edges_v
  UNION ALL
  SELECT graph, dst AS country, src AS partner, events, 'in' AS dir FROM m3_net_edges_v
),
per_country AS (
  SELECT graph, country,
         COUNT(DISTINCT IF(dir = 'out', partner, NULL)) AS out_degree,
         COUNT(DISTINCT IF(dir = 'in', partner, NULL))  AS in_degree,
         COUNT(DISTINCT partner)                        AS degree,
         SUM(IF(dir = 'out', events, 0))                AS out_strength,
         SUM(IF(dir = 'in', events, 0))                 AS in_strength,
         SUM(events)                                    AS strength
  FROM ends
  GROUP BY graph, country
)
SELECT p.graph, p.country, p.out_degree, p.in_degree, p.degree, p.out_strength, p.in_strength, p.strength,
       p.strength / SUM(p.strength) OVER (PARTITION BY p.graph)              AS strength_share,
       p.strength / p.degree                                                 AS strength_per_partner,
       a.all_strength                                                        AS all_events_strength,
       CASE WHEN p.graph = 'material_conflict' THEN p.strength / a.all_strength END AS conflict_share_of_activity
FROM per_country p
LEFT JOIN (SELECT country AS a_country, strength AS all_strength FROM per_country WHERE graph = 'all') a
       ON p.country = a.a_country
"""
edges_df.createOrReplaceTempView("m3_net_edges_v")
measures_df = spark.sql(CENTRALITY_SQL)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · PageRank without GraphFrames
# MAGIC
# MAGIC **Weighted, directed PageRank.** At every step, each state passes 85 % of its rank to the states it acts on, in
# MAGIC proportion to the number of events. The other 15 % is spread evenly over all states. A state with no outgoing
# MAGIC edges spreads its rank evenly too, so the ranks always sum to 1. Steps repeat until the total change is below
# MAGIC 10⁻¹².
# MAGIC
# MAGIC **What a high PageRank means:** a state that is the *target* of many events, especially events started by states
# MAGIC that are themselves frequent targets. The out-strength column shows the most active *initiators*.
# MAGIC
# MAGIC **Where it runs.** The edge list has a few thousand rows over at most 233 states. Converging takes about 100 steps.
# MAGIC As Spark jobs that would be about 100 small jobs, where the per-job overhead on serverless costs far more than the
# MAGIC arithmetic. So the iteration runs on the driver, in plain Python, over the edge list Spark aggregated.
# MAGIC
# MAGIC **Showing the distributed version agrees.** The same update is also written as Spark DataFrame operations (one
# MAGIC join and one `groupBy` per step, `dc.pagerank_spark_steps`). It runs for 3 steps, and its result must match the
# MAGIC driver version after 3 steps to within 10⁻⁹. That shows the computation can be distributed exactly as written
# MAGIC if the graph grew.

# COMMAND ----------

# AI citation: Claude Code (Claude Opus 5.5, Anthropic) drafted this cell (PageRank on the driver with Spark DataFrame cross-check). Reviewed and tested by Neel Patel.
pr_rows, check_rows = [], []
for graph in ["all", "material_conflict"]:
    g_df = edges_df.where(F.col("graph") == graph).select("src", "dst", F.col("events").cast("double").alias("weight"))
    edge_list = [(r["src"], r["dst"], r["weight"]) for r in g_df.collect()]
    if not edge_list:
        print(f"{graph}: no edges")
        continue
    ranks, iters, delta = dc.pagerank(edge_list, damping=DAMPING)
    pr_rows += [(graph, c, v) for c, v in ranks.items()]

    spark_3 = dc.pagerank_spark_steps(spark, g_df, damping=DAMPING, steps=3)
    driver_3, _, _ = dc.pagerank(edge_list, damping=DAMPING, max_iter=3, tol=0.0)
    max_diff = max(abs(spark_3[c] - driver_3[c]) for c in driver_3)
    assert max_diff < 1e-9, f"Spark and driver PageRank disagree by {max_diff}"
    assert abs(sum(ranks.values()) - 1) < 1e-9
    check_rows.append((graph, len(ranks), len(edge_list), iters, delta, max_diff))

display(pd.DataFrame(check_rows, columns=["graph", "states", "edges", "iterations", "final_l1_change",
                                          "spark_vs_driver_max_diff_after_3_steps"]))
pagerank_df = spark.createDataFrame(pd.DataFrame(pr_rows, columns=["graph", "country", "pagerank"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Rankings
# MAGIC
# MAGIC `RANK()` window functions rank each measure within each graph. Tied states share a rank. The conflict-share
# MAGIC rank only covers states above the activity threshold, and is NULL for the rest. Country names come from a
# MAGIC broadcast join to the CAMEO lookup.

# COMMAND ----------

RANK_SQL = f"""
SELECT m.*, pr.pagerank,
       RANK() OVER (PARTITION BY m.graph ORDER BY pr.pagerank DESC)  AS rank_pagerank,
       RANK() OVER (PARTITION BY m.graph ORDER BY m.strength DESC)   AS rank_strength,
       RANK() OVER (PARTITION BY m.graph ORDER BY m.degree DESC)     AS rank_degree,
       CASE WHEN m.graph = 'material_conflict' AND m.all_events_strength >= {MIN_ACTIVITY}
            THEN RANK() OVER (PARTITION BY m.graph, (m.all_events_strength >= {MIN_ACTIVITY})
                              ORDER BY m.conflict_share_of_activity DESC) END AS rank_conflict_share
FROM m3_net_measures_v m
JOIN m3_net_pagerank_v pr ON m.graph = pr.graph AND m.country = pr.country
"""
measures_df.createOrReplaceTempView("m3_net_measures_v")
pagerank_df.createOrReplaceTempView("m3_net_pagerank_v")
names = F.broadcast(countries_df.select(F.col("code").alias("country"), F.col("name").alias("country_name")))
save(spark.sql(RANK_SQL).join(names, "country", "left"), T_CENTRALITY)
cent = spark.table(T_CENTRALITY)
print(f"{T_CENTRALITY}: {cent.count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Most central states, all events
# MAGIC
# MAGIC Sorted by PageRank, with the raw and normalised strength shown alongside. Where a state ranks high on PageRank
# MAGIC but lower on strength, it receives attention from important states without being in the most events.

# COMMAND ----------

show_cols = ["rank_pagerank", "country", "country_name", "pagerank", "strength", "rank_strength", "strength_share",
             "degree", "strength_per_partner", "out_strength", "in_strength"]
display(cent.where("graph = 'all'").orderBy("rank_pagerank", "country").select(*show_cols).limit(TOP_N))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Most central states, material conflict
# MAGIC
# MAGIC The same ranking on the conflict-only graph, then the states whose interactions are most often material conflict,
# MAGIC among states above the activity threshold.

# COMMAND ----------

display(cent.where("graph = 'material_conflict'").orderBy("rank_pagerank", "country").select(*show_cols).limit(TOP_N))
display(cent.where("rank_conflict_share IS NOT NULL").orderBy("rank_conflict_share", "country")
        .select("rank_conflict_share", "country", "country_name", "conflict_share_of_activity", "strength",
                "all_events_strength", "rank_pagerank").limit(TOP_N))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Heaviest directed conflict edges
# MAGIC
# MAGIC The pairs behind the conflict rankings: who initiates the most material-conflict events against whom.

# COMMAND ----------

src_names = countries_df.select(F.col("code").alias("src"), F.col("name").alias("src_name"))
dst_names = countries_df.select(F.col("code").alias("dst"), F.col("name").alias("dst_name"))
display(edges_df.where("graph = 'material_conflict'")
        .join(F.broadcast(src_names), "src", "left").join(F.broadcast(dst_names), "dst", "left")
        .select("src", "src_name", "dst", "dst_name", "events", "mentions")
        .orderBy(F.desc("events"), "src", "dst").limit(TOP_N))
