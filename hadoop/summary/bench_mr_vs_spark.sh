#!/usr/bin/env bash
# Benchmark: the events-per-country-per-day summary in MapReduce vs Spark on YARN, on the same
# cluster and the same input. Run it INSIDE the namenode container, with nothing else running
# on the cluster:
#   docker exec namenode bash /opt/cp422/summary/bench_mr_vs_spark.sh [runs=3]
#
# First an untimed warm-up reads the whole input once (hdfs dfs -cat), so every timed run starts
# with the HDFS blocks in the OS page cache. Then each run i runs both engines, MR first on odd runs
# and Spark first on even runs, so neither engine always goes first and a slow period on the host
# hits both. Every run's timing is kept; the summary file reports medians over these warm-cache runs.
#
# Output (in $OUT_DIR, default /opt/cp422/output/benchmark, i.e. hadoop/output/benchmark/):
#   mr_vs_spark.csv          one row per run per engine (columns are described in README.md)
#   mr_vs_spark_summary.txt  settings, median/min/max wall seconds per engine, the comparison result
#   compare_mr_vs_spark.txt  compare_outputs.py on the last run's MR and Spark outputs
#   logs/                    full MR and Spark logs for every run
#
# Fixed settings (stated in the CSV and the summary, and chosen so both engines get the same
# parallelism on the 3-worker cluster: 4 vcores and 2048 MB of YARN memory per worker):
#   both  : at most 9 tasks at once (3 workers x 3), the same input glob, $REDUCERS reduce-side
#           tasks / output files (MR reducers = Spark shuffle partitions; Spark's adaptive
#           partition coalescing is off, so it really runs $REDUCERS reduce tasks)
#   MR    : 512 MB map/reduce containers (mapred-site.xml), concurrent maps capped at 9
#   Spark : 3 executors x 3 cores, 512 MB heap + 384 MB overhead each (a 1 GB container), so an
#           executor and the 1 GB Spark AM fit on one worker; 4-core executors would not, because
#           the AM takes a vcore too. Driver in the namenode container (client mode).
# Wall seconds are measured the same way for both: around the client command (hadoop jar /
# spark-submit), so each includes its own start-up (YARN AM, containers, JVMs).
set -euo pipefail

RUNS="${1:-3}"
HERE="$(cd "$(dirname "$0")" && pwd)"
OUT_DIR="${OUT_DIR:-/opt/cp422/output/benchmark}"
INPUT="${INPUT:-/gdelt/raw/events/year=*/month=*/*.CSV}"
REDUCERS="${REDUCERS:-3}"
PARALLEL_TASKS=9
SPARK_EXECUTORS=3
SPARK_CORES=3
SPARK_EXEC_MEM=512m
SPARK_EXEC_OVERHEAD=384m
CSV="$OUT_DIR/mr_vs_spark.csv"
SUMMARY="$OUT_DIR/mr_vs_spark_summary.txt"
mkdir -p "$OUT_DIR/logs"

MR_SETTINGS="reducers=$REDUCERS;map_limit=$PARALLEL_TASKS;container_mb=512"
SPARK_SETTINGS="executors=$SPARK_EXECUTORS;executor_cores=$SPARK_CORES;executor_memory=$SPARK_EXEC_MEM;overhead=$SPARK_EXEC_OVERHEAD;shuffle_partitions=$REDUCERS;aqe_coalesce=false"

now() { date +%s.%N; }
elapsed() { awk -v s="$1" -v e="$2" 'BEGIN{printf "%.2f", e - s}'; }
# Value of key=... in a "RESULT k=v k=v" line.
field() { echo "$1" | tr ' ' '\n' | awk -F= -v k="$2" '$1 == k && !seen { print $2; seen = 1 }'; }

echo "timestamp_utc,engine,run,wall_seconds,input_rows,valid_events,output_rows,settings,input" > "$CSV"

run_mr() {   # $1 = run number
  local i="$1" MR_LOG="$OUT_DIR/logs/mr_run$1.log" R
  echo "== run $i/$RUNS: MapReduce"
  # run_summary_mr.sh times the hadoop jar call itself (excluding its own clean-up of old output).
  MAP_LIMIT=$PARALLEL_TASKS INPUT="$INPUT" LOG_DIR="$OUT_DIR/logs" \
    bash "$HERE/run_summary_mr.sh" "bench_r${i}" "$REDUCERS" | tee "$MR_LOG"
  R=$(grep '^RESULT ' "$MR_LOG" | tail -n 1)
  echo "$(date -u +%FT%TZ),mapreduce,$i,$(field "$R" wall_seconds),$(field "$R" input_rows),$(field "$R" valid_events),$(field "$R" output_rows),$MR_SETTINGS,$INPUT" >> "$CSV"
}

run_spark() {   # $1 = run number
  local i="$1" SP_LOG="$OUT_DIR/logs/spark_run$1.log" SP_OUT="/gdelt/summary/spark_bench_r$1" T0 T1 R REGISTERED
  echo "== run $i/$RUNS: Spark on YARN"
  hdfs dfs -rm -r -f -skipTrash "$SP_OUT" >/dev/null 2>&1 || true
  T0=$(now)
  # Adaptive query execution would merge small shuffle partitions after the map stage, so Spark could
  # run fewer reduce tasks (and write fewer files) than the $REDUCERS MR reducers. It is switched off
  # for the coalescing step only, so both engines do the same reduce-side work.
  spark-submit \
    --master yarn --deploy-mode client \
    --num-executors "$SPARK_EXECUTORS" --executor-cores "$SPARK_CORES" \
    --executor-memory "$SPARK_EXEC_MEM" --conf spark.executor.memoryOverhead="$SPARK_EXEC_OVERHEAD" \
    --conf spark.dynamicAllocation.enabled=false \
    --conf spark.sql.adaptive.coalescePartitions.enabled=false \
    "$HERE/summary_spark.py" "$INPUT" "$SP_OUT" --shuffle-partitions "$REDUCERS" \
    --app-name "gdelt-summary-spark-bench_r${i}" > "$SP_LOG" 2>&1 \
    || { tail -n 40 "$SP_LOG"; echo "Spark job FAILED (log: $SP_LOG)" >&2; exit 1; }
  T1=$(now)
  grep -E '^(== GDELT|  [A-Za-z0-9]+=|job_seconds|RESULT )' "$SP_LOG" || true
  R=$(grep '^RESULT ' "$SP_LOG" | tail -n 1)
  # YARN may grant fewer executors than requested (for example if another application holds
  # containers). Spark then waits up to 30 s (spark.scheduler.maxRegisteredResourcesWaitingTime)
  # and runs with what it has, so the run would not be a 9-vs-9 comparison. Record what it got.
  REGISTERED=$(grep -c 'Registered executor' "$SP_LOG" || true)
  if [ "$REGISTERED" -lt "$SPARK_EXECUTORS" ]; then
    echo "WARNING: run $i: only $REGISTERED of $SPARK_EXECUTORS Spark executors registered; this run is not like-for-like" >&2
    SHORT_RUNS="${SHORT_RUNS:-}${SHORT_RUNS:+ }$i"
  fi
  echo "$(date -u +%FT%TZ),spark,$i,$(elapsed "$T0" "$T1"),$(field "$R" input_rows),$(field "$R" valid_events),$(field "$R" output_rows),$SPARK_SETTINGS;executors_registered=$REGISTERED,$INPUT" >> "$CSV"
}

# Untimed warm-up: read the whole input once, so the HDFS blocks are in the workers' OS page cache
# before ANY timed run. Without it, only the engine that runs first in run 1 would pay for a cold cache.
echo "== warm-up (untimed): reading $INPUT once"
WARM_T0=$(now)
hdfs dfs -cat "$INPUT" > /dev/null
echo "   warm-up read took $(elapsed "$WARM_T0" "$(now)") s"

for i in $(seq 1 "$RUNS"); do
  # Swap the order on even runs (MR, Spark / Spark, MR / ...), so neither engine always goes first.
  if [ $((i % 2)) -eq 1 ]; then run_mr "$i"; run_spark "$i"; else run_spark "$i"; run_mr "$i"; fi
done

echo "== comparing the last run's outputs"
TMP="$(mktemp -d)"
hdfs dfs -getmerge "/gdelt/summary/mr_bench_r${RUNS}" "$TMP/mr.tsv"
hdfs dfs -getmerge "/gdelt/summary/spark_bench_r${RUNS}" "$TMP/spark.tsv"
set +e
python3 "$HERE/compare_outputs.py" "$TMP/mr.tsv" "$TMP/spark.tsv" | tee "$OUT_DIR/compare_mr_vs_spark.txt"
SAME=${PIPESTATUS[0]}
set -e
rm -rf "$TMP"

{
  echo "MR vs Spark benchmark: events per country per day ($(date -u +%FT%TZ))"
  echo "input           : $INPUT"
  echo "runs per engine : $RUNS (after one untimed warm-up read; MR first on odd runs, Spark first on even runs)"
  echo "mapreduce       : $MR_SETTINGS"
  echo "spark           : $SPARK_SETTINGS"
  if [ -n "${SHORT_RUNS:-}" ]; then
    echo "WARNING         : Spark got fewer than $SPARK_EXECUTORS executors in run(s) $SHORT_RUNS (see executors_registered in the CSV)"
  fi
  echo
  python3 - "$CSV" <<'EOF'
import csv, statistics, sys
rows = list(csv.DictReader(open(sys.argv[1])))
for engine in ("mapreduce", "spark"):
    secs = [float(r["wall_seconds"]) for r in rows if r["engine"] == engine]
    if secs:
        print(f"{engine:10s} wall seconds: median {statistics.median(secs):.2f}, "
              f"min {min(secs):.2f}, max {max(secs):.2f} over {len(secs)} runs")
inputs = {r["input_rows"] for r in rows}
outputs = {r["output_rows"] for r in rows}
print(f"input_rows seen : {', '.join(sorted(inputs))} (should be one value)")
print(f"output_rows seen: {', '.join(sorted(outputs))} (should be one value)")
EOF
  echo
  echo "output comparison (last run): $(tail -n 1 "$OUT_DIR/compare_mr_vs_spark.txt")"
} | tee "$SUMMARY"

exit "$SAME"
