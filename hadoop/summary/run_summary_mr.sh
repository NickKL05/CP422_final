#!/usr/bin/env bash
# MapReduce (Hadoop Streaming) job: events per country per day over the raw GDELT files in HDFS.
# Mapper, combiner and reducer are summary_mapper.py, summary_combiner.py and summary_reducer.py;
# the definition is in README.md. Run it INSIDE the namenode container:
#   docker exec namenode bash /opt/cp422/summary/run_summary_mr.sh <tag> [num_reducers]
# Output: HDFS /gdelt/summary/mr_<tag> (one line per key: SQLDATE, country, events, sum_mentions,
# avg_tone). The job log goes to output/summary/summary_mr_<tag>.log. The last line printed is
# a machine-readable RESULT line (bench_mr_vs_spark.sh parses it).
#
# Optional environment variables:
#   INPUT       input glob (default: every ingested month, /gdelt/raw/events/year=*/month=*/*.CSV)
#   OUT         output directory (default /gdelt/summary/mr_<tag>); deleted first if it exists
#   MAP_LIMIT   max concurrent map tasks, 0 = no limit (the benchmark caps it to match Spark's cores)
#   LOG_DIR     where the job log goes (default /opt/cp422/output/summary)
#   LOCAL_TEST  1 = run in Hadoop local mode on the local file system (no cluster needed; used by
#               test_summary_local.sh). INPUT and OUT are then local paths.
set -euo pipefail

TAG="${1:-adhoc}"
REDUCERS="${2:-3}"
HERE="$(cd "$(dirname "$0")" && pwd)"
INPUT="${INPUT:-/gdelt/raw/events/year=*/month=*/*.CSV}"
OUT="${OUT:-/gdelt/summary/mr_${TAG}}"
MAP_LIMIT="${MAP_LIMIT:-0}"
LOG_DIR="${LOG_DIR:-/opt/cp422/output/summary}"
LOG="$LOG_DIR/summary_mr_${TAG}.log"
STREAMING_JAR="$(ls "$HADOOP_HOME"/share/hadoop/tools/lib/hadoop-streaming-*.jar)"
mkdir -p "$LOG_DIR"

# Generic Hadoop options. Local mode: one JVM, local file system instead of HDFS, no YARN.
FS_OPTS=()
JOB_OPTS=()
if [ "${LOCAL_TEST:-0}" = "1" ]; then
  FS_OPTS=(-fs file:///)
  JOB_OPTS=(-D mapreduce.framework.name=local -fs file:///)
fi

hadoop fs ${FS_OPTS[@]+"${FS_OPTS[@]}"} -rm -r -f -skipTrash "$OUT" >/dev/null 2>&1 || true
hadoop fs ${FS_OPTS[@]+"${FS_OPTS[@]}"} -mkdir -p "$(dirname "$OUT")"

echo "== summary MR job: tag=$TAG reducers=$REDUCERS map_limit=$MAP_LIMIT"
echo "   input=$INPUT"
echo "   output=$OUT (log: $LOG)"
# The combiner runs the same partial-sum logic on each map task's sorted output before the
# shuffle (see summary_combiner.py), so reducers receive one record per key per map task
# instead of one record per event.
START=$(date +%s.%N)
hadoop jar "$STREAMING_JAR" \
  ${JOB_OPTS[@]+"${JOB_OPTS[@]}"} \
  -D mapreduce.job.name="gdelt-summary-mr-${TAG}" \
  -D mapreduce.job.reduces="$REDUCERS" \
  -D mapreduce.job.running.map.limit="$MAP_LIMIT" \
  -files "$HERE/summary_mapper.py,$HERE/summary_combiner.py,$HERE/summary_reducer.py" \
  -input "$INPUT" \
  -output "$OUT" \
  -mapper "python3 summary_mapper.py" \
  -combiner "python3 summary_combiner.py" \
  -reducer "python3 summary_reducer.py" > "$LOG" 2>&1 || { tail -n 40 "$LOG"; echo "MR job FAILED (log: $LOG)" >&2; exit 1; }
END=$(date +%s.%N)
WALL=$(awk -v s="$START" -v e="$END" 'BEGIN{printf "%.2f", e - s}')

# The job's counters are the LAST "Counters: N" block in the log. (In local mode each task also
# logs its own block first; on YARN the client prints only the job's.) Lines look like "    Name=value".
FINAL_COUNTERS="$(awk '/Counters: [0-9]+$/ { block = "" } { block = block $0 "\n" } END { printf "%s", block }' "$LOG")"
counter() { echo "$FINAL_COUNTERS" | grep -E "^[[:space:]]+$1=" | tail -n 1 | sed 's/.*=//'; }
echo "== counters"
echo "$FINAL_COUNTERS" | grep -E "^[[:space:]]+(Map input records|Map output records|Combine input records|Combine output records|Reduce input records|Reduce output records|Launched map tasks|Launched reduce tasks|Data-local map tasks|Rack-local map tasks)=" | sed 's/^[[:space:]]*/  /' || true
echo "  -- GDELT Summary group"
echo "$FINAL_COUNTERS" | grep -E "^[[:space:]]+(InputLines|SkippedFieldCount|SkippedEmptyActor1Country|SkippedBadSqldate|SkippedBadNumMentions|SkippedBadAvgTone|ValidEvents)=" | sed 's/^[[:space:]]*/  /' || true

echo "wall_seconds (hadoop jar, submit to finish): $WALL"
echo "RESULT engine=mr wall_seconds=$WALL input_rows=$(counter 'Map input records') valid_events=$(counter ValidEvents) output_rows=$(counter 'Reduce output records') reducers=$REDUCERS map_limit=$MAP_LIMIT"
