#!/usr/bin/env bash
# A small MapReduce job over the raw GDELT files in HDFS: one map task per 32 MB block counts its rows
# (`wc -l`), and one reducer sums them. Used for the data-locality counters and as the "job that
# keeps running" in the fault-tolerance demo. Also cross-checks the row totals from ingestion.
# Run inside the namenode container:  bash /opt/cp422/demos/rowcount_job.sh <tag> [max_concurrent_maps]
# max_concurrent_maps (0 = no limit) throttles the job so it outlives the about 90 s dead-node timeout
# in the fault-tolerance demo. Otherwise all 53 maps finish in about 15 s.
#
# AI citation: Claude Code (Claude Opus 5.5, Anthropic) drafted this script. Reviewed and tested by Nick Kunde-Lenny.
set -euo pipefail

TAG="${1:-adhoc}"
MAP_LIMIT="${2:-0}"
HERE="$(cd "$(dirname "$0")" && pwd)"
OUT="/gdelt/demo/rowcount_${TAG}"
STREAMING_JAR="$(ls "$HADOOP_HOME"/share/hadoop/tools/lib/hadoop-streaming-*.jar)"

hdfs dfs -rm -r -f -skipTrash "$OUT" >/dev/null 2>&1 || true

hadoop jar "$STREAMING_JAR" \
  -D mapreduce.job.name="gdelt-rowcount-${TAG}" \
  -D mapreduce.job.running.map.limit="$MAP_LIMIT" \
  -files "$HERE/sum_reducer.py" \
  -input '/gdelt/raw/events/year=*/month=*/*.CSV' \
  -output "$OUT" \
  -mapper "wc -l" \
  -reducer "python3 sum_reducer.py" \
  -numReduceTasks 1

echo "== result"
hdfs dfs -cat "$OUT/part-*"
