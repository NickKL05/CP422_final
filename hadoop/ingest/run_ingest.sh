#!/usr/bin/env bash
# Run the MapReduce (Hadoop Streaming) ingestion job. Run it INSIDE the namenode container:
#   docker exec namenode bash /opt/cp422/ingest/run_ingest.sh 2026-10-01 2026-10-31 [LINES_PER_MAP]
# Captured output goes to hadoop/output/ on the host.
#
# AI citation: Claude Code (Claude Opus 5.5, Anthropic) drafted this driver script
# (job options, manifest capture, summary). Reviewed and tested by Nick Kunde-Lenny.
set -euo pipefail

START="${1:-2026-10-01}"
END="${2:-2026-10-31}"
LINES_PER_MAP="${3:-2}"          # URLs per map task. 31 files / 2 = 16 parallel map tasks.

HERE="$(cd "$(dirname "$0")" && pwd)"
OUT_DIR=/opt/cp422/output
TS="$(date +%Y%m%d_%H%M%S)"
URLS_HDFS="/gdelt/input/urls_${START}_${END}.txt"
JOB_OUT="/gdelt/manifests/ingest_${TS}"
STREAMING_JAR="$(ls "$HADOOP_HOME"/share/hadoop/tools/lib/hadoop-streaming-*.jar)"
LOG="$OUT_DIR/ingest_${TS}.log"
mkdir -p "$OUT_DIR"

echo "== building URL list for $START .. $END"
python3 "$HERE/make_url_list.py" --start "$START" --end "$END" --out /tmp/urls.txt
hdfs dfs -put -f /tmp/urls.txt "$URLS_HDFS"

echo "== submitting ingestion job (log: output/$(basename "$LOG"))"
SECONDS=0
# Speculative execution is OFF: a speculative duplicate map would download the same files twice.
# Map-only (0 reducers): each mapper's output is its own slice of the manifest.
hadoop jar "$STREAMING_JAR" \
  -D mapreduce.job.name="gdelt-ingest ${START}..${END}" \
  -D mapreduce.input.lineinputformat.linespermap="$LINES_PER_MAP" \
  -D mapreduce.map.speculative=false \
  -D mapreduce.task.timeout=1800000 \
  -D mapreduce.map.maxattempts=4 \
  -files "$HERE/ingest_mapper.py" \
  -inputformat org.apache.hadoop.mapred.lib.NLineInputFormat \
  -input "$URLS_HDFS" \
  -output "$JOB_OUT" \
  -mapper "python3 ingest_mapper.py" \
  -cmdenv HDFS_RAW_ROOT=/gdelt/raw/events \
  -numReduceTasks 0 2>&1 | tee "$LOG"
ELAPSED=$SECONDS

MANIFEST="$OUT_DIR/ingest_manifest_${TS}.tsv"
{
  printf 'file\tstatus\trows\tzip_bytes\tcsv_bytes\tseconds\tworker_host\turl\terror\n'
  hdfs dfs -cat "$JOB_OUT/part-*" | sort
} > "$MANIFEST"

SUMMARY="$OUT_DIR/ingest_summary_${TS}.txt"
{
  echo "GDELT ingestion run $TS"
  echo "date range        : $START .. $END"
  echo "lines per map     : $LINES_PER_MAP"
  echo "wall-clock seconds: $ELAPSED"
  echo
  echo "files by status:"
  tail -n +2 "$MANIFEST" | cut -f2 | sort | uniq -c
  echo
  echo "files and rows ingested per worker (shows the work was spread over the cluster):"
  tail -n +2 "$MANIFEST" | awk -F'\t' '$2=="OK"{f[$7]++; r[$7]+=$3} END{for(h in f) printf "  %-10s %3d files %10d rows\n", h, f[h], r[h]}' | sort
  echo
  echo "total rows ingested this run: $(tail -n +2 "$MANIFEST" | awk -F'\t' '{s+=$3} END{print s+0}')"
  echo
  echo "HDFS raw area after the run:"
  hdfs dfs -count -h /gdelt/raw/events
  hdfs dfs -du -s -h /gdelt/raw/events
} | tee "$SUMMARY"

if tail -n +2 "$MANIFEST" | cut -f2 | grep -q FAILED; then
  echo "WARNING: some files failed; see $MANIFEST. Re-run the same command to retry only the missing files." >&2
  exit 1
fi
