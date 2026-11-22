#!/usr/bin/env bash
# Data-locality demo: (1) where HDFS put every block (fsck), (2) proof that YARN ran map tasks on
# the nodes holding their blocks (the "Data-local map tasks" counter of a job over that data).
# Run inside the namenode container:  bash /opt/cp422/demos/locality.sh
#
# AI citation: Claude Code (Claude Opus 5.5, Anthropic) drafted this script. Reviewed and tested by Nick Kunde-Lenny.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
OUT_DIR=/opt/cp422/output
mkdir -p "$OUT_DIR"

echo "== 1. block placement (hdfs fsck)"
hdfs dfsadmin -report > "$OUT_DIR/dfsadmin_report.txt"
hdfs fsck /gdelt/raw/events -files -blocks -locations > "$OUT_DIR/fsck_blocks.txt" 2>&1
python3 "$HERE/summarize_fsck.py" "$OUT_DIR/fsck_blocks.txt" "$OUT_DIR/dfsadmin_report.txt" \
  | tee "$OUT_DIR/block_placement_summary.txt"
echo
# Stop before fsck's (empty) "Erasure Coded Block Groups" section, so its zero totals don't get mixed in.
sed '/Erasure Coded Block Groups/q' "$OUT_DIR/fsck_blocks.txt" \
  | grep -E "Total size|Total files|Total blocks|Minimally replicated|Under-replicated|Default replication|Average block replication|Number of data-nodes|is HEALTHY|is CORRUPT" \
  | tee -a "$OUT_DIR/block_placement_summary.txt"

echo
echo "== 2. locality counters from a MapReduce job over the same blocks"
bash "$HERE/rowcount_job.sh" locality 2>&1 | tee "$OUT_DIR/rowcount_locality.log"
grep -E "Launched map tasks|Data-local map tasks|Rack-local map tasks|Other local map tasks|total_rows|input_splits" \
  "$OUT_DIR/rowcount_locality.log" | sed 's/^[[:space:]]*//' | tee "$OUT_DIR/locality_counters.txt"
