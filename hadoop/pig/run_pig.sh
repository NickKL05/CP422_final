#!/usr/bin/env bash
# Runs the Pig conflict filter (conflict_filter.pig), times it, and writes a human-readable summary.
#
# Run inside the namenode container:
#   bash /opt/cp422/pig/run_pig.sh                 # mapreduce mode (default): HDFS input, jobs on YARN
#   INPUT='/data/*.export.CSV' OUT_DIR=/scratch/pig bash /opt/cp422/pig/run_pig.sh local
#                                                    # local mode, local files (development)
#
# Environment overrides (all optional except INPUT in local mode):
#   INPUT     input files. Default (mapreduce): /gdelt/raw/events/year=*/month=*/*.CSV in HDFS
#   PIG_OUT   where Pig writes its result folders. Default: /gdelt/pig/conflict (HDFS) or /tmp/cp422_pig/conflict (local)
#   OUT_DIR   where the log and summary go. Default (mapreduce): /opt/cp422/output/pig (= hadoop/output/pig
#             in the repo, the captured cluster output). Default (local): /tmp/cp422_pig/out, because a
#             development run is not an official result and must not land in hadoop/output/
#
# Output in $OUT_DIR:
#   pig_conflict_<mode>_<ts>.log          full Pig console output
#   pig_conflict_<mode>_<ts>_summary.txt  counts, timing, QuadClass split, top dyads, unmatched codes, sample rows
#   pig_conflict_<mode>_<ts>_*.tsv        the small summary tables, for the notebook to display
# The full events output stays in $PIG_OUT (it's too big for the repo).
set -euo pipefail

MODE="${1:-mapreduce}"
HERE="$(cd "$(dirname "$0")" && pwd)"
LOOKUP_LOCAL="$HERE/../lookups/country_centroids.tsv"
if [ "$MODE" = local ]; then
  OUT_DIR="${OUT_DIR:-/tmp/cp422_pig/out}"
else
  OUT_DIR="${OUT_DIR:-/opt/cp422/output/pig}"
fi
TS="$(date -u +%Y%m%d_%H%M%S)"
TAG="pig_conflict_${MODE}_${TS}"
LOG="$OUT_DIR/${TAG}.log"
SUMMARY="$OUT_DIR/${TAG}_summary.txt"

mkdir -p "$OUT_DIR"
[ -s "$LOOKUP_LOCAL" ] || { echo "missing lookup: $LOOKUP_LOCAL" >&2; exit 1; }

case "$MODE" in
  mapreduce)
    INPUT="${INPUT:-/gdelt/raw/events/year=*/month=*/*.CSV}"
    PIG_OUT="${PIG_OUT:-/gdelt/pig/conflict}"
    LOOKUP="/gdelt/lookups/country_centroids.tsv"
    # The lookup has to be in HDFS so every map task can read it for the replicated join.
    hdfs dfs -mkdir -p /gdelt/lookups "$(dirname "$PIG_OUT")"
    hdfs dfs -put -f "$LOOKUP_LOCAL" "$LOOKUP"
    hdfs dfs -rm -r -f -skipTrash "$PIG_OUT" >/dev/null 2>&1 || true
    read_out() { hdfs dfs -cat "$PIG_OUT/$1/part-*"; }
    ;;
  local)
    INPUT="${INPUT:?set INPUT to the local GDELT CSV files, e.g. INPUT='/data/*.export.CSV'}"
    PIG_OUT="${PIG_OUT:-/tmp/cp422_pig/conflict}"
    LOOKUP="$LOOKUP_LOCAL"
    rm -rf "$PIG_OUT"
    mkdir -p "$(dirname "$PIG_OUT")"
    read_out() { cat "$PIG_OUT/$1"/part-*; }
    ;;
  *)
    echo "usage: $0 [mapreduce|local]" >&2
    exit 2
    ;;
esac

echo "== Pig conflict filter: mode=$MODE input=$INPUT output=$PIG_OUT"
START_UTC="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
T0="$(date +%s.%N)"
# -logfile: Pig's own error log (only written on failure). Console output (both streams) goes to $LOG.
if ! pig -x "$MODE" -logfile "$OUT_DIR/${TAG}_pig_errors.log" \
        -param INPUT="$INPUT" -param LOOKUP="$LOOKUP" -param OUTPUT="$PIG_OUT" \
        "$HERE/conflict_filter.pig" >"$LOG" 2>&1; then
  echo "Pig failed; last lines of $LOG:" >&2
  tail -n 40 "$LOG" >&2
  exit 1
fi
T1="$(date +%s.%N)"
END_UTC="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
WALL="$(awk -v a="$T0" -v b="$T1" 'BEGIN { printf "%.1f", b - a }')"

# Small result tables, copied next to the summary.
for t in summary_counts summary_quadclass summary_top_dyads summary_unmatched_codes; do
  read_out "$t" > "$OUT_DIR/${TAG}_${t#summary_}.tsv"
done
metric() {  # value of one counter from summary_counts (0 if Pig wrote no line for it)
  awk -F'\t' -v m="$1" '$1 == m { v = $2 } END { print (v == "" ? 0 : v) }' "$OUT_DIR/${TAG}_counts.tsv"
}

{
  echo "Pig conflict filter (hadoop/pig/conflict_filter.pig)"
  echo "mode:        $MODE"
  echo "input:       $INPUT"
  echo "lookup:      $LOOKUP ($(wc -l < "$LOOKUP_LOCAL") country centroids)"
  echo "pig output:  $PIG_OUT"
  echo "started:     $START_UTC"
  echo "finished:    $END_UTC"
  echo "wall time:   ${WALL} s (whole pig command, including job start-up)"
  echo
  echo "== Row counts"
  printf '%-46s %12s\n' "input rows"                                 "$(metric input_rows)"
  printf '%-46s %12s\n' "conflict rows (QuadClass 3/4, two countries)" "$(metric conflict_rows)"
  printf '%-46s %12s\n' "joined rows (both centroids found)"         "$(metric joined_rows)"
  printf '%-46s %12s\n' "rows dropped (a country had no centroid)"   "$(metric unmatched_rows)"
  # Counted from the files themselves. (In local mode Pig's own "stored N records" line for a
  # multi-query job can say 0 even though the rows were written.)
  printf '%-46s %12s\n' "lines in events/ output (wc -l)"            "$(read_out events | wc -l)"
  echo
  echo "== Conflict events per QuadClass (3 = verbal conflict, 4 = material conflict)"
  printf '%-10s %16s %16s\n' quad_class conflict_events joined_events
  awk -F'\t' '{ printf "%-10s %16s %16s\n", $1, $2, $3 }' "$OUT_DIR/${TAG}_quadclass.tsv"
  echo
  echo "== Top 20 directed dyads (Actor1CountryCode -> Actor2CountryCode), all conflict events"
  printf '%-6s %-6s %10s\n' actor1 actor2 events
  awk -F'\t' '{ printf "%-6s %-6s %10s\n", $1, $2, $3 }' "$OUT_DIR/${TAG}_top_dyads.tsv"
  echo
  echo "== Actor country codes with no centroid ($(wc -l < "$OUT_DIR/${TAG}_unmatched_codes.tsv") codes; top 20 by event sides)"
  printf '%-6s %12s\n' code event_sides
  head -n 20 "$OUT_DIR/${TAG}_unmatched_codes.tsv" | awk -F'\t' '{ printf "%-6s %12s\n", $1, $2 }'
  echo
  echo "== Sample output rows (first 10 lines of $PIG_OUT/events)"
  echo "event_id	sqldate	a1_code	a2_code	quad_class	event_root_code	goldstein	num_mentions	avg_tone	a1_lat	a1_lon	a2_lat	a2_lon"
  # head closes the pipe early; "|| true" stops that SIGPIPE from failing the script under pipefail.
  read_out events 2>/dev/null | head -n 10 || true
  echo
  echo "== Pig job statistics (from the log)"
  sed -n '/^HadoopVersion/,/^Job DAG/p' "$LOG" | head -n 40 || true
} > "$SUMMARY"

cat "$SUMMARY"
echo "== wrote $SUMMARY and $LOG"
