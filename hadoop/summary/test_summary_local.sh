#!/usr/bin/env bash
# Local test of the summary job WITHOUT a cluster, inside the team image (cp422/hadoop-stack:1.0).
#   test_summary_local.sh <input glob of GDELT 1.0 daily files> <test output dir>
# Example (from the repo root on the host; the sample files are mounted read-only into a
# year=/month= layout like the one in HDFS):
#   docker run --rm --entrypoint bash -v "$PWD":/repo -v <sample>/events:/data/raw/events/year=2026/month=10:ro \
#     -v <scratch>:/sp cp422/hadoop-stack:1.0 /repo/hadoop/summary/test_summary_local.sh \
#     '/data/raw/events/year=*/month=*/*.CSV' /sp/test_outputs/summary
#
# Checks:
#   (a) shell pipe  : cat | mapper | sort | combiner | sort | reducer  (Streaming's data flow by hand)
#   (b) Hadoop local mode: the real Streaming job via run_summary_mr.sh, on the local file system
#   (c) Spark local[2]   : summary_spark.py, with ANSI mode on (as on Databricks serverless)
#   (d) compare_outputs.py: (b) vs (c), and (a) vs (b)
#   (e) an independent awk count of events for 3 keys, checked against all three outputs
#   (f) 20 synthetic edge-case lines that hit every skip rule: mapper and Spark counters must match
# Test outputs are NOT official results; the official runs are on the cluster (bench_mr_vs_spark.sh).
set -euo pipefail

INPUT="${1:?input glob}"
OUT="${2:?test output dir}"
HERE="$(cd "$(dirname "$0")" && pwd)"
rm -rf "$OUT" && mkdir -p "$OUT"
# shellcheck disable=SC2086  # the glob must expand here, for cat and awk
FILES=$(ls $INPUT)
echo "input files:"; echo "$FILES" | sed 's/^/  /'

echo "== (a) shell pipe"
# shellcheck disable=SC2086
cat $FILES | python3 "$HERE/summary_mapper.py" 2> "$OUT/pipe_counters.txt" \
  | LC_ALL=C sort | python3 "$HERE/summary_combiner.py" \
  | LC_ALL=C sort | python3 "$HERE/summary_reducer.py" > "$OUT/pipe.tsv"
sed 's/^reporter:counter:GDELT Summary,/  /' "$OUT/pipe_counters.txt"
echo "  output rows: $(wc -l < "$OUT/pipe.tsv")"

echo "== (b) Hadoop Streaming, local mode"
TEST_DIR="$OUT"  # (a separate name: in the prefix below, OUT= is expanded before LOG_DIR=)
LOCAL_TEST=1 INPUT="$INPUT" LOG_DIR="$TEST_DIR" OUT="$TEST_DIR/mr_local" \
  bash "$HERE/run_summary_mr.sh" local 2 | tee "$OUT/mr_local_run.txt"

echo "== (c) Spark local[2], ANSI on"
# file:// because the image's core-site.xml makes HDFS the default file system.
# Adaptive partition coalescing off, as in bench_mr_vs_spark.sh: Spark then writes 2 files, like MR's 2 reducers.
spark-submit --master 'local[2]' --driver-memory 1g \
  --conf spark.eventLog.enabled=false --conf spark.ui.enabled=false \
  --conf spark.sql.adaptive.coalescePartitions.enabled=false \
  "$HERE/summary_spark.py" "file://$INPUT" "file://$OUT/spark_local" --shuffle-partitions 2 \
  > "$OUT/spark_local.log" 2>&1 || { tail -n 40 "$OUT/spark_local.log"; exit 1; }
grep -E '^(== GDELT|  [A-Za-z0-9]+=|job_seconds|RESULT )' "$OUT/spark_local.log" | tee "$OUT/spark_local_run.txt"

echo "== (d) compare MR (local mode) vs Spark"
python3 "$HERE/compare_outputs.py" "$OUT/mr_local" "$OUT/spark_local" | tee "$OUT/compare_mr_vs_spark.txt"
# Same reduce-side parallelism: with coalescing off, Spark writes one file per shuffle partition, like MR.
# shellcheck disable=SC2012
MR_FILES=$(ls "$OUT"/mr_local/part-* | wc -l); SP_FILES=$(ls "$OUT"/spark_local/part-* | wc -l)
echo "  part files: mr=$MR_FILES spark=$SP_FILES"
[ "$MR_FILES" -eq "$SP_FILES" ] || { echo "part-file count differs (adaptive coalescing not off?)"; exit 1; }
echo "== (d) compare shell pipe vs MR (local mode)"
python3 "$HERE/compare_outputs.py" "$OUT/pipe.tsv" "$OUT/mr_local" --names pipe mr | tee "$OUT/compare_pipe_vs_mr.txt"

echo "== (e) independent awk event counts for 3 keys"
# The 3 keys with the most events, any 3 would do. awk applies the same skip rules on its own.
KEYS=$(sort -t$'\t' -k3,3nr "$OUT/pipe.tsv" | awk 'NR <= 3' | cut -f1,2)
cat "$OUT"/mr_local/part-* > "$OUT/mr_all.tsv"
cat "$OUT"/spark_local/part-* > "$OUT/spark_all.tsv"
while IFS=$'\t' read -r D C; do
  # shellcheck disable=SC2086
  AWK=$(awk -F'\t' -v d="$D" -v c="$C" '
    # AvgTone: mantissa with 1-15 integer digits (or .digits), optional e-notation with 1-2 digits.
    function tone_ok(t,  m, e, p, q) {
      p = index(tolower(t), "e"); m = t; e = ""
      if (p) { m = substr(t, 1, p - 1); e = substr(t, p + 1) }
      sub(/^[+-]/, "", m)
      if (m !~ /^([0-9]+(\.[0-9]*)?|\.[0-9]+)$/) return 0
      if (m ~ /^[0-9]/) { q = m; sub(/\..*/, "", q); if (length(q) > 15) return 0 }
      if (p) { sub(/^[+-]/, "", e); if (e !~ /^[0-9]+$/ || length(e) > 2) return 0 }
      return 1
    }
    NF == 58 && $8 != "" && length($2) == 8 && $2 ~ /^[0-9]+$/ &&
    length($32) >= 1 && length($32) <= 9 && $32 ~ /^[0-9]+$/ &&
    tone_ok($35) && $2 == d && $8 == c { n++ }
    END { print n + 0 }' $FILES)
  get() { awk -F'\t' -v d="$D" -v c="$C" '$1 == d && $2 == c { print $3 }' "$1"; }
  P=$(get "$OUT/pipe.tsv"); M=$(get "$OUT/mr_all.tsv"); S=$(get "$OUT/spark_all.tsv")
  if [ "$AWK" = "$P" ] && [ "$AWK" = "$M" ] && [ "$AWK" = "$S" ]; then OK=OK; else OK=MISMATCH; fi
  echo "  $D $C  awk=$AWK pipe=$P mr=$M spark=$S  $OK"
done <<< "$KEYS" | tee "$OUT/awk_check.txt"
rm -f "$OUT/mr_all.tsv" "$OUT/spark_all.tsv"

grep -q MISMATCH "$OUT/awk_check.txt" && { echo "awk check FAILED"; exit 1; }

echo "== (f) edge cases: every skip rule, mapper vs Spark"
# The sample files are almost entirely clean, so build 20 lines that hit each rule on purpose:
# the first 20 lines of the first input file, all given Actor1CountryCode USA, then mutated.
# Expected: 3 x FieldCount (57 fields, 59 fields, empty line), 1 x EmptyActor1Country,
# 1 x BadSqldate (7 digits), 1 x BadNumMentions ("3.0"), 3 x BadAvgTone ("nan", " 1.5", "1e999").
# "1.5e-05" is valid, so 11 valid events.
mkdir -p "$OUT/edge/in"
FIRST=$(echo "$FILES" | awk 'NR == 1')
awk -F'\t' -v OFS='\t' 'NR > 20 { exit } { $8 = "USA" }
  NR == 1 { NF = 57 }            NR == 2 { $59 = "extra" }    NR == 3 { $8 = "" }
  NR == 4 { $2 = "2026101" }     NR == 5 { $32 = "3.0" }      NR == 6 { $35 = "nan" }
  NR == 7 { $35 = "1.5e-05" }    NR == 8 { $35 = " 1.5" }     NR == 9 { $0 = "" }
  NR == 10 { $35 = "1e999" }     { print }' "$FIRST" > "$OUT/edge/in/edge.CSV"
python3 "$HERE/summary_mapper.py" < "$OUT/edge/in/edge.CSV" 2> "$OUT/edge/mapper_counters.txt" \
  | LC_ALL=C sort | python3 "$HERE/summary_combiner.py" | LC_ALL=C sort \
  | python3 "$HERE/summary_reducer.py" > "$OUT/edge/pipe.tsv"
spark-submit --master 'local[1]' --driver-memory 1g \
  --conf spark.eventLog.enabled=false --conf spark.ui.enabled=false \
  "$HERE/summary_spark.py" "file://$OUT/edge/in/edge.CSV" "file://$OUT/edge/spark" \
  > "$OUT/edge/spark.log" 2>&1 || { tail -n 40 "$OUT/edge/spark.log"; exit 1; }
python3 - "$OUT/edge/mapper_counters.txt" "$OUT/edge/spark.log" <<'PY' | tee "$OUT/edge/counters_check.txt"
import re, sys
expected = {"InputLines": 20, "SkippedFieldCount": 3, "SkippedEmptyActor1Country": 1,
            "SkippedBadSqldate": 1, "SkippedBadNumMentions": 1, "SkippedBadAvgTone": 3,
            "ValidEvents": 11}
mr = {k: int(v) for k, v in re.findall(r"GDELT Summary,(\w+),(\d+)", open(sys.argv[1]).read())}
sp = {k: int(v) for k, v in re.findall(r"^  (\w+)=(\d+)$", open(sys.argv[2]).read(), re.M)}
ok = True
for name, want in expected.items():
    good = mr.get(name) == want and sp.get(name) == want
    ok = ok and good
    print(f"  {name:26s} expected={want:3d} mapper={mr.get(name)} spark={sp.get(name)} "
          f"{'OK' if good else 'MISMATCH'}")
print("EDGE COUNTERS OK" if ok else "EDGE COUNTERS MISMATCH")
PY
grep -q "EDGE COUNTERS OK" "$OUT/edge/counters_check.txt"
python3 "$HERE/compare_outputs.py" "$OUT/edge/pipe.tsv" "$OUT/edge/spark" --names pipe spark \
  | tee "$OUT/edge/compare.txt"
tail -n 1 "$OUT/compare_mr_vs_spark.txt" | grep -q IDENTICAL
tail -n 1 "$OUT/compare_pipe_vs_mr.txt" | grep -q IDENTICAL
echo "ALL LOCAL CHECKS PASSED"
