#!/usr/bin/env bash
# (A&B) Fault-tolerance demo. Run on the HOST (Git Bash), from the hadoop/ folder:
#   bash demos/fault_tolerance.sh            # stops only the DataNode daemon on worker3
#   bash demos/fault_tolerance.sh --container # stops the whole worker3 container (DataNode + NodeManager)
#
# Timeline:
#   1. fsck "before" snapshot (all blocks have 2 replicas)
#   2. start a MapReduce job over the raw data
#   3. while its maps are running, stop worker3's DataNode
#   4. every 10 s, log live/dead DataNodes, under-replicated blocks and job state:
#      the job keeps reading the surviving replicas, and about 90 s later the NameNode declares
#      worker3 dead and re-replicates its blocks onto worker1/worker2 until under-replicated = 0
#   5. "after" snapshot, then bring worker3 back and show the excess replicas being cleaned up
#
# AI citation: Claude Code (Claude Opus 5.5, Anthropic) drafted this script
# (timeline, polling, capture). Reviewed and tested by Nick Kunde-Lenny.
set -uo pipefail
export MSYS_NO_PATHCONV=1   # stop Git Bash rewriting /gdelt/... into Windows paths

MODE="${1:-}"
VICTIM=worker3
OUT=output/fault_tolerance
mkdir -p "$OUT"
TIMELINE="$OUT/ft_02_timeline.txt"

nn() { docker exec namenode bash -c "$*"; }
stamp() { date -u +%H:%M:%SZ; }   # UTC, to line up with the container/job logs
live() { nn "hdfs dfsadmin -report -live 2>/dev/null | grep -c '^Name:'"; }
dead() { nn "hdfs dfsadmin -report -dead 2>/dev/null | grep -c '^Name:'"; }
under() { nn "hdfs fsck /gdelt/raw/events 2>/dev/null | grep -i 'Under-replicated blocks' | awk '{print \$3}'"; }
fsck_summary() { nn "hdfs fsck /gdelt/raw/events 2>/dev/null | grep -E 'Total blocks|Minimally replicated|Under-replicated|Over-replicated|Missing blocks|Corrupt blocks|Average block replication|Number of data-nodes|Status'"; }
# Track THIS run's job by application id: earlier runs leave finished apps with the same name.
running_app() { nn "yarn application -list -appStates RUNNING 2>/dev/null | awk '/gdelt-rowcount-ft/{print \$1}' | head -1"; }
job_state() { nn "curl -s http://namenode:8088/ws/v1/cluster/apps/$APP | python3 -c 'import json,sys; a=json.load(sys.stdin)[\"app\"]; print(a[\"state\"]+\"/\"+a[\"finalStatus\"])'"; }

echo "== 1. before"
{ echo "# $(stamp) before failure"; fsck_summary; nn "hdfs dfsadmin -report -live | grep -E '^(Name|Hostname|DFS Used:)'"; } | tee "$OUT/ft_01_before.txt"

echo "== 2. starting MapReduce job"
# One map at a time, so the job runs for about 3.5 min and is still going when the NameNode declares
# worker3 dead (about 90 s) and while it re-replicates (about 2 min). With 2 at a time, the job ended
# just 2 s after the node was declared dead.
docker exec namenode bash /opt/cp422/demos/rowcount_job.sh ft 1 > "$OUT/ft_job.log" 2>&1 &
JOB_PID=$!
APP=""
until [ -n "$APP" ]; do sleep 2; APP=$(running_app); done
echo "   job is $APP"
sleep 8   # let the first map tasks start reading blocks

echo "== 3. $(stamp) killing DataNode on $VICTIM"
if [ "$MODE" = "--container" ]; then
  docker stop "$VICTIM" >/dev/null
  ACTION="docker stop $VICTIM"
else
  docker exec "$VICTIM" hdfs --daemon stop datanode
  ACTION="hdfs --daemon stop datanode on $VICTIM"
fi
echo "# $(stamp) FAILURE INJECTED: $ACTION" | tee "$TIMELINE"
printf '%-9s %-5s %-5s %-17s %s\n' time live dead under_replicated job | tee -a "$TIMELINE"

# Record each milestone the first time it is seen (seconds after the failure). "Re-replicated" means
# under-replicated went above 0 after the node was declared dead and then back to 0. Right after the
# node is declared dead, the NameNode can briefly still report 0 before it has counted the missing replicas.
T0=$SECONDS
DEAD_AT=""; SEEN_UNDER=0; REPL_AT=""; JOB_AT=""
while :; do
  L=$(live); D=$(dead); U=$(under); J=$(job_state)
  printf '%-9s %-5s %-5s %-17s %s\n' "$(stamp)" "$L" "$D" "$U" "$J" | tee -a "$TIMELINE"
  T=$((SECONDS - T0))
  [ -z "$DEAD_AT" ] && [ "${D:-0}" -ge 1 ] && DEAD_AT=$T
  [ -n "$DEAD_AT" ] && [ "${U:-0}" -gt 0 ] && SEEN_UNDER=1
  [ -z "$REPL_AT" ] && [ "$SEEN_UNDER" = 1 ] && [ "${U:-1}" = "0" ] && REPL_AT=$T
  job_done=0; kill -0 "$JOB_PID" 2>/dev/null || job_done=1
  [ -z "$JOB_AT" ] && [ "$job_done" = 1 ] && JOB_AT=$T
  if [ -n "$JOB_AT" ] && [ -n "$REPL_AT" ]; then break; fi
  if [ "$T" -gt 900 ]; then echo "# timed out after 15 min" | tee -a "$TIMELINE"; break; fi
  sleep 10
done
wait "$JOB_PID"; JOB_RC=$?
{
  echo "# milestones, seconds after the failure (to within one 15 s sample):"
  echo "#   worker3 declared dead      : ${DEAD_AT:-n/a} s"
  echo "#   re-replication complete    : ${REPL_AT:-n/a} s"
  echo "#   MapReduce job finished     : ${JOB_AT:-n/a} s  (exit code $JOB_RC)"
} | tee -a "$TIMELINE"

echo "== 4. after (cluster running on 2 DataNodes)"
{
  echo "# $(stamp) after re-replication"
  fsck_summary
  echo
  echo "# job result and failure-related counters"
  grep -E "total_rows|input_splits|Launched map tasks|Failed map tasks|Killed map tasks|Data-local|Rack-local|completed successfully|failed" "$OUT/ft_job.log" | sed 's/^[[:space:]]*//'
} | tee "$OUT/ft_03_after.txt"

echo "== 5. restoring $VICTIM"
if [ "$MODE" = "--container" ]; then docker start "$VICTIM" >/dev/null; else docker exec "$VICTIM" hdfs --daemon start datanode; fi
until [ "$(live)" -ge 3 ]; do sleep 3; done
sleep 20   # the returning node reports its old replicas, so those blocks are briefly over-replicated
{ echo "# $(stamp) $VICTIM back (excess replicas get deleted by the NameNode)"; fsck_summary; } | tee "$OUT/ft_04_restored.txt"
