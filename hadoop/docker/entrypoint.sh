#!/usr/bin/env bash
# Container entrypoint. The role comes from the compose "command": master | worker | hbase | client
# AI citation: Claude Code (Claude Opus 5.5, Anthropic) drafted this start-up script
# (role dispatch, start-up ordering and waits, one-time HDFS init). Reviewed and tested by Member 1.
set -uo pipefail

ROLE="${1:-client}"
log() { echo "[entrypoint $(hostname) $(date +%H:%M:%S)] $*"; }

wait_for_port() {  # host port
  until nc -z "$1" "$2" 2>/dev/null; do log "waiting for $1:$2"; sleep 3; done
}

live_datanodes() {
  hdfs dfsadmin -report -live 2>/dev/null | grep -c '^Name:' || true
}

wait_for_datanodes() {  # minimum number of live DataNodes
  until [ "$(live_datanodes)" -ge "$1" ]; do log "waiting for >= $1 live DataNodes"; sleep 3; done
}

# Stale pid files from a previous container run would make "--daemon start" refuse to start.
rm -rf /tmp/pids && mkdir -p /tmp/pids /data

# Runs in the background on the master: one-time HDFS layout, once enough DataNodes can take writes.
init_hdfs() {
  hdfs dfsadmin -safemode wait >/dev/null
  wait_for_datanodes 2
  hdfs dfs -mkdir -p /tmp /user/root /spark /spark-logs \
                     /gdelt/raw/events /gdelt/input /gdelt/manifests /gdelt/demo
  hdfs dfs -chmod 1777 /tmp
  if ! hdfs dfs -test -e /spark/spark-libs.jar; then
    log "uploading Spark jars archive to HDFS (one time)"
    jar cv0f /tmp/spark-libs.jar -C "$SPARK_HOME/jars/" . >/dev/null
    hdfs dfs -put -f /tmp/spark-libs.jar /spark/spark-libs.jar && rm -f /tmp/spark-libs.jar
  fi
  log "HDFS initialised"
}

case "$ROLE" in
  master)
    if [ ! -f /data/nn/current/VERSION ]; then
      log "formatting NameNode (first start)"
      hdfs namenode -format -force -nonInteractive -clusterId cp422
    fi
    hdfs --daemon start namenode
    yarn --daemon start resourcemanager
    mapred --daemon start historyserver
    init_hdfs &
    ;;
  worker)
    # Every worker runs a DataNode AND a NodeManager, so YARN can schedule map tasks on the
    # node that holds the block (data locality).
    wait_for_port namenode 9000
    hdfs --daemon start datanode
    wait_for_port namenode 8031
    yarn --daemon start nodemanager
    ;;
  hbase)
    wait_for_port namenode 9000
    hdfs dfsadmin -safemode wait >/dev/null
    wait_for_datanodes 2
    hbase-daemon.sh start master
    hbase-daemon.sh start thrift
    ;;
  client)
    log "client container: nothing to start"
    exec sleep infinity
    ;;
  *)
    exec "$@"
    ;;
esac

log "role '$ROLE' started; following daemon logs"
mkdir -p "$HADOOP_HOME/logs" "$HBASE_HOME/logs"
touch "$HADOOP_HOME/logs/.keep.log"
exec tail -F "$HADOOP_HOME"/logs/*.log "$HBASE_HOME"/logs/*.log 2>/dev/null
