# Local Hadoop cluster (Member 1)

<!-- AI citation: Claude Code (Claude Opus 5.5, Anthropic) drafted this README's run instructions. Reviewed by Member 1. -->

This folder holds everything that runs on the local Docker cluster: HDFS, YARN, the MapReduce
ingestion job, the Pig and HBase pieces, and the scaling study. The Databricks notebook does **not**
run these scripts. It only displays their captured output from `output/`.

## Topology

| Container | Daemons | Host ports |
| --- | --- | --- |
| `namenode` | NameNode, YARN ResourceManager, MapReduce JobHistory; client for `hadoop jar`, `pig`, `spark-submit` | 9870 (HDFS UI), 8088 (YARN UI), 19888 (JobHistory), 4040 (Spark UI) |
| `worker1`, `worker2`, `worker3` | DataNode + NodeManager (co-located, so maps can read local blocks) | none |
| `hbase` | HBase standalone (master, regionserver, ZooKeeper), data stored on HDFS; Thrift server | 16010 (HBase UI), 9090 (Thrift) |

Explicit settings (in `docker/conf/hadoop/`):

| Setting | Value | Why |
| --- | --- | --- |
| `dfs.blocksize` | 32 MB | A daily GDELT file is about 51 MB, so each file spans 2 blocks |
| `dfs.replication` | 2 | Survives losing 1 of 3 DataNodes, with a third node to re-replicate onto |
| Dead-node timeout | about 90 s | Heartbeat 3 s, recheck 30 s (the default is about 10.5 min), for the fault-tolerance demo |
| YARN per worker | 2048 MB, 4 vcores | 12 vcores in total; DominantResourceCalculator so vcores are enforced |
| Map/reduce container | 512 MB | Up to 4 tasks per worker |

Versions: Hadoop 3.3.6, Pig 0.17.0, HBase 2.5.10 (hadoop3 build), Spark 3.5.3, Java 8.

## 0. One-time host setup (Windows / Docker Desktop)

The memory limits add up to about 12 GB. Docker Desktop's WSL2 VM has to be allowed more than that,
and the scaling study needs 8+ cores. Edit `%USERPROFILE%\.wslconfig`, then run `wsl --shutdown`
and restart Docker Desktop:

```ini
[wsl2]
networkingMode=mirrored
processors=12
memory=18GB
```

## 1. Start the cluster

```bash
cd hadoop
docker compose up -d --build
```

The first build downloads about 1.7 GB of Apache tarballs. When `docker logs namenode` shows
`HDFS initialised` (about a minute), check:

- http://localhost:9870 shows 3 live DataNodes
- http://localhost:8088 shows 3 active NodeManagers with 12 vcores in total

Stop it with `docker compose stop` (keeps the data) or `docker compose down -v` (wipes HDFS).

## 2. Ingest the sample month (MapReduce, Hadoop Streaming)

```bash
docker exec namenode bash /opt/cp422/ingest/run_ingest.sh 2026-10-01 2026-10-31
```

How it works:

1. `make_url_list.py` reads GDELT's `md5sums` and `filesizes` indexes and writes one line per daily
   file: `url md5 bytes`. That file goes to `/gdelt/input/` in HDFS.
2. A **map-only** streaming job reads it with `NLineInputFormat` (2 URLs per map task, so 16 parallel
   map tasks spread over the 3 NodeManagers).
3. Each mapper (`ingest_mapper.py`) downloads its ZIPs (3 retries, MD5 check), unzips them and runs
   `hdfs dfs -put` into `/gdelt/raw/events/year=YYYY/month=MM/YYYYMMDD.export.CSV`. Partitions use
   the file's publication date (DATEADDED), not the event date `SQLDATE`.
4. Each mapper emits a manifest row (`file, status, rows, bytes, seconds, worker host`) and
   Streaming counters (`GDELT Ingest` group: files, rows, bytes, retries, failures).

Speculative execution is off so no file is downloaded twice. Existing files are skipped, so
re-running the same command after a failure only fetches what's missing.

Captured output: `output/ingest_<ts>.log` (job log with counters), `output/ingest_manifest_<ts>.tsv`,
and `output/ingest_summary_<ts>.txt` (files per worker, total rows, HDFS size).

## 3. Block placement and data locality

```bash
docker exec namenode bash /opt/cp422/demos/locality.sh
```

- `output/fsck_blocks.txt`: raw `hdfs fsck /gdelt/raw/events -files -blocks -locations`
- `output/block_placement_summary.txt`: replicas per DataNode, replication histogram, example blocks
- `output/locality_counters.txt`: `Data-local map tasks` vs `Rack-local map tasks` from a row-count
  MapReduce job over the same blocks (one map per block). The row total should match the ingestion manifest.

## 4. (A&B) Fault tolerance: lose a DataNode mid-job

Run from **Git Bash on the host**, inside `hadoop/`:

```bash
bash demos/fault_tolerance.sh
```

Use `--container` to `docker stop` the whole worker instead of just its DataNode. The demo job is
limited to 1 concurrent map task (`rowcount_job.sh ft 1`). Unthrottled, it finishes in about 15 s,
well before the NameNode declares the node dead (about 90 s). Timestamps are in UTC so they line
up with the job logs. The whole demo takes about 6 minutes. Output goes to `output/fault_tolerance/`:

- `ft_01_before.txt`: healthy, 3 live DataNodes
- `ft_02_timeline.txt`: every 10 s, live/dead DataNodes, under-replicated blocks, job state. The job
  keeps running on the surviving replicas; about 90 s after the stop, worker3 is marked dead and
  under-replicated blocks climb, then fall back to 0 as HDFS copies them to worker1 and worker2.
- `ft_03_after.txt`: job succeeded, with the same row total and 0 under-replicated blocks on 2 nodes
- `ft_04_restored.txt`: worker3 back; its stale replicas show up as over-replicated and get removed

## For other members

- **Pig (Member 2):** `docker exec -it namenode pig -x mapreduce script.pig`. Raw data is under `/gdelt/raw/events`.
- **HBase (Member 5):** `docker exec -it hbase hbase shell`, or Thrift from the host on `localhost:9090`.
- **Spark on YARN (Members 2 and 5):** `docker exec -it namenode spark-submit --num-executors N --executor-cores C job.py`.
  The jars archive is pre-uploaded to `hdfs:///spark/spark-libs.jar`, so timings don't include the jar upload.
- Put scripts anywhere under `hadoop/`. That folder is mounted at `/opt/cp422` in `namenode` and `hbase`,
  and anything written to `/opt/cp422/output` appears in `hadoop/output/` for the notebook to display.
