# Pig conflict filter (Member 2)

`conflict_filter.pig` keeps the **conflict events between two different countries** and attaches a
latitude/longitude to both actor countries:

1. **Load** the GDELT 1.0 daily files (58 tab-separated columns, no header) with an explicit schema, every
   column a `chararray`, in the official codebook order (the same list as `gc.GDELT_V1_COLUMNS`).
2. **Filter:** `QuadClass` is 3 (verbal conflict) or 4 (material conflict), and `Actor1CountryCode` and
   `Actor2CountryCode` are both non-empty and different.
3. **Cast** only the fields that are kept (`QuadClass` and `NumMentions` to int, `GoldsteinScale` and
   `AvgTone` to double). A malformed number becomes null; it doesn't fail the job.
4. **Replicated (map-side) join**, twice: Actor1's country code, then Actor2's, to
   `../lookups/country_centroids.tsv` (about 190 rows, see `../lookups/README.md`). Pig loads the lookup
   into memory in every map task, so the event data is never shuffled for the join. Both joins are
   `LEFT OUTER`, so events with an unknown country are kept long enough to be counted, then split off.
5. **Store** the joined events plus four small summaries.

## Output (under the Pig output folder, `$PIG_OUT`)

| Folder | Columns |
| --- | --- |
| `events/` | `event_id, sqldate, a1_code, a2_code, quad_class, event_root_code, goldstein, num_mentions, avg_tone, a1_lat, a1_lon, a2_lat, a2_lon` (only events where both countries have a centroid) |
| `summary_counts/` | `metric, n` for `input_rows`, `conflict_rows`, `joined_rows`, `unmatched_rows` |
| `summary_quadclass/` | `quad_class, conflict_events, joined_events` |
| `summary_top_dyads/` | `a1_code, a2_code, events`: top 20 directed dyads over **all** conflict events (ties broken by code) |
| `summary_unmatched_codes/` | `code, event_sides`: country codes with no centroid, counted once per event side |

`sqldate` stays a `yyyyMMdd` string. No recency rule is applied here; that's part of the cleaning step
on Databricks. Remember that `SQLDATE` can be up to about 10 years before `DATEADDED`.

## Run it

`run_pig.sh` runs the script, times it, copies the small summary tables, and writes a readable summary.
In mapreduce mode (the cluster run) everything goes to `${OUT_DIR:-/opt/cp422/output/pig}`, which is
`hadoop/output/pig/` in the repo, the captured output the `02_clean_events` notebook displays. In local
mode the default is `/tmp/cp422_pig/out` inside the container instead, because a development run is not
an official result and must not end up in `hadoop/output/`:

- `pig_conflict_<mode>_<UTC timestamp>.log`: full Pig console output
- `pig_conflict_<mode>_<ts>_summary.txt`: counts, wall time, QuadClass split, top dyads, unmatched codes, sample rows, Pig job statistics
- `pig_conflict_<mode>_<ts>_{counts,quadclass,top_dyads,unmatched_codes}.tsv`

The full `events/` output stays in `$PIG_OUT`, because it's too big for the repo.

### On the cluster (mapreduce mode, the default)

```bash
docker exec namenode bash /opt/cp422/pig/run_pig.sh
```

This puts the lookup into HDFS at `/gdelt/lookups/country_centroids.tsv`, reads
`/gdelt/raw/events/year=*/month=*/*.CSV`, and writes to `/gdelt/pig/conflict` (deleted first, so the
command can be re-run). Override with `INPUT=...` and `PIG_OUT=...`, for example one day only:

```bash
docker exec -e INPUT=/gdelt/raw/events/year=2026/month=10/20261001.export.CSV namenode bash /opt/cp422/pig/run_pig.sh
```

Pig 0.17.0 in mapreduce mode works on Hadoop 3.3.6 with this image, and needs no extra properties.
It was tested on a one-worker copy of the cluster with the 2-day sample: the script became 15 YARN
jobs and took about 5 minutes. Most of that time is per-job start-up, because the summaries each add
GROUP/ORDER jobs. The counts and all 8,732 `events/` rows were identical to local mode (that test used
the earlier 187-row lookup, before the PSE override; with the current lookup, local mode gives 9,296
joined rows on the same sample). The log has
only harmless warnings: "No job jar file set" (once per job), "ATS is disabled" and the usual
native-library notice. If a run fails, the last 40 lines of
the log are printed, and the whole log stays in `hadoop/output/pig/`.

### Locally (local mode, for development)

Local mode runs Pig in a single JVM on local files, with no HDFS or YARN. `INPUT` is required. Mount a
scratch folder and point `OUT_DIR` (log and summary) and `PIG_OUT` (Pig's result folders) at it to keep
the results after the container exits:

```bash
docker run --rm -v "$PWD/hadoop":/opt/cp422 -v /path/to/sample:/data:ro -v /path/to/scratch:/scratch \
    -e OUT_DIR=/scratch/pig -e PIG_OUT=/scratch/pig/conflict --entrypoint bash \
    cp422/hadoop-stack:1.0 -c "INPUT='/data/*.export.CSV' bash /opt/cp422/pig/run_pig.sh local"
```

## Checking the numbers without Pig

The conflict-row count can be checked with `awk` on the raw files (1-based columns: 8 =
`Actor1CountryCode`, 18 = `Actor2CountryCode`, 30 = `QuadClass`):

```bash
awk -F'\t' '($30=="3" || $30=="4") && $8!="" && $18!="" && $8!=$18' /path/to/*.export.CSV | wc -l
```

and the joined-row count by also requiring both codes to be in `country_centroids.tsv`:

```bash
awk -F'\t' 'NR==FNR {c[$1]; next} ($30=="3" || $30=="4") && $8!="" && $18!="" && $8!=$18 && ($8 in c) && ($18 in c)' \
    hadoop/lookups/country_centroids.tsv /path/to/*.export.CSV | wc -l
```
