<!-- Draft v2 (Member 2), 2026-11-26. [TBD: ...] marks a number that comes from the full-data Databricks run
     (workspace.gdelt.clean_audit, events_bronze). All other figures were measured on the data described in the text. -->

## Dataset Profile

### 2.1 Sources

The project uses the event tables of the Global Database of Events, Language and Tone (GDELT). GDELT codes news reports into events between two actors using the CAMEO (Conflict and Mediation Event Observations) scheme. Each event has an action code, a QuadClass (1 verbal cooperation, 2 material cooperation, 3 verbal conflict, 4 material conflict), a Goldstein score, coverage counts (`NumMentions`, `NumSources`, `NumArticles`), the average tone of the reporting articles (`AvgTone`), and geographic fields for both actors and for the action [1], [3], [4]. Two published forms of the event table are used [9]:

| Source | Publication unit | File name | Columns | `DATEADDED` format | Role in the project |
| --- | --- | --- | ---: | --- | --- |
| GDELT 1.0 daily event files [1], [5] | One file per day | `YYYYMMDD.export.CSV` (zipped) | 58 | `YYYYMMDD` | Main batch dataset: the full-year Delta table on Databricks and the HDFS sample month |
| GDELT 2.0 15-minute update files [2], [6] | One file every 15 minutes | `YYYYMMDDHHMMSS.export.CSV` (zipped) | 61 | `YYYYMMDDHHMMSS` (UTC) | Streaming input. It shares the 58 GDELT 1.0 fields |

Both formats are tab-separated, UTF-8 and have no header row. The three extra columns in GDELT 2.0 are `Actor1Geo_ADM2Code`, `Actor2Geo_ADM2Code` and `ActionGeo_ADM2Code`. Each one sits directly after the matching `*_ADM1Code` column, so every later geographic column in a 2.0 row is shifted relative to 1.0 [2]. The pipeline also uses three GDELT lookup files: `CAMEO.country.txt`, `CAMEO.eventcodes.txt` and `FIPS.country.txt` [7]. The two CAMEO files are kept in the repository's `lookups/` folder exactly as published, with their checksums, and are shared by the cleaning step and the dyad notebooks. `FIPS.country.txt` is used only to build the country-coordinate lookup for the Pig job (Section 3.1.2.2).

### 2.2 Size

#### 2.2.1 Full year on Databricks

The study period is November 2025 to October 2026: 365 daily files and 37,225,966 rows. They are stored as the Delta table `workspace.gdelt.events_bronze`, which takes 1.76 GiB. Rows per month, counted by file date:

| File month | Rows | Files |
| --- | ---: | ---: |
| 2025-11 | 3,218,988 | 30 |
| 2025-12 | 3,004,321 | 31 |
| 2026-01 | 3,309,934 | 31 |
| 2026-02 | 3,079,386 | 28 |
| 2026-03 | 3,508,491 | 31 |
| 2026-04 | 3,080,608 | 30 |
| 2026-05 | 3,101,213 | 31 |
| 2026-06 | 2,891,095 | 30 |
| 2026-07 | 3,001,613 | 31 |
| 2026-08 | 2,809,109 | 31 |
| 2026-09 | 3,025,268 | 30 |
| 2026-10 | 3,195,940 | 31 |
| **Total** | **37,225,966** | **365** |

That is about 102,000 events per day on average. Monthly volume ranges from 2.81 million (August 2026) to 3.51 million (March 2026).

#### 2.2.2 Sample month on the local Hadoop cluster

October 2026 is the sample month. Its 31 daily files take 1,256,569,308 bytes (about 1.2 GB, or 40.5 MB per file on average) in HDFS under `/gdelt/raw/events/year=2026/month=10/`. With a 32 MB block size they occupy 53 blocks, stored with 2 replicas each. The MapReduce ingestion counted 3,195,940 rows, exactly the number in the Databricks table for that month.

#### 2.2.3 Development samples

Code was developed and checked locally on two smaller extracts:

| Extract | Files | Rows | Bytes |
| --- | --- | ---: | ---: |
| GDELT 1.0 daily | `20261001.export.CSV`, `20261002.export.CSV` | 118,572 + 109,627 = 228,199 | 46,612,793 + 42,928,155 |
| GDELT 2.0 15-minute | `20261001000000.export.CSV`, `20261001001500.export.CSV` | 2,666 + 1,928 = 4,594 | 1,068,503 + 765,962 |

Every row in the 1.0 extract has exactly 58 fields. Every row in the 2.0 extract has exactly 61.

### 2.3 Schema and key fields

GDELT publishes every value as text. The pipeline uses three explicit schemas and never infers one (`notebooks/gdelt_common.py`):

1. **Raw GDELT 1.0 schema** (`raw_string_schema()`): the 58 codebook columns, all `STRING`, in codebook order. This is the bronze layer.
2. **Raw GDELT 2.0 schema** (`v2_raw_schema()`): the 61 codebook columns, all `STRING`. The reader `v2_to_shared()` then selects the 58 shared columns **by name**, not by position, so the three ADM2 columns cannot move a value into the wrong field. It also turns the 14-digit `DATEADDED` into a UTC timestamp (`dateadded_ts`). The same function works for batch reads and for `spark.readStream`.
3. **Typed schema** (`typed_schema()`, applied by `to_typed()`): dates become `DATE`; counts and classes become `INT`; scores and coordinates become `DOUBLE`; the event ID and all codes stay `STRING`. These are the same names and types that the dyad notebooks (Member 3) derive from the raw table, so they can read the cleaned table without any code change. Every conversion uses `try_cast`, so a malformed value becomes `NULL` instead of failing the job. This matters because Databricks serverless compute runs with ANSI SQL mode on, and under ANSI mode a failed plain `CAST` raises an error [10]. Code columns stay `STRING`: CAMEO event codes have significant leading zeros (`01` and `010` are different codes), and some FeatureIDs are not numeric.

Key fields used by the project:

| Field | Typed as | Meaning [1] | Used for |
| --- | --- | --- | --- |
| `GLOBALEVENTID` | STRING | Unique event identifier | De-duplication |
| `SQLDATE` | DATE | Date the event took place, as inferred from the article | Event-date grouping (daily and weekly windows) |
| `DATEADDED` | DATE | Date the event was added to GDELT, i.e. the file date | Partitioning (`file_year`, `file_month`), report lag (`DATEADDED − SQLDATE`) |
| `Actor1CountryCode`, `Actor2CountryCode` | STRING | CAMEO three-letter country code of each actor | Country dyads, country-day summary |
| `EventCode`, `EventRootCode` | STRING | CAMEO action code (root = first two digits) | Event-type analysis |
| `QuadClass` | INT | 1–4 (cooperation or conflict, verbal or material) | Conflict filter (3 and 4) |
| `GoldsteinScale` | DOUBLE | Theoretical impact on stability, −10 to +10 | Conflict intensity |
| `NumMentions`, `NumSources`, `NumArticles` | INT | Coverage counts | Weighting, coverage measures |
| `AvgTone` | DOUBLE | Mean tone of the articles mentioning the event | The "news tone" variable |
| `ActionGeo_Lat`, `ActionGeo_Long` | DOUBLE | Coordinates of the action location | Coordinate validation, mapping |

Two naming details follow from the platform design. The partition columns are `file_year` and `file_month`, because Spark column names are case-insensitive and GDELT already has a `Year` column. The country codes in `Actor*CountryCode` are CAMEO codes, while the `*Geo_CountryCode` columns use FIPS 10-4 codes [1]. This is why the country-day summary is keyed on `Actor1CountryCode`.

QuadClass distribution in the 2-day daily extract (228,199 rows):

| QuadClass | Meaning | Rows | Share |
| ---: | --- | ---: | ---: |
| 1 | Verbal cooperation | 141,526 | 62.02% |
| 2 | Material cooperation | 26,397 | 11.57% |
| 3 | Verbal conflict | 29,531 | 12.94% |
| 4 | Material conflict | 30,745 | 13.47% |

### 2.4 Event date versus file date

Every GDELT 1.0 daily file holds the events *added* on one day (`DATEADDED`), and that date is the partition key. The *event* date `SQLDATE` can be much earlier, because articles also report on past events. In each month of the full-year table, the earliest `SQLDATE` is almost exactly ten years before the file date (for example, files from November 2025 contain events dated 2015-11-04). The latest `SQLDATE` is the last day of the file month, so no event dates lie in the future.

The 2-day extract shows how the gap is distributed:

| Measure (2-day extract, 228,199 rows) | Value |
| --- | ---: |
| `DATEADDED` values | only the file's own date (20261001 or 20261002) |
| `SQLDATE` range | 2016-10-03 to 2026-10-02 |
| Rows with `SQLDATE` after `DATEADDED` | 0 |
| Rows with `SQLDATE` at most 1 day before `DATEADDED` | 224,616 (98.43%) |
| Rows more than 30 days old | 1,310 (0.57%) |
| Rows more than 365 days old | 28 (0.01%) |

The old events are few, but they are not random. The two largest `SQLDATE` values after the file dates themselves are 2026-09-02 (744 rows), about one month before the files, and 2025-10-01 (692 rows), exactly one year before. The data therefore has spikes at dates such as "a month ago" or "a year ago".

This has two consequences:

- Any analysis by event week must group by `SQLDATE` and must drop events far older than their file date. Otherwise an old event recorded in week *t* would count towards an earlier week, and the timing that the tone-leads-conflict question depends on would be distorted. The cleaning step deliberately keeps these rows. The dyad analysis applies a recency window `0 ≤ DATEADDED − SQLDATE ≤ N` and chooses *N* from the lag distribution of the cleaned rows, so removing old events earlier with a fixed cut-off would bias that choice (Section 2.6).
- Partition pruning works only on the file-date columns. A filter on `SQLDATE` alone still scans every partition, although Delta file statistics reduce the cost.

The full-year age distribution, in 30-day bins of `datediff(DATEADDED, SQLDATE)`, is **[TBD: full-year age histogram from events_bronze, Nov 27]**.

### 2.5 Storage and compute constraints

| Environment | Constraint | Effect on the design |
| --- | --- | --- |
| Databricks Free Edition | Serverless compute only. No custom clusters, and outbound internet access only to a limited set of trusted domains. A fair-usage quota applies: if an account exceeds it, compute is shut down for the rest of the day, and in extreme cases the rest of the month [8] | Only DataFrame and SQL code is used: no RDD API, no `sparkContext`, no `cache()`/`persist()`. ANSI mode is on, so all casts are `try_cast`. The notebooks default to a one-month sample mode, and full-year runs are scheduled ahead of the deadline |
| Databricks Free Edition storage | Unity Catalog only, with no custom workspace storage locations [8]; Delta tables | Raw CSVs are unzipped into a landing volume, written to Delta, and then deleted. The volume is empty after every load. The full year takes 1.76 GiB as Delta |
| Local Hadoop cluster (Docker, one host) | 3 DataNode/NodeManager workers with 2,048 MB and 4 vcores of YARN each (12 vcores in total). 32 MB blocks, replication 2 | HDFS holds only the October sample (1.2 GB of raw data, 2.3 GB with replicas). The benchmark task layout is sized to fit these containers |

### 2.6 Data quality and the cleaning audit (above and beyond)

#### 2.6.1 Rules

The theme asks only for ingestion, filtering, rolling averages and storage. This section goes further: every cleaning rule is measured, so the report can state exactly how many rows each rule removes, per month and per year, instead of reporting only a final row count.

`notebooks/02_clean_events.py` reads `events_bronze` and applies `to_typed()`. It then tags each row with the **first** rule it fails, in a fixed order, so that each dropped row is counted exactly once and `rows_in = rows_clean + rows dropped`. The rules, their order and their names are shared with the dyad notebooks (Member 3), and both use the same CAMEO lookup files in the repository's `lookups/` folder: the official country list [3], 25 region codes that are not states, and a remap of three sub-state codes (`WSB` and `GZS` to `PSE`, `BAG` to `IRQ`).

| # | Rule | A row is dropped when | Reason |
| ---: | --- | --- | --- |
| 1 | `missing_event_id` | `GLOBALEVENTID` is missing | An event without an identifier cannot be de-duplicated |
| 2 | `duplicate_event_id` | its `GLOBALEVENTID` has already been kept | The same event must not be counted twice. The earliest report is kept (smallest `DATEADDED`, then most mentions), with further deterministic tie-breakers so that re-runs give identical tables |
| 3 | `invalid_date` | `SQLDATE` or `DATEADDED` is missing or not a valid date | Daily and weekly windows need a real event date |
| 4 | `missing_country` | `Actor1CountryCode` or `Actor2CountryCode` is missing | Without two countries there is no dyad |
| 5 | `region_code` | either actor is a region code (`EUR`, `MEA`, …) | A region is not a state |
| 6 | `unknown_code` | an actor code is not in the CAMEO country list | Unknown codes are reported, not guessed |
| 7 | `same_country` | both actors are the same state after the sub-state remap | Domestic events are not dyads |
| 8 | `invalid_measure` | `AvgTone` or `QuadClass` is missing, or `NumMentions` is missing or below 1 | Tone and its mention weight are the inputs of the sentiment index |

Invalid coordinates are handled without dropping the row. A latitude/longitude pair that does not parse, lies outside [−90, 90] / [−180, 180], is exactly (0, 0) (a placeholder location), or has only one of its two values, is set to `NULL`, and the audit counts these rows under `invalid_coordinates`. The dyad analysis does not use coordinates, so dropping these rows would only remove valid tone and conflict observations.

The results go to two Delta tables. `workspace.gdelt.events_clean` holds the 58 typed columns under their original names plus lineage columns, partitioned by `file_year` and `file_month`. `workspace.gdelt.clean_audit` holds one row per file month and rule: `action` (drop or set to `NULL`), `rows_in`, `rows_clean`, `rows_affected` and `pct`. Both tables are written with `replaceWhere` on the processed months, so a sample-mode run replaces only October.

#### 2.6.2 Audit on the October 2026 sample

The notebook was run on Databricks serverless compute in sample mode, over the 31 daily files of October 2026 (`workspace.gdelt.clean_audit`). The same code was also run locally (`tests/test_clean_local.py`, Spark 3.5.3 with ANSI mode on) on the 2-day extract, which gives very similar shares:

| Rule | October 2026 (Databricks) | Share | 2-day extract (local) | Share |
| --- | ---: | ---: | ---: | ---: |
| `missing_event_id` | 0 | 0.000% | 0 | 0.000% |
| `duplicate_event_id` | 0 | 0.000% | 0 | 0.000% |
| `invalid_date` | 0 | 0.000% | 0 | 0.000% |
| `missing_country` | 2,407,354 | 75.325% | 174,323 | 76.391% |
| `region_code` | 56,435 | 1.766% | 3,761 | 1.648% |
| `unknown_code` | 1,462 | 0.046% | 95 | 0.042% |
| `same_country` | 189,895 | 5.942% | 13,674 | 5.992% |
| `invalid_measure` | 0 | 0.000% | 0 | 0.000% |
| **Rows in / rows clean** | **3,195,940 / 540,794** | **16.92% kept** | **228,199 / 36,346** | **15.93% kept** |
| `invalid_coordinates` (set to `NULL`, kept) | 21 | 0.001% | 2 | 0.001% |

Checks on the October output (asserted by the notebook on every run): 540,794 distinct event IDs, and `rows_in = rows_clean + rows dropped` exactly (3,195,940 = 540,794 + 2,655,146); every actor is a CAMEO state and the two differ after the remap; no missing tone, class or mention count; no invalid coordinate pair; the event codes are stored as strings. On the 2-day extract: 36,346 distinct event IDs; every actor is a CAMEO state and the two differ after the remap; no missing tone, class or mention count; no invalid coordinate pair. Running the dyad notebooks' own rules on the cleaned rows marks all 36,346 as eligible, and their clean dyadic events are identical whether they start from the raw or the cleaned table (`tests/test_clean_member3_compat.py`).

Findings:

- **Actor countries are the dominant gap.** `Actor1CountryCode` is empty in 102,032 rows (44.71%) and `Actor2CountryCode` in 130,350 rows (57.12%). At least one of them is empty in 174,323 rows (76.39%). A likely reason is that many coded actors carry no country (for example generic roles such as police or protesters), and some events have only one actor. As a result, the clean table keeps roughly one row in six. Volumes in `events_clean` must be read with this in mind.
- **Region codes are the second-largest loss** (3,761 rows): reports that name a region such as Europe or the Middle East as an actor rather than a state.
- **Old events remain, by design.** In October the clean rows still span `SQLDATE` 2016-10-04 to 2026-10-31, and 3,864 of them (0.71%) were reported more than 30 days after the event. The dyad analysis removes them with its recency window.
- **Coordinates are mostly either valid or absent.** 6,171 raw rows (2.70%) have no ActionGeo coordinates. 46 rows have an empty latitude and a longitude of `42`, a half-filled pair. In the clean table, 2 rows had a coordinate pair set to `NULL`, and 54 have no ActionGeo location.
- **No duplicate IDs** occur within October. Duplicates are checked within the processed range: one month in sample mode, the whole year in a full run.
- **The 15-minute files parse cleanly** with the 61-column schema. All 4,594 rows have exactly 61 fields, and `DATEADDED` and `SOURCEURL` fall in the expected columns.

#### 2.6.3 Full-year audit

| File year | Rows in | Missing ID | Duplicate ID | Invalid date | Missing country | Region code | Unknown code | Same country | Invalid measure | Rows clean | Coordinates set to NULL |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 2025 (Nov–Dec) | 6,223,309 | [TBD] | [TBD] | [TBD] | [TBD] | [TBD] | [TBD] | [TBD] | [TBD] | [TBD] | [TBD] |
| 2026 (Jan–Oct) | 31,002,657 | [TBD] | [TBD] | [TBD] | [TBD] | [TBD] | [TBD] | [TBD] | [TBD] | [TBD] | [TBD] |

The rows-in figures are the bronze totals from Section 2.2.1. **[TBD: full-year audit from clean_audit (per year via audit_by_year, and per month), Nov 27]**

### Sources cited in this section

_Numbered for this section only; Member 5 merges them into the report's References._

[1] GDELT Project, *GDELT 1.0 Event Database: Data Format Codebook*. http://data.gdeltproject.org/documentation/GDELT-Data_Format_Codebook.pdf

[2] GDELT Project, *GDELT 2.0 Event Database Codebook* (V2.0). http://data.gdeltproject.org/documentation/GDELT-Event_Codebook-V2.0.pdf

[3] P. A. Schrodt, *CAMEO: Conflict and Mediation Event Observations, Event and Actor Codebook*, version 1.1b3, 2012. http://data.gdeltproject.org/documentation/CAMEO.Manual.1.1b3.pdf

[4] K. Leetaru and P. A. Schrodt, "GDELT: Global Data on Events, Location and Tone, 1979–2012," *ISA Annual Convention*, 2013.

[5] GDELT Project, GDELT 1.0 daily event files index. http://data.gdeltproject.org/events/index.html

[6] GDELT Project, "GDELT 2.0: Our Global World in Realtime," GDELT Blog. https://blog.gdeltproject.org/gdelt-2-0-our-global-world-in-realtime/

[7] GDELT Project, lookup tables: https://www.gdeltproject.org/data/lookups/CAMEO.country.txt, https://www.gdeltproject.org/data/lookups/CAMEO.eventcodes.txt, https://www.gdeltproject.org/data/lookups/FIPS.country.txt

[8] Databricks, "Databricks Free Edition limitations." https://docs.databricks.com/aws/en/getting-started/free-edition-limitations

[9] GDELT Project, data downloads overview. https://www.gdeltproject.org/data.html

[10] Apache Spark, "ANSI Compliance," Spark SQL reference. https://spark.apache.org/docs/latest/sql-ref-ansi-compliance.html
