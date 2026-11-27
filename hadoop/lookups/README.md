# Lookup tables

Small reference files used by the Pig conflict filter (`hadoop/pig/`) and available to the
Databricks notebooks (for example, the CAMEO event-code join).

| File | Columns | Header row? | Source |
| --- | --- | --- | --- |
| `CAMEO.country.txt` | `CODE`, `LABEL` (CAMEO actor country / region code and name) | yes: `CODE	LABEL` | Byte-identical copy of the repo's `lookups/CAMEO.country.txt` (the official GDELT file, CRLF). It is copied here only because the containers mount `hadoop/` and not the repo root; the master copy, plus the CAMEO event codes, the remap and the region list, is in `lookups/` |
| `FIPS.country.txt` | FIPS 10-4 country code, name (the codes used in the `*Geo_CountryCode` columns) | no | Official GDELT lookup, CRLF to LF |
| `country_centroids.tsv` | `cameo_code`, `country_name`, `lat`, `lon` | **no** (so Pig's `PigStorage` can load it directly) | Built by `build_country_centroids.py` from the files above and the events |

All files are tab-separated. Reading the headed file with Spark: `header=true, sep='\t'`.

Notes on the official files: `CAMEO.country.txt` spells Colombia as "Columbia", and lost the accent in
Réunion ("Runion"). `FIPS.country.txt` lists `GV` as "Equatorial Guinea" (in GDELT's geocodes `GV` is
Guinea) and `LO` as "Czechoslovakia" (Slovakia). The files are kept exactly as published; the centroid
builder works around these (see below).

## `country_centroids.tsv`: how it's built

GDELT doesn't publish a latitude/longitude per CAMEO country code, so the builder derives one from the
events themselves:

1. When `Actor1Geo_Type` (or `Actor2Geo_Type`) is `1`, GDELT geocoded that actor to a **whole country**,
   and the lat/long it gives is that country's centroid, with the country's FIPS code in
   `Actor1Geo_CountryCode`. For each such row, the builder counts the pair
   (`Actor1CountryCode`, `Actor1Geo_CountryCode`), and the same for Actor2. The two counts are added.
2. For each CAMEO code, its FIPS candidates are ranked by count. The builder takes the **most frequent
   candidate whose country name matches the CAMEO name** (case, punctuation, "the" and "and" ignored,
   plus a short list of spelling aliases in the script). Usually that's the top candidate. When it isn't,
   the row is marked `name_corrected` in the build report: in the sample, Myanmar actors are geocoded to
   Malaysia (31 rows) more often than to Myanmar (13), and Liechtenstein, Suriname and Tonga have the
   same issue with neighbours.
3. The centroid is the most frequent (lat, long) of that FIPS country's type-1 geocodes. In the sample,
   every country has exactly one value (share 1.00 in the report).
4. **Check against GDELT's own detail geocodes:** the centroid must be within 3,000 km of at least one
   sub-national geocode (types 2–5, any geo block) in the same FIPS country. This drops **MUS
   (Mauritius)**, whose GDELT country centroid is 16°N 146°E, the Northern Mariana Islands, about 8,500 km
   from any Mauritius geocode. Countries with no sub-national geocodes in the input are kept and marked
   `unchecked` in the report.
5. **Sanity check:** the build fails unless USA, CHN, RUS, GBR, IND, BRA, AUS, UKR, ISR, FRA and PSE are
   all within 500 km of their known approximate centres. On the 2-day sample, the largest offsets are IND
   (304 km; GDELT uses 20, 77) and GBR (131 km; GDELT uses 54, −4).

**One manual override, PSE (Occupied Palestinian Territory).** FIPS 10-4 has no Palestine, only `WE`
(West Bank) and `GZ` (Gaza Strip), so no name can match, and PSE actors are geocoded to Israel (`IS`)
most often, which the name check rightly rejects. Without a centroid, every Israel–Palestine conflict
event would be dropped from the Pig join, although ISR→PSE and PSE→ISR are among the top conflict
dyads. `FIPS_OVERRIDES` in the script maps PSE to `WE`/`GZ`, and the builder takes the more frequent of
the two among PSE's own country-level geocodes (status `override` in the report). The coordinates still
come from GDELT: on the 2-day sample that is `WE` (63 of PSE's 489 country-level geocodes, against 44
for `GZ`), 31.666667, 35.25. PSE is also in the sanity check below (within 500 km of 31.9, 35.2), so a
build without it fails.

Why a name check rather than "most frequent FIPS" alone: the plain rule gives wrong answers, for example
GEO (Georgia) → Russia and HKG → United States (1 row each), NMR (North America) → United States, and
EUR (Europe) → United Kingdom. Regions, cities and "the West" have no FIPS country with a matching name,
so they get **no centroid**, without a hand-written exclusion list. The data does support the West Bank
(`WSB` → FIPS `WE`) and Gaza Strip (`GZS` → `GZ`), and the builder would accept them if their actors had
country-level geocodes in the input. In the 2-day sample they don't.

`SSD` (South Sudan) appears in the events but not in `CAMEO.country.txt`, which predates 2011. The script
adds it (`EXTRA_CAMEO`) and prints any other code in the data that isn't in the list (there are none in
the sample).

### Result on the committed file (2-day sample, 2026-10-01 and 2026-10-02)

- Input: 228,199 events. **188 codes got a centroid (PSE through the override); 74 of the 262 CAMEO
  codes did not.**
- No centroid because the name matched no FIPS candidate (regions, cities, or codes whose only geocodes
  were other countries), 18 codes: AFR, CRB, EAF, EUR, GEO, GNB, GNQ, HKG, KNA, MEA, NMR, PGS, SAF, SAS,
  SEA, STP, WAF, WST.
- No centroid because of the detail-geocode check: MUS.
- No centroid because the code had no country-level actor geocodes in two days: the other 55 (mostly small
  territories and the remaining region and city codes, including BAG, WSB and GZS, but also BIH, MTN, ROM,
  SVN, SWZ). Building from more days fills these in.

The exact list, with the counts behind every decision, is printed by the builder (see the command below).

**Effect on the Pig filter:** conflict events where either country has no centroid are not in the Pig
`events/` output, and are counted in `summary_unmatched_codes`. On the 2-day sample (Pig local mode),
699 of the 9,995 conflict events are unmatched; the largest unmatched codes are the regions EUR (288
event sides), AFR (216) and WST (126). The ISR/PSE dyads are joined (411 rows in `events/`).

**Before the official Pig run, rebuild the lookup from the full October month** (on Nick's PC, from the
31 daily files): two days leave out real countries such as BIH, MTN, ROM, SVN and SWZ only because
they had no country-level geocode in those two days. Then re-run the Pig job and check
`summary_unmatched_codes` for any real country that is still missing.

## Rebuild

```bash
cd hadoop/lookups
python3 build_country_centroids.py --cameo CAMEO.country.txt --fips FIPS.country.txt \
    --out country_centroids.tsv  /path/to/20261001.export.CSV /path/to/20261002.export.CSV

# Full October, inside the namenode container (the raw files are in HDFS):
docker exec namenode bash -c 'mkdir -p /tmp/oct && hdfs dfs -get "/gdelt/raw/events/year=2026/month=10/*.CSV" /tmp/oct/ &&
  cd /opt/cp422/lookups && python3 build_country_centroids.py --cameo CAMEO.country.txt --fips FIPS.country.txt \
    --out country_centroids.tsv /tmp/oct/*.CSV > /tmp/centroid_build_report.txt; tail -n 20 /tmp/centroid_build_report.txt'
```

The output depends only on the input files, so the same inputs give the same file. More input days
give more codes a centroid; the 10-country sanity check still has to pass.
