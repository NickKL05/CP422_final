#!/usr/bin/env python3
"""Build hadoop/lookups/country_centroids.tsv: one latitude/longitude per CAMEO actor country code.

GDELT doesn't publish a centroid per CAMEO country code, so this script derives one from the events
themselves. Plain python3 (no Spark), so it runs on any machine and inside the cluster image.

Usage:
    python3 build_country_centroids.py --cameo CAMEO.country.txt --fips FIPS.country.txt \
        --out country_centroids.tsv  EVENTS.CSV [EVENTS.CSV ...]

Inputs are GDELT 1.0 daily export files (58 tab-separated columns, no header).

Method
------
1. GDELT geocodes each actor. When Actor{1,2}Geo_Type == "1" the location is a whole COUNTRY, and
   GDELT gives that country's centroid as Actor{1,2}Geo_Lat/Long, with its FIPS 10-4 code in
   Actor{1,2}Geo_CountryCode. For every such row, count the pair (Actor{1,2}CountryCode [CAMEO],
   Actor{1,2}Geo_CountryCode [FIPS]). Actor1 and Actor2 counts are added together.
2. For each CAMEO code, rank its FIPS candidates by count. Take the most frequent candidate whose
   country NAME agrees with the CAMEO name (see `same_country`). Usually that is the top candidate;
   when it isn't, the top one is a co-located country (for example, Myanmar actors are geocoded to
   Malaysia more often than to Myanmar in the 2-day sample) and the row is marked "name_corrected".
   If no candidate's name agrees, the code gets NO centroid. This is what keeps out regions and
   cities (AFR "Africa", EUR "Europe", NMR "North America", BAG "Baghdad", ...): no FIPS country
   has those names, even though their most frequent geocode is a real country.
3. The centroid of the chosen FIPS country is its most frequent (lat, long) among the type-1 rows.
4. Sanity check: the centroid must be within MAX_KM_FROM_DETAIL km of at least one sub-national
   geocode (types 2-5, any of the three geo blocks) with the same FIPS code. This catches GDELT's
   own bad country centroids (Mauritius is placed at 16N 146E, which is the Northern Mariana
   Islands). Countries with no sub-national geocodes in the input are kept and marked "unchecked".
5. FIPS_OVERRIDES: a code that no FIPS country name can match (PSE) is mapped by hand to the FIPS
   codes that cover it, and takes the most frequent of those among its own geocodes ("override").

Output: tab-separated, NO header row (Pig's PigStorage would read a header as data), columns
    cameo_code, country_name, lat, lon
sorted by cameo_code. A report of every decision (support counts, shares, rejected codes) is printed
to stdout so the build can be checked and repeated.
"""
import argparse
import collections
import math
import re
import sys

# 0-based column indexes in the 58-column GDELT 1.0 layout (see notebooks/gdelt_common.py)
ACTOR_CODE_COL = {1: 7, 2: 17}                    # Actor1CountryCode, Actor2CountryCode
GEO_BLOCK_START = {1: 35, 2: 42, "action": 49}    # *_Geo_Type; then FullName, CountryCode, ADM1, Lat, Long
N_COLS = 58

MAX_KM_FROM_DETAIL = 3000

# CAMEO codes GDELT uses that are missing from the published CAMEO.country.txt list.
# SSD (South Sudan, independent since 2011) appears in the events but not in the list.
EXTRA_CAMEO = {"SSD": "South Sudan"}

# Names that differ between CAMEO and the FIPS/geocode names, after `norm` (spelling or naming
# differences only; each maps the CAMEO name to the name the FIPS side uses).
NAME_ALIASES = {
    "columbia": "colombia",                        # typo in CAMEO.country.txt
    "brunei darussalam": "brunei",
    "ivory coast": "cote divoire",
    "peoples republic of congo": "congo",
    "slovakia": "slovak republic",
    "vietnam": "vietnam republic of",              # GDELT geocode name is "Vietnam, Republic Of"
    "faeroe islands": "faroe islands",
    "falkland islands": "falkland islands islas malvinas",
    "macao": "macau",
    "runion": "reunion",                           # CAMEO.country.txt lost the accent: "R?union"
    "united states virgin islands": "virgin islands",
    "wallis futuna islands": "wallis futuna",
    "pitcairn": "pitcairn islands",
    "svalbard jan mayen islands": "svalbard",
}


# CAMEO codes that no FIPS 10-4 country name can match, mapped by hand to the FIPS codes that cover
# the same territory. The centroid still comes from the data: the most frequent of these candidates
# among the code's own country-level actor geocodes (status "override" in the report).
# PSE (Occupied Palestinian Territory): FIPS has no Palestine, only WE (West Bank) and GZ (Gaza Strip).
# Its actors are geocoded to IS (Israel) most often, which the name check rightly rejects; without
# this entry every Israel-Palestine dyad would be dropped from the Pig join.
FIPS_OVERRIDES = {"PSE": ("WE", "GZ")}

# Sanity check: approximate geographic centres of 10 well-known countries (lat, lon). The build fails
# if any of them is missing or more than KNOWN_TOLERANCE_KM away, because that would mean the
# method is mapping big countries wrongly.
KNOWN_CENTROIDS = {
    "USA": (39.8, -98.6), "CHN": (35.0, 105.0), "RUS": (60.0, 100.0), "GBR": (54.0, -2.0),
    "IND": (22.0, 79.0), "BRA": (-10.0, -55.0), "AUS": (-25.0, 134.0), "UKR": (49.0, 32.0),
    "ISR": (31.5, 34.75), "FRA": (46.0, 2.0),
    "PSE": (31.9, 35.2),     # the FIPS_OVERRIDES entry must have worked (West Bank or Gaza centroid)
}
KNOWN_TOLERANCE_KM = 500


def norm(name):
    """Lower case, no punctuation, and without the words "the" and "and", so that
    "Bahamas, The" == "Bahamas" and "Bosnia-Herzegovina" == "Bosnia and Herzegovina"."""
    words = re.sub(r"[^a-z0-9 ]", " ", name.lower().replace("'", "")).split()
    return " ".join(w for w in words if w not in ("the", "and"))


def same_country(cameo_name, fips_names):
    """True if the CAMEO country name matches any of the names seen for a FIPS code."""
    want = norm(cameo_name)
    want = NAME_ALIASES.get(want, want)
    return any(norm(n) == want for n in fips_names if n)


def read_lookup(path):
    """Two-column tab-separated lookup (CRLF or LF), optional "CODE\tLABEL" header -> {code: label}."""
    out = {}
    with open(path, encoding="utf-8", errors="replace") as f:
        for line in f:
            parts = line.rstrip("\r\n").split("\t")
            if len(parts) >= 2 and parts[0] and parts[0] != "CODE":
                out[parts[0]] = parts[1]
    return out


def to_float(s):
    try:
        return float(s)
    except ValueError:
        return None


def km(lat1, lon1, lat2, lon2):
    """Great-circle distance (haversine), in km."""
    p1, p2 = math.radians(lat1), math.radians(lat2)
    a = (math.sin((p2 - p1) / 2) ** 2
         + math.cos(p1) * math.cos(p2) * math.sin(math.radians(lon2 - lon1) / 2) ** 2)
    return 2 * 6371.0 * math.asin(math.sqrt(min(1.0, a)))


def scan(paths):
    """One pass over the event files. Returns the counters the method needs."""
    pair_counts = collections.defaultdict(collections.Counter)    # cameo -> {fips: n}
    centroid_counts = collections.defaultdict(collections.Counter)  # fips -> {(lat, lon): n}
    geo_names = collections.defaultdict(collections.Counter)      # fips -> {type-1 FullName: n}
    detail_points = collections.defaultdict(set)                  # fips -> {(lat, lon)} of types 2-5
    rows = bad_rows = 0
    for path in paths:
        with open(path, encoding="utf-8", errors="replace") as f:
            for line in f:
                cols = line.rstrip("\r\n").split("\t")
                rows += 1
                if len(cols) != N_COLS:
                    bad_rows += 1
                    continue
                for actor, code_col in ACTOR_CODE_COL.items():
                    g = GEO_BLOCK_START[actor]
                    code, gtype, fips = cols[code_col], cols[g], cols[g + 2]
                    lat, lon = to_float(cols[g + 4]), to_float(cols[g + 5])
                    if code and gtype == "1" and fips and lat is not None and lon is not None:
                        pair_counts[code][fips] += 1
                        centroid_counts[fips][(cols[g + 4], cols[g + 5])] += 1
                        geo_names[fips][cols[g + 1]] += 1
                for g in GEO_BLOCK_START.values():
                    gtype, fips = cols[g], cols[g + 2]
                    lat, lon = to_float(cols[g + 4]), to_float(cols[g + 5])
                    if gtype in ("2", "3", "4", "5") and fips and lat is not None and lon is not None:
                        detail_points[fips].add((round(lat, 2), round(lon, 2)))
    return rows, bad_rows, pair_counts, centroid_counts, geo_names, detail_points


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--cameo", required=True, help="CAMEO.country.txt")
    ap.add_argument("--fips", required=True, help="FIPS.country.txt (FIPS 10-4 code -> name)")
    ap.add_argument("--out", required=True, help="output TSV (no header)")
    ap.add_argument("events", nargs="+", help="GDELT 1.0 daily export CSV files")
    args = ap.parse_args()

    cameo = read_lookup(args.cameo)
    cameo.update({k: v for k, v in EXTRA_CAMEO.items() if k not in cameo})
    fips_lookup = read_lookup(args.fips)
    rows, bad_rows, pair_counts, centroid_counts, geo_names, detail_points = scan(args.events)

    print(f"input rows: {rows}  (skipped, not 58 columns: {bad_rows})")
    print(f"CAMEO codes in list (+ extras {sorted(EXTRA_CAMEO)}): {len(cameo)}")
    not_in_list = sorted(c for c in pair_counts if c not in cameo)
    print(f"actor country codes in data but not in the CAMEO list: {not_in_list or 'none'}")
    print()
    print("code\tstatus\tfips\tn_fips/n_type1\tlat\tlon\tcentroid_share\tnearest_detail_km\tnote")

    out_rows, no_centroid = [], []
    for code in sorted(cameo):
        name = cameo[code]
        cands = pair_counts.get(code, collections.Counter()).most_common()
        total = sum(n for _, n in cands)
        if not cands:
            no_centroid.append((code, name, "no country-level actor geocodes in input"))
            continue
        chosen = None
        for rank, (fips, n) in enumerate(cands):
            if code in FIPS_OVERRIDES:
                if fips in FIPS_OVERRIDES[code]:
                    chosen = (fips, n, "override")
                    break
                continue
            names = [fips_lookup.get(fips, "")] + list(geo_names[fips])
            if same_country(name, names):
                chosen = (fips, n, "ok" if rank == 0 else "name_corrected")
                break
        if chosen is None:
            top = ", ".join(f"{f}:{n}" for f, n in cands[:3])
            want = (f"none of the override FIPS codes {'/'.join(FIPS_OVERRIDES[code])}"
                    if code in FIPS_OVERRIDES else "no FIPS candidate with a matching name")
            no_centroid.append((code, name, f"{want} (top: {top})"))
            continue
        fips, n, status = chosen
        (lat_s, lon_s), c_n = centroid_counts[fips].most_common(1)[0]
        c_share = c_n / sum(centroid_counts[fips].values())
        lat, lon = float(lat_s), float(lon_s)
        pts = detail_points.get(fips)
        if pts:
            nearest = min(km(lat, lon, a, b) for a, b in pts)
            if nearest > MAX_KM_FROM_DETAIL:
                no_centroid.append((code, name, f"GDELT centroid {lat_s},{lon_s} for {fips} is "
                                                f"{nearest:.0f} km from every sub-national {fips} geocode"))
                continue
            nearest_s = f"{nearest:.0f}"
        else:
            nearest_s = "unchecked"
        print(f"{code}\t{status}\t{fips}\t{n}/{total}\t{lat_s}\t{lon_s}\t{c_share:.2f}\t{nearest_s}\t{name}")
        out_rows.append((code, name, lat_s, lon_s))

    with open(args.out, "w", encoding="utf-8", newline="\n") as f:
        for r in out_rows:
            f.write("\t".join(r) + "\n")

    print()
    print(f"wrote {len(out_rows)} centroids to {args.out}")
    print(f"CAMEO codes with NO centroid: {len(no_centroid)}")
    for code, name, why in no_centroid:
        print(f"  {code}\t{name}\t{why}")

    print()
    print(f"sanity check against known approximate centroids (tolerance {KNOWN_TOLERANCE_KM} km):")
    built = {r[0]: (float(r[2]), float(r[3])) for r in out_rows}
    failures = 0
    for code, (klat, klon) in KNOWN_CENTROIDS.items():
        if code not in built:
            print(f"  FAIL {code}: no centroid built")
            failures += 1
            continue
        d = km(klat, klon, *built[code])
        ok = d <= KNOWN_TOLERANCE_KM
        failures += 0 if ok else 1
        print(f"  {'ok  ' if ok else 'FAIL'} {code}: built {built[code][0]},{built[code][1]}  "
              f"known ~{klat},{klon}  off by {d:.0f} km")
    if failures:
        print(f"{failures} sanity check(s) failed")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
