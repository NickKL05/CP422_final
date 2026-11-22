#!/usr/bin/env python3
"""Build the input file for the MapReduce ingestion job: one GDELT daily-file URL per line.

Each output line is:  <url> <md5> <zip_bytes>
The MD5 and size come from GDELT's own index files, so each mapper can check its download.

Usage:  python3 make_url_list.py --start 2026-10-01 --end 2026-10-31 --out urls.txt

AI citation: Claude Code (Claude Opus 5.5, Anthropic) drafted this script
(index parsing, date filtering, output format). Reviewed and tested by Member 1.
"""
import argparse
import datetime as dt
import re
import sys
import urllib.request

BASE = "https://data.gdeltproject.org/events/"
NAME_RE = re.compile(r"^(\d{8})\.export\.CSV\.zip$")


def fetch_index(name):
    """Return {filename: field} parsed from a GDELT index file ('<field> <filename>' per line)."""
    with urllib.request.urlopen(BASE + name, timeout=60) as resp:
        text = resp.read().decode("utf-8", "replace")
    out = {}
    for line in text.splitlines():
        parts = line.split()
        if len(parts) == 2:
            out[parts[1]] = parts[0]
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--start", required=True, help="first file date, YYYY-MM-DD (inclusive)")
    ap.add_argument("--end", required=True, help="last file date, YYYY-MM-DD (inclusive)")
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    start = dt.date.fromisoformat(args.start)
    end = dt.date.fromisoformat(args.end)
    md5s = fetch_index("md5sums")
    sizes = fetch_index("filesizes")

    rows = []
    for fname, md5 in md5s.items():
        m = NAME_RE.match(fname)
        if not m:
            continue  # skip the pre-2013 monthly/yearly archives
        day = dt.datetime.strptime(m.group(1), "%Y%m%d").date()
        if start <= day <= end:
            rows.append((day, BASE + fname, md5, sizes.get(fname, "0")))
    rows.sort()

    expected = (end - start).days + 1
    with open(args.out, "w", newline="\n") as f:
        for _, url, md5, size in rows:
            f.write(f"{url} {md5} {size}\n")

    total_mb = sum(int(r[3]) for r in rows) / 1e6
    print(f"{len(rows)} files ({total_mb:.1f} MB zipped) for {start}..{end}; "
          f"{expected - len(rows)} days missing from the GDELT index", file=sys.stderr)


if __name__ == "__main__":
    main()
