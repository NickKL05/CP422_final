#!/usr/bin/env python3
"""Check that the MapReduce and Spark summary outputs are the same result.

    python3 compare_outputs.py <mr output> <spark output> [--tolerance 1e-6] [--names A B]

Each argument is a local file, or a local directory of part files (part-*; _SUCCESS and .crc
files are ignored). To compare HDFS outputs, copy them down first, for example
`hdfs dfs -getmerge /gdelt/summary/mr_x mr.tsv` (bench_mr_vs_spark.sh does this).

"Identical" means:
  * the same set of (SQLDATE, Actor1CountryCode) keys, each appearing once per side;
  * exactly equal `events` and `sum_mentions` (integers);
  * `avg_tone` within the tolerance (default 1e-6). The two engines add the same doubles in a
    different order, so the sums can differ in the last bits, and a value close to a rounding
    boundary can then print one unit apart in the 6th decimal place. That is a difference of
    1e-6 at most, so the check is |a - b| <= tolerance (plus 1e-12 for float noise in parsing).
Exit code 0 when identical, 1 otherwise. The last line is "IDENTICAL" or "DIFFERENT".
"""
import argparse
import os
import sys


def part_files(path):
    if os.path.isfile(path):
        return [path]
    names = sorted(n for n in os.listdir(path)
                   if n.startswith("part-") and not n.endswith(".crc"))
    return [os.path.join(path, n) for n in names]


def load(path, label):
    rows, dupes, bad = {}, 0, 0
    files = part_files(path)
    if not files:
        sys.exit(f"{label}: no part files found in {path}")
    for name in files:
        with open(name, encoding="utf-8", errors="surrogateescape") as fh:
            for line in fh:
                parts = line.rstrip("\n").split("\t")
                if len(parts) != 5:
                    bad += 1
                    continue
                date, country, events, mentions, tone = parts
                key = (date, country)
                if key in rows:
                    dupes += 1
                rows[key] = (int(events), int(mentions), float(tone))
    print(f"{label:6s}: {len(rows):,} keys from {len(files)} file(s); "
          f"{sum(r[0] for r in rows.values()):,} events; "
          f"{sum(r[1] for r in rows.values()):,} mentions; "
          f"duplicate keys {dupes}; malformed lines {bad}")
    return rows, dupes, bad


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("mr")
    ap.add_argument("spark")
    ap.add_argument("--tolerance", type=float, default=1e-6)
    ap.add_argument("--show", type=int, default=5, help="examples to print per kind of difference")
    ap.add_argument("--names", nargs=2, default=["mr", "spark"], metavar=("A", "B"),
                    help="labels for the two outputs in the report (default: mr spark)")
    args = ap.parse_args()

    a, b = args.names
    mr, mr_dupes, mr_bad = load(args.mr, a)
    sp, sp_dupes, sp_bad = load(args.spark, b)

    only_mr = sorted(mr.keys() - sp.keys())
    only_sp = sorted(sp.keys() - mr.keys())
    both = sorted(mr.keys() & sp.keys())
    count_diff = [k for k in both if mr[k][0] != sp[k][0]]
    mention_diff = [k for k in both if mr[k][1] != sp[k][1]]
    tone_gap = [(abs(mr[k][2] - sp[k][2]), k) for k in both]
    max_gap = max(tone_gap)[0] if tone_gap else 0.0
    tone_diff = [k for gap, k in tone_gap if gap > args.tolerance + 1e-12]
    exact_tone = sum(1 for gap, _ in tone_gap if gap == 0.0)

    print(f"keys only in {a}: {len(only_mr)}; only in {b}: {len(only_sp)}; in both: {len(both)}")
    print(f"events mismatches: {len(count_diff)}; sum_mentions mismatches: {len(mention_diff)}")
    print(f"avg_tone: {exact_tone} of {len(both)} printed identically; max |diff| = {max_gap:.3g}; "
          f"over tolerance {args.tolerance:g}: {len(tone_diff)}")
    for title, keys in [(f"only in {a}", only_mr), (f"only in {b}", only_sp),
                        ("events differ", count_diff), ("sum_mentions differ", mention_diff),
                        ("avg_tone differs", tone_diff)]:
        for k in keys[:args.show]:
            print(f"  {title}: {k} {a}={mr.get(k)} {b}={sp.get(k)}")

    same = not (only_mr or only_sp or count_diff or mention_diff or tone_diff
                or mr_dupes or sp_dupes or mr_bad or sp_bad)
    print("IDENTICAL" if same else "DIFFERENT")
    sys.exit(0 if same else 1)


if __name__ == "__main__":
    main()
