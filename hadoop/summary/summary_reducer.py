#!/usr/bin/env python3
"""Hadoop Streaming reducer: final events per country per day.

Input  (sorted by key): <SQLDATE>|<Actor1CountryCode>  <count>  <sum_mentions>  <sum_tone>
                        (straight from the mapper, or already partly summed by the combiner)
Output (one line per key, tab-separated, the same columns summary_spark.py writes):
    SQLDATE  Actor1CountryCode  events  sum_mentions  avg_tone

avg_tone is sum_tone / events, printed with 6 decimal places. Only here is the average taken,
after all partial sums for the key have been added up (see summary_combiner.py).
"""
import sys


def flush(key, n, mentions, tone):
    # SQLDATE is exactly 8 digits (the mapper checks this), so the "|" is always at position 8.
    sys.stdout.write(f"{key[:8]}\t{key[9:]}\t{n}\t{mentions}\t{tone / n:.6f}\n")


def main():
    key, n, mentions, tone = None, 0, 0, 0.0
    for line in sys.stdin:
        k, c, m, t = line.rstrip("\n").split("\t")
        if k != key:
            if key is not None:
                flush(key, n, mentions, tone)
            key, n, mentions, tone = k, 0, 0, 0.0
        n += int(c)
        mentions += int(m)
        tone += float(t)
    if key is not None:
        flush(key, n, mentions, tone)


if __name__ == "__main__":
    main()
