#!/usr/bin/env python3
"""Hadoop Streaming combiner: partial sums per (SQLDATE, Actor1CountryCode) inside one map task.

Input and output use the same format, which is also the mapper's output format:
    <SQLDATE>|<Actor1CountryCode>  <count>  <sum_mentions>  <sum_tone>

Why a combiner: each daily file has about 100,000 events but only a few hundred distinct
(day, country) keys, because most events in a file share a handful of recent SQLDATEs and the
same big countries. Without a combiner every event becomes a record that is spilled, sorted,
copied over the network and merged by the reducers. With it, each map task sends at most one
record per key it saw, so the shuffle shrinks by roughly the events-per-key ratio (compare the
"Combine input records" and "Combine output records" counters in the job log).

Why it is correct: Hadoop may run the combiner zero, one or several times, on any subset of a
key's records. So it only emits partial (count, sum, sum) triples, never an average. Addition
is associative and commutative, so any grouping gives the same totals. (An average of averages
would be wrong.) The reducer divides at the very end.

sum_tone is written with repr(), which round-trips a Python float exactly, so passing a partial
sum through the combiner loses no precision.
"""
import sys


def flush(key, n, mentions, tone):
    sys.stdout.write(f"{key}\t{n}\t{mentions}\t{tone!r}\n")


def main():
    key, n, mentions, tone = None, 0, 0, 0.0
    for line in sys.stdin:
        k, c, m, t = line.rstrip("\n").split("\t")
        if k != key:  # input is sorted by key, so a new key means the previous one is complete
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
