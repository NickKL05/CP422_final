#!/usr/bin/env python3
"""Hadoop Streaming mapper: events per country per day, from raw GDELT 1.0 daily files.

Input  (stdin): raw GDELT 1.0 daily event lines (58 tab-separated fields, no header). With the
               default TextInputFormat, Streaming passes each line without its byte-offset key.
Output (stdout): one line per valid event, tab-separated:
    <SQLDATE>|<Actor1CountryCode>  1  <NumMentions>  <AvgTone as published>

The value is already in the "partial sum" form (count, sum_mentions, sum_tone) that
summary_combiner.py and summary_reducer.py read, so the combiner can run zero, one or many times
without changing the result. The key is a single field (everything before the first tab), so the
default Streaming partitioner and sort work on it unchanged. SQLDATE is always exactly 8 digits
here, so the reducer can split the key back with key[:8] and key[9:].

Skip rules (the definition in README.md; summary_spark.py applies exactly the same ones, in the
same order, and each skipped line is counted under the FIRST rule it fails):
    SkippedFieldCount          the line does not have exactly 58 tab-separated fields
    SkippedEmptyActor1Country  Actor1CountryCode (field 8) is the empty string
    SkippedBadSqldate          SQLDATE (field 2) is not exactly 8 ASCII digits
    SkippedBadNumMentions      NumMentions (field 32) is not 1-9 ASCII digits
    SkippedBadAvgTone          AvgTone (field 35) is not a decimal number, optionally in e-notation

The number rules use regular expressions instead of int()/float(), because Python's float()
also accepts "nan", "inf", "1_000" and surrounding spaces, and Spark's cast accepts a different
set again. A regex gives both engines the same definition of "parses as a number".

Counters are added up in memory and reported once at the end (one stderr line per counter),
because a reporter line per input line would slow the mapper down for no benefit.
"""
import re
import sys

N_FIELDS = 58
# 0-based positions of the fields we use (the codebook numbers them from 1).
SQLDATE, ACTOR1_COUNTRY, NUM_MENTIONS, AVG_TONE = 1, 7, 31, 34

SQLDATE_RE = re.compile(r"[0-9]{8}")
MENTIONS_RE = re.compile(r"[0-9]{1,9}")
# Decimal number, optionally in e-notation: GDELT writes tones very close to 0 that way
# (for example 1.44699562933207e-05 in the 2026-10-01 file). The digit limits keep every value
# finite, so no engine ever has to add up or print an infinity.
TONE_RE = re.compile(r"[+-]?(?:[0-9]{1,15}(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]{1,2})?")

COUNTERS = ["InputLines", "SkippedFieldCount", "SkippedEmptyActor1Country", "SkippedBadSqldate",
            "SkippedBadNumMentions", "SkippedBadAvgTone", "ValidEvents"]


def skip_reason(f):
    """Name of the first skip rule this list of fields fails, or None if the event is valid."""
    if len(f) != N_FIELDS:
        return "SkippedFieldCount"
    if f[ACTOR1_COUNTRY] == "":
        return "SkippedEmptyActor1Country"
    if not SQLDATE_RE.fullmatch(f[SQLDATE]):
        return "SkippedBadSqldate"
    if not MENTIONS_RE.fullmatch(f[NUM_MENTIONS]):
        return "SkippedBadNumMentions"
    if not TONE_RE.fullmatch(f[AVG_TONE]):
        return "SkippedBadAvgTone"
    return None


def main():
    counts = dict.fromkeys(COUNTERS, 0)
    out = sys.stdout
    # Read bytes and decode ourselves: text-mode stdin would also split lines on a stray "\r".
    for raw in sys.stdin.buffer:
        counts["InputLines"] += 1
        line = raw.decode("utf-8", "replace")
        if line.endswith("\n"):
            line = line[:-1]
        if line.endswith("\r"):  # only matters for the local "cat | mapper" test on CRLF files
            line = line[:-1]
        f = line.split("\t")
        reason = skip_reason(f)
        if reason:
            counts[reason] += 1
            continue
        counts["ValidEvents"] += 1
        out.write(f"{f[SQLDATE]}|{f[ACTOR1_COUNTRY]}\t1\t{f[NUM_MENTIONS]}\t{f[AVG_TONE]}\n")

    for name in COUNTERS:
        sys.stderr.write(f"reporter:counter:GDELT Summary,{name},{counts[name]}\n")
    sys.stderr.flush()


if __name__ == "__main__":
    main()
