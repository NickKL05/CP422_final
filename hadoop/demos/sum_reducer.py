#!/usr/bin/env python3
"""Streaming reducer: add up the per-split row counts produced by the `wc -l` mappers.

AI citation: Claude Code (Claude Opus 5.5, Anthropic) drafted this reducer. Reviewed and tested by Member 1.
"""
import sys

total = splits = 0
for line in sys.stdin:
    parts = line.split()
    if parts:
        total += int(parts[0])
        splits += 1
print(f"total_rows\t{total}")
print(f"input_splits\t{splits}")
