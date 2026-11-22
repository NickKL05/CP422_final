#!/usr/bin/env python3
"""Turn `hdfs fsck -files -blocks -locations` output into a readable block-placement summary.

Usage: python3 summarize_fsck.py fsck_blocks.txt dfsadmin_report.txt

Prints: blocks per file, replica count per DataNode (by hostname), the replication-factor
histogram, and a few example blocks with the nodes that hold them.

AI citation: Claude Code (Claude Opus 5.5, Anthropic) drafted this parser. Reviewed and tested by Member 1.
"""
import collections
import re
import sys

LOC_RE = re.compile(r"DatanodeInfoWithStorage\[([\d.]+):\d+")
BLOCK_RE = re.compile(r"^\d+\. \S+ len=(\d+)")
FILE_RE = re.compile(r"^(/\S+) (\d+) bytes")


def ip_to_host(report_path):
    """dfsadmin -report lists 'Name: <ip>:<port> (...)' then 'Hostname: <host>'. Map ip -> host."""
    mapping, ip = {}, None
    with open(report_path) as f:
        for line in f:
            if line.startswith("Name:"):
                ip = line.split()[1].split(":")[0]
            elif line.startswith("Hostname:") and ip:
                mapping[ip] = line.split()[1].split(".")[0]
                ip = None
    return mapping


def main(fsck_path, report_path):
    hosts = ip_to_host(report_path)
    per_node = collections.Counter()
    repl_hist = collections.Counter()
    files = blocks = 0
    examples = []
    current_file = None

    with open(fsck_path) as f:
        for line in f:
            line = line.strip()
            fm = FILE_RE.match(line)
            if fm:
                files += 1
                current_file = fm.group(1)
                continue
            bm = BLOCK_RE.match(line)
            if bm:
                blocks += 1
                nodes = [hosts.get(ip, ip) for ip in LOC_RE.findall(line)]
                per_node.update(nodes)
                repl_hist[len(nodes)] += 1
                if len(examples) < 6:
                    examples.append((current_file, int(bm.group(1)), nodes))

    print(f"files: {files}   blocks: {blocks}   blocks per file: {blocks / max(files, 1):.2f}")
    print("\nreplicas stored per DataNode:")
    total = sum(per_node.values())
    for node, n in sorted(per_node.items()):
        print(f"  {node:<10} {n:5d} replicas  ({100 * n / max(total, 1):5.1f} %)")
    print("\nreplication factor histogram (replicas -> blocks):")
    for r, n in sorted(repl_hist.items()):
        print(f"  {r} -> {n}")
    print("\nexample block placements:")
    for path, length, nodes in examples:
        print(f"  {path.rsplit('/', 1)[-1]:<22} {length / 2**20:6.1f} MB  on {', '.join(nodes)}")


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
