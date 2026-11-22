#!/usr/bin/env python3
"""Hadoop Streaming mapper: download GDELT daily ZIPs and unpack them into HDFS.

Input  (stdin): lines from make_url_list.py ("<url> <md5> <bytes>"). NLineInputFormat gives each
               map task a few lines, so downloads are spread across all NodeManagers in parallel.
Output (stdout): one manifest row per file, tab-separated:
    file  status  rows  zip_bytes  csv_bytes  seconds  worker_host  url  error

HDFS layout:   /gdelt/raw/events/year=YYYY/month=MM/YYYYMMDD.export.CSV
               (partitioned by the file's publication date, i.e. DATEADDED, not by SQLDATE)

The job is idempotent: files that already exist in HDFS are skipped, so a failed or partial run can
simply be re-run. Downloads are retried and MD5-checked. The `hdfs dfs -put` runs on the worker
that ran the map task, so HDFS puts the first replica of every block on that worker's own DataNode.

AI citation: Claude Code (Claude Opus 5.5, Anthropic) drafted this mapper (streaming protocol,
retry/MD5 logic, counters, HDFS put). Reviewed and tested by Nick Kunde-Lenny.
"""
import hashlib
import os
import re
import socket
import subprocess
import sys
import time
import urllib.request
import zipfile

RAW_ROOT = os.environ.get("HDFS_RAW_ROOT", "/gdelt/raw/events")
HDFS_BIN = os.path.join(os.environ.get("HADOOP_HOME", "/opt/hadoop"), "bin", "hdfs")
HOST = socket.gethostname()
NAME_RE = re.compile(r"(\d{4})(\d{2})(\d{2})\.export\.CSV\.zip$")
MD5_RE = re.compile(r"^[0-9a-f]{32}$")
ATTEMPTS = 3


# --- Hadoop Streaming reporter protocol (stderr lines that YARN turns into counters/status) ---
def counter(name, amount=1):
    sys.stderr.write(f"reporter:counter:GDELT Ingest,{name},{amount}\n")
    sys.stderr.flush()


def status(msg):
    sys.stderr.write(f"reporter:status:{msg}\n")
    sys.stderr.flush()


def hdfs(*args):
    return subprocess.run([HDFS_BIN, "dfs", *args], capture_output=True, text=True)


def parse_line(line):
    """Find the URL and optional MD5. Robust to a leading byte-offset key added by NLineInputFormat."""
    toks = line.split()
    for i, tok in enumerate(toks):
        if tok.startswith("http"):
            md5 = toks[i + 1] if i + 1 < len(toks) and MD5_RE.match(toks[i + 1]) else None
            return tok, md5
    return None, None


def download(url, dest, md5):
    """Stream the URL to disk with retries. Returns bytes written. Raises after ATTEMPTS failures."""
    for attempt in range(1, ATTEMPTS + 1):
        try:
            digest, size = hashlib.md5(), 0
            with urllib.request.urlopen(url, timeout=120) as resp, open(dest, "wb") as out:
                while True:
                    chunk = resp.read(1 << 20)
                    if not chunk:
                        break
                    out.write(chunk)
                    digest.update(chunk)
                    size += len(chunk)
            if md5 and digest.hexdigest() != md5:
                raise IOError(f"md5 mismatch: got {digest.hexdigest()}, expected {md5}")
            return size
        except Exception as exc:  # network error or bad checksum: back off and retry
            counter("DownloadRetries")
            status(f"retry {attempt}/{ATTEMPTS} for {url}: {exc}")
            if attempt == ATTEMPTS:
                raise
            time.sleep(5 * attempt)


def ingest(url, md5):
    m = NAME_RE.search(url)
    if not m:
        raise ValueError("not a GDELT daily export URL")
    year, month, day = m.groups()
    csv_name = f"{year}{month}{day}.export.CSV"
    target_dir = f"{RAW_ROOT}/year={year}/month={month}"
    target = f"{target_dir}/{csv_name}"

    if hdfs("-test", "-e", target).returncode == 0:
        counter("FilesSkippedExisting")
        return [csv_name, "SKIPPED", 0, 0, 0, "0.0"]

    t0 = time.time()
    zip_path = csv_name + ".zip"
    status(f"downloading {url}")
    zip_bytes = download(url, zip_path, md5)

    # Unzip to the task's local working dir, counting rows as we go.
    status(f"unzipping {csv_name}")
    rows = csv_bytes = 0
    with zipfile.ZipFile(zip_path) as zf, zf.open(zf.namelist()[0]) as src, open(csv_name, "wb") as dst:
        for line in src:
            dst.write(line)
            rows += 1
            csv_bytes += len(line)

    status(f"writing {target}")
    hdfs("-mkdir", "-p", target_dir)
    put = hdfs("-put", "-f", csv_name, target)
    if put.returncode != 0:
        raise IOError(f"hdfs put failed: {put.stderr.strip()[-300:]}")
    os.remove(zip_path)
    os.remove(csv_name)

    counter("FilesIngested")
    counter("RowsIngested", rows)
    counter("ZipBytesDownloaded", zip_bytes)
    counter("CsvBytesWritten", csv_bytes)
    return [csv_name, "OK", rows, zip_bytes, csv_bytes, f"{time.time() - t0:.1f}"]


def main():
    for line in sys.stdin:
        url, md5 = parse_line(line)
        if not url:
            continue
        try:
            fields = ingest(url, md5) + [HOST, url, ""]
        except Exception as exc:
            # Record the failure and keep going: one bad file should not kill the other downloads.
            # A re-run picks up only what's missing.
            counter("FilesFailed")
            fields = [url.rsplit("/", 1)[-1], "FAILED", 0, 0, 0, "0.0", HOST, url,
                      str(exc).replace("\t", " ")[:300]]
        print("\t".join(str(f) for f in fields), flush=True)


if __name__ == "__main__":
    main()
