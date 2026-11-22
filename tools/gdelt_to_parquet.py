#!/usr/bin/env python3
"""Fallback loader: download GDELT daily files on a laptop and convert them to Parquet for upload.

Use this only if Databricks Free Edition compute cannot download from data.gdeltproject.org.
The Parquet files have the same 58 string columns as events_bronze plus `source_file`, laid out as
    <out>/events/year=YYYY/month=MM/YYYYMMDD.parquet
Upload <out>/events to /Volumes/workspace/gdelt/raw/landing_parquet/events (Catalog Explorer > Upload,
or `databricks fs cp -r <out>/events dbfs:/Volumes/workspace/gdelt/raw/landing_parquet/events`),
then run 01_load_gdelt with the "source" widget set to parquet_upload.

Setup:  pip install pyarrow
Usage:  python tools/gdelt_to_parquet.py --start 2026-10-01 --end 2026-10-31 --out gdelt_parquet

AI citation: Claude Code (Claude Opus 5.5, Anthropic) drafted this tool. Reviewed and tested by Nick Kunde-Lenny.
"""
import argparse
import io
import os
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "notebooks"))
import gdelt_common as gc  # noqa: E402


def convert(file_info, out_root, tmp_dir):
    import pyarrow as pa
    import pyarrow.csv as pacsv
    import pyarrow.parquet as pq

    day = file_info["date"]
    dest_dir = os.path.join(out_root, "events", f"year={day.year}", f"month={day.month:02d}")
    dest = os.path.join(dest_dir, f"{day:%Y%m%d}.parquet")
    if os.path.exists(dest):
        return file_info["name"], "skipped", pq.ParquetFile(dest).metadata.num_rows

    info = gc.download_and_extract(file_info, tmp_dir, tmp_dir=tmp_dir)
    with open(info["csv_path"], "rb") as f:
        # GDELT is UTF-8, but a few source strings are not; replace bad bytes instead of failing the file.
        data = f.read().decode("utf-8", "replace").encode("utf-8")
    table = pacsv.read_csv(
        io.BytesIO(data),
        read_options=pacsv.ReadOptions(column_names=gc.GDELT_V1_COLUMNS, block_size=64 << 20),
        parse_options=pacsv.ParseOptions(delimiter="\t", quote_char=False, newlines_in_values=False),
        convert_options=pacsv.ConvertOptions(column_types={c: pa.string() for c in gc.GDELT_V1_COLUMNS},
                                             strings_can_be_null=False),
    )
    table = table.append_column("source_file", pa.array([info["name"]] * table.num_rows, pa.string()))
    os.makedirs(dest_dir, exist_ok=True)
    pq.write_table(table, dest + ".part", compression="zstd")
    os.replace(dest + ".part", dest)
    os.remove(info["csv_path"])
    return file_info["name"], "ok", table.num_rows


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--threads", type=int, default=6)
    args = ap.parse_args()

    files = gc.list_daily_files(args.start, args.end)
    print(f"{len(files)} files for {args.start}..{args.end}")
    total = 0
    with tempfile.TemporaryDirectory() as tmp, ThreadPoolExecutor(args.threads) as pool:
        for name, status, rows in pool.map(lambda f: convert(f, args.out, tmp), files):
            total += rows
            print(f"  {name:<28} {status:<8} {rows:>9,} rows")
    print(f"total rows: {total:,}  ->  {os.path.join(args.out, 'events')}")


if __name__ == "__main__":
    main()
