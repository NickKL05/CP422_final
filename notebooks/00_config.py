# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Shared configuration
# MAGIC Every other notebook starts with two cells: one that declares the `sample_mode` widget, then `%run ./00_config`
# MAGIC (copy the first two code cells of `01_load_gdelt`). This notebook holds the names, date ranges and the `SAMPLE_MODE` flag,
# MAGIC so that Run All on fresh compute only touches the small sample.
# MAGIC
# MAGIC | Mode | Date range (file dates) | Used for |
# MAGIC | --- | --- | --- |
# MAGIC | `SAMPLE_MODE = true` (default) | 2026-10-01 to 2026-10-31 | Development, the end-to-end Run All check, and the same month as the local Hadoop sample |
# MAGIC | `SAMPLE_MODE = false` | `FULL_START` to `FULL_END` | Full-data runs (finish by Nov 27, spread across accounts) |

# COMMAND ----------

# AI citation: Claude Code (Claude Opus 5.5, Anthropic) drafted this config cell (widget, names, date ranges).
# Reviewed and tested by Nick Kunde-Lenny.
import os, sys

# Databricks Git folders put the notebook's folder on sys.path, so gdelt_common.py imports directly.
# The fallback covers the case where the notebooks were imported without the repo.
try:
    _nb_dir = os.path.dirname(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get())
    _ws_dir = "/Workspace" + _nb_dir
    if os.path.isdir(_ws_dir) and _ws_dir not in sys.path:
        sys.path.insert(0, _ws_dir)
except Exception:
    pass

# The calling notebook declares the "sample_mode" widget in a cell before its %run, because a widget
# created here doesn't show in the caller's parameter bar. If the caller forgot, default to sample mode.
try:
    SAMPLE_MODE = dbutils.widgets.get("sample_mode") == "true"
except Exception:
    SAMPLE_MODE = True
    print("note: no 'sample_mode' widget in the calling notebook; defaulting to SAMPLE_MODE=True")

# Unity Catalog locations (Free Edition gives every account the "workspace" catalog).
CATALOG = "workspace"
SCHEMA = "gdelt"
VOLUME = "raw"
VOLUME_ROOT = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

# Tables
EVENTS_BRONZE = f"{CATALOG}.{SCHEMA}.events_bronze"   # raw GDELT rows as strings, partitioned by file_year/file_month (Nick Kunde-Lenny)
EVENTS_CLEAN = f"{CATALOG}.{SCHEMA}.events_clean"     # typed and cleaned events from the cleaning step (Member 2)
LOAD_LOG = f"{CATALOG}.{SCHEMA}.load_log"             # one row per loaded month: files, rows, seconds (Nick Kunde-Lenny)

# Date ranges (GDELT file dates = DATEADDED)
SAMPLE_START, SAMPLE_END = "2026-10-01", "2026-10-31"
FULL_START, FULL_END = "2025-11-01", "2026-10-31"     # one year first, then move FULL_START back as quota allows

RANGE_START, RANGE_END = (SAMPLE_START, SAMPLE_END) if SAMPLE_MODE else (FULL_START, FULL_END)

# Row count of the same month on the local Hadoop cluster (hadoop/output/ingest_summary_*.txt), used as a cross-check.
HADOOP_SAMPLE_ROWS = 3_195_940

print(f"SAMPLE_MODE={SAMPLE_MODE}  range={RANGE_START}..{RANGE_END}  bronze={EVENTS_BRONZE}")
