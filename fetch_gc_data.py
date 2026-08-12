"""
XAU (via COMEX GC futures) tick data pipeline - Databento
============================================================

Pulls ~3 years 8 months of GC continuous front-month (volume roll)
TRADES + OHLCV-1m data from Databento, and converts it into year-partitioned
Parquet files:

    XAU_GC_DATA/
      2022/GC_TRADES_2022.parquet, GC_OHLCV1M_2022.parquet
      2023/GC_TRADES_2023.parquet, GC_OHLCV1M_2023.parquet
      2024/GC_TRADES_2024.parquet, GC_OHLCV1M_2024.parquet
      2025/GC_TRADES_2025.parquet, GC_OHLCV1M_2025.parquet
      2026/GC_TRADES_2026.parquet, GC_OHLCV1M_2026.parquet

USAGE
-----
1. Set your API key:  $env:DATABENTO_API_KEY = "db-xxxxxxxx"   (PowerShell)
2. Install deps:       pip install databento pandas pyarrow

3. STAGE 1 (free, no data pulled) - see exact size & cost first:
       python fetch_gc_data.py --check

4. STAGE 2 (submits paid batch jobs) - only after you're happy with Stage 1:
       python fetch_gc_data.py --download

5. STAGE 3 (once batch jobs are marked 'done' on your Databento dashboard,
   or after --download finishes waiting):
       python fetch_gc_data.py --convert
"""

import argparse
import os
from pathlib import Path

import pandas as pd
import databento as db

# ---------------------------------------------------------------------------
# CONFIG - adjust here if needed
# ---------------------------------------------------------------------------
DATASET = "GLBX.MDP3"
SYMBOL = "GC.v.0"          # continuous front-month, volume-based roll
STYPE_IN = "continuous"
SCHEMAS = ["trades", "ohlcv-1m"]
START_DATE = "2022-12-01"
END_DATE = "2026-07-31"
PROJECT_ROOT = Path(__file__).resolve().parent   # folder this script lives in
OUTPUT_ROOT = PROJECT_ROOT / "XAU_GC_DATA"
RAW_DBN_DIR = PROJECT_ROOT / "raw_dbn"  # where batch job output (.dbn.zst) lands


def get_client() -> db.Historical:
    api_key = os.environ.get("DATABENTO_API_KEY")
    if not api_key:
        raise SystemExit(
            "DATABENTO_API_KEY not set. Run: export DATABENTO_API_KEY='db-xxxx'"
        )
    return db.Historical(api_key)


# ---------------------------------------------------------------------------
# STAGE 1: cost / size check (no data pulled, no cost incurred)
# ---------------------------------------------------------------------------
def check_cost():
    client = get_client()
    print(f"Checking cost & size for {SYMBOL} ({STYPE_IN}), "
          f"{START_DATE} -> {END_DATE}\n")

    total_cost = 0.0
    for schema in SCHEMAS:
        size_bytes = client.metadata.get_billable_size(
            dataset=DATASET,
            symbols=[SYMBOL],
            stype_in=STYPE_IN,
            schema=schema,
            start=START_DATE,
            end=END_DATE,
        )
        cost = client.metadata.get_cost(
            dataset=DATASET,
            symbols=[SYMBOL],
            stype_in=STYPE_IN,
            schema=schema,
            start=START_DATE,
            end=END_DATE,
        )
        total_cost += cost
        print(f"  {schema:10s}  size: {size_bytes / 1e9:8.2f} GB   "
              f"cost: ${cost:8.2f}")

    print(f"\n  TOTAL estimated cost: ${total_cost:.2f}")
    print("\nNothing has been downloaded. Review the numbers above, then run "
          "with --download when ready.")


# ---------------------------------------------------------------------------
# STAGE 2: submit batch jobs (this is the step that incurs cost)
# ---------------------------------------------------------------------------
def submit_download():
    client = get_client()
    RAW_DBN_DIR.mkdir(parents=True, exist_ok=True)

    confirm = input(
        f"This will submit paid batch jobs for {SYMBOL} "
        f"({', '.join(SCHEMAS)}) from {START_DATE} to {END_DATE}.\n"
        f"Type 'yes' to proceed: "
    )
    if confirm.strip().lower() != "yes":
        print("Aborted, nothing submitted.")
        return

    job_ids = []
    for schema in SCHEMAS:
        job = client.batch.submit_job(
            dataset=DATASET,
            symbols=[SYMBOL],
            stype_in=STYPE_IN,
            schema=schema,
            start=START_DATE,
            end=END_DATE,
            encoding="dbn",
            compression="zstd",
            split_duration="year",   # ask Databento to pre-split output by year
        )
        job_ids.append(job["id"])
        print(f"Submitted {schema} job: {job['id']}")

    # Persist job IDs so --convert only ever touches OUR jobs, never other
    # unrelated jobs that may exist on the account.
    tracker = PROJECT_ROOT / "submitted_jobs.txt"
    with open(tracker, "a") as f:
        for jid in job_ids:
            f.write(jid + "\n")

    print("\nJobs submitted. Track progress at "
          "https://databento.com/platform/jobs or via "
          "client.batch.list_jobs(). This can take from minutes to a "
          "couple hours depending on queue and data volume.")
    print("Job IDs:", job_ids)
    print(f"(saved to {tracker})")
    print("\nOnce jobs show state='done', run: python fetch_gc_data.py --convert")


# ---------------------------------------------------------------------------
# STAGE 3: download finished batch files + convert to year-partitioned Parquet
# ---------------------------------------------------------------------------
def convert_to_parquet():
    client = get_client()
    RAW_DBN_DIR.mkdir(parents=True, exist_ok=True)

    tracker = PROJECT_ROOT / "submitted_jobs.txt"
    if not tracker.exists():
        print(f"No {tracker.name} found - can't tell which jobs are ours. "
              "Re-run --download first, or manually create this file with "
              "one job ID per line.")
        return

    with open(tracker) as f:
        our_job_ids = {line.strip() for line in f if line.strip()}

    all_jobs = client.batch.list_jobs()
    our_jobs = [j for j in all_jobs if j["id"] in our_job_ids]
    done_jobs = [j for j in our_jobs if j["state"] == "done"]
    pending_jobs = [j for j in our_jobs if j["state"] != "done"]

    if pending_jobs:
        print("Still waiting on these jobs (not yet 'done'):")
        for j in pending_jobs:
            print(f"  {j['id']}  state={j['state']}")

    if not done_jobs:
        print("\nNone of our jobs are done yet. Check back later.")
        return

    print(f"\n{len(done_jobs)} of our job(s) ready. Downloading only these:")
    for job in done_jobs:
        job_id = job["id"]
        job_dir = RAW_DBN_DIR / job_id
        job_dir.mkdir(parents=True, exist_ok=True)
        print(f"Downloading job {job_id} ...")
        client.batch.download(output_dir=str(job_dir), job_id=job_id)

    # Convert only files from OUR job folders to Parquet, partitioned by year
    for job in done_jobs:
        job_dir = RAW_DBN_DIR / job["id"]
        for dbn_path in job_dir.rglob("*.dbn*"):
            store = db.DBNStore.from_file(str(dbn_path))
            df = store.to_df()
            if df.empty:
                continue

            schema_name = store.schema.name if hasattr(store.schema, "name") else str(store.schema)
            schema_lower = schema_name.lower()
            if "trade" in schema_lower:
                label = "TRADES"
            elif "ohlcv" in schema_lower:
                label = "OHLCV1M"
            else:
                label = schema_lower.upper().replace("-", "")

            df["year"] = df.index.year if df.index.name == "ts_event" else df["ts_event"].dt.year
            for year, year_df in df.groupby("year"):
                out_dir = OUTPUT_ROOT / str(year)
                out_dir.mkdir(parents=True, exist_ok=True)
                out_file = out_dir / f"GC_{label}_{year}.parquet"

                year_df = year_df.drop(columns=["year"])
                if out_file.exists():
                    # append-safe: read, concat, dedupe, rewrite (fine at this scale)
                    existing = pd.read_parquet(out_file)
                    year_df = pd.concat([existing, year_df]).drop_duplicates()

                year_df.to_parquet(out_file, engine="pyarrow", compression="zstd")
                print(f"  wrote {out_file}  ({len(year_df):,} rows)")

    print("\nDone. Output structure under:", OUTPUT_ROOT.resolve())


# ---------------------------------------------------------------------------
# STAGE 3b: convert already-downloaded local .dbn.zst files (no API calls)
# ---------------------------------------------------------------------------
def convert_local(source_dir: str):
    src = Path(source_dir)
    if not src.exists():
        print(f"Folder not found: {src}")
        return

    dbn_files = list(src.rglob("*.dbn*"))
    if not dbn_files:
        print(f"No .dbn/.dbn.zst files found under {src}")
        return

    print(f"Found {len(dbn_files)} DBN file(s) under {src}. Converting...")
    for dbn_path in dbn_files:
        store = db.DBNStore.from_file(str(dbn_path))
        df = store.to_df()
        if df.empty:
            continue

        schema_name = store.schema.name if hasattr(store.schema, "name") else str(store.schema)
        schema_lower = schema_name.lower()
        if "trade" in schema_lower:
            label = "TRADES"
        elif "ohlcv" in schema_lower:
            label = "OHLCV1M"
        else:
            label = schema_lower.upper().replace("-", "")

        df["year"] = df.index.year if df.index.name in ("ts_event", "ts_recv") else df["ts_event"].dt.year
        for year, year_df in df.groupby("year"):
            out_dir = OUTPUT_ROOT / str(year)
            out_dir.mkdir(parents=True, exist_ok=True)
            out_file = out_dir / f"GC_{label}_{year}.parquet"

            year_df = year_df.drop(columns=["year"])
            if out_file.exists():
                existing = pd.read_parquet(out_file)
                year_df = pd.concat([existing, year_df]).drop_duplicates()

            year_df.to_parquet(out_file, engine="pyarrow", compression="zstd")
            print(f"  wrote {out_file}  ({len(year_df):,} rows)")

    print("\nDone. Output structure under:", OUTPUT_ROOT.resolve())


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true", help="Stage 1: cost/size check only")
    parser.add_argument("--download", action="store_true", help="Stage 2: submit batch jobs")
    parser.add_argument("--convert", action="store_true", help="Stage 3: download (via API) + convert to Parquet")
    parser.add_argument("--convert-local", metavar="FOLDER",
                         help="Stage 3b: convert already-downloaded .dbn.zst files in FOLDER (no API calls)")
    args = parser.parse_args()

    if args.check:
        check_cost()
    elif args.download:
        submit_download()
    elif args.convert:
        convert_to_parquet()
    elif args.convert_local:
        convert_local(args.convert_local)
    else:
        parser.print_help()