"""Run the pipeline:  python etl/pipeline.py

Optional flags:
  --input PATH           use another CSV instead of raw_data/diaries_transactions_all.csv
  --allow-corrupt-ids    continue even if IDs are in scientific notation (testing only)
  --example-row ID       print the raw row and its transformed row for that source_row_id
  --no-write             run extract and transform but write no files

Each run appends one JSON line to logs/run_log.jsonl: start time, end time,
rows read, rows rejected, status and any error. Exit code is 0 on success, 1 on failure.
"""
import argparse
import json
import logging
import sys
import traceback
from datetime import datetime, timezone

import pandas as pd

import config
from extract import ExtractError, extract
from transform import ReconciliationError, TransformResult, transform

log = logging.getLogger("pipeline")

OUTPUT_TABLES = ["fact_transaction", "fact_account_balance", "dim_date",
                 "dim_household", "dim_account", "dim_trx_type", "rejects"]


def write_outputs(result: TransformResult, out_dir=config.STAGING_DIR) -> dict:
    """Write each table to staging as Parquet (CSV if pyarrow is missing).

    Files are overwritten, never appended, so a re-run cannot duplicate rows.
    Returns {table: rows_written}.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    written = {}
    for name in OUTPUT_TABLES:
        df = getattr(result, name)
        try:
            df.to_parquet(out_dir / f"{name}.parquet", index=False)
        except ImportError:
            log.warning("pyarrow not installed; writing %s as CSV", name)
            df.to_csv(out_dir / f"{name}.csv", index=False)
        written[name] = len(df)
    return written


def show_example(raw: pd.DataFrame, result: TransformResult, row_id: int) -> None:
    """Print one concrete before/after row."""
    before_cols = ["source_row_id", "hh_ids", "account_ids", "trx_date",
                   "transaction_date", "trx_class_code", "trx_family_code",
                   "trx_family_desc", "trx_type_desc", "trx_prx_purpose_fd",
                   "trx_mode_desc", "trx_direction", "trx_bsheet_direction",
                   "trx_value_kes", "account_formal"]
    before = raw[raw["source_row_id"] == row_id]
    if before.empty:
        print(f"source_row_id {row_id} not found")
        return
    print("\nBEFORE (raw text)")
    print(before[before_cols].iloc[0].to_string())
    after = result.fact_transaction[result.fact_transaction["source_row_id"] == row_id]
    if after.empty:
        bal = result.fact_account_balance[result.fact_account_balance["source_row_id"] == row_id]
        rej = result.rejects[result.rejects["source_row_id"] == row_id]
        print("\nAFTER: not in fact_transaction;",
              "routed to fact_account_balance" if len(bal) else
              f"rejected ({rej['reject_reason'].iloc[0]})" if len(rej) else "missing!")
        return
    print("\nAFTER (fact_transaction)")
    print(after.iloc[0].to_string())


def run(input_path=config.RAW_PATH, allow_corrupt_ids=False, write=True,
        example_row=None) -> dict:
    start = datetime.now(timezone.utc)
    summary = {"run_id": start.strftime("%Y%m%dT%H%M%SZ"),
               "start_time": start.isoformat(), "status": "running",
               "input": str(input_path)}
    try:
        raw, report = extract(input_path, allow_corrupt_ids=allow_corrupt_ids)
        summary.update(report)
        result = transform(raw)
        summary.update(result.stats)
        if write:
            summary["tables_written"] = write_outputs(result)
        if example_row is not None:
            show_example(raw, result, example_row)
        summary["status"] = "success"
    except (ExtractError, ReconciliationError) as exc:
        summary.update(status="failed", error=f"{type(exc).__name__}: {exc}")
        log.error("%s", summary["error"])
    except Exception as exc:  # unexpected: keep the traceback in the log
        summary.update(status="failed", error=f"{type(exc).__name__}: {exc}")
        log.error("Unexpected failure\n%s", traceback.format_exc())
    finally:
        end = datetime.now(timezone.utc)
        summary["end_time"] = end.isoformat()
        summary["duration_seconds"] = round((end - start).total_seconds(), 2)
        if write:
            config.LOG_DIR.mkdir(parents=True, exist_ok=True)
            with open(config.RUN_LOG_PATH, "a", encoding="utf-8") as fh:
                fh.write(json.dumps(summary, default=str) + "\n")
    return summary


def _setup_logging(write: bool) -> None:
    handlers = [logging.StreamHandler(sys.stdout)]
    if write:
        config.LOG_DIR.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(config.PIPELINE_LOG_PATH, encoding="utf-8"))
    logging.basicConfig(level=logging.INFO, handlers=handlers,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--input", default=str(config.RAW_PATH))
    ap.add_argument("--allow-corrupt-ids", action="store_true")
    ap.add_argument("--example-row", type=int)
    ap.add_argument("--no-write", action="store_true")
    args = ap.parse_args(argv)
    _setup_logging(not args.no_write)
    s = run(args.input, args.allow_corrupt_ids, not args.no_write, args.example_row)
    keys = ["status", "rows_read", "fact_rows", "balance_rows", "rows_rejected",
            "duplicates_removed", "duration_seconds"]
    print("\nRUN SUMMARY", {k: s.get(k) for k in keys})
    return 0 if s["status"] == "success" else 1


if __name__ == "__main__":
    sys.exit(main())
