import io
import logging
import os

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from . import config
from .transform import TransformResult

log = logging.getLogger(__name__)

KEY = {"dim_date": "date_key", "dim_household": "household_key",
       "dim_account": "account_key", "dim_trx_type": "trx_type_key",
       "fact_transaction": "transaction_key", "fact_account_balance": "balance_key"}

UPSERT = ["dim_household", "dim_account", "dim_trx_type"]          # insert or update
APPEND_ONLY = ["dim_date", "fact_transaction", "fact_account_balance"]  # insert if new
AUDIT_COLUMN = {"fact_transaction": "loaded_run_id", "fact_account_balance": "loaded_run_id"}
MERGE_ORDER = ["dim_date", "dim_household", "dim_account", "dim_trx_type",
               "fact_transaction", "fact_account_balance"]            # parents before children
LOAD_ORDER = MERGE_ORDER + ["etl_rejects"]
RESULT_ATTR = {"etl_rejects": "rejects"}          # table name -> TransformResult field

# (table, column): SUM over the final rows matching the staged keys must equal the dataframe
SUM_CHECKS = [("fact_transaction", "value_kes"),
              ("fact_transaction", "operating_net_kes"),
              ("fact_account_balance", "balance_kes")]

ORPHAN_CHECKS = {
    "fact_transaction.household_key": "SELECT count(*) FROM fact_transaction f LEFT JOIN dim_household d ON d.household_key = f.household_key WHERE d.household_key IS NULL",
    "fact_transaction.account_key": "SELECT count(*) FROM fact_transaction f LEFT JOIN dim_account d ON d.account_key = f.account_key WHERE d.account_key IS NULL",
    "fact_transaction.date_key": "SELECT count(*) FROM fact_transaction f LEFT JOIN dim_date d ON d.date_key = f.date_key WHERE d.date_key IS NULL",
    "fact_transaction.trx_type_key": "SELECT count(*) FROM fact_transaction f LEFT JOIN dim_trx_type d ON d.trx_type_key = f.trx_type_key WHERE d.trx_type_key IS NULL",
    "fact_account_balance.account_key": "SELECT count(*) FROM fact_account_balance f LEFT JOIN dim_account d ON d.account_key = f.account_key WHERE d.account_key IS NULL",
}

SAMPLE_QUERY = """
SELECT h.hh_id, d.year_month, SUM(f.operating_net_kes) AS net_kes
FROM fact_transaction f
JOIN dim_household h ON h.household_key = f.household_key
JOIN dim_date d ON d.date_key = f.date_key
WHERE f.include_in_flow
GROUP BY h.hh_id, d.year_month
ORDER BY net_kes ASC
LIMIT 3
"""

RUN_LOG_DDL = """
CREATE TABLE IF NOT EXISTS etl_run_log (
    run_id          text PRIMARY KEY,
    start_time      timestamptz,
    end_time        timestamptz,
    status          text NOT NULL,
    rows_read       integer,
    rows_loaded     integer,
    rows_updated    integer,
    rows_rejected   integer,
    source_checksum text,
    error           text
)"""


class LoadError(Exception):
    """The load failed and was rolled back; the previous data is intact."""


class LoadVerificationError(LoadError):
    """The data loaded but did not match expectations, so it was rolled back."""


def make_engine(url: str = None):
    return create_engine(url or os.environ.get(config.DB_URL_ENV, config.DEFAULT_DB_URL),
                         future=True)


# --------------------------STAGING---------------------------------
def _frame(result: TransformResult, table: str, run_id: str) -> pd.DataFrame:
    df = getattr(result, RESULT_ATTR.get(table, table)).copy()
    if table == "etl_rejects":
        df.insert(0, "run_id", run_id)
    for col in df.columns:                      # dates as plain yyyy-mm-dd text
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].dt.strftime("%Y-%m-%d")
    return df


def _copy(cur, qualified_table: str, df: pd.DataFrame) -> None:
    """Bulk-load with COPY: far faster than row-by-row INSERT."""
    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False, na_rep="\\N")
    buf.seek(0)
    cols = ", ".join(df.columns)
    # cur.copy_expert(
    #     f"COPY {qualified_table} ({cols}) FROM STDIN WITH (FORMAT csv, NULL '\\N')", buf)
    with cur.copy(f"COPY {qualified_table} ({cols}) FROM STDIN WITH (FORMAT csv, NULL '\\N')") as cp:
        while data := buf.read(8192):
            cp.write(data)


def _stage(conn, frames: dict) -> None:
    """Fresh staging tables, typed like the real ones but without constraints,
    so bad data is caught at the merge, by the real constraints."""
    schema, stg = config.DB_SCHEMA, config.DB_STAGING_SCHEMA
    conn.execute(text(f"DROP SCHEMA IF EXISTS {stg} CASCADE"))
    conn.execute(text(f"CREATE SCHEMA {stg}"))
    cur = conn.connection.cursor()                    # same connection, same transaction
    for table in LOAD_ORDER:
        cols = ", ".join(frames[table].columns)
        conn.execute(text(f"CREATE UNLOGGED TABLE {stg}.{table} AS "
                          f"SELECT {cols} FROM {schema}.{table} WITH NO DATA"))
        _copy(cur, f"{stg}.{table}", frames[table])
        log.info("Staged %s: %d rows", table, len(frames[table]))


# -------------------MERGE STAGING AND SCHEMA------------------------------------------
def _upsert_sql(table: str, cols: list) -> str:
    key = KEY[table]
    schema, stg = config.DB_SCHEMA, config.DB_STAGING_SCHEMA
    collist = ", ".join(cols)
    sets = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols if c != key)
    return f"""
        WITH up AS (
            INSERT INTO {schema}.{table} AS t ({collist}, updated_run_id)
            SELECT {collist}, :run_id 
            FROM {stg}.{table}
            ON CONFLICT ({key}) DO UPDATE SET {sets},
                updated_run_id = EXCLUDED.updated_run_id, updated_at = now()
            WHERE t.row_hash IS DISTINCT FROM EXCLUDED.row_hash
            RETURNING (xmax = 0) AS inserted)
        SELECT count(*) FILTER (WHERE inserted), count(*) FILTER (WHERE NOT inserted) FROM up"""


def _append_sql(table: str, cols: list) -> str:
    key = KEY[table]
    schema, stg = config.DB_SCHEMA, config.DB_STAGING_SCHEMA
    collist = ", ".join(cols)
    audit = AUDIT_COLUMN.get(table)
    target = f"{collist}, {audit}" if audit else collist
    source = f"{collist}, :run_id" if audit else collist
    return f"""
        WITH ins AS (
            INSERT INTO {schema}.{table} ({target})
            SELECT {source} FROM {stg}.{table}
            ON CONFLICT ({key}) DO NOTHING RETURNING 1)
        SELECT count(*), 0 FROM ins"""


def _merge(conn, frames: dict, run_id: str) -> dict:
    """Merge staging into the warehouse. Returns {table: inserted/updated/unchanged}."""
    schema, stg = config.DB_SCHEMA, config.DB_STAGING_SCHEMA
    out = {}
    for table in MERGE_ORDER:
        cols = list(frames[table].columns)
        sql = _upsert_sql(table, cols) if table in UPSERT else _append_sql(table, cols)
        inserted, updated = conn.execute(text(sql), {"run_id": run_id}).one()
        staged = len(frames[table])
        out[table] = {"staged": staged, "inserted": inserted, "updated": updated,
                      "unchanged": staged - inserted - updated}
        log.info("Merged %s: +%d new, %d updated, %d unchanged",
                 table, inserted, updated, out[table]["unchanged"])
    # rejects: replace this run's rows, keep other runs' history
    conn.execute(text(f"DELETE FROM {schema}.etl_rejects WHERE run_id = :run_id"),
                 {"run_id": run_id})
    cols = ", ".join(frames["etl_rejects"].columns)
    conn.execute(text(f"INSERT INTO {schema}.etl_rejects ({cols}) "
                      f"SELECT {cols} FROM {stg}.etl_rejects"))
    return out


# --------------------VERIFICAQTI---------------------------------- 
def _existing_rows_checksum(conn, run_id: str) -> dict:
    """Checksum of fact rows loaded by OTHER runs. Append-only means this must be
    identical before and after the merge."""
    out = {}
    for table, cols in (("fact_transaction", "transaction_key || '|' || CAST(value_kes AS text) || '|' || CAST(operating_net_kes AS text)"),
                        ("fact_account_balance", "balance_key || '|' || CAST(balance_kes AS text)")):
        out[table] = conn.execute(text(
            f"SELECT md5(COALESCE(string_agg({cols}, ',' ORDER BY {KEY[table]}), '')) "
            f"FROM {table} WHERE loaded_run_id <> :run_id"), {"run_id": run_id}).scalar_one()
    return out


def _verify(conn, frames, result, merge, before, run_id) -> dict:
    stg = config.DB_STAGING_SCHEMA
    problems = []
    report = {"staged": {}, "merge": merge, "final_counts": {}, "sums": {},
              "orphans": {}, "append_only": {}}

    for table, df in frames.items():                       # 1. staging copy is complete
        n = conn.execute(text(f"SELECT count(*) FROM {stg}.{table}")).scalar_one()
        report["staged"][table] = n
        if n != len(df):
            problems.append(f"staging.{table}: {n} rows, {len(df)} expected")

    for table, m in merge.items():                         # 2. every staged row is accounted for
        if m["unchanged"] < 0:
            problems.append(f"{table}: inserted + updated exceeds staged rows")
        key = KEY[table]                                   # 3. every staged key now exists
        missing = conn.execute(text(
            f"SELECT count(*) FROM {stg}.{table} s LEFT JOIN {table} f ON f.{key} = s.{key} "
            f"WHERE f.{key} IS NULL")).scalar_one()
        if missing:
            problems.append(f"{table}: {missing} staged keys missing after merge")

    for table, col in SUM_CHECKS:                          # 4. money totals match the dataframe
        key = KEY[table]
        db = float(conn.execute(text(
            f"SELECT COALESCE(SUM(f.{col}), 0) FROM {table} f JOIN {stg}.{table} s "
            f"ON s.{key} = f.{key}")).scalar_one())
        want = float(pd.to_numeric(frames[table][col], errors="coerce").sum())
        tol = 0.01 + 0.005 * len(frames[table])            # numeric(…,2) rounds each value
        report["sums"][f"{table}.{col}"] = {"database": round(db, 2), "expected": round(want, 2)}
        if abs(db - want) > tol:
            problems.append(f"SUM({table}.{col}): database {db:.2f} vs expected {want:.2f}")

    after = _existing_rows_checksum(conn, run_id)          # 5. append-only really held
    for table in after:
        same = after[table] == before[table]
        report["append_only"][table] = "unchanged" if same else "CHANGED"
        if not same:
            problems.append(f"{table}: rows from earlier runs were modified")

    n = conn.execute(text("SELECT count(*) FROM etl_rejects WHERE run_id = :r"),
                     {"r": run_id}).scalar_one()
    if n != len(frames["etl_rejects"]):
        problems.append(f"etl_rejects: {n} rows for this run, {len(frames['etl_rejects'])} expected")

    for name, sql in ORPHAN_CHECKS.items():                # 6. no orphan keys anywhere
        n = conn.execute(text(sql)).scalar_one()
        report["orphans"][name] = n
        if n:
            problems.append(f"{n} orphan keys in {name}")

    for table in LOAD_ORDER:
        report["final_counts"][table] = conn.execute(
            text(f"SELECT count(*) FROM {table}")).scalar_one()

    staged_rows = (report["staged"]["fact_transaction"] + report["staged"]["fact_account_balance"]
                   + report["staged"]["etl_rejects"])      # 7. every input row is in one place
    if staged_rows != result.stats["rows_read"]:
        problems.append(f"fact + balance + rejects = {staged_rows}, rows read = {result.stats['rows_read']}")

    sample = conn.execute(text(SAMPLE_QUERY)).all()
    report["lowest_household_months"] = [(r[0], r[1], float(r[2])) for r in sample]

    if problems:
        raise LoadVerificationError("; ".join(problems))
    return report


# ---------------------------LOAD----------------------------------- 
def load_all(result: TransformResult, run_id: str, engine=None,
             mode: str = "incremental") -> dict:
    """Stage, merge and verify in one transaction. Returns a report; raises LoadError."""
    if mode not in ("incremental", "full_refresh"):
        raise ValueError(f"unknown load mode: {mode}")
    engine = engine or make_engine()
    frames = {t: _frame(result, t, run_id) for t in LOAD_ORDER}
    ddl = config.SCHEMA_SQL_PATH.read_text(encoding="utf-8")
    schema = config.DB_SCHEMA
    try:
        with engine.begin() as conn:                      # commit, or roll back on any error
            conn.execute(text("SELECT pg_advisory_xact_lock(hashtext('kfd_warehouse_load'))"))
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
            conn.execute(text(f"SET LOCAL search_path TO {schema}"))
            if mode == "full_refresh":
                for table in reversed(LOAD_ORDER):
                    conn.execute(text(f"DROP TABLE IF EXISTS {table} CASCADE"))
            conn.exec_driver_sql(ddl)                     # CREATE ... IF NOT EXISTS
            
            _stage(conn, frames)
            before = _existing_rows_checksum(conn, run_id)
            merge = _merge(conn, frames, run_id)
            report = _verify(conn, frames, result, merge, before, run_id)
    except LoadError:
        raise
    except (SQLAlchemyError, engine.dialect.dbapi.Error) as exc:
        # COPY runs on the raw DBAPI cursor, so its errors are driver errors,
        # not SQLAlchemy ones; both mean "rolled back, previous data intact".
        raise LoadError(f"{type(exc).__name__}: {getattr(exc, '__cause__', None) or exc}") from exc
    m = report["merge"]
    report["mode"] = mode
    report["rows_loaded"] = m["fact_transaction"]["inserted"] + m["fact_account_balance"]["inserted"]
    report["rows_updated"] = sum(m[t]["updated"] for t in UPSERT)
    report["rows_unchanged"] = (m["fact_transaction"]["unchanged"] + m["fact_account_balance"]["unchanged"])
    log.info("Load verified (%s): %d new fact rows, %d dimension rows updated, %d fact rows already present",
             mode, report["rows_loaded"], report["rows_updated"], report["rows_unchanged"])
    return report


def record_run(engine, summary: dict) -> None:
    """Upsert one row in etl_run_log (its own transaction, so a failed load
    can still be recorded). Never raises: logging must not hide the real error."""
    try:
        with engine.begin() as conn:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {config.DB_SCHEMA}"))
            conn.execute(text(f"SET LOCAL search_path TO {config.DB_SCHEMA}"))
            conn.execute(text(RUN_LOG_DDL))
            conn.execute(text("ALTER TABLE etl_run_log ADD COLUMN IF NOT EXISTS rows_updated integer"))
            conn.execute(text("""
                INSERT INTO etl_run_log (run_id, start_time, end_time, status, rows_read,
                                         rows_loaded, rows_updated, rows_rejected,
                                         source_checksum, error)
                VALUES (:run_id, :start_time, :end_time, :status, :rows_read,
                        :rows_loaded, :rows_updated, :rows_rejected, :checksum, :error)
                ON CONFLICT (run_id) DO UPDATE SET
                    end_time = EXCLUDED.end_time, status = EXCLUDED.status,
                    rows_read = EXCLUDED.rows_read, rows_loaded = EXCLUDED.rows_loaded,
                    rows_updated = EXCLUDED.rows_updated,
                    rows_rejected = EXCLUDED.rows_rejected,
                    source_checksum = EXCLUDED.source_checksum, error = EXCLUDED.error
            """), {
                "run_id": summary["run_id"], "start_time": summary.get("start_time"),
                "end_time": summary.get("end_time"), "status": summary["status"],
                "rows_read": summary.get("rows_read"),
                "rows_loaded": summary.get("rows_loaded"),
                "rows_updated": summary.get("rows_updated"),
                "rows_rejected": summary.get("rows_rejected"),
                "checksum": summary.get("checksum_sha256"), "error": summary.get("error"),
            })
    except Exception as exc:  # noqa: BLE001
        log.warning("Could not write etl_run_log: %s", exc)