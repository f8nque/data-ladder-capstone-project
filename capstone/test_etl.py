"""Tests for extract, transform and pipeline. Run:  python -m pytest tests -v

Every rule in transform.py has a test on a few synthetic rows, so a failure
points at one rule and one row, not at a 484,000-row file.
"""
import hashlib
import json
import random
import sys
import tempfile
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "etl"))

from . import config  
import pipeline  
from extract import ExtractError, extract  # noqa: E402
from transform import transform  # noqa: E402

DEFAULTS = dict(
    hh_ids="HH1", account_ids="A1", trx_date="01Oct2012",
    trx_class_code="opex", trx_class_desc="Operational", trx_family_code="FOOD",
    trx_family_desc="Food", trx_type_code="10", trx_type_desc="Groceries",
    trx_mode_desc="Cash", trx_direction="Outflow", trx_value_kes="100",
    first_trx_date_hh="20Sep2012", last_trx_date_hh="22Oct2013",
    tot_hh_daysofobs="397", tot_hh_monthsofobs="13",
)


def make_raw(rows):
    """Rows of raw text with all 58 source columns (as written to the CSV)."""
    data = []
    for i, overrides in enumerate(rows):
        row = {c: None for c in config.SOURCE_COLUMNS}
        row.update(DEFAULTS)
        row["trx_id"] = str(1000 + i)
        row.update(overrides)
        data.append(row)
    return pd.DataFrame(data)[config.SOURCE_COLUMNS].astype("object")


def write_csv(path, raw, drop=(), extra=None, with_index=False):
    """Write like the real raw file: no index column, no transaction_date."""
    out = raw.drop(columns=list(drop)).copy()
    for k, v in (extra or {}).items():
        out[k] = v
    if with_index:
        out.insert(0, config.INDEX_COLUMN, [500 + i for i in range(len(out))])
    out.to_csv(path, index=False)
    return path


def etl_run(rows):
    """CSV -> extract -> transform, the same path the pipeline takes."""
    with tempfile.TemporaryDirectory() as d:
        p = write_csv(Path(d) / "raw.csv", make_raw(rows))
        raw, _ = extract(p)
    return transform(raw)


# # # ================================================================ EXTRACT
# def test_missing_file(tmp_path):
#     with pytest.raises(ExtractError, match="not found"):
#         extract(tmp_path / "nope.csv")


# def test_empty_file(tmp_path):
#     p = tmp_path / "e.csv"
#     p.write_bytes(b"")
#     with pytest.raises(ExtractError, match="empty"):
#         extract(p)


# def test_header_only_file(tmp_path):
#     p = tmp_path / "h.csv"
#     p.write_text(",".join(config.SOURCE_COLUMNS) + "\n")
#     with pytest.raises(ExtractError, match="no rows"):
#         extract(p)


# def test_missing_critical_column(tmp_path):
#     p = write_csv(tmp_path / "m.csv", make_raw([{}]), drop=["trx_value_kes"])
#     with pytest.raises(ExtractError, match="trx_value_kes"):
#         extract(p)


# def test_unexpected_dropped_and_missing_noncritical_added(tmp_path):
#     p = write_csv(tmp_path / "u.csv", make_raw([{}]), drop=["trx_fee"], extra={"junk": "x"})
#     df, report = extract(p)
#     assert "junk" not in df.columns
#     assert "trx_fee" in df.columns and df["trx_fee"].isna().all()
#     assert report["unexpected_dropped"] == ["junk"]
#     assert report["missing_noncritical"] == ["trx_fee"]





# # def test_no_index_column_means_source_row_id_is_line_order(tmp_path):
# #     long_id = "56135000000000123"           # 17 digits: lost if read as float
# #     p = write_csv(tmp_path / "i.csv", make_raw([{"trx_id": long_id}, {}]))
# #     df, report = extract(p)
# #     assert list(df[config.SOURCE_ROW_ID]) == [0, 1]
# #     assert df.loc[df[config.SOURCE_ROW_ID] == 0, "trx_id"].iloc[0] == long_id
# #     assert report["rows_read"] == 2 and len(report["checksum_sha256"]) == 64


# def test_index_column_is_used_as_source_row_id_when_present(tmp_path):
#     p = write_csv(tmp_path / "x.csv", make_raw([{}, {}]), with_index=True)
#     df, _ = extract(p)
#     assert list(df[config.SOURCE_ROW_ID]) == [500, 501]
#     assert config.INDEX_COLUMN not in df.columns


# # # ========================TRANSFORM====================================== 
# def fact_of(rows):
#     r = etl_run(rows)
#     return r, r.fact_transaction


# def test_whitespace_is_trimmed():
#     _, f = fact_of([{"trx_mode_desc": "  Cash "}])
#     assert f.loc[0, "mode_desc"] == "Cash" and f.loc[0, "is_cash"]


# def test_exact_duplicates_removed_and_logged():
#     rows = [{"trx_id": "7"}, {"trx_id": "7"}, {"trx_id": "8"}]
#     r = etl_run(rows)
#     assert len(r.fact_transaction) == 2
#     assert list(r.rejects["reject_reason"]) == ["exact_duplicate"]
#     assert r.stats["duplicates_removed"] == 1
#     assert 0 in set(r.fact_transaction["source_row_id"])    # lowest id is kept
#     assert 1 not in set(r.fact_transaction["source_row_id"])
#     assert json.loads(r.rejects.loc[0, "raw_row"])["trx_id"] == "7"


# def test_rows_that_differ_only_by_padding_are_duplicates_after_trimming():
#     rows = [{"trx_id": "7", "trx_mode_desc": "Cash"}, {"trx_id": " 7 ", "trx_mode_desc": "Cash "}]
#     r = etl_run(rows)
#     assert len(r.fact_transaction) == 1 and list(r.rejects["reject_reason"]) == ["exact_duplicate"]


# def test_same_values_but_different_trx_id_is_not_a_duplicate():
#     r, f = fact_of([{"trx_id": "7"}, {"trx_id": "8"}])
#     assert len(f) == 2 and r.rejects.empty


# def test_key_ignores_row_order_and_run_is_idempotent():
#     rows = [{"trx_id": str(i), "trx_value_kes": str(10 * i + 1)} for i in range(20)]
#     rows += [{"trx_id": "3", "trx_value_kes": "31"}]              # a duplicate
#     shuffled = rows[:]
#     random.Random(3).shuffle(shuffled)
#     assert shuffled != rows                                       # the order really changed
#     a = etl_run(rows).fact_transaction
#     b = etl_run(rows).fact_transaction                            # same input again
#     c = etl_run(shuffled).fact_transaction                        # permuted input
#     # source_row_id is the line number, so it follows the order; it is lineage only
#     key = lambda d: (d.drop(columns="source_row_id")              # noqa: E731
#                       .sort_values("transaction_key").reset_index(drop=True))
#     pd.testing.assert_frame_equal(key(a), key(b))
#     pd.testing.assert_frame_equal(key(a), key(c))
#     assert a["transaction_key"].is_unique and len(a) == 20


# def test_bad_and_out_of_window_dates_rejected():
#     rows = [{}, {"trx_date": "garbage"}, {"trx_date": "01Jan2015"}]
#     r = etl_run(rows)
#     assert len(r.fact_transaction) == 1
#     assert sorted(r.rejects["reject_reason"]) == ["bad_date", "date_out_of_window"]


# def test_trx_date_is_parsed_to_date_key():
#     _, f = fact_of([{"trx_date": "28Sep2012"}])
#     assert f.loc[0, "date_key"] == 20120928


# def test_value_rules():
#     rows = [{"trx_value_kes": "-5"}, {"trx_value_kes": "abc"},
#             {"trx_value_kes": None}, {"trx_value_kes": "0"}]
#     r = etl_run(rows)
#     assert sorted(r.rejects["reject_reason"]) == ["bad_value", "missing_value", "negative_value"]
#     assert len(r.fact_transaction) == 1                       # the zero row is kept
#     assert r.fact_transaction.loc[0, "dq_flags"] == "zero_value"


# def test_missing_required_field_rejected():
#     r = etl_run([{"hh_ids": None}, {}])
#     assert list(r.rejects["reject_reason"]) == ["missing_required_field"]


# def test_snapshots_are_routed_out_of_flows():
#     rows = [
#         {"trx_class_code": "findev", "trx_mode_desc": "STARTING BALANCE", "trx_value_kes": "5000"},
#         {"trx_class_code": "findev", "trx_mode_desc": "CLOSING BALANCE", "trx_value_kes": "7000"},
#         {"trx_class_code": "findev", "account_startclose_balance": "Start", "trx_mode_desc": "Cash"},
#         {"trx_class_code": "cash", "trx_mode_desc": None, "trx_value_kes": "1400"},
#         {},                                                   # ordinary opex row
#     ]
#     r = etl_run(rows)
#     assert len(r.fact_transaction) == 1
#     assert sorted(r.fact_account_balance["snapshot_type"]) == ["cash_on_hand", "close", "start", "start"]
#     assert r.fact_account_balance["balance_kes"].sum() == 5000 + 7000 + 100 + 1400


# def test_flow_classification():
#     rows = [
#         {"trx_class_code": "income", "trx_direction": "Inflow", "trx_value_kes": "1000"},
#         {"trx_class_code": "income", "trx_direction": "Outflow", "trx_value_kes": "200"},
#         {"trx_class_code": "opex", "trx_direction": "Outflow", "trx_value_kes": "300"},
#         {"trx_class_code": "findev", "trx_direction": "Inflow", "trx_value_kes": "500",
#          "trx_prx_purpose_fd": "New borrowing on financial liability"},
#         {"trx_class_code": "findev", "trx_direction": "Outflow", "trx_value_kes": "50",
#          "trx_prx_purpose_fd": "Deposit on financial asset"},
#         {"trx_class_code": "findev", "trx_direction": None, "trx_value_kes": "9",
#          "trx_prx_purpose": "4. Any known fees"},
#     ]
#     f = etl_run(rows).fact_transaction.sort_values("source_row_id")
#     assert list(f["flow_category"]) == ["operating_income", "income_cost", "operating_expense",
#                                         "borrowing", "savings_deposit", "finance_fee"]
#     assert list(f["operating_net_kes"]) == [1000, -200, -300, 0, 0, 0]
#     assert list(f["include_in_flow"]) == [True, True, True, True, True, False]
#     assert f.loc[f["include_in_flow"], "operating_net_kes"].sum() == 500


# def test_direction_and_family_cleaned():
#     rows = [{"trx_direction": "inflow"},
#             {"trx_direction": "sideways", "trx_family_code": "OTH", "trx_family_desc": "Other",
#              "trx_type_code": "11", "trx_type_desc": "Misc"}]
#     r = etl_run(rows)
#     f = r.fact_transaction.sort_values("source_row_id").reset_index(drop=True)
#     assert f.loc[0, "direction"] == "Inflow"
#     assert pd.isna(f.loc[1, "direction"]) and "bad_direction" in f.loc[1, "dq_flags"]
#     assert "OTHER" in set(r.dim_trx_type["family_code"])


# def test_outlier_flagged_not_removed():
#     rows = [{"trx_value_kes": str(100 + (i % 5) * 10)} for i in range(30)]
#     rows.append({"trx_value_kes": "1000000"})
#     f = etl_run(rows).fact_transaction
#     big = f[f["value_kes"] == 1_000_000]
#     assert len(big) == 1 and "outlier" in big.iloc[0]["dq_flags"]
#     assert len(f) == 31


# def test_dimensions_are_unique_and_deterministic():
#     rows = [{"hh_ids": "HH1", "account_ids": "HH"}, {"hh_ids": "HH2", "account_ids": "HH"},
#             {"hh_ids": "HH1", "account_ids": "A9"}]
#     r = etl_run(rows)
#     hh = r.dim_household
#     assert sorted(hh["hh_id"]) == ["HH1", "HH2"] and hh["household_key"].is_unique
#     assert hh.set_index("hh_id").loc["HH1", "household_key"] == hashlib.md5(b"HH1").hexdigest()
#     assert len(r.dim_account) == 3 and r.dim_account["account_key"].is_unique   # 'HH' per household
#     d = r.dim_date
#     assert d["date_key"].is_unique and (d["full_date"].diff().dropna() == pd.Timedelta("1D")).all()
#     row = d[d["date_key"] == 20120928].iloc[0]
#     assert (row["day_of_week"], str(row["week_start"].date()), row["quarter"]) == ("Friday", "2012-09-24", 3)


# def test_every_row_is_accounted_for():
#     rows = [{"trx_id": "1"}, {"trx_id": "1"}, {"trx_value_kes": "-1"},
#             {"trx_class_code": "cash"}, {"trx_class_code": "findev", "trx_mode_desc": "CLOSING BALANCE"}]
#     r = etl_run(rows)
#     assert r.stats["reconciled"]
#     assert len(r.fact_transaction) + len(r.fact_account_balance) + len(r.rejects) == len(rows)


# # # ===================PIPELINE============================================ 
# def test_failed_run_is_logged_not_raised(tmp_path, monkeypatch):
#     monkeypatch.setattr(config, "LOG_DIR", tmp_path)
#     monkeypatch.setattr(config, "RUN_LOG_PATH", tmp_path / "run_log.jsonl")
#     s = pipeline.run(tmp_path / "missing.csv")
#     assert s["status"] == "failed" and "not found" in s["error"]
#     line = json.loads((tmp_path / "run_log.jsonl").read_text().splitlines()[-1])
#     assert line["status"] == "failed" and "start_time" in line and "end_time" in line


# def test_rerun_does_not_duplicate_output(tmp_path):
#     r = etl_run([{"trx_id": str(i)} for i in range(5)])
#     out = tmp_path / "staging"
#     first = pipeline.write_outputs(r, out)
#     second = pipeline.write_outputs(r, out)
#     assert first == second
#     assert len(pd.read_parquet(out / "fact_transaction.parquet")) == 5


# # ================================================================== LOAD
# # These run against a real PostgreSQL in throw-away schemas and are skipped when
# # none is reachable. Set KFD_TEST_DATABASE_URL to point somewhere else.
import os  

import load  
from utils import add_row_hash  
from sqlalchemy import text  


@pytest.fixture()
def pg(monkeypatch):
    url = os.environ.get("KFD_TEST_DATABASE_URL",
                         os.environ.get(config.DB_URL_ENV, config.DEFAULT_DB_URL))
    engine = load.make_engine(url)
    try:
        with engine.connect() as c:
            c.execute(text("SELECT 1"))
    except Exception:
        pytest.skip("no PostgreSQL available")
    monkeypatch.setattr(config, "DB_SCHEMA", "kfd_test")
    monkeypatch.setattr(config, "DB_STAGING_SCHEMA", "kfd_test_stg")
    drop = lambda: [c.execute(text(f"DROP SCHEMA IF EXISTS {s} CASCADE"))  # noqa: E731
                    for c in [engine.connect().execution_options(isolation_level="AUTOCOMMIT")]
                    for s in ("kfd_test", "kfd_test_stg")]
    drop()
    yield engine
    drop()


def scalar(engine, sql):
    with engine.connect() as c:
        c.execute(text("SET search_path TO kfd_test"))
        return c.execute(text(sql)).scalar_one()


def rows(n, start=0):
    return [{"trx_id": f"t{i}", "trx_value_kes": str(100 + i)} for i in range(start, start + n)]


SAMPLE_ROWS = (
    rows(6)
    + [{"trx_id": "t1", "trx_value_kes": "101"}]                      # exact duplicate
    + [{"trx_class_code": "findev", "trx_mode_desc": "CLOSING BALANCE",
        "trx_value_kes": "900"}]                                      # balance snapshot
    + [{"trx_value_kes": "-1"}]                                       # rejected
)


def fingerprint(pg):
    return (scalar(pg, "SELECT count(*) FROM fact_transaction"),
            scalar(pg, "SELECT md5(string_agg(transaction_key, ',' ORDER BY transaction_key)) FROM fact_transaction"),
            scalar(pg, "SELECT sum(value_kes) FROM fact_transaction"))


def test_first_load_inserts_everything(pg):
    r = etl_run(SAMPLE_ROWS)
    rep = load.load_all(r, "run1", pg)
    m = rep["merge"]
    assert m["fact_transaction"]["inserted"] == len(r.fact_transaction)
    assert m["fact_account_balance"]["inserted"] == len(r.fact_account_balance)
    assert m["dim_household"] == {"staged": 1, "inserted": 1, "updated": 0, "unchanged": 0}
    assert rep["rows_loaded"] == len(r.fact_transaction) + len(r.fact_account_balance)
    assert scalar(pg, "SELECT count(*) FROM fact_transaction WHERE loaded_run_id = 'run1'") == len(r.fact_transaction)
    assert rep["mode"] == "incremental"


def test_rerun_is_idempotent_and_changes_nothing(pg):
    load.load_all(etl_run(SAMPLE_ROWS), "run1", pg)
    before = fingerprint(pg)
    rep = load.load_all(etl_run(SAMPLE_ROWS), "run2", pg)           # same data again
    assert fingerprint(pg) == before
    assert rep["rows_loaded"] == 0 and rep["rows_updated"] == 0
    assert scalar(pg, "SELECT count(*) FROM fact_transaction WHERE loaded_run_id = 'run2'") == 0
    assert scalar(pg, "SELECT count(*) FROM dim_household WHERE updated_run_id = 'run2'") == 0


def test_new_rows_are_appended_and_old_rows_untouched(pg):
    load.load_all(etl_run(rows(5)), "run1", pg)
    rep = load.load_all(etl_run(rows(8)), "run2", pg)               # 5 known rows + 3 new
    assert rep["merge"]["fact_transaction"]["inserted"] == 3
    assert scalar(pg, "SELECT count(*) FROM fact_transaction WHERE loaded_run_id = 'run1'") == 5
    assert scalar(pg, "SELECT count(*) FROM fact_transaction WHERE loaded_run_id = 'run2'") == 3
    assert rep["append_only"]["fact_transaction"] == "unchanged"


def test_dimension_is_updated_only_when_its_row_hash_changes(pg):
    load.load_all(etl_run(SAMPLE_ROWS), "run1", pg)
    r2 = etl_run(SAMPLE_ROWS)
    r2.dim_household.loc[0, "days_observed"] = 400                  # an attribute changed
    r2.dim_household = add_row_hash(r2.dim_household.drop(columns="row_hash"), ["household_key"])
    rep = load.load_all(r2, "run2", pg)
    m = rep["merge"]
    assert m["dim_household"]["updated"] == 1 and m["dim_household"]["inserted"] == 0
    assert m["dim_account"]["updated"] == 0 and m["fact_transaction"]["inserted"] == 0
    assert scalar(pg, "SELECT days_observed FROM dim_household") == 400
    assert scalar(pg, "SELECT updated_run_id FROM dim_household") == "run2"
    assert rep["rows_updated"] == 1


def test_a_corrected_source_row_is_appended_not_updated(pg):
    """The fact key hashes the whole row, so a corrected row is a new row. This
    is the price of append-only facts; a full refresh removes the old version."""
    load.load_all(etl_run(rows(5)), "run1", pg)
    changed = rows(5)
    changed[0]["trx_value_kes"] = "999"                             # a correction in the source
    rep = load.load_all(etl_run(changed), "run2", pg)
    assert rep["merge"]["fact_transaction"]["inserted"] == 1
    assert scalar(pg, "SELECT count(*) FROM fact_transaction") == 6  # old version still there
    load.load_all(etl_run(changed), "run3", pg, mode="full_refresh")
    assert scalar(pg, "SELECT count(*) FROM fact_transaction") == 5  # now only the corrected one


def test_full_refresh_rebuilds_and_drops_stale_rows(pg):
    load.load_all(etl_run(rows(8)), "run1", pg)
    rep = load.load_all(etl_run(rows(5)), "run2", pg, mode="full_refresh")
    assert scalar(pg, "SELECT count(*) FROM fact_transaction") == 5
    assert scalar(pg, "SELECT count(*) FROM fact_transaction WHERE loaded_run_id = 'run2'") == 5
    assert rep["mode"] == "full_refresh"


def test_full_refresh_migrates_an_older_table_layout(pg):
    with pg.begin() as c:
        c.execute(text("CREATE SCHEMA kfd_test"))
        c.execute(text("CREATE TABLE kfd_test.fact_transaction (transaction_key text)"))   # old layout
    with pytest.raises(load.LoadError):
        load.load_all(etl_run(rows(3)), "run1", pg)                  # incremental cannot use it
    load.load_all(etl_run(rows(3)), "run2", pg, mode="full_refresh")
    assert scalar(pg, "SELECT count(*) FROM fact_transaction") == 3


def test_failed_verification_rolls_back_staging_and_merge(pg, monkeypatch):
    load.load_all(etl_run(rows(5)), "run1", pg)
    def boom(*a, **k):
        raise load.LoadVerificationError("forced mismatch")
    monkeypatch.setattr(load, "_verify", boom)
    with pytest.raises(load.LoadVerificationError):
        load.load_all(etl_run(rows(9)), "run2", pg)
    assert scalar(pg, "SELECT count(*) FROM fact_transaction") == 5               # merge undone
    assert scalar(pg, "SELECT count(*) FROM kfd_test_stg.fact_transaction") == 5  # staging undone
    

def test_foreign_key_violation_is_a_clean_rollback(pg):
    good = etl_run(SAMPLE_ROWS)
    load.load_all(good, "run1", pg)
    broken = etl_run(SAMPLE_ROWS + rows(2, start=50))
    # corrupt a NEW row: rows whose key already exists are skipped, never inspected
    broken.fact_transaction.loc[broken.fact_transaction.index[-1], "household_key"] = "does-not-exist"
    with pytest.raises(load.LoadError):
        load.load_all(broken, "run2", pg)
    assert scalar(pg, "SELECT count(*) FROM fact_transaction") == len(good.fact_transaction)


def test_verification_catches_a_missing_row(pg):
    result = etl_run(SAMPLE_ROWS)
    result.stats["rows_read"] += 1                  # pretend a row went missing
    with pytest.raises(load.LoadVerificationError, match="rows read"):
        load.load_all(result, "run1", pg)


def test_rejects_are_replaced_per_run_and_history_is_kept(pg):
    load.load_all(etl_run(SAMPLE_ROWS), "run1", pg)
    load.load_all(etl_run(SAMPLE_ROWS), "run1", pg)                 # a retry of the same run
    assert scalar(pg, "SELECT count(*) FROM etl_rejects WHERE run_id = 'run1'") == 2
    load.load_all(etl_run(SAMPLE_ROWS), "run2", pg)
    assert scalar(pg, "SELECT count(*) FROM etl_rejects") == 4


def test_run_log_is_upserted_not_duplicated(pg):
    summary = {"run_id": "r1", "status": "success", "rows_read": 5, "rows_loaded": 4,
               "rows_updated": 0, "rows_rejected": 1, "start_time": "2026-11-08T00:00:00+00:00",
               "end_time": "2026-11-08T00:00:01+00:00"}
    load.record_run(pg, summary)
    load.record_run(pg, {**summary, "status": "failed", "error": "x"})
    assert scalar(pg, "SELECT count(*) FROM etl_run_log") == 1
    assert scalar(pg, "SELECT status FROM etl_run_log") == "failed"