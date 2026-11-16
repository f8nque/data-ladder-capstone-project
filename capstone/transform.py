"""TRANSFORM: turn raw text rows into the star schema tables.

Each step documents: the business rule, what could go wrong, how it is tested
(see tests/test_etl.py). Rows are never silently dropped: every input row ends
up in exactly one place (fact, balance fact, or rejects), and `transform`
raises ReconciliationError if the counts do not add up.
"""
import logging
from dataclasses import dataclass

import numpy as np
import pandas as pd
import gc
import json
from pathlib import Path

from . import config
from .utils import (add_row_hash, add_transaction_key, clean_text, make_rejects,
                    md5_key, remove_exact_duplicates)

log = logging.getLogger(__name__)


class ReconciliationError(Exception):
    """Rows in != rows out. Something was lost or invented."""


@dataclass
class TransformResult:
    fact_transaction: pd.DataFrame
    fact_account_balance: pd.DataFrame
    dim_date: pd.DataFrame
    dim_household: pd.DataFrame
    dim_account: pd.DataFrame
    dim_trx_type: pd.DataFrame
    rejects: pd.DataFrame
    stats: dict


# to be used in filtering the raw data into fact and dim table
def _reject(df, mask, reason, bucket):
    mask = mask.fillna(False).astype(bool)
    if mask.any():
        bucket.append(make_rejects(df[mask], reason))
        log.info("Rejected %d rows: %s", int(mask.sum()), reason)
    return df[~mask]


def _add_flag(flags: pd.Series, mask: pd.Series, name: str) -> pd.Series:
    mask = mask.fillna(False).astype(bool)
    return flags.where(~mask, np.where(flags == "", name, flags + ";" + name))


# ------------------DATE PROCESSING---------------------------------- 
def parse_dates(df: pd.DataFrame) -> pd.Series:
    """
        Parse trx_date (for example 20Sep2012)
    """
    return pd.to_datetime(df["trx_date"], format=config.SOURCE_DATE_FORMAT,
                          errors="coerce")


def parse_source_date(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, format=config.SOURCE_DATE_FORMAT, errors="coerce")


# ----------------------CATEGORY MAPPING------------------------- 
def _lookup(s: pd.Series, mapping: dict) -> pd.Series:
    return s.astype("object").str.lower().map(mapping)


def standardise_categories(df: pd.DataFrame) -> pd.DataFrame:
    """One spelling per category and a controlled direction vocabulary.

    Rule: direction is exactly Inflow/Outflow, bsheet_direction exactly
    Increase/Decrease; family code OTH is OTHER. Anything else becomes missing
    and is flagged, never guessed. Test: test_direction_and_family_cleaned.
    """
    out = df.copy()
    out["trx_family_code"] = out["trx_family_code"].replace(config.FAMILY_CODE_FIXES)
    out["direction"] = _lookup(out["trx_direction"],
                               {"inflow": "Inflow", "outflow": "Outflow"})
    out["bsheet_direction"] = _lookup(out["trx_bsheet_direction"],
                                      {"increase": "Increase", "decrease": "Decrease"})
    return out


# -----------------BALANCE SNAPSHOT---------------------------- 
def snapshot_type(df: pd.DataFrame) -> pd.Series:
    """'start', 'close', 'cash_on_hand' or '' for ordinary transactions.
    Opening balance, closing balance, cash_on_hand e.t.c

    *Rule: balance rows are stocks, not flows.*

    Three source signals mark them:
    the mode (STARTING/CLOSINstep 5: balance snapshG BALANCE), the start/close column, and the
    purpose text '1. Starting balance (today)'. Class cash rows are cash-on-hand
    readings. Risk: if a balance is summed as a flow it swamps all operating
    activity. Test: test_snapshots_are_routed_out_of_flows.
    """
    mode = df["trx_mode_desc"].astype("object").str.upper()
    sc = df["account_startclose_balance"].astype("object").str.lower()
    purpose = df["trx_prx_purpose"].astype("object").str.lower()
    is_start = ((mode == "STARTING BALANCE") | (sc == "start")
                | purpose.str.startswith("1. starting balance", na=False))
    is_close = (mode == "CLOSING BALANCE") | (sc == "close")
    is_cash = df["trx_class_code"] == "cash"
    return pd.Series(np.select([is_close, is_start, is_cash],
                               ["close", "start", "cash_on_hand"], default=""),
                     index=df.index)


# ------------------------FLOW --------------------------- 
FEE_INTEREST = ("finance_fee", "interest_accrual")


def classify_flows(df: pd.DataFrame) -> pd.DataFrame:
    """flow_category, signed operating_net_kes, include_in_flow.

    Rules (direction is relative to the household):
      income Inflow            -> operating_income   (+value)
      income Outflow           -> income_cost        (-value)
      opex                     -> operating_expense  (-value)
      findev by purpose        -> savings_deposit, savings_withdrawal,
                                  borrowing, repayment (0: not operating)
      findev, other purposes   -> finance_fee, interest_accrual,
                                  contribution_payment, payout_claim
      anything unclassifiable  -> other_findev / unknown, flagged
    include_in_flow = the row has a direction. Fees and interest accruals have
    none, so they are kept but excluded from flow totals.
    Risk: summing every inflow minus every outflow counts moving money between
    accounts as income and spending. Test: test_flow_classification.
    """
    out = df.copy()
    cls, d = out["trx_class_code"], out["trx_direction"]
    pf = out["trx_prx_purpose_fd"]
    p = out["trx_prx_purpose"].astype("object").str.lower()
    fin = cls == "findev"

    conds = [
        (cls == "income") & (d == "Inflow"),
        (cls == "income") & (d == "Outflow"),
        cls == "opex",
        cls == "assets",
        fin & (pf == "Deposit on financial asset"),
        fin & (pf == "Withdrawal on financial asset"),
        fin & (pf == "New borrowing on financial liability"),
        fin & (pf == "Repayment on financial liability"),
        fin & p.str.contains("fee", na=False),
        fin & p.str.contains("interest", na=False),
        fin & p.str.contains("payments or contributions", na=False),
        fin & p.str.contains("payouts or claims", na=False),
        fin,
    ]
    names = ["operating_income", "income_cost", "operating_expense",
             "asset_transaction", "savings_deposit", "savings_withdrawal",
             "borrowing", "repayment", "finance_fee", "interest_accrual",
             "contribution_payment", "payout_claim", "other_findev"]
    out["flow_category"] = np.select(conds, names, default="unknown")

    value = out["value_kes"]
    out["operating_net_kes"] = np.select(
        [out["flow_category"] == "operating_income",
         out["flow_category"].isin(["income_cost", "operating_expense"])],
        [value, -value], default=0.0)
    out["include_in_flow"] = d.notna()
    return out


def flag_outliers(df: pd.DataFrame) -> pd.Series:
    """Robust z-score of log(1 + value) within each transaction family.

    Rule: flag, never delete: a big loan is real. Risk: a mean/std test is
    dominated by the outliers themselves, hence median and MAD.
    Test: test_outlier_flagged_not_removed.
    """
    x = np.log1p(df["value_kes"])
    grp = df["trx_family_desc"].astype("object").fillna("?")
    med = x.groupby(grp).transform("median")
    mad = (x - med).abs().groupby(grp).transform("median")
    size = x.groupby(grp).transform("size")
    z = 0.6745 * (x - med) / mad.replace(0, np.nan)
    return (z > config.OUTLIER_Z) & (size >= config.OUTLIER_MIN_GROUP) & (df["value_kes"] > 0)


# --------------------DIMENSION TABLES------------------------------------- 
def build_dim_date() -> pd.DataFrame:
    """One row per calendar day, generated, not taken from the transactions."""
    days = pd.date_range(config.DATE_WINDOW_START, config.DATE_WINDOW_END, freq="D")
    return pd.DataFrame({
        "date_key": days.strftime("%Y%m%d").astype("int64"),
        "full_date": days,
        "year": days.year,
        "month": days.month,
        "year_month": days.strftime("%Y-%m"),
        "week_start": days - pd.to_timedelta(days.day_of_week, unit="D"),
        "quarter": days.quarter,
        "day_of_week": days.day_name(),
        "is_month_end": days.is_month_end,
        "season": [config.SEASON_BY_MONTH[m] for m in days.month],
    })


def build_dim_household(kept: pd.DataFrame):
    """One row per hh_id. The source repeats household attributes on every row,
    so check they agree; report households where they do not."""
    attrs = ["first_trx_date_hh", "last_trx_date_hh",
             "tot_hh_daysofobs", "tot_hh_monthsofobs"]
    g = kept.groupby("hh_ids")
    inconsistent = int((g[attrs].nunique() > 1).any(axis=1).sum())
    first = g[attrs].first()
    dim = pd.DataFrame({
        "household_key": md5_key(first.index.to_series()).to_numpy(),
        "hh_id": first.index.to_numpy(),
        "first_trx_date": parse_source_date(first["first_trx_date_hh"]).to_numpy(),
        "last_trx_date": parse_source_date(first["last_trx_date_hh"]).to_numpy(),
        "days_observed": pd.to_numeric(first["tot_hh_daysofobs"]).astype("Int64").to_numpy(),
        "months_observed": pd.to_numeric(first["tot_hh_monthsofobs"]).astype("Int64").to_numpy(),
        "site": None,     
        "county": None,
    })
    return dim, inconsistent


def build_dim_account(kept: pd.DataFrame) -> pd.DataFrame:
    """One row per (hh_id, account_id): 'HH' is reused by every household."""
    g = kept.groupby(["hh_ids", "account_ids"])
    #creating a named aggregation - this is the same as g.first() but we need to change column names
    a = g.agg(owner_member_id=("m_ids_owner", "first"),
              bsheet_designation=("account_bsheet_desig", "first"),
              formality=("account_formal", "first"),
              liquidity=("account_liquid", "first"),
              first_acc=("first_trx_date_acc", "first"),
              last_acc=("last_trx_date_acc", "first")).reset_index()
    return pd.DataFrame({
        "account_key": md5_key(a["hh_ids"], a["account_ids"]).to_numpy(),
        "hh_id": a["hh_ids"], 
        "account_id": a["account_ids"],
        "owner_member_id": a["owner_member_id"],
        "bsheet_designation": a["bsheet_designation"],
        "formality": a["formality"],
         "liquidity": a["liquidity"],
        "first_trx_date": parse_source_date(a["first_acc"]),
        "last_trx_date": parse_source_date(a["last_acc"]),
    })


def build_dim_trx_type(kept: pd.DataFrame):
    cols = ["trx_type_code", "trx_type_desc", "trx_family_code", "trx_family_desc",
            "trx_class_code", "trx_class_desc"]
    conflicts = int((kept.groupby("trx_type_code")[
        ["trx_family_desc", "trx_class_code"]].nunique() > 1).any(axis=1).sum())
    t = kept[cols].drop_duplicates(subset="trx_type_code").reset_index(drop=True) # no need to look for first non-na
    return pd.DataFrame({
        "trx_type_key": md5_key(t["trx_type_code"]).to_numpy(),
        "type_code": t["trx_type_code"], 
        "type_desc": t["trx_type_desc"],
        "family_code": t["trx_family_code"],
         "family_desc": t["trx_family_desc"],
        "class_code": t["trx_class_code"],
         "class_desc": t["trx_class_desc"],
        "income_source_group": t["trx_family_desc"].map(config.INCOME_SOURCE_GROUP),
        "instrument_group": t["trx_family_desc"].map(config.INSTRUMENT_GROUP),
    }), conflicts


# --------------------------------------------------------- orchestration
FACT_COLUMNS = [
    "transaction_key", "source_row_id", "source_trx_id", "household_key",
    "account_key", "date_key", "trx_type_key", "member_id", "mode_desc",
    "is_cash", "is_digital", "purpose_fd", "direction", "bsheet_direction",
    "value_kes", "inkind_value_kes", "fee_kes", "distance_km", "is_gift",
    "flow_category", "operating_net_kes", "include_in_flow", "dq_flags",
    "trx_dq_round",
]
BALANCE_COLUMNS = ["balance_key", "source_row_id", "account_key", "household_key",
                   "date_key", "snapshot_type", "balance_kes"]


def transform_and_stage(raw: pd.DataFrame, staging_dir: Path = None) -> dict:
    """Builds star schema tables and streams them directly to staging parquet files 
    to prevent memory exhaustion."""
    staging_dir.mkdir(parents=True, exist_ok=True)
    rows_read = len(raw)
    rejects: list = []
    stats = {"rows_read": rows_read}

    # 1. Preprocess & Deduplicate
    df = add_transaction_key(clean_text(raw))
    df, dups = remove_exact_duplicates(df)
    if len(dups):
        rejects.append(make_rejects(dups, "exact_duplicate"))

    stats["duplicates_removed"] = len(dups)
    stats["duplicate_kes"] = float(pd.to_numeric(dups["trx_value_kes"], errors="coerce").sum())
    
    del dups
    gc.collect()

    log.info("Removed exact duplicate rows (%.0f KES)", stats["duplicate_kes"])
    assert df["transaction_key"].is_unique, "transaction_key not unique after dedupe"

    # 2. Required fields, dates, values
    required = df[["hh_ids", "account_ids", "trx_class_code", "trx_type_code"]].isna().any(axis=1)
    df = _reject(df, required, "missing_required_field", rejects)

    df["_date"] = parse_dates(df)
    df = _reject(df, df["_date"].isna(), "bad_date", rejects)

    outside = (df["_date"] < config.DATE_WINDOW_START) | (df["_date"] > config.DATE_WINDOW_END)
    df = _reject(df, outside, "date_out_of_window", rejects)

    df["value_kes"] = pd.to_numeric(df["trx_value_kes"], errors="coerce")
    df = _reject(df, df["trx_value_kes"].isna(), "missing_value", rejects)
    df = _reject(df, df["value_kes"].isna(), "bad_value", rejects)
    df = _reject(df, df["value_kes"] < 0, "negative_value", rejects)

    # 3. Categories & Snapshots
    df = standardise_categories(df)
    df["snapshot_type"] = snapshot_type(df)
    snap = df["snapshot_type"] != ""
    start_and_close = snap & (
        (df["trx_mode_desc"].astype("object").str.upper() == "STARTING BALANCE")
        & (df["account_startclose_balance"].astype("object").str.lower() == "close")
        | (df["trx_mode_desc"].astype("object").str.upper() == "CLOSING BALANCE")
        & (df["account_startclose_balance"].astype("object").str.lower() == "start"))
    df = _reject(df, start_and_close, "snapshot_conflict", rejects)
    snap = df["snapshot_type"] != ""

    # 4. Keys shared by both facts
    df["household_key"] = md5_key(df["hh_ids"])
    df["account_key"] = md5_key(df["hh_ids"], df["account_ids"])
    df["trx_type_key"] = md5_key(df["trx_type_code"])
    df["date_key"] = df["_date"].dt.strftime("%Y%m%d").astype("int64")

    kept = df
    
    # Build and immediately write dimensions to parquet
    dim_household, hh_inconsistent = build_dim_household(kept)
    dim_household = add_row_hash(dim_household, ["household_key"])
    dim_household.to_parquet(staging_dir / "dim_household.parquet", index=False)
    del dim_household

    dim_account = add_row_hash(build_dim_account(kept), ["account_key"])
    dim_account.to_parquet(staging_dir / "dim_account.parquet", index=False)
    del dim_account

    dim_trx_type, type_conflicts = build_dim_trx_type(kept)
    dim_trx_type = add_row_hash(dim_trx_type, ["trx_type_key"])
    dim_trx_type.to_parquet(staging_dir / "dim_trx_type.parquet", index=False)
    del dim_trx_type

    build_dim_date().to_parquet(staging_dir / "dim_date.parquet", index=False)
    gc.collect()

    # 5. Balance fact -> Write to parquet immediately
    b = df[snap]
    balance = pd.DataFrame({
        "balance_key": b["transaction_key"],
        "source_row_id": b["source_row_id"],
        "account_key": b["account_key"],
        "household_key": b["household_key"],
        "date_key": b["date_key"], 
        "snapshot_type": b["snapshot_type"],
        "balance_kes": b["value_kes"],
    })[BALANCE_COLUMNS].reset_index(drop=True)
    
    balance.to_parquet(staging_dir / "fact_account_balance.parquet", index=False)
    balance_len = len(balance)
    del b, balance
    gc.collect()

    # 6. Transaction fact -> Write to parquet immediately
    t = classify_flows(df[~snap].copy())
    
    # Free up main df memory since we have split into snapshots and transactions
    del df, kept
    gc.collect()

    flags = pd.Series("", index=t.index, dtype="object")
    flags = _add_flag(flags, t["value_kes"] == 0, "zero_value")
    flags = _add_flag(flags, flag_outliers(t), "outlier")
    flags = _add_flag(flags, t["trx_direction"].notna() & t["direction"].isna(), "bad_direction")
    flags = _add_flag(flags, t["flow_category"].isin(["other_findev", "unknown"]), "unclassified_row")
    t["dq_flags"] = flags

    num = lambda c: pd.to_numeric(t[c], errors="coerce") 
    mode = t["trx_mode_desc"].astype("object")
    
    fact = pd.DataFrame({
        "transaction_key": t["transaction_key"],
        "source_row_id": t["source_row_id"],
        "source_trx_id": t["trx_id"],
        "household_key": t["household_key"], 
        "account_key": t["account_key"],
        "date_key": t["date_key"], 
        "trx_type_key": t["trx_type_key"],
        "member_id": t["m_ids_trx"],
        "mode_desc": t["trx_mode_desc"],
        "is_cash": mode.str.lower() == "cash",
        "is_digital": mode.str.contains("mobile money|m-pesa", case=False, na=False),
        "purpose_fd": t["trx_prx_purpose_fd"],
        "direction": t["direction"], 
        "bsheet_direction": t["bsheet_direction"],
        "value_kes": t["value_kes"],
        "inkind_value_kes": num("trx_inkind_value_kes"),
        "fee_kes": num("trx_fee"), 
        "distance_km": num("trx_distance_km"),
        "is_gift": num("ddd_gift") == 1,
        "flow_category": t["flow_category"],
        "operating_net_kes": t["operating_net_kes"],
        "include_in_flow": t["include_in_flow"],
        "dq_flags": t["dq_flags"],
        "trx_dq_round": num("trx_dq_round").astype("Int64"),
    })[FACT_COLUMNS].reset_index(drop=True)

    fact.to_parquet(staging_dir / "fact_transaction.parquet", index=False)
    fact_len = len(fact)
    
    # Extract flow categories for stats before deleting fact
    flow_categories_dict = fact["flow_category"].value_counts().to_dict()
    dq_flag_counts_dict = fact["dq_flags"].str.split(";").explode().replace("", np.nan).dropna().value_counts().to_dict()
    
    del t, fact
    gc.collect()

    # Rejects
    rej = (pd.concat(rejects, ignore_index=True) if rejects
           else pd.DataFrame(columns=config.REJECT_COLUMNS))
    rej.to_parquet(staging_dir / "rejects.parquet", index=False)
    rej_len = len(rej)
    rejects_by_reason_dict = rej["reject_reason"].value_counts().to_dict() if len(rej) > 0 else {}
    del rej
    gc.collect()

    # 7. Reconcile
    accounted = fact_len + balance_len + rej_len
    if accounted != rows_read:
        raise ReconciliationError(
            f"rows read {rows_read} != fact {fact_len} + balance {balance_len} "
            f"+ rejects {rej_len} = {accounted}")

    stats.update({
        "fact_rows": fact_len, 
        "balance_rows": balance_len,
        "rows_rejected": rej_len,
        "rejects_by_reason": rejects_by_reason_dict,
        "flow_categories": flow_categories_dict,
        "dq_flag_counts": dq_flag_counts_dict,
        "households_with_inconsistent_attributes": hh_inconsistent,
        "type_codes_with_conflicting_family": type_conflicts,
        "reconciled": True,
    })
    
    # Save stats JSON
    with open(staging_dir / "stats.json", "w") as f:
        json.dump(stats, f, default=str)

    return stats
