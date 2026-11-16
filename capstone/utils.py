
import hashlib
import json

import pandas as pd

from . import config


def md5_key(*parts: pd.Series) -> pd.Series:
    """Deterministic surrogate key: md5 of the natural key parts.

    Rule: keys come from the natural key only, never from attributes, and are
    identical on every run. Risk: a key built from changing attributes would
    change between runs and break joins.
    """
    s = parts[0].astype("object").fillna("").astype(str)
    for p in parts[1:]:
        s = s + "|" + p.astype("object").fillna("").astype(str)
    return s.map(lambda v: hashlib.md5(v.encode("utf-8")).hexdigest())


def add_row_hash(df: pd.DataFrame, exclude: list) -> pd.DataFrame:
    """Add row_hash: md5 of every column except `exclude` (the key columns).

    Used on dimensions to detect change: an existing row is updated only when
    its row_hash differs from the staged one. It is not the row's identity.
    """
    out = df.copy()
    cols = [c for c in out.columns if c not in exclude]
    out["row_hash"] = md5_key(*[out[c] for c in cols])
    return out


def clean_text(df: pd.DataFrame) -> pd.DataFrame:
    """Trim whitespace and turn empty strings into missing.

    Rule: ' Cash ' and 'Cash' are the same value. Risk: spelling variants
    split one category into several and hide duplicates.
    """
    out = df.copy()
    for col in config.SOURCE_COLUMNS:
        s = out[col].astype("object").str.strip()
        out[col] = s.mask(s == "")
    return out


def add_transaction_key(df: pd.DataFrame) -> pd.DataFrame:
    """Hash of all source columns (trimmed text, nothing else cleaned).

    Rule: the key comes from content, not row position. Risk: a positional key
    changes when row order changes, so a re-run would not be idempotent.
    """
    out = df.copy()
    out["transaction_key"] = md5_key(*[out[c] for c in config.SOURCE_COLUMNS])
    return out


def remove_exact_duplicates(df: pd.DataFrame):
    """Return (unique rows, removed copies).

    Rows identical on every source column are copies of one record. The first
    copy (lowest source_row_id) is kept. The copies are identical, so which one
    survives changes only the lineage column source_row_id, never the data or
    the key. Risk: deleting two genuine purchases that look alike; the survey's
    own trx_id is part of the comparison, which makes that unlikely.
    """
    ordered = df.sort_values(config.SOURCE_ROW_ID, kind="mergesort")
    dup = ordered.duplicated(subset=config.SOURCE_COLUMNS, keep="first")
    return ordered[~dup], ordered[dup]


def make_rejects(rows: pd.DataFrame, reason: str) -> pd.DataFrame:
    """Rejected rows are kept, with the reason and the raw row as JSON."""
    if rows.empty:
        return pd.DataFrame(columns=config.REJECT_COLUMNS)
    raw = rows[config.SOURCE_COLUMNS].apply(
        lambda r: json.dumps(r.dropna().to_dict(), ensure_ascii=False), axis=1)
    return pd.DataFrame({
        "source_row_id": rows[config.SOURCE_ROW_ID].to_numpy(),
        "transaction_key": rows["transaction_key"].to_numpy(),
        "reject_reason": reason,
        "value_kes": pd.to_numeric(rows["trx_value_kes"], errors="coerce").to_numpy(),
        "raw_row": raw.to_numpy(),
    })