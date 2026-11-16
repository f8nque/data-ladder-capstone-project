#Extract validate 
import hashlib
import logging
from pathlib import Path

import pandas as pd

from . import config

log = logging.getLogger(__name__)


class ExtractError(Exception):
    """The raw file cannot be used. The message says why."""


def file_checksum(path: Path) -> str:
    """SHA-256 of the file, stored in the run log to detect a changed source."""
    sha = hashlib.sha256()
    with open(path, "rb") as fh:
        for block in iter(lambda: fh.read(1 << 20), b""):
            sha.update(block)
    return sha.hexdigest()


def extract(path: Path = config.RAW_PATH, allow_corrupt_ids: bool = False):
    """Return (raw rows as text plus source_row_id, report dict)."""
    path = Path(path)
    if not path.exists():
        raise ExtractError(f"Raw file not found: {path}")
    if not path.is_file():
        raise ExtractError(f"Raw path is not a file: {path}")
    if path.stat().st_size == 0:
        raise ExtractError(f"Raw file is empty (0 bytes): {path}")

    # importing the whole raw data as string 
    # field becomes missing: the strings "NA" or "None" stay as they are.
    try:
        df = pd.read_csv(path, dtype=str, keep_default_na=False, na_values=[""])
    except pd.errors.EmptyDataError as exc:
        raise ExtractError(f"Raw file has no header: {path}") from exc
    except (pd.errors.ParserError, UnicodeDecodeError) as exc:
        raise ExtractError(f"Raw file cannot be parsed as CSV: {exc}") from exc

    if df.empty:
        raise ExtractError(f"Raw file has a header but no rows: {path}")

    report = {
        "path": str(path),
        "checksum_sha256": file_checksum(path),
        "rows_read": len(df),
        "columns_in_file": len(df.columns),
    }

    # Current dataset does not have an index column but if can add
    if config.INDEX_COLUMN in df.columns:
        ids = pd.to_numeric(df.pop(config.INDEX_COLUMN), errors="coerce")
        if ids.isna().any() or ids.duplicated().any():
            log.warning("Index column is not a clean unique integer; "
                        "falling back to line order for source_row_id")
            ids = pd.Series(range(len(df)), index=df.index)
        df.insert(0, config.SOURCE_ROW_ID, ids.astype("int64"))
    else:
        log.warning("No index column in file; source_row_id = line order")
        df.insert(0, config.SOURCE_ROW_ID, pd.Series(range(len(df)), index=df.index))

    present = set(df.columns)
    #in the dataset we have critical columns needed for transformation
    missing_critical = [c for c in config.CRITICAL_COLUMNS if c not in present]
    if missing_critical:
        raise ExtractError(f"Critical columns missing: {missing_critical}")

    missing_other = [c for c in config.SOURCE_COLUMNS
                     if c not in present and c not in config.CRITICAL_COLUMNS]
    for col in missing_other:
        df[col] = pd.Series([None] * len(df), index=df.index, dtype="object")
    if missing_other:
        log.warning("Non-critical columns missing, added empty: %s", missing_other)

    expected = set(config.SOURCE_COLUMNS) | {config.SOURCE_ROW_ID}
    unexpected = [c for c in df.columns if c not in expected]
    if unexpected:
        log.warning("Unexpected columns dropped: %s", unexpected)
        df = df.drop(columns=unexpected)

    # corrupt = 0
    # for col in config.ID_COLUMNS:
    #     corrupt += int(df[col].astype("object").str.match(
    #         config.SCI_NOTATION_PATTERN, na=False).sum())
    # report["corrupt_id_cells"] = corrupt
    # if corrupt:
    #     msg = (f"{corrupt} ID cells are in scientific notation (for example "
    #            "5.61348E+16). The file was probably saved through a spreadsheet "
    #            "and the IDs are destroyed. Re-export the original.")
    #     if not allow_corrupt_ids:
    #         raise ExtractError(msg)
    #     log.warning("%s Continuing because allow_corrupt_ids is set.", msg)

    report["missing_noncritical"] = missing_other
    report["unexpected_dropped"] = unexpected
    df = df[[config.SOURCE_ROW_ID] + config.SOURCE_COLUMNS]

    log.info("Extracted %d rows, %d source columns", len(df), len(config.SOURCE_COLUMNS))
    return df, report