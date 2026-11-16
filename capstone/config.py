"""Project constants for the Kenya Financial Diaries pipeline.

Everything that names a file, a column or a business threshold lives here,
so the other modules contain logic only.
"""
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent #CAPSTONE FOLDER
RAW_DIR = PROJECT_ROOT / "raw_data"
RAW_FILE_NAME = "diaries_transactions_all.csv"
RAW_PATH = RAW_DIR / RAW_FILE_NAME

STAGING_DIR = PROJECT_ROOT / "staging"
LOG_DIR = PROJECT_ROOT / "logs"
RUN_LOG_PATH = LOG_DIR / "run_log.jsonl"
PIPELINE_LOG_PATH = LOG_DIR / "pipeline.log"
SCHEMA_SQL_PATH = Path(__file__).resolve().parent / "schema.sql"

SCHEMA_MARTS_SQL_PATH = Path(__file__).resolve().parent / "marts_schema.sql"


# --------------POSTGRESQL CONFIG----------------------------------------------- 

DB_URL_ENV = "KFD_DATABASE_URL"
DEFAULT_DB_URL = "postgresql+psycopg2://etl_user:JustLogin1234@localhost:5432/financial_etl"
DB_SCHEMA = "warehouse"            # final tables
DB_STAGING_SCHEMA = "staging"      # per-run staging tables, rebuilt every load
DB_MARTS_SCHEMA = "marts"

# --------------------------------------------------------------- columns
# The raw file has no index column, so source_row_id is the line number in the
# file. It is lineage only: it is never part of the duplicate test or the hash.
# A copy of the file that was re-saved from pandas may carry an unnamed index
# column; if so it is used as source_row_id instead.
INDEX_COLUMN = "Unnamed: 0"
SOURCE_ROW_ID = "source_row_id"



# The 58 raw columns.
SOURCE_COLUMNS = [
    "hh_ids", "unique_hhs", "first_trx_date_hh", "last_trx_date_hh",
    "tot_hh_daysofobs", "tot_hh_monthsofobs", "interview_designation",
    "int_date", "int_month", "int_year", "int_yr_mo", "first_int_date",
    "account_ids", "unique_accnts", "m_ids_owner", "unique_hm_owner",
    "account_bsheet_desig", "account_startclose_balance", "account_formal",
    "account_liquid", "first_trx_date_acc", "last_trx_date_acc",
    "tot_acc_daysofobs", "tot_acc_monthsofobs", "trx_id", "m_ids_trx",
    "trx_date", "trx_month", "trx_year", "trx_yr_mo", "trx_dq_round",
    "trx_stdtime_days_hh", "trx_stdtime_mnths_hh", "trx_stdtime_days_acc",
    "trx_stdtime_mnths_acc", "trx_class_code", "trx_class_desc",
    "trx_family_code", "trx_family_desc", "trx_type_code", "trx_type_desc",
    "trx_prx_purpose", "trx_prx_purpose_fd", "trx_fee", "trx_bsheet_direction",
    "trx_mode_code", "trx_mode_desc", "trx_place_incommunity",
    "trx_distance_km", "trx_outlet", "trx_direction", "trx_value_kes",
    "trx_value_usd", "ddd_gift", "trx_inkind_units", "trx_inkind_value_usd",
    "trx_inkind_value_kes", "trx_stdtime_mnths_hh_nr",
]

# Without these the pipeline cannot work: a missing one stops the run.
# Any other missing column is added as empty and logged as a warning.
CRITICAL_COLUMNS = [
    "hh_ids", "account_ids", "trx_id", "trx_date",
    "trx_class_code", "trx_family_desc", "trx_type_code", "trx_type_desc",
    "trx_prx_purpose", "trx_prx_purpose_fd", "trx_mode_desc", "trx_direction",
    "trx_value_kes", "account_startclose_balance", "account_formal",
]

MART_TABLES  = [
    "mart_household_month",
    "mart_household_resilience",
    "mart_household_inclusion",
    "mart_formal_adoption",
    "mart_instrument_usage",
    "mart_mode_month",
    "mart_category_month",
    "mart_monthly_totals",
    "mart_household_profile",
    "mart_diversity_vs_shortage",
    "mart_savings_by_income_quintile",
    "mart_savings_vs_borrowing",
    "mart_associations",
    "mart_borrowing_low_vs_high",
    "mart_borrowing_low_vs_high_summary",
    "mart_borrowing_purpose",
    "mart_income_sources",
    "mart_vulnerability_by_group",
    "mart_shock_simulation",
    "mart_shock_summary",
    "mart_kpis",
    "mart_data_quality",
]

# Identifier columns. A spreadsheet round trip turns these into 5.6E+16,
# which destroys them, so extract refuses such files.
ID_COLUMNS = ["account_ids", "m_ids_owner", "m_ids_trx", "trx_id"]
SCI_NOTATION_PATTERN = r"^\d(\.\d+)?[Ee]\+\d+$"

# ----------------------RULES--------------------------------- 
DATE_WINDOW_START = "2012-09-01"   # collection period, with some margin
DATE_WINDOW_END = "2013-12-31"
SOURCE_DATE_FORMAT = "%d%b%Y"      # trx_date, e.g. 20Sep2012

FAMILY_CODE_FIXES = {"OTH": "OTHER"}   # same description, two codes

OUTLIER_Z = 6.0            # robust z-score on log(1 + value)
OUTLIER_MIN_GROUP = 20     # smallest family that gets an outlier test

# Assumed Kenyan rainfall calendar, to be confirmed per site.
SEASON_BY_MONTH = {
    1: "dry", 2: "dry", 3: "long_rains", 4: "long_rains", 5: "long_rains",
    6: "dry", 7: "dry", 8: "dry", 9: "dry",
    10: "short_rains", 11: "short_rains", 12: "short_rains",
}

# Derived groupings for dim_trx_type (keyed on trx_family_desc).
INCOME_SOURCE_GROUP = {
    "Agriculture": "agriculture",
    "Regular employment": "wages",
    "Casual/temp employment": "wages",
    "Self-employment": "business",
    "Resources Received": "transfers_other",
    "Resources Given": "transfers_other",
    "Rental or other": "transfers_other",
    "Non-employment": "transfers_other",
    "Passthrough Income": "transfers_other",
}
INSTRUMENT_GROUP = {
    "Formal loan": "formal_loan",
    "Loan from employer": "employer_credit",
    "Advance": "employer_credit",
    "Formal savings: with access": "formal_savings",
    "Formal savings: restricted access": "formal_savings",
    "Formal insurance": "insurance",
    "Informal group": "informal_group",
    "Informal P2P": "informal_p2p",
    "Informal savings: keep money at home": "informal_savings",
    "Supplier credit": "informal_credit",
    "Layaway": "informal_credit",
    "Pawning assets": "informal_credit",
    "Arrears owed to or owed by respondents": "informal_credit",
    "Mobile Money": "mobile_money",
}

FORMALITIES = {
    "formal_loan": "formal",
    "employer_credit": "other",
    "formal_savings": "formal",
    "insurance": "formal",
    "informal_group": "informal",
    "informal_p2p": "informal",
    "informal_credit": "informal",
    "mobile_money": "formal",
}

# -----------------------------------------------
REJECT_COLUMNS = ["source_row_id", "transaction_key", "reject_reason",
                  "value_kes", "raw_row"]

#---------------------------------------------------
SHOCK_VALUES = "(10), (25), (50)"

# Parameters for individual marts
MART_PARAMETERS = {
    "mart_formal_adoption": {
        "min_side_months": 3,
    },

    "mart_borrowing_low_vs_high": {
        "min_band_months": 3,
    },
}