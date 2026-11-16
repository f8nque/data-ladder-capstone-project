-- Star schema for the Kenya Financial Diaries warehouse.
--
-- Tables are created IF NOT EXISTS and then maintained by load.py:
--   dimensions  insert new rows, update existing rows when row_hash changed
--   facts       append only (rows that already exist are never touched)
-- A full refresh (load.py mode "full_refresh") drops and recreates them.
-- etl_run_log is never dropped.

CREATE TABLE IF NOT EXISTS dim_date (
    date_key      integer PRIMARY KEY,          -- yyyymmdd
    full_date     date    NOT NULL UNIQUE,
    year          smallint NOT NULL,
    month         smallint NOT NULL,
    year_month    text    NOT NULL,
    week_start    date    NOT NULL,
    quarter       smallint NOT NULL,
    day_of_week   text    NOT NULL,
    is_month_end  boolean NOT NULL,
    season        text
);

CREATE TABLE IF NOT EXISTS dim_household (
    household_key   text PRIMARY KEY,           -- md5(hh_id)
    hh_id           text NOT NULL UNIQUE,       -- natural key
    first_trx_date  date,
    last_trx_date   date,
    days_observed   integer,
    months_observed integer,
    site            text,
    county          text,
    row_hash        text NOT NULL,              -- md5 of the attributes: change detection
    updated_run_id  text,
    updated_at      timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dim_account (
    account_key        text PRIMARY KEY,        -- md5(hh_id|account_id)
    hh_id              text NOT NULL REFERENCES dim_household (hh_id),
    account_id         text NOT NULL,
    owner_member_id    text,
    bsheet_designation text,
    formality          text,
    liquidity          text,
    first_trx_date     date,
    last_trx_date      date,
    row_hash           text NOT NULL,
    updated_run_id     text,
    updated_at         timestamptz NOT NULL DEFAULT now(),
    UNIQUE (hh_id, account_id)                  -- natural (composite) key
);

CREATE TABLE IF NOT EXISTS dim_trx_type (
    trx_type_key        text PRIMARY KEY,       -- md5(type_code)
    type_code           text NOT NULL UNIQUE,   -- natural key
    type_desc           text,
    family_code         text,
    family_desc         text,
    class_code          text,
    class_desc          text,
    income_source_group text,
    instrument_group    text,
    row_hash            text NOT NULL,
    updated_run_id      text,
    updated_at          timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS fact_transaction (
    transaction_key   text PRIMARY KEY,         -- md5 of all raw columns (the row hash)
    source_row_id     bigint NOT NULL,
    source_trx_id     text,
    household_key     text NOT NULL REFERENCES dim_household (household_key),
    account_key       text NOT NULL REFERENCES dim_account (account_key),
    date_key          integer NOT NULL REFERENCES dim_date (date_key),
    trx_type_key      text NOT NULL REFERENCES dim_trx_type (trx_type_key),
    member_id         text,
    mode_desc         text,
    is_cash           boolean NOT NULL,
    is_digital        boolean NOT NULL,
    purpose_fd        text,
    direction         text CHECK (direction IN ('Inflow', 'Outflow')),
    bsheet_direction  text CHECK (bsheet_direction IN ('Increase', 'Decrease')),
    value_kes         numeric(16, 2) NOT NULL CHECK (value_kes >= 0),
    inkind_value_kes  numeric(16, 2),
    fee_kes           numeric(16, 2),
    distance_km       numeric(10, 3),
    is_gift           boolean NOT NULL,
    flow_category     text NOT NULL,
    operating_net_kes numeric(16, 2) NOT NULL,
    include_in_flow   boolean NOT NULL,
    dq_flags          text NOT NULL DEFAULT '',
    trx_dq_round      integer,
    loaded_run_id     text NOT NULL,            -- the run that first inserted this row
    loaded_at         timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS fact_account_balance (
    balance_key    text PRIMARY KEY,
    source_row_id  bigint NOT NULL,
    account_key    text NOT NULL REFERENCES dim_account (account_key),
    household_key  text NOT NULL REFERENCES dim_household (household_key),
    date_key       integer NOT NULL REFERENCES dim_date (date_key),
    snapshot_type  text NOT NULL CHECK (snapshot_type IN ('start', 'close', 'cash_on_hand')),
    balance_kes    numeric(16, 2) NOT NULL,
    loaded_run_id  text NOT NULL,
    loaded_at      timestamptz NOT NULL DEFAULT now()
);

-- One batch of rejects per run; re-running a run id replaces that run's rows.
CREATE TABLE IF NOT EXISTS etl_rejects (
    reject_id       bigserial PRIMARY KEY,
    run_id          text NOT NULL,
    source_row_id   bigint,
    transaction_key text,
    reject_reason   text NOT NULL,
    value_kes       numeric(16, 2),
    raw_row         text
);

-- @indexes
-- Everything below is run after load.py has checked that the tables above have
-- the columns the code writes, so an older table layout gives a clear message.
CREATE INDEX IF NOT EXISTS ix_fact_trx_household_date ON fact_transaction (household_key, date_key);
CREATE INDEX IF NOT EXISTS ix_fact_trx_type ON fact_transaction (trx_type_key);
CREATE INDEX IF NOT EXISTS ix_fact_trx_run ON fact_transaction (loaded_run_id);
CREATE INDEX IF NOT EXISTS ix_etl_rejects_run ON etl_rejects (run_id);