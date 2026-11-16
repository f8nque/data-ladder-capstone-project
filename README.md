# Kenya Financial Diaries ETL

    raw_data/diaries_transactions_all.csv  ->  extract -> transform -> load
        load = stage (schema "staging", rebuilt every run) -> merge into schema "warehouse"
               facts: append-only    dimensions: insert, or update on conflict when row_hash changed

| Path | Purpose |
|---|---|
| `config.py` | all file names, columns, mappings and thresholds |
| `extract.py`, `transform.py`, `load.py`, `utils.py` | the three stages and shared helpers |
| `schema.sql`, `mart_schemas` | warehouse,stage, marts tables with primary and foreign keys |
| `pipeline.py` | one-command local runner (`python etl/pipeline.py [--load]`) |
| `marts.py` | called by airflow to create data marts |
| `capstone_etl_dags.py` | the Airflow DAG: extract >> transform >> load >> report |
| `tests/` | pytest suite (load tests need PostgreSQL, DAG tests need Airflow; both skip if absent) |



## Run pytests
    python -m pytest tests -v

## Install capstone module

    python -m venv .venv && . .venv/bin/activate
    
    pip install .
    
Database: set `KFD_DATABASE_URL` (for example `postgresql+psycopg2://user:pass@host:5432/kfd`).

###  Airflow (tested with Airflow 3.4.0 on Python 3.14.4)

##  Airflow Installation

    pip install --upgrade pip

    AIRFLOW_VERSION=3.4.0
    PYTHON_VERSION="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
    CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
    pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

    export AIRFLOW_HOME=$PWD/airflow_home
    export AIRFLOW__CORE__DAGS_FOLDER=$PWD/dags
    export AIRFLOW__CORE__LOAD_EXAMPLES=False
    export AIRFLOW_CONN_KFD_POSTGRES='postgresql://USER:PASSWORD@HOST:5432/DBNAME'

    airflow db migrate
    airflow dags test kfd_pipeline      # one full run in the terminal
    airflow standalone                  # scheduler + UI on http://localhost:8080; trigger "kfd_pipeline"
    
    
## Login and Dag Runs
    navigate to localhost:8080 
    cat ~/airflow/simple_auth_manager_passwords.json.generated to get the username and password
    copy capstone_etl_dags.py to ~/airflow/dags 
    
    navigate to the 

Run parameters (set when triggering): `input_path`, `allow_corrupt_ids` (testing only),
`full_refresh` (rebuild the warehouse tables instead of merging).
To schedule the DAG set `KFD_SCHEDULE` (for example `@weekly`) before starting Airflow.
Use `--full-refresh` / `full_refresh` when a cleaning rule or the key definition changes, when
source rows were corrected (a corrected row has a new key, so a merge appends it and keeps the old
one), and for the first run after upgrading from an older table layout.
Database precedence: Airflow connection `kfd_postgres`, then `KFD_DATABASE_URL`.
`dags test` was run for this project; `airflow standalone` and the UI were not.

## Troubleshooting

`LoadError: The warehouse tables do not match the code ... lacks [...]` (or a raw PostgreSQL
`column "row_hash" does not exist`): the code and the table definitions come from different
versions. Copy the current `etl/schema.sql` next to `load.py` and `transform.py`; if the database
still holds an older layout, run once with `--full-refresh`.