import csv
import json
from dataclasses import dataclass
from pathlib import Path
import pendulum
import pandas as pd
import shutil
import json

from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import text

# Get PostgreSQL credentials from Airflow
AIRFLOW_CONNECTION_ID = "capstone_db_connect"
DATA_DIR = Path(__file__).resolve().parent / 'files'
STAGING_DIR = DATA_DIR / 'staging'
RAW_TRANSACTIONS_FILE = DATA_DIR / "diaries_transactions_all.csv"

from capstone import extract, transform, load, marts


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

def save_to_staging(result: TransformResult, staging_dir: Path = STAGING_DIR) -> Path:
    """Saves all DataFrames in TransformResult as Parquet files and stats as JSON."""
    staging_dir.mkdir(parents=True, exist_ok=True)
    
    result.fact_transaction.to_parquet(staging_dir / "fact_transaction.parquet", index=False)
    result.fact_account_balance.to_parquet(staging_dir / "fact_account_balance.parquet", index=False)
    result.dim_date.to_parquet(staging_dir / "dim_date.parquet", index=False)
    result.dim_household.to_parquet(staging_dir / "dim_household.parquet", index=False)
    result.dim_account.to_parquet(staging_dir / "dim_account.parquet", index=False)
    result.dim_trx_type.to_parquet(staging_dir / "dim_trx_type.parquet", index=False)
    result.rejects.to_parquet(staging_dir / "rejects.parquet", index=False)
    
    with open(staging_dir / "stats.json", "w") as f:
        json.dump(result.stats, f, default=str)
        
    return staging_dir

def read_from_staging(staging_dir: Path = STAGING_DIR) -> TransformResult:
    """Reads staging files back into a TransformResult dataclass instance."""
    stats_file = staging_dir / "stats.json"
    stats = {}
    if stats_file.exists():
        with open(stats_file, "r") as f:
            stats = json.load(f)
            
    return TransformResult(
        fact_transaction=pd.read_parquet(staging_dir / "fact_transaction.parquet"),
        fact_account_balance=pd.read_parquet(staging_dir / "fact_account_balance.parquet"),
        dim_date=pd.read_parquet(staging_dir / "dim_date.parquet"),
        dim_household=pd.read_parquet(staging_dir / "dim_household.parquet"),
        dim_account=pd.read_parquet(staging_dir / "dim_account.parquet"),
        dim_trx_type=pd.read_parquet(staging_dir / "dim_trx_type.parquet"),
        rejects=pd.read_parquet(staging_dir / "rejects.parquet"),
        stats=stats
    )


@dag(
    dag_id="capstone_etl_process",
    schedule=None,
    start_date=pendulum.datetime(2026, 11, 10, tz="utc"),
    catchup=False,
    tags=["Capstone Project"],
)
def capstone_etl_incremental():

    @task()
    def test_connection():
        try:
            postgres_hook = PostgresHook(postgres_conn_id=AIRFLOW_CONNECTION_ID)
            engine = postgres_hook.get_sqlalchemy_engine()
            with engine.connect() as conn:
                result = conn.execute(text("SELECT 1;")).fetchone()
            print(f"Connection test successful! Result: {result}")
            return True
        except :
            return False

    @task(multiple_outputs=True)
    def extract_data_from_csv(path: Path | str = RAW_TRANSACTIONS_FILE, **context):
        run_id = context['dag_run'].run_id
        run_staging_dir = STAGING_DIR / run_id
        run_staging_dir.mkdir(parents=True, exist_ok=True)

        df, report = extract.extract(path)

        raw_df_path = run_staging_dir / "raw_transactions.parquet"
        df.to_parquet(raw_df_path, index=False)

        # Explicitly delete the dataframe from local memory and clear garbage
        del df
        import gc
        gc.collect()
        return {
            "raw_df_path": str(raw_df_path),
            "report": report
        }
    
    @task()
    def transform_df_to_warehouse(raw_df_path, **context):
        run_id = context['dag_run'].run_id
        dynamic_staging_dir = STAGING_DIR / run_id

        raw_df = pd.read_parquet(raw_df_path)

        #result = transform.transform(raw_df)
        stats = transform.transform_and_stage(raw_df, dynamic_staging_dir)
        # Free up memory
        del raw_df
        import gc
        gc.collect()
        
        return str(dynamic_staging_dir)

    @task()
    def load_into_postgres(staging_dir_str: str, **context):
        run_id = context['dag_run'].run_id
        postgres_hook = PostgresHook(postgres_conn_id=AIRFLOW_CONNECTION_ID)
        staging_path = Path(staging_dir_str)
        result = read_from_staging(staging_path)
        try:
            engine = postgres_hook.get_sqlalchemy_engine()
            return load.load_all(result, run_id, engine=engine, mode="incremental")
        except Exception as e:
            print(e)
            raise

    # @task()
    # def cleanup_staging(staging_dir_str: str):
    #     staging_path = Path(staging_dir_str)
    #     if staging_path.exists():
    #         shutil.rmtree(staging_path)
    #         print(f"Cleaned up staging directory: {staging_path}")

    # # Task execution wiring using TaskFlow data dependencies
    conn_tested = test_connection()
    raw_staging_result = extract_data_from_csv()
    
    # Ensure connection test runs before extraction
    conn_tested >> raw_staging_result
    
    save_file_location = transform_df_to_warehouse(raw_staging_result['raw_df_path'])
    report = load_into_postgres(save_file_location)
    # cleanup_staging(save_file_location)




@dag(
    dag_id="process_data_marts",
    schedule=None,
    start_date=pendulum.datetime(2026, 11, 10, tz="utc"),
    catchup=False,
    tags=["Capstone Project"],
)
def data_marts_processor():
    @task 
    def build_data_marts():
        return marts.build_marts()

capstone_etl_incremental()
data_marts_processor()