from pathlib import Path
from typing import List
from datetime import datetime, timedelta
import logging

from utils.TelegramAlert import TelegramAlert
from utils.constants import ETLStatusEnum
from etl_core.DB import DB
from etl_core.etl.Extracter import Extracter
from etl_core.etl.Transformer import Transformer
from etl_core.etl.Loader import Loader

from airflow import DAG
from airflow.decorators import task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from setup_gx import GreatExpectationSetup

RAW_SALES_BASE_NAME = "raw_sales"
DATA_DIR = "/opt/airflow/data"
UNPROCESSED_FILES_DIR = f"{DATA_DIR}/unprocessed"
PROCESSED_FILES_DIR = f"{DATA_DIR}/processed"
FAILED_FILES_DIR = f"{DATA_DIR}/failed"

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1),
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": TelegramAlert.send,
}

# initialize logger
logger = logging.getLogger("airflow.task")


def get_csv_files_to_process(dir: str) -> List[Path]:
    """Return a list of all csv files which are valid for analysis"""

    # create Path for directory with unprocessed csv files
    unprocessed_dir = Path(dir)

    # check if directory exists
    if not unprocessed_dir.exists():
        unprocessed_dir.mkdir(parents=True, exist_ok=True)

    file_paths = []

    # go through every file
    for path in unprocessed_dir.iterdir():

        # add file to final list if it is valid for analysis
        if path.name.startswith(RAW_SALES_BASE_NAME) and path.suffix == ".csv":
            file_paths.append(path)

        # notify that file is not valid for analysis and move file to failed directory
        else:
            logger.warning(
                "File %s in %s is not valid for analysis. It will be skipped.",
                path.name,
                UNPROCESSED_FILES_DIR,
            )

    return file_paths


def move_csv_file(destination_dir: str, file_path: Path):
    """Move processed csv file to processed folder"""

    # create Path for directory with unprocessed csv files
    processed_dir = Path(destination_dir)

    # create destination folder if not exists
    if not processed_dir.exists():
        processed_dir.mkdir(parents=True, exist_ok=True)

    # move file
    try:
        destination_path = processed_dir / file_path.name
        file_path.replace(destination_path)
    except FileNotFoundError:
        logger.exception("Error. Source file %s doesn't exist", file_path.name)
    except Exception as e:
        logger.exception("Unexpected error: %s", e)


with DAG(
    "etl_pipeline_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    @task
    def get_files_to_process_task() -> List[str]:
        """Returns list of str of csv files to process"""
        files = get_csv_files_to_process(dir=UNPROCESSED_FILES_DIR)
        return [str(f.absolute()) for f in files]

    @task(max_active_tis_per_dag=1)
    def process_one_file_task(file_path: str):
        """
        Handles ETL pipeline for one particular file at a time.
        In case of error moves file to 'failed' directory and raises error.
        """

        file = Path(file_path)
        db = DB()
        engine = db.get_engine()
        loader = Loader(engine=engine)
        gx_tool = GreatExpectationSetup(conn_id="mysql_dwh")

        try:
            df = Extracter.get_df_by_path(file_path=file)
            normalized_df, failed_df = Transformer.normalize_df(df=df)

            # upload failed records to database
            loader.upload_failed_records(df=failed_df, filename=file.name)

            # fill stage table with this csv file data
            loader.fill_stage_table(df=normalized_df, filename=file.name)

            # validate data using Great Expectations
            gx_tool.run_gx_validation(
                table_name="stg_raw_sales", suite_name="stg_sales_suite"
            )

            # execute SCD2 for products and customers, update fact table
            loader.handle_pipeline(filename=file.name)

            # insert current file process result to etl_stats table
            loader.insert_etl_stats(
                filename=file.name, error="", status=ETLStatusEnum.SUCCESS.value
            )

            move_csv_file(destination_dir=PROCESSED_FILES_DIR, file_path=file)

        except Exception as e:

            # insert current file process result to etl_stats table
            loader.insert_etl_stats(
                filename=file.name, error=str(e), status=ETLStatusEnum.FAILED.value
            )

            move_csv_file(destination_dir=FAILED_FILES_DIR, file_path=file)

            raise Exception(f"Skipping {file.name}. Error: {e}")

    # Establish relationship
    files_list = get_files_to_process_task()
    process_files = process_one_file_task.expand(file_path=files_list)

    # Trigger data analysis dag
    trigger_analytics = TriggerDagRunOperator(
        task_id="trigger_analytics_dag",
        trigger_dag_id="data_analytics_dag",
        wait_for_completion=False,
    )

    process_files >> trigger_analytics
