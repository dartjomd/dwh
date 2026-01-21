from datetime import datetime
import logging

from airflow import DAG
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.decorators import task
import pandas as pd

from utils.TelegramAlert import TelegramAlert
from utils.constants import TableNameEnum

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1),
    "retries": 0,
}

# initialize logger
logger = logging.getLogger("airflow.task")


def get_etl_daily_report() -> pd.DataFrame:
    """Get ETL daily report from database"""

    mysql_hook = MySqlHook(mysql_conn_id="mysql_dwh")

    query = f"""
        SELECT `status`, COUNT(*) AS `total`
        FROM `{TableNameEnum.ETL_STATS.value}`
        WHERE DATE(`processed_at`) = CURRENT_DATE
        GROUP BY `status`;
    """

    return mysql_hook.get_pandas_df(query)


with DAG("daily_etl_statistics_dag", default_args=default_args, catchup=False) as dag:

    @task
    def send_etl_daily_report_task():
        """Send ETL daily statistics(failed/success) from database to Telegram"""

        try:
            # get report
            df = get_etl_daily_report()

            # send report
            TelegramAlert.send_etl_report(df=df)

        except Exception as e:
            logger.exception("Unable to send ETL daily report")
            raise Exception(e)

    send_etl_daily_report_task()
