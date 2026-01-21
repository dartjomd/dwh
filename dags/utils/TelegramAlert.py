# airflow_ignore_file

import logging
from datetime import datetime
from typing import Dict

from airflow.providers.telegram.hooks.telegram import TelegramHook
import pandas as pd
from utils.constants import ETLStatusEnum

# initialize logger
logger = logging.getLogger(__name__)


class TelegramAlert:

    @staticmethod
    def _get_task_info(context) -> Dict[str, str]:
        """Return task info as a dict"""

        ti = context.get("task_instance")

        return {
            "dag_id": ti.dag_id,
            "task_id": ti.task_id,
            "exception": context.get("exception"),
            "log_url": ti.log_url,
        }

    @staticmethod
    def send(context):
        """Callback function to send telegram alert if Great Expectations failed"""

        # retrieve data
        task_info = TelegramAlert._get_task_info(context)

        # form message
        message = (
            f"❌ <b>Pipeline Failure</b>\n\n"
            f"<b>DAG:</b> <code>{task_info['dag_id']}</code>\n"
            f"<b>Task:</b> {task_info['task_id']}\n"
            f"<b>Error:</b> {str(task_info['exception'])[:300]}...\n\n"
            f"🔗<b>View Logs</b><code>{task_info['log_url']}</code>"
        )

        # send message and log it
        hook = TelegramHook(telegram_conn_id="telegram_conn_id")
        hook.send_message({"text": message, "parse_mode": "HTML"})
        logging.info("Telegram pipepine fail report sent successfully.")

    @staticmethod
    def send_etl_report(df: pd.DataFrame):
        """Function to send daily ETL reports"""

        # convert DataFrame to dict
        data = dict(zip(df["status"], df["total"]))

        # retrieve certain data from dict
        date_now = datetime.now().strftime("%Y-%m-%d")
        success_count = data.get(ETLStatusEnum.SUCCESS.value, 0)
        failed_count = data.get(ETLStatusEnum.FAILED.value, 0)
        total = success_count + failed_count

        # form message
        message = (
            f"📊 <b>ETL Daily Report</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"📅 Date: <code>{date_now}</code>\n\n"
            f"✅ <b>Success:</b> {success_count}\n"
            f"❌ <b>Failed:</b> {failed_count}\n"
            f"🔹 <b>Total:</b> {total}\n\n"
            f"{'🚀 Everything is correct!' if failed_count == 0 else '⚠️ ERROR!'}"
        )

        # send message and log it
        hook = TelegramHook(telegram_conn_id="telegram_conn_id")
        hook.send_message({"text": message, "parse_mode": "HTML"})
        logging.info("Telegram daily ETL report sent successfully.")
