# airflow_ignore_file

# import logging
from typing import Dict
from airflow.providers.telegram.hooks.telegram import TelegramHook


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

        try:
            task_info = TelegramAlert._get_task_info(context)

            message = (
                f"❌ <b>Pipeline Failure</b>\n\n"
                f"<b>DAG:</b> <code>{task_info['dag_id']}</code>\n"
                f"<b>Task:</b> {task_info['task_id']}\n"
                f"<b>Error:</b> {str(task_info['exception'])[:200]}...\n\n"
                f"🔗<b>View Logs</b><code>{task_info['log_url']}</code>"
            )

            hook = TelegramHook(telegram_conn_id="telegram_conn_id")
            hook.send_message({"text": message, "parse_mode": "HTML"})
            # logging.info("Telegram alert sent successfully.")

        except Exception as e:
            # logging.error(f"Failed to send Telegram alert: {e}")
            print(e)
