from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from datetime import datetime, timedelta

# Настройки по умолчанию
default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Описание самого процесса (DAG)
with DAG(
    "check_my_dwh_connection",  # Имя, которое ты увидишь в интерфейсе
    default_args=default_args,
    schedule_interval=None,  # Запуск только вручную кнопкой
    catchup=False,
) as dag:

    # Задача: просто считаем количество строк в одной из твоих таблиц
    task_check_db = MySqlOperator(
        task_id="count_products",
        mysql_conn_id="mysql_dwh",  # ВАЖНО: это ID, который ты только что сохранил в Admin -> Connections
        sql="SELECT COUNT(*) FROM dim_product;",  # Замени dim_product на любую свою таблицу
    )

    task_check_db
