from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "data_analytics_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    template_searchpath="/opt/airflow/dags/analytics_sql",
) as dag:
    # Create data marts and views for all analysis methods
    prepare_tables_and_views = MySqlOperator(
        task_id="prepare_tables_and_views",
        mysql_conn_id="mysql_dwh",
        sql="prepare_tables_and_views.sql",
        autocommit=True,
    )

    # Run aggregation by category analysis
    execute_category_sales_analysis = MySqlOperator(
        task_id="execute_category_sales_analysis",
        mysql_conn_id="mysql_dwh",
        sql="info_by_category.sql",
    )

    # Run abc analysis
    execute_abc_analysis = MySqlOperator(
        task_id="run_abc_analysis",
        mysql_conn_id="mysql_dwh",
        sql="abc_analysis.sql",
    )

    # Run price elasticity analysis
    execute_price_elasticity_analysis = MySqlOperator(
        task_id="execute_price_elasticity_analysis",
        mysql_conn_id="mysql_dwh",
        sql="price_elasticity_SCD2.sql",
    )

    # Run customer retention rate analysis
    execute_customer_retention_rate_analysis = MySqlOperator(
        task_id="execute_customer_retention_rate_analysis",
        mysql_conn_id="mysql_dwh",
        sql="customer_retention_rate.sql",
    )

    # Run revenue by city analysis
    execute_revenue_by_city_analysis = MySqlOperator(
        task_id="execute_revenue_by_city_analysis",
        mysql_conn_id="mysql_dwh",
        sql="revenue_by_city.sql",
    )

    prepare_tables_and_views >> [
        execute_abc_analysis,
        execute_category_sales_analysis,
        execute_price_elasticity_analysis,
        execute_customer_retention_rate_analysis,
        execute_revenue_by_city_analysis,
    ]
