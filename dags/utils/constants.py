# airflow_ignore_file

from enum import Enum


class ETLStatusEnum(Enum):
    SUCCESS = "success"
    FAILED = "failed"


class TableNameEnum(Enum):
    STAGE_SALES = "stg_raw_sales"
    FAILED_SALES = "failed_sales"
    FACT_SALES = "fact_sales"
    DIM_CUSTOMERS = "dim_customers"
    DIM_PRODUCTS = "dim_products"
    ETL_STATS = "etl_stats"


class ProcedureNameEnum(Enum):
    SP_LOAD_DIM_CUSTOMERS = "sp_load_dim_customers"
    SP_LOAD_DIM_PRODUCTS = "sp_load_dim_products"
    SP_LOAD_FACT_SALES = "sp_load_fact_sales"
