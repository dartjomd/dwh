import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine
from utils.constants import ETLStatusEnum, TableNameEnum, ProcedureNameEnum


class Loader:
    """Class for handling loading data to database and handling historical updates"""

    def __init__(self, engine: Engine):
        self.engine = engine

    def insert_etl_stats(self, filename: str, error: str, status: ETLStatusEnum):
        """Insert new ETL statistic line"""

        # query to insert statistics line
        query = f"""
                INSERT INTO {TableNameEnum.ETL_STATS.value} (file_name, status, error_message)
                VALUES (%s, %s, %s)
            """
        # insert statistics line
        with self.engine.begin() as connection:
            connection.execute(query, (filename, status, error))

    def fill_stage_table(self, df: pd.DataFrame):
        """Execute SQL query to fill stage table with relevant file data"""

        # check if DataFrame is empty
        if df.empty:
            return

        with self.engine.begin() as connection:
            # truncate stage table before processing new file
            connection.execute(text(f"TRUNCATE {TableNameEnum.STAGE_SALES.value};"))

            # upload DataFrame to stage table
            df.to_sql(
                name=TableNameEnum.STAGE_SALES.value,
                con=connection,
                if_exists="append",
                index=False,
            )

    def upload_failed_records(self, df: pd.DataFrame):
        """Upload failed records from CSV file to database"""

        # check if DataFrame is empty
        if df.empty:
            return

        with self.engine.begin() as connection:
            # upload DataFrame to stage table
            df.to_sql(
                name=TableNameEnum.FAILED_SALES.value,
                con=connection,
                if_exists="append",
                index=False,
            )

    def handle_pipeline(self, filename: str):
        """Start SCD2 for dimensions(products and customers) and fact table"""

        # establish connection
        connection = self.engine.raw_connection()
        cursor = connection.cursor()

        try:
            # call procedure for SCD2 for customers
            cursor.callproc(ProcedureNameEnum.SP_LOAD_DIM_CUSTOMERS.value)
            # call procedure for SCD2 for products
            cursor.callproc(ProcedureNameEnum.SP_LOAD_DIM_PRODUCTS.value)
            # call procedure for fact table
            cursor.callproc(ProcedureNameEnum.SP_LOAD_FACT_SALES.value)

            # commit if no errors occured
            connection.commit()
            print(f"[{filename}] has been successfully handled")

        except Exception as e:
            # rollback in case of error
            connection.rollback()
            print(f"Error while handling {filename}: {e}")
            raise e
        finally:
            # always close connection
            cursor.close()
            connection.close()
