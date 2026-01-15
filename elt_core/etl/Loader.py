import pandas as pd
from sqlalchemy import Engine, text


class Loader:
    """Class for handling loading data to database and handling historical updates"""

    STAGE_TABLE_NAME = "stg_raw_sales"

    def __init__(self, engine: Engine):
        self.engine = engine

    def create_stage_table(self, df: pd.DataFrame):
        """Execute SQL query to fill stage table with relevant file data"""

        with self.engine.begin() as connection:
            # truncate stage table before processing new file
            connection.execute(text(f"TRUNCATE {self.STAGE_TABLE_NAME};"))

            # upload DataFrame to stage table
            df.to_sql(
                name=self.STAGE_TABLE_NAME,
                con=self.engine,
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
            cursor.callproc("sp_load_dim_customers")
            # call procedure for SCD2 for products
            cursor.callproc("sp_load_dim_products")
            # call procedure for fact table
            cursor.callproc("sp_load_fact_sales")

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
