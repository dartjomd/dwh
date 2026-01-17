import pandas as pd
from sqlalchemy.engine import Engine


class Transformer:
    """Class for normalizing DataFrame with data"""

    @staticmethod
    def normalize_df(df: pd.DataFrame, filename: str) -> pd.DataFrame:
        """
        Normalize DataFrame: fill empty cells, normalize column names

        :param df: given DataFrame to normalize
        :filename: filename for csv file from which DataFrame was extracted
        """

        # copy DataFrame
        df = df.copy()

        # set columns to lower case and replace empty spaces and dashes with underscore
        df.columns = df.columns.str.lower().str.replace(" ", "_").str.replace("-", "_")

        # required columns list
        required_columns = {
            "transaction_id",
            "transaction_date",
            "customer_id",
            "first_name",
            "city",
            "email",
            "product_id",
            "product_name",
            "product_category",
            "price",
            "quantity",
            "load_timestamp",
        }

        # check that file has all necessary columns
        if not required_columns.issubset(set(df.columns)):
            raise ValueError(
                f"Error: Columns {required_columns - set(df.columns)} are not present in a file {filename}"
            )

        # clean DataFrame columns empty cells
        df["price"] = pd.to_numeric(df["price"], errors="coerce")
        df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
        df["price"] = df["price"].fillna(0.0)
        df["quantity"] = df["quantity"].fillna(0).astype(int)
        df["transaction_date"] = (
            df["transaction_date"].astype(str).str.replace("-", "").str.replace("/", "")
        )

        # drop duplicates and keep last incoming record
        df = df.drop_duplicates(subset=["transaction_id"], keep="last")

        # check that transaction date column is in format YYYYMMDD
        if not df["transaction_date"].str.match(r"^\d{8}$").all():
            raise ValueError(
                f"Error: Transaction date is not valid in a file {filename}"
            )

        # Make sure date is handled correctly
        df["load_timestamp"] = pd.to_datetime(df["load_timestamp"])

        # check that none of the columns is empty
        for col in df.columns:
            if df[col].isna().sum() > 0:
                raise ValueError(f"Error: Column {col} is empty in a file {filename}")

        return df
