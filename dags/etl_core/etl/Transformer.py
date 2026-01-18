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

        # DataFrame with failed records
        failed_df = pd.DataFrame([])

        # set columns to lower case and replace empty spaces and dashes with underscore
        df.columns = df.columns.str.lower().str.replace(" ", "_").str.replace("-", "_")

        # clean DataFrame columns empty cells
        df["price"] = pd.to_numeric(df["price"], errors="coerce")
        df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
        df["transaction_date"] = (
            df["transaction_date"].astype(str).str.replace("-", "").str.replace("/", "")
        )

        # drop duplicates and keep last incoming record
        df = df.drop_duplicates(subset=["transaction_id"], keep="last")

        # Make sure date is handled correctly
        df["load_timestamp"] = pd.to_datetime(df["load_timestamp"], errors="coerce")

        # check that none of the columns is empty
        for col in df.columns:
            if df[col].isna().sum() > 0:
                raise ValueError(f"Error: Column {col} is empty in a file {filename}")

        return df
