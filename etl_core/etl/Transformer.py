from typing import Tuple
import pandas as pd


class Transformer:
    """Class for normalizing DataFrame with data"""

    @staticmethod
    def normalize_df(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Normalize DataFrame: fill empty cells, normalize column names

        :param df: given DataFrame to normalize
        :param filename: filename for csv file from which DataFrame was extracted
        """

        # copy DataFrame
        df = df.copy()

        # DataFrame with failed records
        failed_df = pd.DataFrame([])
        all_failed_list: list[pd.DataFrame] = []

        # set columns to lower case and replace empty spaces and dashes with underscore
        df.columns = df.columns.str.lower().str.replace(" ", "_").str.replace("-", "_")

        # clean DataFrame columns empty cells
        df["price"] = pd.to_numeric(df["price"], errors="coerce")
        df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")

        df["transaction_date"] = pd.to_datetime(
            df["transaction_date"], format="%Y%m%d", errors="coerce"
        )
        df["load_timestamp"] = pd.to_datetime(df["load_timestamp"], errors="coerce")

        # drop duplicates and keep last incoming record
        duplicate_mask = df.duplicated(subset=["transaction_id"], keep="last")
        if duplicate_mask.any():
            duplicated_df = df[duplicate_mask].copy()
            duplicated_df["rejection_reason"] = "duplicated transaction_id"
            all_failed_list.append(duplicated_df)
            df = df[~duplicate_mask]

        # fill rejection_reason column
        for col in df.columns:
            null_mask = df[col].isna()
            if null_mask.any():
                null_df = df[null_mask].copy()
                null_df["rejection_reason"] = f'empty or wrong data in "{col}" column'
                all_failed_list.append(null_df)
                df = df[~null_mask]

        # fill failed DataFrame
        if all_failed_list:
            failed_df = pd.concat(all_failed_list, ignore_index=True)
        else:
            failed_df = pd.DataFrame(columns=[*df.columns, "rejection_reason"])

        # set all columns type as string to avoid MySQL errors
        failed_df = failed_df.astype(str)

        # change date type after all potential error records have been dropped
        df["transaction_date"] = df["transaction_date"].dt.strftime("%Y-%m-%d")

        return df, failed_df
