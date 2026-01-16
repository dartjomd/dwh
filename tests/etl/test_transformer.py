from pathlib import Path
import pandas as pd
import pytest

from dags.etl_core.etl.Transformer import Transformer
from dags.etl_core.etl.Extracter import Extracter


class TestTransformer:

    def test_normalize_success(self, raw_sales_file_path: Path):
        """Check that data is normalized properly and DataFrame is returned"""

        # Extract DataFrame from csv file
        df = Extracter.get_df_by_path(file_path=raw_sales_file_path)

        # Normalize DataFrame
        normalized_df = Transformer.normalize_df(df=df, filename=raw_sales_file_path)

        assert isinstance(normalized_df, pd.DataFrame)
        assert not normalized_df.empty
        assert len(normalized_df) == 4

        # Define required columns
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

        # Check that all required columns in DataFrame are present
        assert required_columns.issubset(normalized_df.columns)

        # Check that wrong values in price and quantity columns are handled correctly
        assert normalized_df.iloc[0]["price"] == 0.0
        assert normalized_df.iloc[0]["quantity"] == 0.0

        # Check that there is no empty cells in DataFrame
        assert normalized_df.isna().sum() == 0

        # Check that every record is unique
        assert normalized_df["transaction_id"].is_unique

        # Check that column types are correct
        assert pd.api.types.is_datetime64_any_dtype(normalized_df["load_timestamp"])
        assert pd.api.types.is_numeric_dtype(normalized_df["price"])
        assert pd.api.types.is_object_dtype(normalized_df["transaction_date"])

    def test_normalize_empty_column(self):
        """Check correct work when data has empty column"""

        bad_data = pd.DataFrame(
            {
                "transaction_id": [1, 2],
                "transaction_date": ["20230101", "20230102"],
                "price": ["100", "200"],
                "quantity": ["1", "2"],
                "city": [None, None],  # empty column
                "first_name": ["Mark", "Alex"],
                "email": ["a@b.com", "c@d.com"],
                "product_id": [10, 11],
                "product_name": ["A", "B"],
                "product_category": ["Cat", "Cat"],
                "load_timestamp": [pd.Timestamp.now(), pd.Timestamp.now()],
            }
        )

        # Check that Transformer raises an error
        with pytest.raises(ValueError, match="is empty in a file"):
            Transformer.normalize_df(bad_data, "sample_raw_sales.csv")

    def test_normalize_wrong_transaction_date(self):
        """Check correct work when transaction date is in wrong format"""

        bad_data = pd.DataFrame(
            {
                "transaction_id": [1, 2],
                "transaction_date": ["2023-01-01 00:00:00", "20230102"],
                "price": ["100", "200"],
                "quantity": ["1", "2"],
                "city": ["Helsinki", "London"],
                "first_name": ["Mark", "Alex"],
                "email": ["a@b.com", "c@d.com"],
                "product_id": [10, 11],
                "product_name": ["A", "B"],
                "product_category": ["Cat", "Cat"],
                "load_timestamp": [pd.Timestamp.now(), pd.Timestamp.now()],
            }
        )

        # Check that Transformer raises an error
        with pytest.raises(ValueError, match="Transaction date is not valid in a file"):
            Transformer.normalize_df(bad_data, "sample_raw_sales.csv")
