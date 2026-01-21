from pathlib import Path
import pandas as pd

from etl_core.etl.Transformer import Transformer
from etl_core.etl.Extracter import Extracter


class TestTransformer:

    def test_normalize_success(self, raw_sales_file_path: Path):
        """Check that data is normalized properly and DataFrame is returned"""

        # Extract DataFrame from csv file
        df = Extracter.get_df_by_path(file_path=raw_sales_file_path)

        # Normalize DataFrame
        normalized_df, failed_df = Transformer.normalize_df(df=df)

        assert isinstance(normalized_df, pd.DataFrame)
        assert not normalized_df.empty
        assert (
            len(normalized_df) == 3
        )  # triggers when duplicates are not removed by Transformer

        # Check that wrong values in price and quantity columns are handled correctly
        assert normalized_df.iloc[0]["price"] == 10.0
        assert normalized_df.iloc[0]["quantity"] == 13

        # Check that there is no empty cells in DataFrame
        assert normalized_df.isna().sum().sum() == 0

        # Check that every record is unique
        assert normalized_df["transaction_id"].is_unique

        # Check that column types are correct
        assert pd.api.types.is_datetime64_any_dtype(normalized_df["load_timestamp"])
        assert pd.api.types.is_numeric_dtype(normalized_df["price"])
        assert pd.api.types.is_object_dtype(normalized_df["transaction_date"])

    def test_normalize_wrong_transaction_date(self):
        """Check correct work when transaction date is in wrong format"""

        bad_data = pd.DataFrame(
            {
                "transaction_id": [1, 2],
                "transaction_date": ["wrong_date", "20230102"],
                "price": ["100", "200"],
                "customer_id": ["id-1", "id-2"],
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

        df, failed_df = Transformer.normalize_df(bad_data)

        assert df.iloc[0]["transaction_date"] == "2023-01-02"
        assert failed_df.iloc[0]["transaction_date"] == "NaT"
