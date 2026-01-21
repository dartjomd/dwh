from pathlib import Path
import pytest

from etl_core.etl.Extracter import Extracter


class TestExtracter:

    def test_good_file(self, raw_sales_file_path: str):
        """Check that extractor works correctly with normal files"""

        df = Extracter.get_df_by_path(file_path=raw_sales_file_path)

        assert not df.empty
        assert len(df) == 5

    def test_file_not_found(self):
        """Check that extractor raises an error when file is not found"""

        with pytest.raises(FileNotFoundError, match="not found"):
            Extracter.get_df_by_path(file_path="wrong_file_path.csv")

    def test_file_is_empty(self, tmp_path: Path):
        """Check that extractor raises an error when file is empty"""

        empty_file = tmp_path / "empty_data.csv"
        empty_file.write_text("")

        with pytest.raises(ValueError, match="is empty"):
            Extracter.get_df_by_path(file_path=empty_file)

    def test_file_is_corrupted_or_format_is_wrong(self, tmp_path: Path):
        """Check that extractor raises an error when file corrupted or format is wrong"""

        corrupted_file = tmp_path / "corrupted_file.csv"
        corrupted_file.write_bytes(b"\x00\xff\xfe\xfd")

        with pytest.raises(Exception, match="was not processed"):
            Extracter.get_df_by_path(file_path=corrupted_file)
