import pytest
from unittest.mock import MagicMock

from dags.etl_core.etl.Loader import Loader


class TestLoader:

    @pytest.fixture
    def mock_loader(self):
        """Returns mock loader instance"""

        mock_db = MagicMock()
        return Loader(engine=mock_db)

    def test_handle_pipeline_success(self, mock_loader):
        """Test that handle pipeline method is executed correctly"""

        # create mock connection to db
        mock_conn = mock_loader.engine.raw_connection.return_value
        mock_cursor = mock_conn.cursor.return_value

        # call method
        mock_loader.handle_pipeline("test_file.csv")

        # check that 3 procedures have been called
        assert mock_cursor.callproc.call_count == 3

        # check that every procedure has been called
        mock_cursor.callproc.assert_any_call("sp_load_dim_customers")
        mock_cursor.callproc.assert_any_call("sp_load_dim_products")
        mock_cursor.callproc.assert_any_call("sp_load_fact_sales")

        # check that everything is commited
        mock_conn.commit.assert_called_once()
        mock_conn.close.assert_called_once()
        mock_cursor.close.assert_called_once()

    def test_handle_pipeline_failure(self, mock_loader):
        """Test that handle pipeline method corretly handles errors"""

        # create mock connection to db
        mock_conn = mock_loader.engine.raw_connection.return_value
        mock_cursor = mock_conn.cursor.return_value

        # imitate error
        mock_cursor.callproc.side_effect = Exception("Database Crash!")

        # call method with error
        with pytest.raises(Exception):
            mock_loader.handle_pipeline("test_file.csv")

        # check that everything is commited
        mock_conn.rollback.assert_called_once()
        mock_conn.close.assert_called_once()
        mock_cursor.close.assert_called_once()

    def test_fill_stage_table(self, mock_loader):
        """Test that method correctly truncates and fills stage table"""

        # create mock connection to db
        mock_conn = mock_loader.engine.begin.return_value.__enter__.return_value

        # mock DataFrame
        df = MagicMock()

        # execute method
        mock_loader.fill_stage_table(df)

        # check TRUNCATE was called
        mock_conn.execute.assert_called_once()
        args, _ = mock_conn.execute.call_args
        assert "TRUNCATE" in str(args[0])

        # check to_sql was called properly
        df.to_sql.assert_called_once()
        _, kwargs = df.to_sql.call_args
        assert kwargs["con"] == mock_conn
