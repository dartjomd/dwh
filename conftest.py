from pathlib import Path
import pytest


@pytest.fixture
def raw_sales_file_path() -> Path:
    """Returns path to test raw sales file"""
    current_dir = Path(__file__).parent
    return current_dir / "tests" / "data" / "sample_raw_sales.csv"
