from pathlib import Path
import pandas as pd


class Extracter:
    """Class for extracting data from CSV and loading it to STAGE TABLE"""

    @staticmethod
    def get_df_by_path(file_path: Path) -> pd.DataFrame:
        """Extract particular csv file with data by given file path and return it as DataFrame"""

        try:
            # create DataFrame from csv file
            df = pd.read_csv(filepath_or_buffer=file_path, sep=",")
            return df

        # file not found error
        except FileNotFoundError as e:
            raise FileNotFoundError(f"File {file_path} not found. {e}")

        # empty file error
        except pd.errors.EmptyDataError as e:
            raise ValueError(f"File {file_path} is empty. {e}")

        # file is corrupted or wrong format
        except pd.errors.ParserError as e:
            raise ValueError(f"File {file_path} is corrupted or format is wrong. {e}")

        # other errors
        except Exception as e:
            raise Exception(f"File {file_path} was not processed. {e}")
