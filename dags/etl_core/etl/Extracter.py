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
        except Exception as e:
            print(f"Error while extracting file {file_path}.")
            raise Exception(f"File was not processed. {e}")
