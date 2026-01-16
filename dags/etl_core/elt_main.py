from pathlib import Path

import pandas as pd
from DB import DB
from etl.Extracter import Extracter
from etl.Transformer import Transformer
from etl.Loader import Loader

RAW_SALES_BASE_NAME = "raw_sales"
DATA_DIR = "data"
UNPROCESSED_FILES_DIR = f"{DATA_DIR}/unprocessed"
PROCESSED_FILES_DIR = f"{DATA_DIR}/processed"
FAILED_FILES_DIR = f"{DATA_DIR}/failed"


def get_csv_files_to_process(dir: str) -> list[Path]:
    """Return a list of all csv files which are valid for analysis"""

    # create Path for directory with unprocessed csv files
    unprocessed_dir = Path(dir)

    # check if directory exists
    if not unprocessed_dir.exists():
        unprocessed_dir.mkdir(parents=True, exist_ok=True)

    file_paths = []

    # go through every file
    for path in unprocessed_dir.iterdir():
        # add file to final list if it is valid for analysis
        if path.name.startswith(RAW_SALES_BASE_NAME) and path.suffix == ".csv":
            file_paths.append(path)
        # notify that file is not valid for analysis
        else:
            print(
                f"File {path.name} in {UNPROCESSED_FILES_DIR} is not valid for analysis. It will be skipped."
            )

    return file_paths


def move_csv_file(destination_dir: str, file_path: Path):
    """Move processed csv file to processed folder"""

    # create Path for directory with unprocessed csv files
    processed_dir = Path(destination_dir)

    # check that destination folder exists
    if not processed_dir.exists():
        processed_dir.mkdir(parents=True, exist_ok=True)

    # move file
    try:
        destination_path = processed_dir / file_path.name
        file_path.replace(destination_path)
    except FileNotFoundError:
        print(f"Error. Source file {file_path.name} doesn't exist")
    except Exception as e:
        print(f"Error {e}")


# if __name__ == "__main__":
#     # files_to_process = get_csv_files_to_process(dir=UNPROCESSED_FILES_DIR)
#     # print(pd.read_csv(files_to_process[0]))
#     try:
#         db = DB()
#         engine = db.get_engine()
#         loader = Loader(engine=engine)
#         files_to_process = get_csv_files_to_process(dir=UNPROCESSED_FILES_DIR)

#         for file in files_to_process:
#             try:
#                 df = Extracter.get_df_by_path(file_path=file)
#                 normalized_df = Transformer.normalize_df(df=df, filename=file.name)
#                 loader.fill_stage_table(df=normalized_df)
#                 loader.handle_pipeline(filename=file.name)

#                 move_csv_file(destination_dir=PROCESSED_FILES_DIR, file_path=file)
#             except Exception as e:
#                 move_csv_file(destination_dir=FAILED_FILES_DIR, file_path=file)
#                 print(f"Fatal error. Skipping {file.name}.")
#                 print(e)

#     except Exception as e:
#         print(e)
