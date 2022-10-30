from sql_metadata import Parser
from kink import di
from uuid import uuid4
from contextlib import contextmanager
import os


@contextmanager
def load_csv_to_spark_session(sql_query: str = None, file_names: list = []):
    if sql_query:
        file_names += Parser(sql_query).tables

    csv_filenames = di["CSV_FILENAMES_DICT"]
    spark = di["SPARK_SESSION"]
    csv_folder_path = di["CSV_FOLDER_PATH"]

    for filename in file_names:
        if filename.lower() in csv_filenames:
            df = (
                spark.read.format("csv")
                .option("header", True)
                .load(os.path.join(csv_folder_path, csv_filenames[filename]))
            )
            df.createOrReplaceTempView(filename)
            di["LOADED_VIEWS"][filename] = df
    try:
        yield di["LOADED_VIEWS"]
    except Exception as e:
        raise e
    finally:
        for filename in file_names:
            if filename in di["LOADED_VIEWS"]:
                del di["LOADED_VIEWS"][filename]
    return file_names


def get_header_details(df):
    return {col[0]: col[1] for col in df.dtypes}


def generate_mock_data():
    csv_folder_path = di["CSV_FOLDER_PATH"]

