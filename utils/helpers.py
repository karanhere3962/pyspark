from sql_metadata import Parser
from kink import di
from uuid import uuid4
from contextlib import contextmanager
from faker import Faker
import os
import random
import csv
import pathlib


def load_csv_filenames():
    di["CSV_FILENAMES_DICT"] = {
        pathlib.Path(str(filename).lower()).stem: os.path.basename(str(filename))
        for filename in list(pathlib.Path(di["CSV_FOLDER_PATH"]).glob(r"*.csv"))
    }


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
                .option("inferSchema", "true")
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
    rows = []
    for file_id in range(1, 4):
        file_name = f"csv{file_id}.csv"
        if not os.path.exists(os.path.join(csv_folder_path, file_name)):
            rows = []
            for row_id in range(1000000):
                row = []
                for column_id in range(1, 51):
                    if row_id == 0:
                        row.append(f"Column_{column_id}")
                    else:
                        if column_id == 1:
                            row.append(row_id + 1)
                        else:
                            row.append(random.randint(0, 10000000))

                rows.append(row)
            with open(os.path.join(csv_folder_path, file_name), "w") as file:
                csvwriter = csv.writer(file)
                csvwriter.writerows(rows)
