from pyspark.sql import SparkSession
from fastapi import FastAPI
from routes import router
from kink import di
import pathlib
import os

app = FastAPI()


@app.on_event("startup")
def init_app():
    di["BASE_DIR"] = os.path.dirname(__file__)
    di["CSV_FOLDER_PATH"] = os.path.join(di["BASE_DIR"], "csv_files")
    di["CSV_FILENAMES_DICT"] = {
        pathlib.Path(str(filename).lower()).stem: os.path.basename(str(filename))
        for filename in list(pathlib.Path(di["CSV_FOLDER_PATH"]).glob(r"*.csv"))
    }
    di["LOADED_VIEWS"] = {}
    di["SPARK_SESSION"] = SparkSession.builder.appName("SparkApp").getOrCreate()


app.include_router(router)
