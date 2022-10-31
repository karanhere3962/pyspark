from pyspark.sql import SparkSession
from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware
from utils.helpers import generate_mock_data, load_csv_filenames
from routes import router
from kink import di
import pathlib
import os

app = FastAPI()

origins = ["*"]
app.add_middleware(
    CORSMiddleware, allow_origins=origins, allow_methods=["*"], allow_headers=["*"]
)


@app.on_event("startup")
def init_app():
    di["BASE_DIR"] = os.path.dirname(__file__)
    di["CSV_FOLDER_PATH"] = os.path.join(di["BASE_DIR"], "csv_files")
    di["LOADED_VIEWS"] = {}
    di["SPARK_SESSION"] = SparkSession.builder.appName("SparkApp").getOrCreate()
    print("Generating mock data.")
    generate_mock_data()
    print("Completed generating data.")
    load_csv_filenames()


app.include_router(router)
