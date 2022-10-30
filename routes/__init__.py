from fastapi import APIRouter, status
from fastapi.responses import JSONResponse, Response
from utils.helpers import get_header_details, load_csv_to_spark_session
from schemas import SQLAPISchema
from kink import di
import os
import json


router = APIRouter()


@router.post("/run-sql")
def run_sql_query(query_details: SQLAPISchema):
    spark_session = di["SPARK_SESSION"]
    with load_csv_to_spark_session(sql_query=query_details.sql_query) as loaded_views:
        df = spark_session.sql(query_details.sql_query)
        result = df.rdd.map(lambda row: row.asDict()).collect()
        return JSONResponse(
            content=result[query_details.offset : min(len(result), query_details.limit)]
        )


@router.post("/get-table-details")
def get_table_details(tablenames: list = []):
    csv_filenames = di["CSV_FILENAMES_DICT"]
    with load_csv_to_spark_session(
        file_names=tablenames or csv_filenames.keys()
    ) as loaded_views:
        table_details = {}
        for tablename in csv_filenames:
            df = loaded_views[tablename]
            table_details[tablename] = {
                "column_details": get_header_details(df),
                "total_row_count": df.count(),
            }
        return table_details
