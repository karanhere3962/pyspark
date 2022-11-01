from fastapi import APIRouter, status, HTTPException
from fastapi.responses import JSONResponse, Response
from utils.helpers import get_header_details, load_csv_to_spark_session
from schemas import SQLAPISchema
from kink import di
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, lit
from pyspark.sql import utils as sputils
import traceback

router = APIRouter()


@router.post("/run-sql")
def run_sql_query(query_details: SQLAPISchema):
    spark_session = di["SPARK_SESSION"]
    with load_csv_to_spark_session(sql_query=query_details.sql_query) as loaded_views:
        try:
            df = spark_session.sql(query_details.sql_query)
            df = df.withColumn("index", row_number().over(Window().orderBy(lit("A"))))
            result = (
                df.filter(
                    (df.index - 1 >= query_details.offset)
                    & (df.index - 1 < query_details.limit)
                )
                .drop("index")
                .rdd.map(lambda row: row.asDict())
                .collect()
            )
        except sputils.AnalysisException as e:
            traceback.print_exc()
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))

        except Exception as e:
            traceback.print_exc()
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
            )
        return JSONResponse(content=result)


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

