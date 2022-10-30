from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
import pyspark.sql.functions as F
import os


# sc = SparkContext(
#     conf=SparkConf().setAppName("MyApp").setMaster("spark://localhost:7077")
# )
# os.environ["SPARK_LOCAL_IF"] = "localhost:8080"

BASE_DIR = os.path.dirname(__file__)
CSV_FILE_DIR = os.path.join(BASE_DIR, "csv_files")


spark = SparkSession.builder.appName("SparkApp").getOrCreate()

udp = (
    spark.read.format("csv")
    .option("header", True)
    .load(os.path.join(CSV_FILE_DIR, "udp.csv"))
)
ud = (
    spark.read.format("csv")
    .option("header", True)
    .load(os.path.join(CSV_FILE_DIR, "udp.csv"))
)

udp.createTempView("UserDataProvided")
ud.createTempView("UserDevice")

df = spark.sql(
    "select udp.temperatureBody, ud.uuid, udp.uuid from UserDataProvided udp, UserDevice ud where udp.temperatureBody != 'null' and ud.uuid = udp.uuid"
)
print(udp.printSchema())
print(df.count())
for col in df.dtypes:
    print(col)
