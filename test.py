from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
import pyspark.sql.functions as F
import os
from faker import Faker
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, lit

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
# df = df.withColumn("index", row_number().over(Window().orderBy(lit("A"))))
# print(df.filter((df.index > 2) & (df.index < 100)).rdd.collect())
df = df.rdd.zipWithIndex().toDF()
print(df.show())
for index, row in enumerate(df.rdd.toLocalIterator()):
    print(row._2)
    print(row._1.asDict())

print(udp.printSchema())
print(df.count())
# for col in df.dtypes:
#     print(col)
# import secrets

# mail_list = {}
# collisions = 0
# eng_dialects = [
#     "en-US",
#     "ko",
#     "fr-CA",
#     "de-DE",
#     "ga",
#     "it",
#     "el",
#     "zh",
#     "en-IN",
#     "cs",
#     "nl",
#     "id",
#     "lt",
#     "en-NZ",
# ]
# fake_data_generator = [Faker(dialect) for dialect in eng_dialects]
# for i in range(1000000):
#     fake_data = secrets.choice(fake_data_generator).profile()
#     if fake_data.get("mail") in mail_list:
#         collisions += 1
#         print(fake_data.get("mail"), i, collisions)
#     mail_list[fake_data.get("mail")] = fake_data

# print("Collisions : ", collisions)
