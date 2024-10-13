import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, sum, desc, date_format, make_date, max, concat, when, lpad
from datetime import datetime

# url = "https://data.cityofnewyork.us/resource/7ym2-wayt.json?$limit=25000&$offset=150000"
# response = requests.get(url)
# data = response.json()


# Create a Spark session
spark = SparkSession.builder.appName("WebServerLogs").getOrCreate()


#spark_df = spark.createDataFrame(data)
input_path = "Automated_Traffic_Volume_Counts_20240328.csv"
# input_path = "output"
spark_df = spark.read.csv(input_path, header=True)

def add_day_of_week(df):
    df = df.withColumn("Date", make_date(col("Yr"), col("M"), col("D")))
    df = df.withColumn("DayOfWeek", date_format(col("Date"), "EEEE"))
    df = df.withColumn("MM", when(col("MM") == "0", "00").otherwise(col("MM")))
    df = df.withColumn("HH", lpad(col("HH"), 2, "0"))
    df = df.withColumn("HHMM", concat(col("HH"), col("MM")))
    return df



spark_df = add_day_of_week(spark_df)
spark_df.write.csv("dayofweek", header=True, mode='overwrite')