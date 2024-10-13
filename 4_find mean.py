import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, sum, desc, date_format, make_date, mean, stddev
from datetime import datetime

# url = "https://data.cityofnewyork.us/resource/7ym2-wayt.json?$limit=25000&$offset=150000"
# response = requests.get(url)
# data = response.json()


# Create a Spark session
spark = SparkSession.builder.appName("WebServerLogs").getOrCreate()


#spark_df = spark.createDataFrame(data)
input_path = "Station1"
# input_path = "output"
spark_df = spark.read.csv(input_path, header=True)

monday_volume_Mean = spark_df.filter(col("DayOfWeek") == "Monday").agg(mean("Vol").alias("MondayVolumeMean")).collect()[0]["MondayVolumeMean"]
tuesday_volume_Mean = spark_df.filter(col("DayOfWeek") == "Tuesday").agg(mean("Vol").alias("TuesdayVolumeMean")).collect()[0]["TuesdayVolumeMean"]
wednesday_volume_Mean = spark_df.filter(col("DayOfWeek") == "Wednesday").agg(mean("Vol").alias("WednesdayVolumeMean")).collect()[0]["WednesdayVolumeMean"]
thursday_volume_Mean = spark_df.filter(col("DayOfWeek") == "Thursday").agg(mean("Vol").alias("ThursdayVolumeMean")).collect()[0]["ThursdayVolumeMean"]
friday_volume_Mean = spark_df.filter(col("DayOfWeek") == "Friday").agg(mean("Vol").alias("FridayVolumeMean")).collect()[0]["FridayVolumeMean"]
saturday_volume_Mean = spark_df.filter(col("DayOfWeek") == "Saturday").agg(mean("Vol").alias("SaturdayVolumeMean")).collect()[0]["SaturdayVolumeMean"]
sunday_volume_Mean = spark_df.filter(col("DayOfWeek") == "Sunday").agg(mean("Vol").alias("SundayVolumeMean")).collect()[0]["SundayVolumeMean"]

matching_rows = spark_df.filter((col("DayOfWeek") == "Monday") & (col("HH") == 12) & (col("MM") == 00))

print("Monday mean traffic volume: ", monday_volume_Mean)
print("Tuesday mean traffic volume: ", tuesday_volume_Mean)
print("Wednesday mean traffic volume: ", wednesday_volume_Mean)
print("Thursday mean traffic volume: ", thursday_volume_Mean)
print("Friday mean traffic volume: ", friday_volume_Mean)
print("Saturday mean traffic volume: ", saturday_volume_Mean)
print("Sunday mean traffic volume: ", sunday_volume_Mean)
matching_rows.show()
