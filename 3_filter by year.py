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

top_5_df = spark.read.csv("top_5_counts", header=True)
WktGeom1 = top_5_df.collect()[0]["WktGeom"]
WktGeom2 = top_5_df.collect()[1]["WktGeom"]
WktGeom3 = top_5_df.collect()[2]["WktGeom"]
WktGeom4 = top_5_df.collect()[3]["WktGeom"]
WktGeom5 = top_5_df.collect()[4]["WktGeom"]

Station1_df = spark_df.filter(col("WktGeom") == WktGeom1)
Station2_df = spark_df.filter(col("WktGeom") == WktGeom2)
Station3_df = spark_df.filter(col("WktGeom") == WktGeom3)
Station4_df = spark_df.filter(col("WktGeom") == WktGeom4)
Station5_df = spark_df.filter(col("WktGeom") == WktGeom5)

# Station6_df = spark_df.filter(col("SegmentID") == 193991)
# Station7_df = spark_df.filter(col("SegmentID") == 142386)
# Station8_df = spark_df.filter(col("SegmentID") == 139303)
# Station9_df = spark_df.filter(col("SegmentID") == 193992)
# Station10_df = spark_df.filter(col("SegmentID") == 9012150)

def add_day_of_week(df):
    # Create a new column with the day of the week
    df = df.withColumn("Date", make_date(col("Yr"), col("M"), col("D")))
    # Create a new column with the day of the week
    df = df.withColumn("DayOfWeek", date_format(col("Date"), "EEEE"))
    df = df.withColumn("MM", when(col("MM") == "0", "00").otherwise(col("MM")))
    df = df.withColumn("HH", lpad(col("HH"), 2, "0"))
    df = df.withColumn("HHMM", concat(col("HH"), col("MM")))
    return df

Station1_df = add_day_of_week(Station1_df)
Station2_df = add_day_of_week(Station2_df)
Station3_df = add_day_of_week(Station3_df)
Station4_df = add_day_of_week(Station4_df)
Station5_df = add_day_of_week(Station5_df)

# Station6_df = add_day_of_week(Station6_df)
# Station7_df = add_day_of_week(Station7_df)
# Station8_df = add_day_of_week(Station8_df)
# Station9_df = add_day_of_week(Station9_df)
# Station10_df = add_day_of_week(Station10_df)

Station1_lastest_year = Station1_df.agg(max("Yr").alias("LatestYear")).collect()[0]["LatestYear"]
Station2_lastest_year = Station2_df.agg(max("Yr").alias("LatestYear")).collect()[0]["LatestYear"]
Station3_lastest_year = Station3_df.agg(max("Yr").alias("LatestYear")).collect()[0]["LatestYear"]
Station4_lastest_year = Station4_df.agg(max("Yr").alias("LatestYear")).collect()[0]["LatestYear"]
Station5_lastest_year = Station5_df.agg(max("Yr").alias("LatestYear")).collect()[0]["LatestYear"]


# Write the DataFrame to a CSV file
Station1_df.filter(col("Yr") < Station1_lastest_year).write.csv("Station1", header=True, mode='overwrite')
Station2_df.filter(col("Yr") < Station2_lastest_year).write.csv("Station2", header=True, mode='overwrite')
Station3_df.filter(col("Yr") < Station3_lastest_year).write.csv("Station3", header=True, mode='overwrite')
Station4_df.filter(col("Yr") < Station4_lastest_year).write.csv("Station4", header=True, mode='overwrite')
Station5_df.filter(col("Yr") < Station5_lastest_year).write.csv("Station5", header=True, mode='overwrite')

Station1_df.filter(col("Yr") == Station1_lastest_year).write.csv("Station1_input", header=True, mode='overwrite')
Station2_df.filter(col("Yr") == Station2_lastest_year).write.csv("Station2_input", header=True, mode='overwrite')
Station3_df.filter(col("Yr") == Station3_lastest_year).write.csv("Station3_input", header=True, mode='overwrite')
Station4_df.filter(col("Yr") == Station4_lastest_year).write.csv("Station4_input", header=True, mode='overwrite')
Station5_df.filter(col("Yr") == Station5_lastest_year).write.csv("Station5_input", header=True, mode='overwrite')

# Station6_df.write.csv("Station6", header=True, mode='overwrite')
# Station7_df.write.csv("Station7", header=True, mode='overwrite')
# Station8_df.write.csv("Station8", header=True, mode='overwrite')
# Station9_df.write.csv("Station9", header=True, mode='overwrite')
# Station10_df.write.csv("Station10", header=True, mode='overwrite')

# spark_df = add_day_of_week(spark_df)
# spark_df.write.csv("dayofweek", header=True, mode='overwrite')