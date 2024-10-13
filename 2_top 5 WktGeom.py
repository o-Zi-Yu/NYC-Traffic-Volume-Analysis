import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, sum, desc, mean

# url = "https://data.cityofnewyork.us/resource/7ym2-wayt.json?$limit=25000&$offset=150000"
# response = requests.get(url)
# data = response.json()


# Create a Spark session
spark = SparkSession.builder.appName("WebServerLogs").getOrCreate()


#spark_df = spark.createDataFrame(data)
input_path = "dayofweek"
# input_path = "output"
spark_df = spark.read.csv(input_path, header=True)


# find the top 5 WktGeom with most rows
sorted_df = spark_df.groupBy("WktGeom", "SegmentID", "Direction").count()
top_5_values = sorted_df.orderBy(desc("count")).limit(5)

# find the top 5 WktGeom with the highest mean traffic volume
sorted2_df = spark_df.filter(col("Yr") > 2008).groupBy("WktGeom", "SegmentID").agg(mean("vol").alias("MeanVolume"))
top5_mean_volumes = sorted2_df.orderBy(desc("MeanVolume")).limit(5)

top_5_values.show()
top5_mean_volumes.show()

top_5_values.write.csv("top_5_counts", mode="overwrite", header=True)