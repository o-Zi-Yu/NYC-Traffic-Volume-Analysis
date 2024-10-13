

import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, sum, desc, date_format, make_date, mean, stddev
from datetime import datetime
import csv
import os

# url = "https://data.cityofnewyork.us/resource/7ym2-wayt.json?$limit=25000&$offset=150000"
# response = requests.get(url)
# data = response.json()



# Create a Spark session
spark = SparkSession.builder.appName("WebServerLogs").getOrCreate()

global normal_counter, abnormal_counter
normal_counter = 0
abnormal_counter = 0

#spark_df = spark.createDataFrame(data)
input_path = "Station4"
# input_path = "output"
spark_df = spark.read.csv(input_path, header=True)
static_df = spark.read.option("header", "true").csv(input_path)
schema = static_df.schema
streaming_df = spark.readStream.schema(schema).option("header", "true").csv(input_path+"_input")

vol_dict = {}
# query = streaming_df.writeStream.outputMode("append").format("console").start()


def anomaly_detection(batch_df, batch_id):
    global normal_counter, abnormal_counter
    for row in batch_df.collect():
        day_of_week = row["DayOfWeek"]
        hour = row["HH"]
        minute = row["MM"]
        volume = int(row["Vol"])
        date = row["Date"]

        dict_name = str(day_of_week)+str(hour)+str(minute)

        if dict_name not in vol_dict:
            matching_rows = static_df.filter((col("DayOfWeek") == day_of_week) & (col("HH") == hour) & (col("MM") == minute))
            vol_std = matching_rows.agg(stddev("Vol").alias("VolumeStdDev")).collect()[0]["VolumeStdDev"]
            vol_mean = matching_rows.agg(mean("Vol").alias("VolumeMean")).collect()[0]["VolumeMean"]
            vol_dict[dict_name] = (vol_mean, vol_std)
        if volume > vol_dict[dict_name][0] + 2 * vol_dict[dict_name][1] or volume < vol_dict[dict_name][0] - 2 * vol_dict[dict_name][1]:
            abnormal_counter += 1
            print("Anomaly Detected! Volume: ", volume, "Mean: ", vol_dict[dict_name][0], "StdDev: ", vol_dict[dict_name][1], "Anomaly Counter: ", abnormal_counter)
            with open(input_path+"_Anomaly.csv", mode='a', newline='') as file:
                writer = csv.DictWriter(file, fieldnames=["Date", "HH", "MM", "DayOfWeek", "Vol"])
                if file.tell() == 0:
                    writer.writeheader()
                data = [{"Date": date, "HH": hour, "MM": minute, "DayOfWeek": day_of_week, "Vol": volume}]
                for row in data:
                    writer.writerow(row)
        else:
            normal_counter += 1
            print("No Anomaly Detected. Volume: ", volume, "Mean: ", vol_dict[dict_name][0], "StdDev: ", vol_dict[dict_name][1], "Normal Counter: ", normal_counter)

        # Show the matching rows
        # matching_rows.show()

os.remove(input_path+"_Anomaly.csv")

# Start the streaming query to process each micro-batch
query = streaming_df.writeStream \
    .trigger(processingTime="10 seconds") \
    .outputMode("append") \
    .format("console") \
    .foreachBatch(anomaly_detection) \
    .option("maxFilesPerTrigger", 1) \
    .start()

query.awaitTermination()

# Stop the Spark session
spark.stop()

