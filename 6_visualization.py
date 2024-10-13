from pyproj import Proj, transform, Transformer


import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, sum, monotonically_increasing_id
from pyspark.sql.types import FloatType


import folium
import altair as alt
import vega_datasets
import json
import pandas as pd



from wktplot import WKTPlot


def convert_coordinates(df, x, y, from_epsg, to_epsg):
    # Define the projection for EPSG:2263 (NAD83)
    from_proj = Proj(init=f'epsg:{from_epsg}')

    # Define the projection for WGS 84
    to_proj = Proj(init=f'epsg:{to_epsg}')

    # Convert EPSG:2263 to WGS 84
    ll = transform(from_proj, to_proj, x, y)

    return ll

def extract_coordinates(point_str):
    point_str = point_str.replace("POINT(", "").replace(")", "")
    x, y = point_str.split(" ")
    return float(x), float(y)

# # Define the projection for EPSG:2263 (NAD83)
# nad83 = Proj(init='epsg:2263')

# # Define the projection for WGS 84
# wgs84 = Proj(init='epsg:4326')

# # Example coordinate in EPSG:2263 (NAD83) format
# x = [987654.321, 887654.321]
# y = [123456.789, 223456.789]

# # Convert EPSG:2263 to WGS 84
# ll = transform(nad83, wgs84, x, y)

# print(f"Converted to WGS 84: {ll[0]}, {ll[1]}")





# Create a Spark session
spark = SparkSession.builder.appName("WebServerLogs").getOrCreate()


#spark_df = spark.createDataFrame(data)
#input_path = "Automated_Traffic_Volume_Counts_20240328.csv"
input_path = "top_5_counts"
spark_df = spark.read.csv(input_path, header=True)
# spark_df = spark_df.withColumn("id", monotonically_increasing_id())
transformer = Transformer.from_crs("EPSG:2263", "EPSG:4326")

# bronx_df = spark_df.filter(col('Boro').contains("Bronx"))
# brooklyn_df = spark_df.filter(col('Boro').contains("Brooklyn"))
# manhattan_df = spark_df.filter(col('Boro').contains("Manhattan"))
# queens_df = spark_df.filter(col('Boro').contains("Queens"))
# staten_island_df = spark_df.filter(col('Boro').contains("Staten Island"))

def convert_coordinates(df):
    WktGeom_df = df.select("WktGeom").dropDuplicates()
    WktGeom_list = WktGeom_df.rdd.flatMap(lambda x: x).collect()
    WktGeom_df = WktGeom_df.withColumn("id", monotonically_increasing_id()) 
    longitudes = []
    latitudes = []
    for i in range(len(WktGeom_list)):
        point_str = WktGeom_list[i].replace("POINT (", "").replace(")", "")
        x, y = point_str.split(" ")
        x = float(x)
        y = float(y)
        longitudes_x, latitudes_y = transformer.transform(x, y)
        longitudes.append(longitudes_x)
        latitudes.append(latitudes_y)
    # longitudes_df = spark.createDataFrame(enumerate(longitudes), ["id", "longitudes"])
    # longitudes_df = longitudes_df.withColumn("id", monotonically_increasing_id())
    # latitudes_df = spark.createDataFrame(enumerate(latitudes), ["id", "latitudes"])
    # latitudes_df = latitudes_df.withColumn("id", monotonically_increasing_id())
    # WktGeom_df = WktGeom_df.join(longitudes_df, "id").join(latitudes_df, "id")
    # WktGeom_df = WktGeom_df.drop("id")
    return longitudes, latitudes


# longitudes = []
# latitudes = []
# for i in range(len(WktGeom_list)):
#     point_str = WktGeom_list[i].replace("POINT (", "").replace(")", "")
#     x, y = point_str.split(" ")
#     x = float(x)
#     y = float(y)
#     longitudes_x, latitudes_y = transformer.transform(x, y)
#     longitudes.append(longitudes_x)
#     latitudes.append(latitudes_y)
# longitudes_df = spark.createDataFrame(enumerate(longitudes), ["id", "longitudes"])
# longitudes_df = longitudes_df.withColumn("id", monotonically_increasing_id())
# latitudes_df = spark.createDataFrame(enumerate(latitudes), ["id", "latitudes"])
# latitudes_df = latitudes_df.withColumn("id", monotonically_increasing_id())
# WktGeom_df = WktGeom_df.join(longitudes_df, "id").join(latitudes_df, "id")
# WktGeom_df = WktGeom_df.drop("id")

# WktGeom_df.write.csv("demo4_output", header=True, mode='overwrite')


# bronx_df = WktGeom_df.filter(col('Boro').contains("Bronx"))
# brooklyn_df = WktGeom_df.filter(col('Boro').contains("Brooklyn"))
# manhattan_df = WktGeom_df.filter(col('Boro').contains("Manhattan"))
# queens_df = WktGeom_df.filter(col('Boro').contains("Queens"))
# staten_island_df = WktGeom_df.filter(col('Boro').contains("Staten Island"))


def get_coordinates(df):
    longitudes = df.select("longitudes").rdd.flatMap(lambda x: x).collect()
    latitudes = df.select("latitudes").rdd.flatMap(lambda x: x).collect()
    return list(zip(list(set(longitudes)), list(set(latitudes))))

# bronx_df = convert_coordinates(bronx_df)
# brooklyn_df = convert_coordinates(brooklyn_df)
# manhattan_df = convert_coordinates(manhattan_df)
# queens_df = convert_coordinates(queens_df)
# staten_island_df = convert_coordinates(staten_island_df)
station_coordinates = convert_coordinates(spark_df)
# bronx_df.show()

# bronx_coordinates = get_coordinates(bronx_df)
# brooklyn_coordinates = get_coordinates(brooklyn_df)
# manhattan_coordinates = get_coordinates(manhattan_df)
# queens_coordinates = get_coordinates(queens_df)
# staten_island_coordinates = get_coordinates(staten_island_df)


# Select the "WktGeom" column and convert it to a string
#wktgeom_df = spark_df.select("WktGeom").withColumn("WktGeom", col("WktGeom").cast("string"))


# y2011_df = spark_df.filter(col("M") == 1)

# # Write the DataFrame to a CSV file
# output_path = "output_2011"
# y2011_df.write.csv(output_path, header=True, mode='overwrite')

# Read the CSV file back into a new DataFrame
#wktgeom_df = spark.read.csv(output_path, header=True)



# plot = WKTPlot(title="My_first_plot", save_dir="")

# wkt_list = []

# for row in wktgeom_df.collect():
#     wktgeom_string = str(row["WktGeom"])
#     if wktgeom_string not in wkt_list:
#         wkt_list.append(wktgeom_string)
#         plot.add_shape(wktgeom_string, fill_color=(50, 205, 50, 0.25), fill_alpha=0.7, size=30)

# plot.save()




# Create a map centered at a specific location (e.g., New York City)
map_center = [40.7128, -74.0060]
zoom_start = 10
m = folium.Map(location=map_center, zoom_start=zoom_start)

# bronx_group = folium.FeatureGroup("bronx_group").add_to(m)
# brooklyn_group = folium.FeatureGroup("brooklyn_group").add_to(m)
# manhattan_group = folium.FeatureGroup("manhattan_group").add_to(m)
# queens_group = folium.FeatureGroup("queens_group").add_to(m)
# staten_island_group = folium.FeatureGroup("staten_island_group").add_to(m)
station_group = folium.FeatureGroup("station_group").add_to(m)

# coordinates = list(zip(latitudes, longitudes))
# Add a colored marker at a specific location (e.g., New York City)

# def add_marker(coordinates, boro_color, group):
#     for coord in coordinates:
#         folium.Marker(location=coord, icon=folium.Icon(color=boro_color)).add_to(group)

# add_marker(bronx_coordinates, "red", bronx_group)
# add_marker(brooklyn_coordinates, "blue", brooklyn_group)
# add_marker(manhattan_coordinates, "green", manhattan_group)
# add_marker(queens_coordinates, "purple", queens_group)
# add_marker(staten_island_coordinates, "orange", staten_island_group)


station_name = ["Station1", "Station2", "Station3", "Station4", "Station5"]
def station_marker(station_id):
    station1_df = spark.read.csv(station_id, header=True)
    station1_df = station1_df.limit(5000)
    station1_pandas_df = station1_df.toPandas()
    scatter = (
        alt.Chart(station1_pandas_df)
        .mark_bar()
        .encode(
            x=alt.X("Vol:Q", title="Sum of Volume"),
            y=alt.Y("DayOfWeek:N", title="Day of Week"),
        )
    )

    vega_lite = folium.VegaLite(
        scatter,
        width="100%",
        height="100%",
    )
    marker = folium.Marker(location=(station_coordinates[0][station_name.index(station_id)], station_coordinates[1][station_name.index(station_id)]), icon=folium.Icon(color="red")).add_to(station_group)
    popup = folium.Popup()
    vega_lite.add_to(popup)
    popup.add_to(marker)

for i in range(5):
    station_marker(station_name[i])

# Add a colored marker at a specific location (e.g., New York City)


# folium.Marker(location=map_center, popup=marker_label, icon=folium.Icon(color=marker_color)).add_to(m)

# Save the map as an HTML file
m.save('top5_map.html')
