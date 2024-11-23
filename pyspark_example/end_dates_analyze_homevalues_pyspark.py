import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, min, max, lit, explode, struct, array, when, first, last
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder \
    .appName("HomeValuesAnalysis") \
    .getOrCreate()

# Check if the input file path is provided as a command-line argument
if len(sys.argv) != 2:
    print("Usage: spark-submit <script>.py <input_csv_file>")
    sys.exit(1)

input_csv_file = sys.argv[1]

# Read the CSV file into a DataFrame
df = spark.read.csv("home_data/homevalues_original.csv", header=True, inferSchema=True)

# Filter out rows with any empty home value columns
home_value_columns = [c for c in df.columns if c.startswith('20')]
df = df.filter(
    ~(
        sum([when(col(c).isNull() | (col(c) == 0) | (col(c) == ''), 1).otherwise(0) for c in home_value_columns]) > 0
    )
)

# Select metadata columns and create the regions DataFrame
regions_df = df.select(
    col("RegionID").cast("int"),
    col("SizeRank").cast("int"),
    col("RegionName"),
    col("RegionType"),
    col("StateName"),
    col("State"),
    col("Metro"),
    col("CountyName")
)

# Select home values columns and create the home_values DataFrame
home_values_df = df.select(
    col("RegionID").cast("int"),
    *[col(c).alias(c.replace('-', '_')).cast("decimal(18, 2)") for c in home_value_columns]
)

# Melt the home_values DataFrame to have RegionID, Date, and Value columns
home_values_melted_df = home_values_df.select(
    col("RegionID"),
    explode(
        array(*[struct(lit(c.replace('_', '-')).alias("Date"), col(c).alias("Value")) for c in home_values_df.columns if c != "RegionID"])
    ).alias("DateValue")
).select(
    col("RegionID"),
    col("DateValue.Date"),
    col("DateValue.Value")
)

# Define window specification to order by Date within each RegionID
window_spec = Window.partitionBy("RegionID").orderBy("Date")

# Get the oldest and newest values for each region
oldest_newest_values_df = home_values_melted_df.withColumn("OldestValue", first("Value").over(window_spec)) \
    .withColumn("NewestValue", last("Value").over(window_spec)) \
    .select("RegionID", "OldestValue", "NewestValue") \
    .distinct()

# Calculate the percentage change in home value for each region
percentage_change_df = oldest_newest_values_df.withColumn(
    "PercentageChange",
    ((col("NewestValue") - col("OldestValue")) / col("OldestValue")) * 100
)

# Join the percentage change DataFrame with the regions DataFrame to get the region names
result_df = percentage_change_df.join(regions_df, on="RegionID") \
    .select("RegionName", "PercentageChange")

# Find the city with the largest percentage increase
largest_increase_df = result_df.orderBy(col("PercentageChange").desc()).limit(1)

# Find the city with the smallest percentage increase (or biggest percentage decrease)
smallest_increase_df = result_df.orderBy(col("PercentageChange").asc()).limit(1)

# Show the results
print("City with the largest percentage increase in home value:")
largest_increase_df.show()

print("City with the smallest percentage increase (or biggest percentage decrease) in home value:")
smallest_increase_df.show()

# Write the results to output files
largest_increase_df.coalesce(1).write.csv("largest_increase.csv", header=True, mode="overwrite")
smallest_increase_df.coalesce(1).write.csv("smallest_increase.csv", header=True, mode="overwrite")

# Stop the Spark session
spark.stop()

