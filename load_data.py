from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("LoadData").getOrCreate()

# Adjust the path if data.csv is in the same directory
df = spark.read.csv("data.csv", header=True, inferSchema=True)

df.show()

spark.stop()
