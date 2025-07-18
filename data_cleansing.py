from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("DataCleansing").getOrCreate()

df = spark.read.csv("data.csv", header=True, inferSchema=True)

print("Original Data:")
df.show()

# Drop rows with any null values
df_dropna = df.dropna()

print("After dropping rows with nulls:")
df_dropna.show()

# Fill missing values: Age=0, Salary=average salary, Category='Unknown'
avg_salary = df.selectExpr("avg(Salary)").collect()[0][0]

df_filled = df.fillna({'Age': 0, 'Salary': avg_salary, 'Category': 'Unknown'})

print("After filling missing values:")
df_filled.show()

spark.stop()
