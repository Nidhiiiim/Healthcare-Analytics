from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

spark = SparkSession.builder.appName("Healthcare Data Analysis").getOrCreate()

healthcare_data = spark.read.csv("/Users/nidhimishra/Downloads/healthcare_dataset.csv", header=True, inferSchema=True)

# HNDLING MISSING DATA BY DEFAULT VALUE
healthcare_data = healthcare_data.na.fill({"age": 0, "gender": "unknown"})

# Drppoing duplicates
healthcare_data = healthcare_data.dropDuplicates()

healthcare_data = healthcare_data.withColumn("Discharge Date", to_date(col("Discharge Date"), "MM/dd/yyyy"))
healthcare_data.show(10)