from pyspark.sql import SparkSession

# initializing spark session
spark = SparkSession.builder.appName("Healthcare Data Analysis").getOrCreate()

# Loading data
healthcare_data = spark.read.csv("/Users/nidhimishra/Downloads/healthcare_dataset.csv", header=True, inferSchema=True)

healthcare_data.show(10)

# printing Schema

healthcare_data.printSchema()

healthcare_data.describe().show()


