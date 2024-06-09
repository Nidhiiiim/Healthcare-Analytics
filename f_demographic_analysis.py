from pyspark.sql.functions import count, avg
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

spark = SparkSession.builder.appName("Healthcare Data Analysis").getOrCreate()

healthcare_data = spark.read.csv("/Users/nidhimishra/Downloads/healthcare_dataset.csv", header=True, inferSchema=True)

age_distribution = healthcare_data.groupBy("Age").count().orderBy("Age")
age_distribution.show()

gender_distribution = healthcare_data.groupBy("Gender").count()
gender_distribution.show()

