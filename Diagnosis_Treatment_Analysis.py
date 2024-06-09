from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# initializing spark session
spark = SparkSession.builder.appName("Healthcare Data Analysis").getOrCreate()

# Loading data
healthcare_data = spark.read.csv("/Users/nidhimishra/Downloads/healthcare_dataset.csv", header=True, inferSchema=True)

# Common Diagnosis
common_diagnosis = healthcare_data.groupby("Medical Condition").count().orderBy(col("count").desc())
common_diagnosis.show()

# Treatment Diagnosis
TreatmentEffectiveness = healthcare_data.groupby("Medication", "Test Results").count()
TreatmentEffectiveness.show()
