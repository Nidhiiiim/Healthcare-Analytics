from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# Initialize Spark session
spark = SparkSession.builder.appName("Healthcare Data Analysis").getOrCreate()

# Load data
healthcare_data = spark.read.csv("/Users/nidhimishra/Downloads/healthcare_dataset.csv", header=True, inferSchema=True)

# Group by Gender and calculate average age
age_gender_grouping = healthcare_data.groupBy("Gender").agg({"Age": "avg"})
age_gender_grouping.show()

# Convert the Medical Condition column to numeric labels using StringIndexer
indexer = StringIndexer(inputCol="Medical Condition", outputCol="label")
indexed_data = indexer.fit(healthcare_data).transform(healthcare_data)

# Ensure the data types are correct
indexed_data = indexed_data.withColumn("Age", col("Age").cast("double"))
indexed_data = indexed_data.withColumn("Billing Amount", col("Billing Amount").cast("double"))

# Split the indexed data into training and test sets
training_data, test_data = indexed_data.randomSplit([0.8, 0.2], seed=42)

# Combine feature columns into a single feature vector column
assembler = VectorAssembler(inputCols=["Age", "Billing Amount"], outputCol="features")

# Initialize the logistic regression model for multi-class classification
lr = LogisticRegression(featuresCol="features", labelCol="label", family="multinomial")

# Create a pipeline with the assembler and logistic regression stages
pipeline = Pipeline(stages=[assembler, lr])

# Fit the model on the training data
model = pipeline.fit(training_data)

# Make predictions on the test data
predictions = model.transform(test_data)

# Show the predictions
predictions.select("features", "label", "prediction").show()

# Evaluate the model using MulticlassClassificationEvaluator
evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print(f"Test set accuracy = {accuracy}")
