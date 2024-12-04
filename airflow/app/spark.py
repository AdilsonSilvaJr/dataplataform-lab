from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName('SimpleApp').getOrCreate()

# Define mock data
data = [("John Doe", 25), ("Jane Smith", 30), ("Bob Johnson", 35)]

# Define the schema
columns = ["Name", "Age"]

# Create DataFrame
df = spark.createDataFrame(data, columns)

# Show DataFrame
df.show()
