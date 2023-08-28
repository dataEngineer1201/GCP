from pyspark.sql import SparkSession

# Initialize a Spark session
spark = SparkSession.builder \
    .appName("CreateBQTableAndLoadData") \
    .getOrCreate()

# Define the GCS paths to the schema JSON and CSV data files
schema_path_gcs = "gs://sam01/schema.json"
csv_path_gcs = "gs://sam01/marksheet.csv"

# Read the schema JSON from GCS
schema_json = spark.read.text(schema_path_gcs).first()[0]

# Load CSV data from GCS into a DataFrame
data_df = spark.read.csv(csv_path_gcs, header=True, inferSchema=True)

# Create BigQuery table using the defined schema
table_name = "my-new-project0123.DATASET_ID.table0021"  # Change to your desired table name
data_df.write \
    .format("bigquery") \
    .option("table", table_name) \
    .option("temporaryGcsBucket", "sam01") \
    .option("schema", schema_json) \
    .mode("overwrite") \
    .save()

# Stop the Spark session
spark.stop()
