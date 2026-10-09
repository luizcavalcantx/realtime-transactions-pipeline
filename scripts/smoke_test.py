import os

from pyspark.sql import SparkSession

bucket = os.environ["S3_BUCKET"]
path = f"s3a://{bucket}/smoke/delta_table"

spark = (
    SparkSession.builder.appName("smoke-test")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
    .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
    .config("spark.hadoop.fs.s3a.endpoint", "s3.us-east-1.amazonaws.com")
    .getOrCreate()
)

df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "value"])
df.write.format("delta").mode("overwrite").save(path)

result = spark.read.format("delta").load(path)
assert result.count() == 2, "Unexpected row count"
print("SMOKE TEST OK: wrote and read a Delta table on S3")
spark.stop()
