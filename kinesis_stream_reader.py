# This streaming module will be a continuously running application to read in the Streams from Kinesis
# To kick start module spark-submit --master yarn --deploy-mode cluster --jars spark-sql-kinesis_2.11-2.3.1.jar \
# kinesis_stream_reader.py
from __future__ import print_function

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as fun

# Create a SparkSession to get into Spark
spark = SparkSession \
    .builder \
    .appName("StructuredBinlogKinesisReader") \
    .getOrCreate()

# Create Schema for incoming events which will be used to read the row for each table
schema = StructType() \
     .add("row", StructType() \
          .add("values", StringType())) \
     .add("schema", StringType()) \
     .add("table", StringType()) \
     .add("type", StringType())

# This establishes the connection to kinesis stream to start reading the stream
kinesisDF = spark \
  .readStream \
  .format("kinesis") \
  .option("streamName", "DataTest") \
  .option("initialPosition", "earliest") \
  .option("endpointUrl", "https://kinesis.us-west-2.amazonaws.com") \
  .load()

# Transform the stream into more meaningful data and write that to console for now
# You cast the incoming stream as json objects as we put in and then using above defined schema
# You should be able to read in the required
kinesisDF.selectExpr("CAST (data as String) jsonData") \
  .select(fun.from_json("jsonData", schema).alias("incoming")) \
  .filter("incoming.type == 'WriteRowsEvent' and incoming.table == 'applicants'") \
  .select("incoming.type", "incoming.table", "incoming.row.values") \
  .writeStream \
  .format("console") \
  .start() \
  .awaitTermination()





