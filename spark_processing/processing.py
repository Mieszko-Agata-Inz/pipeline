#https://medium.com/plumbersofdatascience/a-beginners-guide-building-your-first-dockerized-streaming-pipeline-3bd5a62046e1
from pyspark.sql import SparkSession
import os, findspark
from pyspark.sql.types import StructType,StructField,IntegerType,FloatType,StringType
#from pyspark.sql.functions import from_json, col, udf
#import uuid
#import redis

# def save_to_redis(writeDF, epoch_id):
#   print("Printing epoch_id: ")
#   print(epoch_id)
  
#   writeDF.write \
#     .format("org.apache.spark.sql.cassandra")\
#     .mode('append')\
#     .options(table="order_table", keyspace="order_ks")\
#     .save()
  
#   print(epoch_id,"saved to Cassandra")


# setup arguments

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.3.2 processing.py'
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.0 processing.py'
findspark.init()

schema = StructType([
    StructField("x", StringType()),
    StructField("y", FloatType()),
])

spark = SparkSession \
    .builder \
    .appName("Spark Kafka Streaming Data Pipeline") \
    .master("local[*]") \
    .config("spark.driver.host", "localhost")\
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

input_df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "127.0.0.1:9092") \
  .option("subscribe", "weather_data") \
  .option("startingOffsets", "earliest") \
  .load() 

input_df.printSchema()

# expanded_df = input_df \
#   .selectExpr("CAST(value AS STRING)") \
#   .select(from_json(col("value"),schema).alias("order")) \
#   .select("order.*")

#aggregated_df = input_df.groupBy("x").avg()

# uuid_udf = udf(lambda: str(uuid.uuid4()), StringType()).asNondeterministic()
# expanded_df = expanded_df.withColumn("uuid", uuid_udf())
# expanded_df.printSchema()

# query1 = expanded_df.writeStream \
#   .trigger(processingTime="15 seconds") \
#   .foreachBatch(save_to_redis) \
#   .outputMode("update") \
#   .start()

input_df.writeStream \
  .outputMode("update") \
  .format("console") \
  .option("truncate", False) \
  .start() \
  .awaitTermination()

# customers_df = spark.read.csv("customers.csv", header=True, inferSchema=True)
# customers_df.printSchema()

# sales_df = expanded_df.join(customers_df, expanded_df.customer_id == customers_df.customer_id, how="inner")
# sales_df.printSchema()

# final_df = sales_df.groupBy("source", "state") \
#   .agg({"total":"sum"}).select("source", "state", col("sum(total)").alias("total_sum_amount"))
# final_df.printSchema()

# Output to Console
# final_df.writeStream \
#   .trigger(processingTime="15 seconds") \
#   .outputMode("update") \
#   .format("console") \
#   .option("truncate", False) \
#   .start()

# query2 = final_df.writeStream \
#   .trigger(processingTime="15 seconds") \
#   .outputMode("complete") \
#   .foreachBatch(save_to_mysql) \
#   .start()

# query2.awaitTermination()