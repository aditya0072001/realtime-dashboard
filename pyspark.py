from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp

spark = SparkSession.builder.appName("SocialMediaAnalytics").getOrCreate()

# Load data from Cassandra
df = spark.read.format("org.apache.spark.sql.cassandra") \
  .options(table="tweets", keyspace="your_keyspace") \
  .load()

# Filter and transform data
df_filtered = df.filter(col("created_at").isNotNull() & col("text").isNotNull())
df_transformed = df_filtered.select(col("id"), col("text"), to_timestamp(col("created_at")).alias("created_at"), col("user_id"))

# Aggregate data by hour
df_hourly_counts = df_transformed \
  .groupBy(col("user_id"), window(col("created_at"), "1 hour").alias("hour_window")) \
  .count()

# Write data to Elasticsearch
df_hourly_counts.write.format("org.elasticsearch.spark.sql") \
  .option("es.nodes.wan.only", "true") \
  .option("es.port", "9200") \
  .option("es.nodes", "your_elasticsearch_host") \
  .option("es.resource", "socialmediaanalytics/tweets") \
  .mode("append").save()
