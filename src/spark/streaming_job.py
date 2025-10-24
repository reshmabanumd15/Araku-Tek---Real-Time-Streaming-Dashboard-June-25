# Spark 3.x Structured Streaming job
# Reads Kafka topics (orders, clicks), aggregates per 1-minute windows, writes to Parquet & Redshift (foreachBatch).
import json, os
from pyspark.sql import SparkSession, functions as F, types as T

CONF_PATH = os.environ.get("SPARK_CONF", "configs/spark.conf.json")

def load_conf(path):
    with open(path) as f:
        return json.load(f)

def parse_orders(df):
    schema = T.StructType([
        T.StructField("event_id", T.StringType()),
        T.StructField("customer_id", T.IntegerType()),
        T.StructField("order_id", T.StringType()),
        T.StructField("amount", T.DoubleType()),
        T.StructField("currency", T.StringType()),
        T.StructField("channel", T.StringType()),
        T.StructField("event_ts", T.StringType())
    ])
    json_col = F.from_json(F.col("value").cast("string"), schema).alias("j")
    return df.select(json_col).select("j.*").withColumn("event_time", F.to_timestamp("event_ts"))

def parse_clicks(df):
    schema = T.StructType([
        T.StructField("event_id", T.StringType()),
        T.StructField("customer_id", T.IntegerType()),
        T.StructField("page", T.StringType()),
        T.StructField("referrer", T.StringType()),
        T.StructField("device", T.StringType()),
        T.StructField("event_ts", T.StringType())
    ])
    json_col = F.from_json(F.col("value").cast("string"), schema).alias("j")
    return df.select(json_col).select("j.*").withColumn("event_time", F.to_timestamp("event_ts"))

def write_parquet(df, output_path, checkpoint, name):
    return (df.writeStream
            .format("parquet")
            .option("path", os.path.join(output_path, name))
            .option("checkpointLocation", os.path.join(checkpoint, name))
            .outputMode("append")
            .start())

def foreach_batch_redshift(conf):
    def _write(df, epoch_id):
        if df.rdd.isEmpty(): return
        # Aggregate to hourly metrics for Redshift upsert
        agg = (df
               .withColumn("hr", F.date_format("event_time", "yyyy-MM-dd HH:00:00"))
               .groupBy("hr")
               .agg(F.count("*").alias("events"), F.sum("amount").alias("amount_sum")))
        (agg.write
            .format("jdbc")
            .option("url", conf["redshift"]["url"])
            .option("user", conf["redshift"]["user"])
            .option("password", conf["redshift"]["password"])
            .option("dbtable", "public.orders_hourly_agg")
            .mode("append")
            .save())
    return _write

def main():
    conf = load_conf(CONF_PATH)
    spark = (SparkSession.builder
             .appName("rt-streaming-dashboard")
             .getOrCreate())

    kafka_servers = conf["kafka.bootstrap.servers"]
    orders = (spark.readStream
              .format("kafka")
              .option("kafka.bootstrap.servers", kafka_servers)
              .option("subscribe", "orders")
              .option("startingOffsets", "latest")
              .load())
    clicks = (spark.readStream
              .format("kafka")
              .option("kafka.bootstrap.servers", kafka_servers)
              .option("subscribe", "clicks")
              .option("startingOffsets", "latest")
              .load())

    orders_p = parse_orders(orders)
    clicks_p = parse_clicks(clicks)

    # Example aggregations
    orders_min = (orders_p
                  .withWatermark("event_time", "5 minutes")
                  .groupBy(F.window("event_time", "1 minute"), F.col("channel"))
                  .agg(F.count("*").alias("orders"), F.sum("amount").alias("revenue")))

    clicks_min = (clicks_p
                  .withWatermark("event_time", "5 minutes")
                  .groupBy(F.window("event_time", "1 minute"), F.col("page"))
                  .agg(F.count("*").alias("clicks")))

    # Write to Parquet (S3/HDFS) for downstream BI
    q1 = write_parquet(orders_min, conf["output.parquet.path"], conf["checkpoint.location"], "orders_min")
    q2 = write_parquet(clicks_min, conf["output.parquet.path"], conf["checkpoint.location"], "clicks_min")

    # Also upsert hourly orders to Redshift (example via append to staging table)
    (orders_p.writeStream
        .outputMode("append")
        .foreachBatch(foreach_batch_redshift(conf))
        .option("checkpointLocation", os.path.join(conf["checkpoint.location"], "orders_redshift_hourly"))
        .start())

    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
