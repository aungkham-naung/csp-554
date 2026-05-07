import os
from decimal import Decimal

import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    first,
    from_json,
    last,
    to_timestamp,
    window,
)
from pyspark.sql.functions import max as spark_max
from pyspark.sql.functions import min as spark_min
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

REGION = os.getenv("AWS_REGION", "us-east-2")
STREAM_NAME = os.getenv("KINESIS_STREAM", "crypto-trades-kinesis")
DYNAMODB_TABLE = os.getenv("DYNAMODB_TABLE", "CryptoMetricsKinesis")
S3_BUCKET = os.getenv("S3_BUCKET", "csp544-crypto-pipeline")
S3_OUTPUT = f"s3://{S3_BUCKET}/ohlc-kinesis/"
S3_CHECKPOINT = f"s3://{S3_BUCKET}/checkpoints-kinesis/"

spark = (
    SparkSession.builder.appName("CryptoStreamProcessorKinesis")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

trade_schema = StructType([
    StructField("symbol", StringType()),
    StructField("price", DoubleType()),
    StructField("quantity", DoubleType()),
    StructField("trade_time", LongType()),
    StructField("buyer_maker", BooleanType()),
    StructField("trade_id", LongType()),
])

# Qubole Kinesis source returns:
#   partitionKey (string), data (binary), approximateArrivalTimestamp (timestamp),
#   sequenceNumber (string), shardId (string), streamName (string)
# NOTE: the connector's bundled AWS SDK predates IMDSv2, so on EMR 7.x we must
# pass explicit credentials rather than rely on the EC2 instance profile.
kinesis_opts = {
    "streamName": STREAM_NAME,
    "endpointUrl": f"https://kinesis.{REGION}.amazonaws.com",
    "kinesisRegion": REGION,
    "startingPosition": "LATEST",
    "kinesis.executor.maxFetchTimeInMs": "1000",
}
aws_key = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY")
if aws_key and aws_secret:
    kinesis_opts["awsAccessKeyId"] = aws_key
    kinesis_opts["awsSecretKey"] = aws_secret
else:
    raise SystemExit(
        "Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY env vars before spark-submit "
        "(Qubole connector cannot use IMDSv2 instance profile on EMR 7.x)."
    )

raw = (
    spark.readStream.format("kinesis")
    .options(**kinesis_opts)
    .load()
)

trades = (
    raw.select(from_json(col("data").cast("string"), trade_schema).alias("d"))
    .select("d.*")
    .withColumn("event_time", to_timestamp(col("trade_time") / 1000))
)

ohlc = (
    trades.withWatermark("event_time", "30 seconds")
    .groupBy(col("symbol"), window("event_time", "1 minute"))
    .agg(
        first("price").alias("open"),
        spark_max("price").alias("high"),
        spark_min("price").alias("low"),
        last("price").alias("close"),
        spark_sum("quantity").alias("volume"),
        count("*").alias("trade_count"),
        (spark_sum(col("price") * col("quantity")) / spark_sum("quantity")).alias("vwap"),
        avg("price").alias("avg_price"),
    )
    .select(
        col("symbol"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "open", "high", "low", "close",
        "volume", "trade_count", "vwap", "avg_price",
    )
)


def write_to_dynamodb(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return
    dynamodb = boto3.resource("dynamodb", region_name=REGION)
    table = dynamodb.Table(DYNAMODB_TABLE)
    rows = batch_df.collect()
    with table.batch_writer() as batch:
        for r in rows:
            batch.put_item(Item={
                "symbol": r["symbol"],
                "window_start": r["window_start"].isoformat(),
                "window_end": r["window_end"].isoformat(),
                "open": Decimal(str(round(r["open"], 2))),
                "high": Decimal(str(round(r["high"], 2))),
                "low": Decimal(str(round(r["low"], 2))),
                "close": Decimal(str(round(r["close"], 2))),
                "volume": Decimal(str(round(r["volume"], 6))),
                "trade_count": int(r["trade_count"]),
                "vwap": Decimal(str(round(r["vwap"], 2))),
                "avg_price": Decimal(str(round(r["avg_price"], 2))),
            })
    print(f"[Batch {batch_id}] Wrote {len(rows)} rows to {DYNAMODB_TABLE}")


s3_query = (
    ohlc.writeStream.format("parquet")
    .option("path", S3_OUTPUT)
    .option("checkpointLocation", S3_CHECKPOINT + "s3/")
    .outputMode("append")
    .trigger(processingTime="1 minute")
    .start()
)

dynamo_query = (
    ohlc.writeStream.foreachBatch(write_to_dynamodb)
    .outputMode("update")
    .option("checkpointLocation", S3_CHECKPOINT + "dynamo/")
    .trigger(processingTime="1 minute")
    .start()
)

print(f"Pipeline running: Kinesis({STREAM_NAME}) -> Spark -> S3({S3_OUTPUT}) + DynamoDB({DYNAMODB_TABLE})")
spark.streams.awaitAnyTermination()
