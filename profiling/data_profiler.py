import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, countDistinct, percentile_approx, stddev
from pyspark.sql.functions import max as spark_max
from pyspark.sql.functions import min as spark_min

S3_BUCKET = os.getenv("S3_BUCKET", "csp544-crypto-pipeline")
INPUT_PATH = os.getenv(
    "PROFILE_INPUT", f"s3://{S3_BUCKET}/raw-data/captured_trades.jsonl"
)
OUTPUT_PATH = os.getenv("PROFILE_OUTPUT", f"s3://{S3_BUCKET}/profiling/stats/")

spark = SparkSession.builder.appName("CryptoDataProfiler").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("=" * 70)
print("CSP 544 - DATA PROFILING REPORT")
print("=" * 70)
print(f"Input: {INPUT_PATH}")

df = spark.read.json(INPUT_PATH)
total = df.count()

print(f"\n1. RECORD COUNT: {total:,}")
print(f"   Unique symbols: {df.select('symbol').distinct().count()}")

time_range = df.agg(
    spark_min("trade_time").alias("earliest"),
    spark_max("trade_time").alias("latest"),
).collect()[0]
duration_sec = (time_range["latest"] - time_range["earliest"]) / 1000
print(f"   Time span: {duration_sec:.0f}s ({duration_sec/60:.1f} min)")

print("\n   Per-Symbol Breakdown:")
per_symbol = df.groupBy("symbol").agg(
    count("*").alias("records"),
    countDistinct("trade_id").alias("unique_trades"),
    spark_min("price").alias("min_price"),
    spark_max("price").alias("max_price"),
    avg("price").alias("avg_price"),
    stddev("price").alias("stddev_price"),
    avg("quantity").alias("avg_qty"),
)
per_symbol.show(truncate=False)

unique = df.dropDuplicates(["trade_id", "symbol"]).count()
dups = total - unique
print(f"2. DUPLICATES: {dups:,} of {total:,} ({(dups/total*100):.2f}%)")

print(f"\n3. NULL VALUES:")
for c in df.columns:
    nulls = df.filter(col(c).isNull()).count()
    print(f"   {c:<15} {'CLEAN' if nulls == 0 else f'{nulls} nulls'}")

print(f"\n4. PRICE OUTLIERS (IQR):")
symbols = [r["symbol"] for r in df.select("symbol").distinct().collect()]
total_outliers = 0
for sym in sorted(symbols):
    sdf = df.filter(col("symbol") == sym)
    q = sdf.approxQuantile("price", [0.25, 0.5, 0.75], 0.01)
    if len(q) < 3:
        continue
    q1, med, q3 = q
    iqr = q3 - q1
    lo, hi = q1 - 1.5 * iqr, q3 + 1.5 * iqr
    out = sdf.filter((col("price") < lo) | (col("price") > hi)).count()
    total_outliers += out
    print(f"   {sym}: median=${med:.2f} IQR=${iqr:.2f} bounds=[${lo:.2f}, ${hi:.2f}] outliers={out}")

if "capture_time" in df.columns:
    print(f"\n5. EVENT-TIME LAG:")
    lag = df.withColumn("lag_ms", col("capture_time") - col("trade_time"))
    s = lag.agg(
        avg("lag_ms").alias("avg"),
        percentile_approx("lag_ms", 0.50).alias("p50"),
        percentile_approx("lag_ms", 0.95).alias("p95"),
        percentile_approx("lag_ms", 0.99).alias("p99"),
    ).collect()[0]
    print(f"   Avg: {s['avg']:.0f}ms | P50: {s['p50']}ms | P95: {s['p95']}ms | P99: {s['p99']}ms")

uniq_rate = unique / total if total else 1.0
outlier_free = 1.0 - (total_outliers / total) if total else 1.0
score = (1.0 * 0.4 + uniq_rate * 0.3 + outlier_free * 0.3) * 100
print(f"\n6. QUALITY SCORE: {score:.1f}%")

per_symbol.write.mode("overwrite").parquet(OUTPUT_PATH)
print(f"\nSaved per-symbol stats to {OUTPUT_PATH}")
print("=" * 70)
spark.stop()
