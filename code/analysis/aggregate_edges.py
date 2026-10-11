from pyspark.sql import SparkSession
from pyspark.sql import functions as F

RAW_PATH = "/data/raw_parquet"            # host-mounted folder Spark wrote to
OUT_PATH = "/data/aggregated/edges"       # final aggregated edges


def main():
    spark = (
        SparkSession.builder
        .appName("EdgeAggregation")
        .master("spark://spark-master:7077")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print("Reading raw edges from:", RAW_PATH)
    df = spark.read.parquet(RAW_PATH)

    # Basic aggregations
    agg = (
        df.groupBy("src", "dst")
        .agg(
            F.count("*").alias("count"),
            F.avg("sentiment_score").alias("avg_sentiment"),
            F.stddev("sentiment_score").alias("stddev_sentiment"),
            F.min("timestamp").alias("first_seen"),
            F.max("timestamp").alias("last_seen"),
        )
        .orderBy(F.desc("count"))
    )

    print(f"Writing aggregated edges to: {OUT_PATH}")
    agg.write.mode("overwrite").parquet(OUT_PATH)

    spark.stop()


if __name__ == "__main__":
    main()
