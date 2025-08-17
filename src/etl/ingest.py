from pyspark.sql import functions as F
from src.utils.spark_session import get_spark


def main() -> None:
    spark = get_spark("FD-Ingest", profile="dev")

    raw = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv("data/raw/transactions/*.csv")
    )

    # Standardize types and partition column
    bronze = (
        raw
        .withColumn("txn_ts", F.to_timestamp("txn_ts"))
        .withColumn("amount", F.col("amount").cast("double"))
        .withColumn("is_fraud", F.col("is_fraud").cast("int"))
        .withColumn("date", F.to_date("txn_ts"))
    )

    (
        bronze.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("date")
        .save("data/bronze/transactions")
    )
    print("✅ Bronze written -> data/bronze/transactions")


if __name__ == "__main__":
    main()