from pyspark.sql import DataFrame
from src.utils.spark_session import get_spark


def main() -> None:
    spark = get_spark("FD-Ingest", profile="dev")
    # Placeholder ingest: create empty Delta table with minimal schema
    df: DataFrame = spark.createDataFrame([], "txn_id string, date string")
    (
        df.write.format("delta")
        .mode("overwrite")
        .partitionBy("date")
        .save("data/bronze/transactions")
    )
    print("Bronze seeded.")


if __name__ == "__main__":
    main()