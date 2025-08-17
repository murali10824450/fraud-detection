from src.utils.spark_session import get_spark


def main() -> None:
    spark = get_spark("FD-Gold", profile="dev")
    silver = spark.read.format("delta").load("data/silver/transactions")
    (
        silver.write.format("delta")
        .mode("overwrite")
        .partitionBy("date")
        .save("data/gold/features")
    )
    print("Gold features written.")


if __name__ == "__main__":
    main()