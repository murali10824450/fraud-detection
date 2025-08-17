from src.utils.spark_session import get_spark


def main() -> None:
    spark = get_spark("FD-Silver", profile="dev")
    bronze = spark.read.format("delta").load("data/bronze/transactions")
    (
        bronze.write.format("delta")
        .mode("overwrite")
        .partitionBy("date")
        .save("data/silver/transactions")
    )
    print("Silver written.")


if __name__ == "__main__":
    main()