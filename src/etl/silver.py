from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast
from src.utils.spark_session import get_spark


def main() -> None:
    spark = get_spark("FD-Silver", profile="dev")

    bronze = spark.read.format("delta").load("data/bronze/transactions")

    customers = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv("data/ref/customers.csv")
    )

    merchants = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv("data/ref/merchants.csv")
    )

    silver = (
        bronze
        .dropDuplicates(["txn_id"])  # idempotent load
        .join(broadcast(customers), on="customer_id", how="left")
        .join(broadcast(merchants), on="merchant_id", how="left")
    )

    (
        silver.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("date")
        .save("data/silver/transactions")
    )
    print("✅ Silver written -> data/silver/transactions")


if __name__ == "__main__":
    main()