from pyspark.sql import functions as F
from pyspark.sql.window import Window
from src.utils.spark_session import get_spark


def main() -> None:
    spark = get_spark("FD-Gold", profile="dev")

    df = spark.read.format("delta").load("data/silver/transactions")

    # 1-hour window per customer using rangeBetween on "txn_ts" seconds
    w = (
        Window.partitionBy("customer_id")
        .orderBy(F.col("txn_ts").cast("long"))
        .rangeBetween(-3600, 0)
    )

    feat = (
        df
        .withColumn("amt_mean_1h", F.mean("amount").over(w))
        .withColumn("amt_std_1h", F.stddev_pop("amount").over(w))
        .withColumn("velocity_1h", F.count(F.lit(1)).over(w))
        .withColumn(
            "z_amount",
            F.when(F.col("amt_std_1h") > 0,
                   (F.col("amount") - F.col("amt_mean_1h")) / F.col("amt_std_1h"))
             .otherwise(F.lit(0.0))
        )
    )

    (
        feat.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("date")
        .save("data/gold/features")
    )
    print("✅ Gold features written -> data/gold/features")


if __name__ == "__main__":
    main() 