from src.utils.spark_session import get_spark


def main() -> None:
    spark = get_spark("FD-StreamScore", profile="dev")
    print("Streaming placeholder. Add Kafka source + foreachBatch scoring.")


if __name__ == "__main__":
    main()