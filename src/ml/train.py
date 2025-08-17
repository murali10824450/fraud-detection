from src.utils.spark_session import get_spark


def main() -> None:
    spark = get_spark("FD-Train", profile="dev")
    print("Train placeholder. Add ML pipeline here.")


if __name__ == "__main__":
    main()