import argparse

from src.etl import etl
from src.fill_dm_table import fill_dm_table
from src.transfer_to_mysql import transfer_dm_to_mysql


def main() -> None:
    parser = argparse.ArgumentParser(description="SQL labs runner")
    parser.add_argument(
        "--step",
        choices=["etl", "dm", "transfer", "all"],
        default="all",
        help="what to run",
    )
    args = parser.parse_args()

    if args.step in ("etl", "all"):
        etl()
        print("etl completed")

    if args.step in ("dm", "all"):
        fill_dm_table()
        print("dm completed")

    if args.step in ("transfer", "all"):
        copied = transfer_dm_to_mysql()
        print(f"transferred rows to mysql: {copied}")


if __name__ == "__main__":
    main()
