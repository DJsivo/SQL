from src.fill_structured_table import fill_structured_table
from src.get_dataset import get_dataset
from src.load_data_to_db import load_data_to_db


def etl() -> None:
    dataset = get_dataset()
    load_data_to_db(dataset)
    fill_structured_table()
