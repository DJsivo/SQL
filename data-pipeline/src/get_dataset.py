from __future__ import annotations

from datetime import date, timedelta
import random
from typing import Any


def get_dataset(row_count: int = 200, days_back: int = 30) -> list[dict[str, Any]]:
    random.seed(42)

    cities = ["omsk", "novosibirsk", "tomsk", "krasnoyarsk"]
    categories = ["electronics", "home", "books", "food"]
    statuses = ["new", "processed", "shipped", "cancelled"]
    systems = ["crm", "site", "mobile"]

    data: list[dict[str, Any]] = []
    today = date.today()

    for i in range(row_count):
        report_date = today - timedelta(days=random.randint(0, days_back))
        order_id = random.randint(100000, 999999)
        quantity = random.randint(1, 10)
        unit_price = round(random.uniform(100, 20000), 2)
        discount_pct = round(random.uniform(0, 30), 2)

        record = {
            "report_date": report_date.isoformat(),
            "order_id": str(order_id),
            "customer_name": f"customer_{random.randint(1, 30)}",
            "city": random.choice(cities),
            "product_category": random.choice(categories),
            "status": random.choice(statuses),
            "quantity": str(quantity),
            "unit_price": str(unit_price),
            "discount_pct": str(discount_pct),
            "source_system": random.choice(systems),
        }

        # Намеренно вносим аномалии, чтобы чистить в SQL.
        if i % 13 == 0:
            record["quantity"] = "-5"
        if i % 11 == 0:
            record["status"] = "unknown"
        if i % 17 == 0:
            record["report_date"] = "broken-date"
        if i % 19 == 0:
            record["unit_price"] = "bad-price"
        if i % 23 == 0:
            record["discount_pct"] = "105.5"
        if i % 29 == 0:
            record["customer_name"] = "   "
        if i % 31 == 0:
            record["city"] = ""
        if i % 37 == 0:
            record["order_id"] = "abc123"
        if i % 41 == 0:
            record["source_system"] = "   "

        data.append(record)

    return data
