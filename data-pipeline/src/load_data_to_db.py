from __future__ import annotations

from typing import Any

from src.db import pg_conn


INSERT_SQL = """
insert into s_sql_dds.t_sql_source_unstructured (
    report_date,
    order_id,
    customer_name,
    city,
    product_category,
    status,
    quantity,
    unit_price,
    discount_pct,
    source_system
)
values (%(report_date)s, %(order_id)s, %(customer_name)s, %(city)s, %(product_category)s,
        %(status)s, %(quantity)s, %(unit_price)s, %(discount_pct)s, %(source_system)s);
"""


def load_data_to_db(dataset: list[dict[str, Any]]) -> None:
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.executemany(INSERT_SQL, dataset)
        conn.commit()
