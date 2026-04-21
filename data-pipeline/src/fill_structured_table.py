from datetime import date, timedelta

from src.db import pg_conn


def fill_structured_table(start_date: date | None = None, end_date: date | None = None) -> None:
    end_date = end_date or date.today()
    start_date = start_date or (end_date - timedelta(days=30))

    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "select s_sql_dds.fn_etl_data_load(%s::date, %s::date);",
                (start_date, end_date),
            )
        conn.commit()
