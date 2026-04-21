from datetime import date, timedelta

from src.db import pg_conn


def fill_dm_table(start_dt: date | None = None, end_dt: date | None = None) -> None:
    end_dt = end_dt or date.today()
    start_dt = start_dt or (end_dt - timedelta(days=30))

    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "select s_sql_dds.fn_dm_data_load(%s::date, %s::date);",
                (start_dt, end_dt),
            )
        conn.commit()
