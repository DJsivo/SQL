from datetime import date, timedelta

from src.db import pg_conn, mysql_conn


def transfer_dm_to_mysql(start_dt: date | None = None, end_dt: date | None = None) -> int:
    end_dt = end_dt or date.today()
    start_dt = start_dt or (end_dt - timedelta(days=30))

    with pg_conn() as pg:
        with pg.cursor() as cur:
            cur.execute(
                """
                select
                    report_date,
                    order_id,
                    customer_id,
                    city_id,
                    product_category_id,
                    status_id,
                    source_system_id,
                    quantity,
                    unit_price,
                    discount_pct,
                    net_amount
                from s_sql_dds.v_dm_task
                where report_date between %s::date and %s::date;
                """,
                (start_dt, end_dt),
            )
            rows = cur.fetchall()

    if not rows:
        return 0

    insert_sql = """
    insert into t_dm_task (
        report_date,
        order_id,
        customer_id,
        city_id,
        product_category_id,
        status_id,
        source_system_id,
        quantity,
        unit_price,
        discount_pct,
        net_amount
    )
    values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    on duplicate key update
        customer_id = values(customer_id),
        city_id = values(city_id),
        product_category_id = values(product_category_id),
        status_id = values(status_id),
        source_system_id = values(source_system_id),
        quantity = values(quantity),
        unit_price = values(unit_price),
        discount_pct = values(discount_pct),
        net_amount = values(net_amount);
    """

    with mysql_conn() as my:
        with my.cursor() as cur:
            cur.executemany(insert_sql, rows)
        my.commit()

    return len(rows)
