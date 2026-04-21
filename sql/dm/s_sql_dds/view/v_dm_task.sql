create or replace view s_sql_dds.v_dm_task as
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
from s_sql_dds.t_dm_task;
