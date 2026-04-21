create table if not exists s_sql_dds.t_sql_source_unstructured (
    report_date varchar(50),
    order_id varchar(50),
    customer_name varchar(255),
    city varchar(255),
    product_category varchar(255),
    status varchar(50),
    quantity varchar(50),
    unit_price varchar(50),
    discount_pct varchar(50),
    source_system varchar(100)
);
