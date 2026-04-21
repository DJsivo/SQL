create database if not exists sql_dm;
use sql_dm;

create table if not exists t_dm_task (
    report_date date not null,
    order_id bigint not null,
    customer_id bigint not null,
    city_id bigint not null,
    product_category_id bigint not null,
    status_id bigint not null,
    source_system_id bigint not null,
    quantity int not null,
    unit_price decimal(14,2) not null,
    discount_pct decimal(5,2) not null,
    net_amount decimal(14,2) not null,
    primary key (report_date, order_id)
);
