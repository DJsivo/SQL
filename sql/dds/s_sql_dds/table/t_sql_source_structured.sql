create table if not exists s_sql_dds.t_sql_source_structured (
    report_date date not null,
    order_id bigint not null,
    customer_name varchar(255) not null,
    city varchar(255) not null,
    product_category varchar(255) not null,
    status varchar(50) not null,
    quantity int not null,
    unit_price numeric(14,2) not null,
    discount_pct numeric(5,2) not null,
    source_system varchar(100) not null,
    load_dttm timestamp not null default now(),
    constraint pk_t_sql_source_structured primary key (report_date, order_id)
);
