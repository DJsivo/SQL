create table if not exists s_sql_dds.t_dim_customer (
    id bigserial primary key,
    name varchar(255) not null unique
);

create table if not exists s_sql_dds.t_dim_city (
    id bigserial primary key,
    name varchar(255) not null unique
);

create table if not exists s_sql_dds.t_dim_product_category (
    id bigserial primary key,
    name varchar(255) not null unique
);

create table if not exists s_sql_dds.t_dim_status (
    id bigserial primary key,
    name varchar(50) not null unique
);

create table if not exists s_sql_dds.t_dim_source_system (
    id bigserial primary key,
    name varchar(100) not null unique
);

create table if not exists s_sql_dds.t_dm_task (
    report_date date not null,
    order_id bigint not null,
    customer_id bigint not null,
    city_id bigint not null,
    product_category_id bigint not null,
    status_id bigint not null,
    source_system_id bigint not null,
    quantity int not null,
    unit_price numeric(14,2) not null,
    discount_pct numeric(5,2) not null,
    net_amount numeric(14,2) not null,
    constraint pk_t_dm_task primary key (report_date, order_id),
    constraint fk_t_dm_task_customer foreign key (customer_id) references s_sql_dds.t_dim_customer (id),
    constraint fk_t_dm_task_city foreign key (city_id) references s_sql_dds.t_dim_city (id),
    constraint fk_t_dm_task_category foreign key (product_category_id) references s_sql_dds.t_dim_product_category (id),
    constraint fk_t_dm_task_status foreign key (status_id) references s_sql_dds.t_dim_status (id),
    constraint fk_t_dm_task_system foreign key (source_system_id) references s_sql_dds.t_dim_source_system (id)
);
