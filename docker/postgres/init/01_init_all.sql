create schema if not exists s_sql_dds;

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

create or replace function s_sql_dds.fn_etl_data_load(start_date date, end_date date)
returns integer
language plpgsql
as
$$
declare
    v_rows integer;
begin
    delete from s_sql_dds.t_sql_source_structured
    where report_date between start_date and end_date;

    with cleaned as (
        select
            case
                when report_date ~ '^\d{4}-\d{2}-\d{2}$' then report_date::date
                else null
            end as report_date,
            case
                when order_id ~ '^\d+$' then order_id::bigint
                else null
            end as order_id,
            nullif(trim(lower(customer_name)), '') as customer_name,
            nullif(trim(lower(city)), '') as city,
            nullif(trim(lower(product_category)), '') as product_category,
            nullif(trim(lower(status)), '') as status,
            case
                when quantity ~ '^-?\d+$' then quantity::int
                else null
            end as quantity,
            case
                when unit_price ~ '^\d+(\.\d+)?$' then unit_price::numeric(14,2)
                else null
            end as unit_price,
            case
                when discount_pct ~ '^\d+(\.\d+)?$' then discount_pct::numeric(5,2)
                else null
            end as discount_pct,
            nullif(trim(lower(source_system)), '') as source_system
        from s_sql_dds.t_sql_source_unstructured
    ),
    valid_rows as (
        select
            report_date,
            order_id,
            customer_name,
            city,
            product_category,
            status,
            quantity,
            unit_price,
            discount_pct,
            source_system,
            row_number() over (
                partition by report_date, order_id
                order by report_date desc, order_id desc
            ) as rn
        from cleaned
        where report_date between start_date and end_date
          and report_date is not null
          and order_id is not null
          and customer_name is not null
          and city is not null
          and product_category is not null
          and status in ('new', 'processed', 'shipped', 'cancelled')
          and quantity between 1 and 1000
          and unit_price between 0 and 1000000
          and discount_pct between 0 and 100
          and source_system is not null
    )
    insert into s_sql_dds.t_sql_source_structured (
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
    select
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
    from valid_rows
    where rn = 1;

    get diagnostics v_rows = row_count;
    return v_rows;
end;
$$;

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

create or replace function s_sql_dds.fn_dm_data_load(start_dt date, end_dt date)
returns integer
language plpgsql
as
$$
declare
    v_rows integer;
begin
    insert into s_sql_dds.t_dim_customer (name)
    select distinct customer_name
    from s_sql_dds.t_sql_source_structured
    where report_date between start_dt and end_dt
    on conflict (name) do nothing;

    insert into s_sql_dds.t_dim_city (name)
    select distinct city
    from s_sql_dds.t_sql_source_structured
    where report_date between start_dt and end_dt
    on conflict (name) do nothing;

    insert into s_sql_dds.t_dim_product_category (name)
    select distinct product_category
    from s_sql_dds.t_sql_source_structured
    where report_date between start_dt and end_dt
    on conflict (name) do nothing;

    insert into s_sql_dds.t_dim_status (name)
    select distinct status
    from s_sql_dds.t_sql_source_structured
    where report_date between start_dt and end_dt
    on conflict (name) do nothing;

    insert into s_sql_dds.t_dim_source_system (name)
    select distinct source_system
    from s_sql_dds.t_sql_source_structured
    where report_date between start_dt and end_dt
    on conflict (name) do nothing;

    delete from s_sql_dds.t_dm_task
    where report_date between start_dt and end_dt;

    insert into s_sql_dds.t_dm_task (
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
    select
        s.report_date,
        s.order_id,
        c.id as customer_id,
        ci.id as city_id,
        p.id as product_category_id,
        st.id as status_id,
        ss.id as source_system_id,
        s.quantity,
        s.unit_price,
        s.discount_pct,
        round(s.quantity * s.unit_price * (1 - s.discount_pct / 100.0), 2) as net_amount
    from s_sql_dds.t_sql_source_structured s
    join s_sql_dds.t_dim_customer c on c.name = s.customer_name
    join s_sql_dds.t_dim_city ci on ci.name = s.city
    join s_sql_dds.t_dim_product_category p on p.name = s.product_category
    join s_sql_dds.t_dim_status st on st.name = s.status
    join s_sql_dds.t_dim_source_system ss on ss.name = s.source_system
    where s.report_date between start_dt and end_dt;

    get diagnostics v_rows = row_count;
    return v_rows;
end;
$$;

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

