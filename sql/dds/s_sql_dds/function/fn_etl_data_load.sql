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
