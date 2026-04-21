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
