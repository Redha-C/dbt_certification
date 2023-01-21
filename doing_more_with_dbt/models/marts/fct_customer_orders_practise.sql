-- Import CTEs 
with orders as (
    select * from {{ source('jaffle_shop','orders') }}
),

payment as (
    select * from {{ source('stripe','payment') }}
),

customers as (
    select * from {{ source('jaffle_shop','customers') }}
),

-- Logical CTEs 
payment_amount_paid as (
    select 
        ORDERID as order_id, 
        max(CREATED) as payment_finalized_date, 
        sum(AMOUNT) / 100.0 as total_amount_paid
    from 
        payment
    where 
        STATUS <> 'fail'
    group by 1
) ,

customer_orders as (
    select 
        customers.ID as customer_id, 
        min(ORDER_DATE) as first_order_date, 
        max(ORDER_DATE) as most_recent_order_date, 
        count(orders.ID) AS number_of_orders
    from 
        customers
    left join 
        orders
    on orders.USER_ID = customers.ID 
    group by 1
),

paid_orders as (
    select 
        orders.ID as order_id,
        orders.USER_ID as customer_id,
        orders.ORDER_DATE as order_placed_at,
        orders.STATUS as order_status,
        payment_amount_paid.total_amount_paid,
        payment_amount_paid.payment_finalized_date,
        customers.FIRST_NAME as customer_first_name,
        customers.last_name as customer_last_name
    FROM 
        orders
    left join 
        payment_amount_paid 
        ON orders.ID = payment_amount_paid.order_id
    left join 
        customers 
        on orders.USER_ID = customers.ID 
),

customer_lifetime_info as (
    select
        paid_orders.order_id,
        sum(paid_orders_2.total_amount_paid) as clv_bad
    from 
        paid_orders
    left join 
        paid_orders as paid_orders_2 
        on paid_orders.customer_id = paid_orders_2.customer_id 
        and paid_orders.order_id >= paid_orders_2.order_id
    group by 1
    order by paid_orders.order_id
),

paid_customer_orders_join as (
    select 
        paid_orders.*,
        ROW_NUMBER() OVER (ORDER BY paid_orders.order_id) as transaction_seq,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY paid_orders.order_id) as customer_sales_seq,
        CASE 
            WHEN customer_orders.first_order_date = paid_orders.order_placed_at
            THEN 'new'
            ELSE 'return' 
        END as nvsr,
        customer_lifetime_info.clv_bad as customer_lifetime_value,
        customer_orders.first_order_date as fdos
    from 
        paid_orders
    left join 
        customer_orders 
        USING (customer_id)
    left outer join 
        customer_lifetime_info
        on customer_lifetime_info.order_id = paid_orders.order_id
),

-- Final CTE 
final as (
    select
        *
    FROM 
        paid_customer_orders_join
    ORDER BY 
        order_id
)

-- Simple final statement 
select * from final 
