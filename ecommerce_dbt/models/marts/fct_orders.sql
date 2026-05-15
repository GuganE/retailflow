with orders as (
    select * from {{ ref('stg_orders') }}
),

items as (
    select
        order_id,
        count(*)            as item_count,
        sum(price)          as total_item_value,
        sum(freight_value)  as total_freight_value
    from {{ ref('stg_order_items') }}
    group by order_id
),

payments as (
    select
        order_id,
        sum(payment_value) as total_payment_value
    from {{ ref('stg_order_payments') }}
    group by order_id
),

customers as (
    select * from {{ ref('stg_customers') }}
)

select
    o.order_id,
    o.customer_id,
    c.customer_city,
    c.customer_state,
    o.order_status,
    o.purchased_at,
    o.delivered_to_customer_at,
    o.estimated_delivery_at,
    datediff('day', o.purchased_at, o.delivered_to_customer_at) as days_to_deliver,
    datediff('day', o.delivered_to_customer_at, o.estimated_delivery_at) as days_early_or_late,
    i.item_count,
    i.total_item_value,
    i.total_freight_value,
    p.total_payment_value
from orders o
left join customers c  on o.customer_id = c.customer_id
left join items i      on o.order_id = i.order_id
left join payments p   on o.order_id = p.order_id
