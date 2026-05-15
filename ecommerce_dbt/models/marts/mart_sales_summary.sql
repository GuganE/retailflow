with fct as (
    select * from {{ ref('fct_orders') }}
    where order_status = 'delivered'
)

select
    date_trunc('month', purchased_at)       as month,
    count(distinct order_id)                as total_orders,
    count(distinct customer_id)             as unique_customers,
    round(sum(total_payment_value), 2)      as total_revenue,
    round(avg(total_payment_value), 2)      as avg_order_value,
    round(avg(days_to_deliver), 1)          as avg_delivery_days
from fct
group by 1
order by 1
