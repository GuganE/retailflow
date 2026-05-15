with items as (
    select * from {{ ref('stg_order_items') }}
),

orders as (
    select order_id, order_status
    from {{ ref('stg_orders') }}
    where order_status = 'delivered'
),

products as (
    select * from {{ ref('stg_products') }}
)

select
    p.product_id,
    p.product_category_name,
    count(distinct i.order_id)     as total_orders,
    sum(i.price)                   as total_revenue,
    round(avg(i.price), 2)         as avg_price
from items i
join orders o    on i.order_id = o.order_id
join products p  on i.product_id = p.product_id
group by 1, 2
order by total_revenue desc
