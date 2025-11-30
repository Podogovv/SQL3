create table customer (
    customer_id int not null primary key,
    first_name text,
    last_name text,
    gender text,
    dob date,
    job_title text,
    job_industry_category text,
    wealth_segment text,
    deceased_indicator char(1),
    owns_car bool,
    address text,
    postcode int,
    state text,
    country text,
    property_valuation int
);

create table product (
    product_id int not null primary key,
    brand text,
    product_line text,
    product_class text,
    product_size text,
    list_price int,
    standard_cost int
);

CREATE TABLE orders (
    order_id int primary key,
    customer_id int not null,
    order_date date not null,
    online_order bool,
    order_status bool not null
);

create table order_items (
    order_item_id int primary key,
    order_id int not null,
    product_id int not null,
    quantity int not null,
    item_list_price_at_sale float4 not null,
    item_standard_cost_at_sale float4
);

--1
select
    job_industry_category,
    count(*) AS customer_count
from customer
group by job_industry_category
order by customer_count desc;
    
--2
select
    extract(year from o.order_date) as year,
    extract(month from o.order_date) as month,
    c.job_industry_category,
    sum(oi.quantity * oi.item_list_price_at_sale) as total_revenue
from orders o
join customer c on o.customer_id = c.customer_id
join order_items oi on o.order_id = oi.order_id
where o.order_status = 'Approved'
group by
    extract(year from o.order_date),
    extract(month from o.order_date),
    c.job_industry_category
order by
    year,
    month,
    c.job_industry_category;

--3
select 
    p.brand,
    count(distinct o.order_id) as unique_online_orders
from product p
left join order_items oi on p.product_id = oi.product_id
left join orders o 
    on oi.order_id = o.order_id
    and o.order_status = 'Approved'
    and o.online_order = 'Yes'
left join customer c 
    ON o.customer_id = c.customer_id
    and c.job_industry_category = 'IT'
group by p.brand
order by p.brand;

--4
--Решение с использованием GROUP BY
select
    c.customer_id,
    c.first_name,
    c.last_name,
    sum(oi.quantity * oi.item_list_price_at_sale) as total_revenue,
    min(oi.quantity * oi.item_list_price_at_sale) as min_order_amount,
    max(oi.quantity * oi.item_list_price_at_sale) as max_order_amount,
    count(distinct o.order_id) as total_orders,
    avg(order_sum) as avg_order_amount
from customer c
join orders o on c.customer_id = o.customer_id
join order_items oi on o.order_id = oi.order_id
join (
    select
        o2.order_id,
        sum(oi2.quantity * oi2.item_list_price_at_sale) as order_sum
    from orders o2
    join order_items oi2 on o2.order_id = oi2.order_id
    group by o2.order_id
) os on o.order_id = os.order_id
group by c.customer_id, c.first_name, c.last_name
order by total_revenue desc, total_orders desc;

--Решение с использованием только оконных функций (window functions)
with order_sums as (
    select
        o.order_id,
        o.customer_id,
        sum(oi.quantity * oi.item_list_price_at_sale) as order_sum
    from orders o
    join order_items oi on o.order_id = oi.order_id
    group by o.order_id, o.customer_id
),
window_calc as (
    select
        c.customer_id,
        c.first_name,
        c.last_name,
        order_sum,
        sum(order_sum) over (partition by c.customer_id) as total_revenue,
        min(order_sum) over (partition by c.customer_id) as min_order_amount,
        max(order_sum) over (partition by c.customer_id) as max_order_amount,
        count(order_sum) over (partition by c.customer_id) as total_orders,
        avg(order_sum) over (partition by c.customer_id) as avg_order_amount,
        row_number() over (partition by c.customer_id order by order_sum desc) as rn
    from customer c
    join order_sums os on c.customer_id = os.customer_id
)
select
    customer_id,
    first_name,
    last_name,
    total_revenue,
    min_order_amount,
    max_order_amount,
    total_orders,
    avg_order_amount
from window_calc
where rn = 1
order by total_revenue desc, total_orders desc;


--5
with customer_totals as (
    select
        c.customer_id,
        c.first_name,
        c.last_name,
        coalesce(sum(oi.quantity * oi.item_list_price_at_sale), 0) as total_revenue
    from customer c
    left join orders o on c.customer_id = o.customer_id
    left join order_items oi on o.order_id = oi.order_id
    group by c.customer_id, c.first_name, c.last_name
),
ranked_min as (
    select *
    from customer_totals
    order by total_revenue asc
    limit 3
),
ranked_max as (
    select *
    from customer_totals
    order by total_revenue desc
    limit 3
)
select *
from ranked_min
union all
select *
from ranked_max
order by total_revenue;


--6
with order_sums as (
    select
        o.order_id,
        o.customer_id,
        o.order_date,
        sum(oi.quantity * oi.item_list_price_at_sale) as order_sum
    from orders o
    join order_items oi on o.order_id = oi.order_id
    group by o.order_id, o.customer_id, o.order_date
),
ordered as (
    select
        customer_id,
        order_id,
        order_date,
        order_sum,
        row_number() over (
            partition by customer_id
            order by order_date
        ) as rn
    from order_sums
)
select
    customer_id,
    order_id,
    order_date,
    order_sum
from ordered
where rn = 2
order by customer_id;

--7
with ordered as (
    select
        o.customer_id,
        o.order_id,
        o.order_date,
        lag(o.order_date) over (
            partition by o.customer_id
            order by o.order_date
        ) as prev_order_date
    from orders o
),
intervals as (
    select
        customer_id,
        order_id,
        order_date,
        prev_order_date,
        (order_date - prev_order_date) as diff_days
    from ordered
    where prev_order_date is not null
),
max_intervals as (
    select
        customer_id,
        max(diff_days) as max_interval_days
    from intervals
    group by customer_id
)
select
    c.first_name,
    c.last_name,
    c.job_title,
    mi.max_interval_days
from max_intervals mi
join customer c on c.customer_id = mi.customer_id
order by mi.max_interval_days desc;

--8
with order_sums as (
    select
        o.customer_id,
        sum(oi.quantity * oi.item_list_price_at_sale) as order_sum
    from orders o
    join order_items oi on o.order_id = oi.order_id
    group by o.customer_id
),
customer_totals as (
    select
        c.customer_id,
        c.first_name,
        c.last_name,
        c.wealth_segment,
        coalesce(os.order_sum, 0) as total_revenue
    from customer c
    left join order_sums os on c.customer_id = os.customer_id
),
ranked as (
    select
        *,
        rank() over (
            partition by wealth_segment
            order by total_revenue desc
        ) as rnk
    from customer_totals
)
select
    first_name,
    last_name,
    wealth_segment,
    total_revenue
from ranked
where rnk <= 5
order by wealth_segment, total_revenue desc;

