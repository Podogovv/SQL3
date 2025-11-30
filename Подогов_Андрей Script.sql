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
SELECT 
    job_industry_category,
    COUNT(*) AS customer_count
FROM customer
GROUP BY job_industry_category
ORDER BY customer_count DESC;
    
 --2
SELECT
    EXTRACT(YEAR FROM o.order_date) AS year,
    EXTRACT(MONTH FROM o.order_date) AS month,
    c.job_industry_category,
    SUM(oi.quantity * oi.item_list_price_at_sale) AS total_revenue
FROM orders o
JOIN customer c ON o.customer_id = c.customer_id
JOIN order_items oi ON o.order_id = oi.order_id
WHERE o.order_status = 'Approved'
GROUP BY
    EXTRACT(YEAR FROM o.order_date),
    EXTRACT(MONTH FROM o.order_date),
    c.job_industry_category
ORDER BY
    year,
    month,
    c.job_industry_category;


--3
SELECT 
    p.brand,
    COUNT(DISTINCT o.order_id) AS unique_online_orders
FROM product p
LEFT JOIN order_items oi ON p.product_id = oi.product_id
LEFT JOIN orders o 
    ON oi.order_id = o.order_id
    AND o.order_status = 'Approved'
    AND o.online_order = 'Yes'
LEFT JOIN customer c 
    ON o.customer_id = c.customer_id
    AND c.job_industry_category = 'IT'
GROUP BY p.brand
ORDER BY p.brand;

--4
--Решение с использованием GROUP BY
SELECT
    c.customer_id,
    c.first_name,
    c.last_name,
    SUM(oi.quantity * oi.item_list_price_at_sale) AS total_revenue,
    MIN(oi.quantity * oi.item_list_price_at_sale) AS min_order_amount,
    MAX(oi.quantity * oi.item_list_price_at_sale) AS max_order_amount,
    COUNT(DISTINCT o.order_id) AS total_orders,
    AVG(order_sum) AS avg_order_amount
FROM customer c
JOIN orders o ON c.customer_id = o.customer_id
JOIN order_items oi ON o.order_id = oi.order_id
JOIN (
    -- подзапрос для суммирования по каждому заказу
    SELECT
        o2.order_id,
        SUM(oi2.quantity * oi2.item_list_price_at_sale) AS order_sum
    FROM orders o2
    JOIN order_items oi2 ON o2.order_id = oi2.order_id
    GROUP BY o2.order_id
) os ON o.order_id = os.order_id
GROUP BY c.customer_id, c.first_name, c.last_name
ORDER BY total_revenue DESC, total_orders DESC;

--Решение с использованием только оконных функций (window functions)
WITH order_sums AS (
    SELECT
        o.order_id,
        o.customer_id,
        SUM(oi.quantity * oi.item_list_price_at_sale) AS order_sum
    FROM orders o
    JOIN order_items oi ON o.order_id = oi.order_id
    GROUP BY o.order_id, o.customer_id
),
window_calc AS (
    SELECT
        c.customer_id,
        c.first_name,
        c.last_name,
        order_sum,
        SUM(order_sum) OVER (PARTITION BY c.customer_id) AS total_revenue,
        MIN(order_sum) OVER (PARTITION BY c.customer_id) AS min_order_amount,
        MAX(order_sum) OVER (PARTITION BY c.customer_id) AS max_order_amount,
        COUNT(order_sum) OVER (PARTITION BY c.customer_id) AS total_orders,
        AVG(order_sum) OVER (PARTITION BY c.customer_id) AS avg_order_amount,
        ROW_NUMBER() OVER (PARTITION BY c.customer_id ORDER BY order_sum DESC) AS rn
    FROM customer c
    JOIN order_sums os ON c.customer_id = os.customer_id
)
SELECT
    customer_id,
    first_name,
    last_name,
    total_revenue,
    min_order_amount,
    max_order_amount,
    total_orders,
    avg_order_amount
FROM window_calc
WHERE rn = 1  -- выбираем одну строку на клиента
ORDER BY total_revenue DESC, total_orders DESC;


--5
WITH order_sums AS (
    -- Сумма по каждому заказу
    SELECT
        o.customer_id,
        SUM(oi.quantity * oi.item_list_price_at_sale) AS order_sum
    FROM orders o
    JOIN order_items oi ON o.order_id = oi.order_id
    GROUP BY o.customer_id
),
customer_totals AS (
    -- Сумма заказов по каждому клиенту (клиенты без заказов → 0)
    SELECT
        c.customer_id,
        c.first_name,
        c.last_name,
        COALESCE(os.order_sum, 0) AS total_revenue
    FROM customer c
    LEFT JOIN order_sums os ON c.customer_id = os.customer_id
),
ranked AS (
    -- Ранжирование по суммам
    SELECT
        *,
        RANK() OVER (ORDER BY total_revenue ASC)  AS r_min,
        RANK() OVER (ORDER BY total_revenue DESC) AS r_max
    FROM customer_totals
)
SELECT
    customer_id,
    first_name,
    last_name,
    total_revenue,
    CASE 
        WHEN r_min <= 3 THEN 'TOP-3 MIN'
        WHEN r_max <= 3 THEN 'TOP-3 MAX'
    END AS category
FROM ranked
WHERE r_min_


--6
WITH order_sums AS (
    -- Сумма каждой транзакции (каждого заказа)
    SELECT
        o.order_id,
        o.customer_id,
        o.order_date,
        SUM(oi.quantity * oi.item_list_price_at_sale) AS order_sum
    FROM orders o
    JOIN order_items oi ON o.order_id = oi.order_id
    GROUP BY o.order_id, o.customer_id, o.order_date
),
ordered AS (
    -- Нумерация транзакций каждого клиента по времени
    SELECT
        customer_id,
        order_id,
        order_date,
        order_sum,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY order_date
        ) AS rn
    FROM order_sums
)
SELECT
    customer_id,
    order_id,
    order_date,
    order_sum
FROM ordered
WHERE rn = 2        -- только вторая транзакция
ORDER BY customer_id;

--7
WITH ordered AS (
    -- Упорядочиваем заказы клиента по дате
    SELECT
        o.customer_id,
        o.order_id,
        o.order_date,
        LAG(o.order_date) OVER (
            PARTITION BY o.customer_id
            ORDER BY o.order_date
        ) AS prev_order_date
    FROM orders o
),
intervals AS (
    -- Вычисляем интервалы между соседними заказами
    SELECT
        customer_id,
        order_id,
        order_date,
        prev_order_date,
        (order_date - prev_order_date) AS diff_days
    FROM ordered
    WHERE prev_order_date IS NOT NULL   -- исключаем клиентов с одним заказом
),
max_intervals AS (
    -- Находим максимальный интервал по каждому клиенту
    SELECT
        customer_id,
        MAX(diff_days) AS max_interval_days
    FROM intervals
    GROUP BY customer_id
)
SELECT
    c.first_name,
    c.last_name,
    c.job_title,
    mi.max_interval_days
FROM max_intervals mi
JOIN customer c ON c.customer_id = mi.customer_id
ORDER BY mi.max_interval_days DESC;

--8
WITH order_sums AS (
    -- Сумма каждого заказа
    SELECT
        o.customer_id,
        SUM(oi.quantity * oi.item_list_price_at_sale) AS order_sum
    FROM orders o
    JOIN order_items oi ON o.order_id = oi.order_id
    GROUP BY o.customer_id
),
customer_totals AS (
    -- Сумма заказов по каждому клиенту (клиенты без заказов → 0)
    SELECT
        c.customer_id,
        c.first_name,
        c.last_name,
        c.wealth_segment,
        COALESCE(os.order_sum, 0) AS total_revenue
    FROM customer c
    LEFT JOIN order_sums os ON c.customer_id = os.customer_id
),
ranked AS (
    -- Ранжирование клиентов внутри каждого сегмента
    SELECT
        *,
        RANK() OVER (
            PARTITION BY wealth_segment
            ORDER BY total_revenue DESC
        ) AS rnk
    FROM customer_totals
)
SELECT
    first_name,
    last_name,
    wealth_segment,
    total_revenue
FROM ranked
WHERE rnk <= 5
ORDER BY wealth_segment, total_revenue DESC;

