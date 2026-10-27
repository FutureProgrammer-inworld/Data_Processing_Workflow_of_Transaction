SELECT
    order_date,
    product_category,
    payment_method,
    SUM(quantity * unit_price * (1 - discount_pct)) AS net_sales,
    SUM(quantity) AS total_quantity,
    COUNT(*) AS total_orders
FROM {{ ref('fct_orders') }}
WHERE status IN ('paid', 'shipped')
GROUP BY
    order_date,
    product_category,
    payment_method