
SELECT
    CAST(order_id AS VARCHAR) AS order_id,
    CAST(customer_id AS VARCHAR) AS customer_id,
    CAST(order_ts AS DATE) AS order_date,
    CAST(order_ts AS TIMESTAMP) AS order_timestamp,
    CAST(quantity AS BIGINT) AS quantity,
    CAST(unit_price AS DOUBLE) AS unit_price,
    CAST(discount_pct AS DOUBLE) AS discount_pct,
    CAST(status AS VARCHAR) AS status,
    CAST(product_category AS VARCHAR) AS product_category,
    CAST(payment_method AS VARCHAR) AS payment_method,
    CAST(gross_amount AS DOUBLE) AS gross_amount,
    CAST(net_amount AS DOUBLE) AS net_amount
FROM {{ source('raw', 'raw_orders') }}
