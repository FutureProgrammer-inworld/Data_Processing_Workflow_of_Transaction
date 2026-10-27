SELECT
    r.order_id,
    r.customer_id,
    r.product_category,
    r.order_timestamp AS order_ts,
    r.order_date,
    r.payment_method,
    r.status,
    r.quantity,
    r.unit_price,
    r.discount_pct,
    r.gross_amount,
    r.net_amount
FROM {{ ref('stg_orders') }} AS r
INNER JOIN {{ ref('dim_customer') }} AS c
    ON r.customer_id = c.customer_id
INNER JOIN {{ ref('dim_product_category') }} AS p
    ON r.product_category = p.product_category