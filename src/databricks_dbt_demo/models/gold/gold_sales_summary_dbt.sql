{{ config(materialized='table') }}

SELECT
    city,
    SUM(amount) AS total_sales,
    COUNT(order_id) AS total_orders
FROM {{ ref('silver_sales_clean_dbt') }}
GROUP BY city