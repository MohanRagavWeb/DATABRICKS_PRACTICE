{{ config(materialized='table') }}

SELECT
    order_id,
    customer,
    city,
    amount,
    order_date
FROM {{ ref('bronze_sales_dbt') }}
WHERE city IS NOT NULL