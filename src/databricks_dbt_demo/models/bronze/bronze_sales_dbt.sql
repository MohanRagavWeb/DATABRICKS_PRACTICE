{{ config(materialized='view') }}

SELECT *
FROM dbt_bronze_sales