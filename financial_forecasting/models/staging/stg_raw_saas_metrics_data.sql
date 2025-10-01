
-- models/staging/stg_raw_saas_metrics_data.sql
WITH source_data AS (
    SELECT
        customer_id,
        subscription_start_date,
        monthly_recurring_revenue,
        churn_date,
        plan_type
    FROM
        {{ source('raw', 'raw_saas_metrics') }}
)
SELECT
    CAST(customer_id AS INTEGER) AS customer_id,
    CAST(subscription_start_date AS DATE) AS subscription_start_date,
    CAST(monthly_recurring_revenue AS NUMERIC) AS monthly_recurring_revenue,
    CAST(churn_date AS DATE) AS churn_date,
    CAST(plan_type AS VARCHAR) AS plan_type
FROM
    source_data
