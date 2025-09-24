
-- Build the final table for analysis.
-- Adding the MRR metric to data from the staging layer.
-- Uses a dbt ref macro.

SELECT
    DATE_TRUNC('month', subscription_start_date) AS subscription_month,
    SUM(monthly_recurring_revenue) AS monthly_recurring_revenue
FROM
    {{ ref('stg_raw_saas_metrics_data') }}
GROUP BY 1
ORDER BY 1
