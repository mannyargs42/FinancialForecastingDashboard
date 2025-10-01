

import os
import subprocess
import sys
from typing import Dict

def create_dbt_profiles(env_vars: Dict[str, str]):
    """
    Creates the profiles.yml file for dbt using environment variables.
    """
    dbt_dir = os.path.join(os.path.expanduser("~"), ".dbt")
    os.makedirs(dbt_dir, exist_ok=True)
    profiles_content = f"""
financial_forecasting:
  target: dev
  outputs:
    dev:
      type: postgres
      host: {env_vars['DB_HOST']}
      port: {env_vars['DB_PORT']}
      user: {env_vars['DB_USER']}
      password: {env_vars['DB_PASS']}
      dbname: {env_vars['DB_NAME']}
      schema: public
"""
    with open(os.path.join(dbt_dir, "profiles.yml"), "w") as f:
        f.write(profiles_content)
    print("profiles.yml created successfully.")


def run_dbt_pipeline():
    """
    Orchestrates the entire dbt pipeline from staging to final marts model.
    """
    # Use a relative path from the current working directory
    project_dir = os.path.join(os.getcwd(), "financial_forecasting")
    
    # Create required directories for the dbt project, if they don't exist
    staging_dir = os.path.join(project_dir, "models", "staging")
    marts_dir = os.path.join(project_dir, "models", "marts")
    os.makedirs(staging_dir, exist_ok=True)
    os.makedirs(marts_dir, exist_ok=True)
    
    # --- Create Core dbt Project Files ---
    print("Creating core dbt project files...")
    
    # sources.yml
    sources_content = """
version: 2
sources:
  - name: raw
    description: "Raw data ingested from external sources."
    schema: public
    tables:
      - name: raw_saas_metrics
        description: "Raw financial metrics data ingested from a JSON file."
"""
    with open(os.path.join(staging_dir, "sources.yml"), "w") as f:
        f.write(sources_content)
        
    # stg_raw_saas_metrics_data.sql (Corrected file name)
    staging_sql_content = """
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
"""
    with open(os.path.join(staging_dir, "stg_raw_saas_metrics_data.sql"), "w") as f:
        f.write(staging_sql_content)
        
    # schema.yml
    schema_content = """
version: 2
models:
  - name: stg_raw_saas_metrics_data
    description: "Staging model for raw SaaS metrics data."
    columns:
      - name: customer_id
        description: "The unique identifier for a customer."
        tests:
          - unique
          - not_null
      - name: subscription_start_date
        description: "The date the customer's subscription began."
        tests:
          - not_null
"""
    with open(os.path.join(staging_dir, "schema.yml"), "w") as f:
        f.write(schema_content)
        
    # fact_monthly_revenue.sql
    marts_sql_content = """
-- models/marts/fact_monthly_revenue.sql
SELECT
    DATE_TRUNC('month', subscription_start_date) AS subscription_month,
    SUM(monthly_recurring_revenue) AS monthly_recurring_revenue
FROM
    {{ ref('stg_raw_saas_metrics_data') }}
GROUP BY 1
ORDER BY 1
"""
    with open(os.path.join(marts_dir, "fact_monthly_revenue.sql"), "w") as f:
        f.write(marts_sql_content)
        
    # Update dbt_project.yml to include new model paths
    dbt_project_path = os.path.join(project_dir, "dbt_project.yml")
    with open(dbt_project_path, 'r') as file:
        content = file.read()
    if 'model-paths:' not in content:
        content = content.replace("config-version: 2", "config-version: 2\n\nmodel-paths: [\"models/staging\", \"models/marts\"]")
    with open(dbt_project_path, 'w') as file:
        file.write(content)
        
    # --- Final Step: Run the dbt Pipeline ---
    print("Running the dbt pipeline...")
    subprocess.run(['dbt', 'run', '--project-dir', project_dir, '--profiles-dir', os.path.join(os.path.expanduser("~"), ".dbt")], check=True)
    print("Pipeline execution complete.")

def main():
    """
    Main function to orchestrate the entire data pipeline.
    """
    # Database connection details retrieved securely from environment variables
    db_host = os.environ.get('DB_HOST')
    db_port = os.environ.get('DB_PORT')
    db_name = os.environ.get('DB_NAME')
    db_user = os.environ.get('DB_USER')
    db_pass = os.environ.get('DB_PASS')
    
    # Check if all environment variables are set
    if not all([db_host, db_port, db_name, db_user, db_pass]):
        print("Error: Database environment variables are not set.", file=sys.stderr)
        print("Please ensure DB_HOST, DB_PORT, DB_NAME, DB_USER, and DB_PASS are set.", file=sys.stderr)
        sys.exit(1)
    
    env_vars = {
        'DB_HOST': db_host,
        'DB_PORT': db_port,
        'DB_NAME': db_name,
        'DB_USER': db_user,
        'DB_PASS': db_pass
    }
    
    create_dbt_profiles(env_vars)
    run_dbt_pipeline()

if __name__ == "__main__":
    main()
