

import os
import sys
import psycopg2
import pandas as pd
from prophet import Prophet
from sqlalchemy import create_engine


def get_db_credentials():
    """Retrieves and validates database credentials from environment variables."""
    db_host = os.environ.get('DB_HOST')
    db_port = os.environ.get('DB_PORT')
    db_name = os.environ.get('DB_NAME')
    db_user = os.environ.get('DB_USER')
    db_pass = os.environ.get('DB_PASS')

    if not all([db_host, db_port, db_name, db_user, db_pass]):
        print("Error: DB environment variables are not set.", file=sys.stderr)
        sys.exit(1)

    return db_host, db_port, db_name, db_user, db_pass

# --- Data Retrieval and Preparation ---
def fetch_and_prepare_data(conn):
    """Pulls data from dbt table and prepares it for Prophet."""
    print("Fetching clean data from fact_monthly_revenue...")
    query = "SELECT subscription_month, monthly_recurring_revenue FROM public.fact_monthly_revenue ORDER BY subscription_month;"

    # Load data directly into a DataFrame
    revenue_df = pd.read_sql(query, conn)

    # Prepare Dataframe for Prophet: rename, convert, and fix timezone
    prophet_df = revenue_df.rename(columns={
        'subscription_month': 'ds',
        'monthly_recurring_revenue': 'y'
    })
    prophet_df['ds'] = pd.to_datetime(prophet_df['ds']).dt.tz_localize(None)

    return prophet_df

# --- Train and Forecast ---
def run_prophet_forecast(prophet_df):
    """Initializes model, fits it, and generates the forecast."""
    print("Training Prophet model and generating 24-month forecast...")
    m = Prophet(
        yearly_seasonality=True,
        changepoint_prior_scale=0.05
    )
    m.fit(prophet_df)

    future = m.make_future_dataframe(periods=24, freq='MS')
    forecast = m.predict(future)

    return forecast

# --- Clean and Ingest ---
def ingest_forecast_to_db(forecast, db_host, db_port, db_name, db_user, db_pass):
    """Cleans the forecast and pushes it back to PostgreSQL using SQLAlchemy."""

    # Clean the DataFrame
    final_forecast_df = forecast[[
        'ds', 'yhat', 'yhat_lower', 'yhat_upper'
    ]].copy()

    final_forecast_df = final_forecast_df.rename(columns={
        'yhat': 'forecasted_mrr',
        'ds': 'subscription_month'
    })

    # Create SQLAlchemy engine
    engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}')

    # Write the DataFrame to a new table
    print(f"Ingesting forecast into fact_monthly_revenue_forecast...")
    final_forecast_df.to_sql(
        'fact_monthly_revenue_forecast',
        engine,
        if_exists='replace',
        index=False
    )
    print("Forecast data successfully loaded.")

# --- Main Orchestration ---
def main():
    db_host, db_port, db_name, db_user, db_pass = get_db_credentials()

    conn = None
    try:
        conn = psycopg2.connect(host=db_host, port=db_port, dbname=db_name, user=db_user, password=db_pass)

        prophet_df = fetch_and_prepare_data(conn)
        forecast = run_prophet_forecast(prophet_df)

        # Ingest the final results
        ingest_forecast_to_db(forecast, db_host, db_port, db_name, db_user, db_pass)

    except Exception as e:
        print(f"An error occurred during forecasting pipeline: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()
