# -*- coding: utf-8 -*-
"""database_setup.ipynb

Original file is located at
    https://colab.research.google.com/drive/1Knwdl0v-nq6d7mGyf3bQqeIVYjpgLNc6
"""

#!/usr/bin/env python3

"""
This script sets up a PostgreSQL database and a 'raw_saas_metrics' table,
then ingests customer data from a JSON file.
"""

import json
import os
import psycopg2

def create_table(conn):
    """
    Creates the 'raw_saas_metrics' table if it does not exist.
    """
    create_table_query = """
    CREATE TABLE IF NOT EXISTS raw_saas_metrics (
        customer_id INT PRIMARY KEY,
        subscription_start_date DATE,
        monthly_recurring_revenue DECIMAL,
        churn_date DATE,
        plan_type VARCHAR(50)
    );
    """
    cursor = conn.cursor()
    cursor.execute(create_table_query)
    conn.commit()
    print("Table 'raw_saas_metrics' is ready.")
    cursor.close()

def ingest_data(conn, file_path="raw_saas_data.json"):
    """
    Ingests data from a JSON file into the 'raw_saas_metrics' table.
    """
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)

        insert_query = """
        INSERT INTO raw_saas_metrics (
            customer_id,
            subscription_start_date,
            monthly_recurring_revenue,
            churn_date,
            plan_type
        ) VALUES (%s, %s, %s, %s, %s);
        """

        cursor = conn.cursor()
        for record in data:
            cursor.execute(
                insert_query,
                (
                    record.get('customer_id'),
                    record.get('subscription_start_date'),
                    record.get('monthly_recurring_revenue'),
                    record.get('churn_date'),
                    record.get('plan_type')
                )
            )
        conn.commit()
        print(f"Successfully ingested {len(data)} records into raw_saas_metrics.")
        cursor.close()

    except (psycopg2.Error, FileNotFoundError) as e:
        print(f"Error during data ingestion: {e}")
        conn.rollback()
        raise

def main():
    """
    Main function to run the database setup and data ingestion process.
    """
    # Database connection details retrieved securely from environment variables
    DB_HOST = os.environ.get('DB_HOST')
    DB_PORT = os.environ.get('DB_PORT')
    DB_NAME = os.environ.get('DB_NAME')
    DB_USER = os.environ.get('DB_USER')
    DB_PASS = os.environ.get('DB_PASS')

    # Check if all environment variables are set
    if not all([DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS]):
        print("Error: Database environment variables are not set.")
        print("Please ensure you have set DB_HOST, DB_PORT, DB_NAME, DB_USER, and DB_PASS.")
        return

    conn = None
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        print("Successfully connected to the database.")

        create_table(conn)

        ingest_data(conn, file_path='raw_saas_data.json')

    except psycopg2.OperationalError as e:
        print(f"Connection error: {e}")
        print("Please ensure your PostgreSQL database is running and the connection details are correct.")

    except FileNotFoundError as e:
        print(f"File not found error: {e}")
        print("Please ensure 'raw_saas_data.json' exists in the correct directory.")

    except Exception as e:
        print(f"An unexpected error occurred: {e}")

    finally:
        if conn is not None:
            conn.close()
            print("Database connection closed.")

if __name__ == "__main__":
    main()
