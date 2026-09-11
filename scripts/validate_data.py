import pandas as pd
import psycopg2
import os
from dotenv import load_dotenv

# Load the database connection URL from our .env file
load_dotenv("config/.env")
DB_URL = os.getenv("DATABASE_URL")

def validate_data():
    # 1. Connect to the database and read the whole stocks table into a DataFrame
    conn = psycopg2.connect(DB_URL)
    df = pd.read_sql("SELECT * FROM stocks ORDER BY date", conn)
    conn.close()

    print("=" * 50)
    print(f"Total rows in database: {len(df)}")
    print("=" * 50)

    # --- Subtask 3.1: Check data types ---
    print("\n--- Column Data Types ---")
    print(df.dtypes)

    # --- Subtask 3.2: Verify dates are valid and sorted ---
    print("\n--- Date Range ---")
    print(f"Earliest date: {df['date'].min()}")
    print(f"Latest date:   {df['date'].max()}")
    is_sorted = df['date'].is_monotonic_increasing
    print(f"Are dates sorted in order? {is_sorted}")

    # --- Subtask 3.3: Null checks and summary statistics ---
    print("\n--- Missing (Null) Values Per Column ---")
    print(df.isnull().sum())

    print("\n--- Summary Statistics ---")
    print(df.describe())

    print("\nValidation complete!")

if __name__ == "__main__":
    validate_data()
