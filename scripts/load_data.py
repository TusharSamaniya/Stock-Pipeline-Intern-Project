import pandas as pd
import psycopg2
import os
import logging
from dotenv import load_dotenv

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

load_dotenv("config/.env")

# Use DATABASE_URL from .env
DB_URL = os.getenv("DATABASE_URL")

def load_data_to_db(csv_path="/app/scripts/transformed_data.csv"):
    try:
        # 1. Read transformed CSV
        df = pd.read_csv(csv_path)
        logging.info(f"Loaded {len(df)} records from {csv_path}")

        # 2. Connect to Database
        conn = psycopg2.connect(DB_URL)
        cur = conn.cursor()

        # 3. Loop through rows and insert
        for _, row in df.iterrows():
            cur.execute("""
                INSERT INTO stocks (symbol, date, open, high, low, close, volume)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (symbol, date) DO NOTHING;
            """, (
                row["symbol"], row["date"], row["open"], row["high"],
                row["low"], row["close"], row["volume"]
            ))

        # 4. Save changes and close
        conn.commit()
        cur.close()
        conn.close()
        logging.info("Successfully inserted data into PostgreSQL.")

    except Exception as e:
        logging.error(f"Error loading data: {e}")
        raise e

if __name__ == "__main__":
    load_data_to_db()
