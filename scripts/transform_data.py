import json
import logging
import pandas as pd

# Configure logging format
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def transform_stock_data(raw_json_path="scripts/raw_data.json"):
    try:
        logging.info(f"Opening raw JSON file from {raw_json_path}")
        with open(raw_json_path, "r") as f:
            data = json.load(f)

        # 1. Extract stock symbol (e.g. "AAPL") from Meta Data
        symbol = data.get("Meta Data", {}).get("2. Symbol", "UNKNOWN")

        # 2. Extract the daily time series dictionary
        time_series = data.get("Time Series (Daily)", {})
        if not time_series:
            logging.warning("Warning: 'Time Series (Daily)' is empty or missing in JSON!")
            return pd.DataFrame()

        records = []

        # 3. Loop through every date and unpack its values
        for date, values in time_series.items():
            records.append({
                "symbol": symbol,
                "date": date,
                "open": float(values["1. open"]),
                "high": float(values["2. high"]),
                "low": float(values["3. low"]),
                "close": float(values["4. close"]),
                "volume": int(values["5. volume"])
            })

        # 4. Convert list of records into a Pandas DataFrame (table)
        df = pd.DataFrame(records)

        # --- DATA CLEANING STEPS ---
        df.drop_duplicates(subset=["date"], inplace=True)
        df.dropna(subset=["date", "open", "high", "low", "close", "volume"], inplace=True)
        df.sort_values("date", inplace=True)
        df.reset_index(drop=True, inplace=True)

        logging.info(f"Successfully transformed {len(df)} records for {symbol}")
        return df

    except Exception as e:
        logging.error(f"Error during data transformation: {e}")
        raise e

if __name__ == "__main__":
    df = transform_stock_data()
    if not df.empty:
        print("--- Transformed Data Preview ---")
        print(df.head())

        # Save to CSV for inspection
        df.to_csv("scripts/transformed_data.csv", index=False)
        logging.info("Saved transformed data to scripts/transformed_data.csv")
