import json
import pandas as pd

def transform_stock_data(raw_json_path="scripts/raw_data.json"):
    # 1. Open and read the raw JSON file
    with open(raw_json_path, "r") as f:
        data = json.load(f)

    # 2. Extract stock symbol (e.g. "AAPL") from Meta Data
    symbol = data.get("Meta Data", {}).get("2. Symbol", "UNKNOWN")

    # 3. Extract the daily time series dictionary
    time_series = data.get("Time Series (Daily)", {})
    records = []

    # 4. Loop through every date and unpack its values
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

    # 5. Convert list of records into a Pandas DataFrame (table)
    df = pd.DataFrame(records)

    # 6. Sort table by date ascending (oldest to newest)
    df.sort_values("date", inplace=True)

    return df

if __name__ == "__main__":
    df = transform_stock_data()
    print("--- Transformed Data Preview ---")
    print(df.head())

    # Save to CSV for inspection
    df.to_csv("scripts/transformed_data.csv", index=False)
    print("\nSaved transformed data to scripts/transformed_data.csv")
