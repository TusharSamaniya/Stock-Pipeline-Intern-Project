import requests
import os
import time
import json
from dotenv import load_dotenv

load_dotenv("config/.env")
API_KEY = os.getenv("API_KEY")

def fetch_stock_data(symbol="AAPL"):
    url = "https://www.alphavantage.co/query"
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": symbol,
        "apikey": API_KEY
    }
    response = requests.get(url, params=params)
    data = response.json()

    if "Error Message" in data:
        print("Invalid API call - check symbol or API key.")
        return None
    elif "Note" in data:
        print("Rate limit reached. Waiting 30 seconds...")
        time.sleep(30)
        return fetch_stock_data(symbol)
    else:
        return data

if __name__ == "__main__":
    stock_data = fetch_stock_data("AAPL")
    if stock_data:
        print(stock_data)
        with open("/opt/airflow/scripts/raw_data.json", "w") as f:
            json.dump(stock_data, f, indent=4)
            print("Raw JSON saved to /opt/airflow/scripts/raw_data.json")