import requests
import os
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
    return data

if __name__ == "__main__":
    stock_data = fetch_stock_data("AAPL")
    print(stock_data)