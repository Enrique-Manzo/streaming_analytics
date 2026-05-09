import requests
import time
import pandas as pd
from datetime import datetime

# =========================
# CONFIG
# =========================

import os

from dotenv import load_dotenv

load_dotenv()

# ── Alpaca credentials ────────────────────────────────────────────────────────
ALPACA_API_KEY: str = os.getenv("ALPACA_API_KEY", "")
ALPACA_API_SECRET: str = os.getenv("ALPACA_API_SECRET", "")
BASE_URL = "https://data.alpaca.markets/v2/stocks/bars"

SYMBOLS = ["AAPL"]
TIMEFRAME = "1Min"
START = "2024-01-01"
END = "2026-01-01"

LIMIT = 1000
MAX_RETRIES = 5
SLEEP_BETWEEN_CALLS = 0.3  # seconds


HEADERS = {
    "APCA-API-KEY-ID": ALPACA_API_KEY,
    "APCA-API-SECRET-KEY": ALPACA_API_SECRET
}


# =========================
# CORE FETCH FUNCTION
# =========================

def fetch_bars(symbol):
    params = {
        "symbols": symbol,
        "timeframe": TIMEFRAME,
        "start": START,
        "end": END,
        "limit": LIMIT
    }

    all_bars = []
    next_page_token = None
    total_requests = 0

    while True:
        if next_page_token:
            params["page_token"] = next_page_token

        for attempt in range(MAX_RETRIES):
            try:
                response = requests.get(BASE_URL, headers=HEADERS, params=params)

                if response.status_code == 429:
                    # Rate limit
                    wait = 2 ** attempt
                    print(f"Rate limited. Sleeping {wait}s...")
                    time.sleep(wait)
                    continue

                response.raise_for_status()
                data = response.json()
                break

            except Exception as e:
                wait = 2 ** attempt
                print(f"Error: {e}. Retrying in {wait}s...")
                time.sleep(wait)
        else:
            raise RuntimeError("Max retries exceeded")

        bars = data.get("bars", {}).get(symbol, [])
        all_bars.extend(bars)

        next_page_token = data.get("next_page_token")
        total_requests += 1

        print(
            f"{symbol} | Request #{total_requests} | "
            f"Total rows: {len(all_bars)}"
        )

        if not next_page_token:
            break

        time.sleep(SLEEP_BETWEEN_CALLS)

    df = pd.DataFrame(all_bars)

    if df.empty:
        return df

    # Rename columns for clarity
    df = df.rename(columns={
        "t": "timestamp",
        "o": "open",
        "h": "high",
        "l": "low",
        "c": "close",
        "v": "volume"
    })

    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df["symbol"] = symbol

    return df


# =========================
# MAIN
# =========================

def main():
    all_dfs = []

    for symbol in SYMBOLS:
        print(f"\nDownloading {symbol}...")
        df = fetch_bars(symbol)

        if not df.empty:
            all_dfs.append(df)

            # Save individual file
            df.to_parquet(f"{symbol}_bars.parquet", index=False)
            print(f"Saved {symbol}_bars.parquet")

    if all_dfs:
        final_df = pd.concat(all_dfs).sort_values("timestamp")

        final_df.to_parquet("all_symbols.parquet", index=False)
        final_df.to_csv("all_symbols.csv", index=False)

        print(f"\nTotal rows: {len(final_df)}")
        print("Saved combined dataset")

    print("\nDone.")


if __name__ == "__main__":
    main()