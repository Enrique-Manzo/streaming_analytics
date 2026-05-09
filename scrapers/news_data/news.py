import requests
import pandas as pd
from datetime import datetime, timedelta
import time


def fetch_news_sentiment(api_key: str, delay_seconds: float = 1.0) -> pd.DataFrame:
    """
    Fetches news sentiment data from Alpha Vantage from February 2026 back to January 2018.

    Uses 14-day windows per request (up to 1000 articles each) to stay within
    API constraints. A delay between requests is applied to respect rate limits
    (free tier: 5 requests/min → 12s delay; premium: reduce as needed).

    Args:
        api_key:        Your Alpha Vantage API key.
        delay_seconds:  Seconds to wait between API calls (default 12 for free tier).

    Returns:
        A pandas DataFrame with columns:
            title,  time_published, summary, source, category_within_source,
            overall_sentiment_score, overall_sentiment_label
    """

    BASE_URL = "https://www.alphavantage.co/query"
    WINDOW_DAYS = 14  # fixed window size — treats every month as 28 days

    # ------------------------------------------------------------------ #
    #  Build list of (time_from, time_to) windows, newest → oldest        #
    # ------------------------------------------------------------------ #
    end_date   = datetime(2026, 2, 28)   # inclusive upper bound
    start_date = datetime(2018, 1,  1)   # inclusive lower bound

    windows: list[tuple[datetime, datetime]] = []
    window_end = end_date
    while window_end > start_date:
        window_start = max(window_end - timedelta(days=WINDOW_DAYS - 1), start_date)
        windows.append((window_start, window_end))
        window_end = window_start - timedelta(days=1)

    print(f"Total windows to fetch: {len(windows)}")

    # ------------------------------------------------------------------ #
    #  Fetch each window                                                   #
    # ------------------------------------------------------------------ #
    all_records: list[dict] = []

    for idx, (w_from, w_to) in enumerate(windows, start=1):
        time_from_str = w_from.strftime("%Y%m%dT%H%M")
        time_to_str   = w_to.strftime("%Y%m%dT2359")

        params = {
            "function":  "NEWS_SENTIMENT",
            "time_from": time_from_str,
            "time_to":   time_to_str,
            "limit":     1000,
            "apikey":    api_key,
        }

        print(f"[{idx}/{len(windows)}] Fetching {time_from_str} → {time_to_str} ...", end=" ")

        try:
            response = requests.get(BASE_URL, params=params, timeout=30)
            response.raise_for_status()
            payload = response.json()
        except requests.exceptions.RequestException as exc:
            print(f"REQUEST ERROR: {exc} — skipping window.")
            time.sleep(delay_seconds)
            continue

        # Alpha Vantage returns an "Information" key when rate-limited
        if "Information" in payload:
            print(f"RATE-LIMITED: {payload['Information']}")
            print("Waiting 60 s before retrying …")
            time.sleep(60)
            # Retry once
            try:
                response = requests.get(BASE_URL, params=params, timeout=30)
                payload  = response.json()
            except requests.exceptions.RequestException as exc:
                print(f"Retry failed: {exc} — skipping window.")
                continue

        feed = payload.get("feed", [])
        print(f"{len(feed)} articles")
        if feed:
            for article in feed:
                all_records.append({
                    "title":                    article.get("title"),
                    "time_published":           article.get("time_published"),
                    "summary":                  article.get("summary"),
                    "source":                   article.get("source"),
                    "category_within_source":   article.get("category_within_source"),
                    "overall_sentiment_score":  article.get("overall_sentiment_score"),
                    "overall_sentiment_label":  article.get("overall_sentiment_label"),
                })
                print(article.get("time_published"))

        if idx < len(windows):           # no need to sleep after the last request
            time.sleep(delay_seconds)

    # ------------------------------------------------------------------ #
    #  Build DataFrame                                                     #
    # ------------------------------------------------------------------ #
    df = pd.DataFrame(all_records, columns=[
        "title",
        "time_published",
        "summary",
        "source",
        "category_within_source",
        "overall_sentiment_score",
        "overall_sentiment_label",
    ])

    # Deduplicate on title (same article may appear in overlapping windows)
    df.drop_duplicates(subset="title", inplace=True)
    df.reset_index(drop=True, inplace=True)

    print(f"\nDone. Total unique articles collected: {len(df)}")
    return df


# ---------------------------------------------------------------------- #
#  Example usage                                                          #
# ---------------------------------------------------------------------- #
if __name__ == "__main__":
    API_KEY = ""

    df = fetch_news_sentiment(api_key=API_KEY)

    # Preview
    print(df.head())
    print(df.dtypes)

    # Persist to disk
    df.to_parquet("news_2018_2026.parquet", index=False)
    # df.to_csv("news_sentiment_2018_2026.csv", index=False)
    print("Saved to news_2018_2026.parquet")