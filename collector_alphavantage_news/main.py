# main.py
# ---------------------------------------------------------------------------
# Cloud Run Job entry point for the Alpha Vantage news collector.
#
# Execution flow:
#   1. Run one collection cycle (fetch → filter → validate → publish)
#   2. Log a human-readable run summary to stdout (captured by Cloud Logging)
#   3. Push Prometheus metrics to the Pushgateway on the shared GCP VM
#   4. Exit with code 0 on success, 1 on unhandled error
#
# There is no long-running server here — this script is designed to be
# triggered every 5 minutes by a Cloud Scheduler → Cloud Run Job schedule.
# ---------------------------------------------------------------------------

import sys

from news_collector import run_collection


def main() -> None:
    try:
        obs = run_collection()
    except Exception as exc:
        print(f"[MAIN] FATAL — unhandled error during collection: {exc}")
        sys.exit(1)

    obs.log_summary()
    obs.push_to_gateway()


if __name__ == "__main__":
    main()
