# test_aapl_routing.py
import asyncio
from unittest.mock import MagicMock, patch
from ws_collector import _process_trade
from observability import ObservabilityState

# Minimal synthetic AAPL trade matching your TradeEvent contract
synthetic_trade = {
    "T": "t",
    "S": "AAPL",
    "i": 123455,
    "x": "C",
    "p": 195.42,
    "s": 100,
    "t": "2026-05-07T10:30:00.000000000Z",
    "c": ["@"],
    "z": "C",
}

async def main():
    obs = ObservabilityState()

    # Patch publisher.publish so no real GCP calls are made
    with patch("ws_collector.publisher") as mock_publisher:
        mock_future = MagicMock()
        mock_future.result.return_value = "mock-message-id"
        mock_publisher.publish.return_value = mock_future
        mock_publisher.topic_path.side_effect = lambda proj, topic: f"projects/{proj}/topics/{topic}"

        await _process_trade(synthetic_trade, obs)

        # Assert it published to the AAPL topic, not the general one
        call_args = mock_publisher.publish.call_args
        topic_used = call_args[0][0]
        print(f"\n✓ Published to topic: {topic_used}")
        assert "aapl" in topic_used.lower(), f"Expected AAPL topic, got: {topic_used}"
        print("✓ Routing assertion passed")

asyncio.run(main())