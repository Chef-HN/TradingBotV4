import asyncio
from datetime import UTC, datetime

from application.services.event_bus import InMemoryEventBus
from application.services.market_data_engine import MarketDataEngine


def test_has_seen_heartbeat_only_after_first_heartbeat_message() -> None:
    engine = MarketDataEngine(InMemoryEventBus())
    assert engine.has_seen_heartbeat("SOL-USD") is False

    asyncio.run(
        engine.process_ws_message(
            {
                "channel": "heartbeats",
                "events": [
                    {
                        "heartbeats": [
                            {
                                "product_id": "SOL-USD",
                                "timestamp": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
                            }
                        ]
                    }
                ],
            }
        )
    )

    assert engine.has_seen_heartbeat("SOL-USD") is True
    assert engine.is_heartbeat_stale("SOL-USD", threshold_seconds=60) is False
