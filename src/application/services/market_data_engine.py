from __future__ import annotations

from collections import deque
from datetime import UTC, datetime
from decimal import Decimal

from domain.enums import EventType
from domain.events import MarketTickReceived, OrderBookUpdated
from domain.models import MarketSnapshot

from infrastructure.coinbase.ws.models import CoinbaseWSMessage, build_orderbook_snapshot, parse_timestamp

from .event_bus import InMemoryEventBus


class MarketDataEngine:
    def __init__(self, event_bus: InMemoryEventBus, volatility_window: int = 50, long_ema_window: int = 200, rsi_window: int = 14) -> None:
        self.event_bus = event_bus
        self.volatility_window = volatility_window
        self.long_ema_window = long_ema_window
        self.rsi_window = rsi_window
        self._prices: dict[str, deque[Decimal]] = {}
        self._long_prices: dict[str, deque[Decimal]] = {}
        # 5-minute candle aggregation for RSI
        self._candle_closes: dict[str, deque[Decimal]] = {}   # completed candle closes
        self._candle_current: dict[str, tuple[int, Decimal]] = {}  # (5min_bucket, last_price)
        self._snapshots: dict[str, MarketSnapshot] = {}
        self._last_heartbeat_at: dict[str, datetime] = {}

    def get_snapshot(self, product_id: str) -> MarketSnapshot | None:
        return self._snapshots.get(product_id)

    def seed_candle_closes(self, product_id: str, closes: list[Decimal]) -> None:
        """Pre-warm RSI with historical 5-min candle closes (oldest first)."""
        candle_closes = self._candle_closes.setdefault(
            product_id, deque(maxlen=self.rsi_window + 2)
        )
        for close in closes:
            candle_closes.append(close)

    def is_heartbeat_stale(self, product_id: str, threshold_seconds: int) -> bool:
        last_seen = self._last_heartbeat_at.get(product_id)
        if last_seen is None:
            return True
        return (datetime.now(UTC) - last_seen).total_seconds() > threshold_seconds

    def has_seen_heartbeat(self, product_id: str) -> bool:
        return product_id in self._last_heartbeat_at

    async def process_ws_message(self, raw_message: dict) -> None:
        message = CoinbaseWSMessage.model_validate(raw_message)
        if message.channel == "heartbeats":
            for event in message.events:
                for heartbeat in event.get("heartbeats", []):
                    self._last_heartbeat_at[heartbeat["product_id"]] = parse_timestamp(heartbeat.get("timestamp"))
            return

        if message.channel == "ticker":
            await self._process_ticker(message)
        elif message.channel == "level2":
            await self._process_level2(message)

    async def _process_ticker(self, message: CoinbaseWSMessage) -> None:
        for event in message.events:
            for ticker in event.get("tickers", []):
                product_id = ticker["product_id"]
                bid = Decimal(str(ticker.get("best_bid", ticker.get("price"))))
                ask = Decimal(str(ticker.get("best_ask", ticker.get("price"))))
                last_trade_price = Decimal(str(ticker["price"]))
                last_trade_size = Decimal(str(ticker.get("last_size", "0")))
                event_time = parse_timestamp(ticker.get("time") or message.timestamp)

                prices = self._prices.setdefault(product_id, deque(maxlen=self.volatility_window))
                prices.append(last_trade_price)
                long_prices = self._long_prices.setdefault(product_id, deque(maxlen=self.long_ema_window))
                long_prices.append(last_trade_price)

                # Aggregate ticks into 5-minute candles; RSI uses candle closes
                candle_closes = self._candle_closes.setdefault(
                    product_id, deque(maxlen=self.rsi_window + 2)
                )
                five_min_bucket = int(event_time.timestamp()) // 300
                current = self._candle_current.get(product_id)
                if current is None:
                    self._candle_current[product_id] = (five_min_bucket, last_trade_price)
                elif current[0] != five_min_bucket:
                    candle_closes.append(current[1])  # finalise previous candle close
                    self._candle_current[product_id] = (five_min_bucket, last_trade_price)
                else:
                    self._candle_current[product_id] = (five_min_bucket, last_trade_price)

                realized_volatility = self._calculate_realized_volatility(prices)
                spread_abs = ask - bid
                mid = (ask + bid) / Decimal("2")
                spread_bps = Decimal("0") if mid == 0 else (spread_abs / mid) * Decimal("10000")
                snapshot = MarketSnapshot(
                    product_id=product_id,
                    bid=bid,
                    ask=ask,
                    mid=mid,
                    microprice=mid,
                    short_vwap=sum(prices) / Decimal(len(prices)),
                    short_ema=self._calculate_ema(prices),
                    long_ema=self._calculate_ema(long_prices),
                    rsi=self._calculate_rsi(candle_closes, self.rsi_window),
                    realized_volatility=realized_volatility,
                    spread_abs=spread_abs,
                    spread_bps=spread_bps,
                    spread_zscore=Decimal("0"),
                    flow_bias=Decimal("0"),
                    top_book_imbalance=Decimal("0.5"),
                    last_trade_price=last_trade_price,
                    last_trade_size=last_trade_size,
                    event_time=event_time,
                    source_latency_ms=0,
                )
                self._snapshots[product_id] = snapshot
                await self.event_bus.publish(
                    MarketTickReceived(
                        correlation_id=f"tick-{product_id}-{int(event_time.timestamp() * 1000)}",
                        event_type=EventType.MARKET_TICK_RECEIVED,
                        product_id=product_id,
                        emitted_at=event_time,
                        producer="MarketDataEngine",
                        snapshot=snapshot,
                    )
                )

    async def _process_level2(self, message: CoinbaseWSMessage) -> None:
        for event in message.events:
            updates = event.get("updates", [])
            if not updates:
                continue
            product_id = event.get("product_id") or updates[0].get("product_id")
            bids = [item for item in updates if item.get("side") == "bid"]
            asks = [item for item in updates if item.get("side") == "offer"]
            if not product_id or not bids or not asks:
                continue
            best_bid = max(bids, key=lambda item: Decimal(str(item["price"])))
            best_ask = min(asks, key=lambda item: Decimal(str(item["price"])))
            snapshot = build_orderbook_snapshot(
                product_id=product_id,
                bid_price=Decimal(str(best_bid["price"])),
                bid_size=Decimal(str(best_bid["new_quantity"])),
                ask_price=Decimal(str(best_ask["price"])),
                ask_size=Decimal(str(best_ask["new_quantity"])),
                sequence=message.sequence_num or 0,
                event_time=parse_timestamp(message.timestamp),
            )
            await self.event_bus.publish(
                OrderBookUpdated(
                    correlation_id=f"l2-{product_id}-{snapshot.sequence}",
                    event_type=EventType.ORDER_BOOK_UPDATED,
                    product_id=product_id,
                    emitted_at=snapshot.event_time,
                    producer="MarketDataEngine",
                    snapshot=snapshot,
                )
            )

    @staticmethod
    def _calculate_ema(prices: deque[Decimal]) -> Decimal:
        if not prices:
            return Decimal("0")
        # Use the deque's configured window (maxlen), not the current fill level
        window = prices.maxlen if prices.maxlen else len(prices)
        multiplier = Decimal("2") / Decimal(window + 1)
        ema = prices[0]
        for price in list(prices)[1:]:
            ema = (price - ema) * multiplier + ema
        return ema

    @staticmethod
    def _calculate_rsi(prices: deque[Decimal], window: int = 14) -> Decimal:
        if len(prices) < window + 1:
            return Decimal("50")
        price_list = list(prices)[-(window + 1):]
        gains: list[Decimal] = []
        losses: list[Decimal] = []
        for prev, curr in zip(price_list, price_list[1:]):
            change = curr - prev
            gains.append(change if change > 0 else Decimal("0"))
            losses.append(-change if change < 0 else Decimal("0"))
        avg_gain = sum(gains) / Decimal(window)
        avg_loss = sum(losses) / Decimal(window)
        if avg_loss == 0:
            return Decimal("100")
        rs = avg_gain / avg_loss
        return (Decimal("100") - (Decimal("100") / (Decimal("1") + rs))).quantize(Decimal("0.01"))

    @staticmethod
    def _calculate_realized_volatility(prices: deque[Decimal]) -> Decimal:
        if len(prices) < 2:
            return Decimal("0")
        returns: list[Decimal] = []
        price_list = list(prices)
        for prev_price, current_price in zip(price_list, price_list[1:]):
            if prev_price == 0:
                continue
            returns.append((current_price / prev_price) - Decimal("1"))
        if not returns:
            return Decimal("0")
        mean_return = sum(returns) / Decimal(len(returns))
        variance = sum((ret - mean_return) * (ret - mean_return) for ret in returns) / Decimal(len(returns))
        return variance.sqrt()
