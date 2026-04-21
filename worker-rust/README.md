# TradingBotV4 Rust Worker (Scaffold)

Este worker implementa el esqueleto de V4 con:

- `TradingKernel` único (misma lógica para paper/live/sim).
- `ExecutionProvider` desacoplado:
  - `BybitLiveExecutionProvider` (placeholder de integración real).
  - `BybitSimulatorExecutionProvider` (incluye lógica de crossing en paper/sim).
- `MarketDataProvider` desacoplado:
  - `SyntheticMarketDataProvider`
  - `ReplayMarketDataProvider`
- Scheduler de cierre diario en zona local (`session_timezone_iana`) con cambios:
  - `next_cycle`: aplica al siguiente ciclo.
  - `immediate`: recalcula inmediatamente el siguiente cierre.

## Variables requeridas (fail-fast)

- `TB_SPACING_BPS`
- `TB_REBALANCE_THRESHOLD_BPS`
- `TB_GRID_LEVELS`
- `TB_LEVEL_SIZE_QUOTE`
- `TB_LOCAL_TIMEZONE_IANA`
- `TB_DAILY_CLOSE_HOUR`
- `TB_DAILY_CLOSE_MINUTE`

## Variables opcionales

- `TB_TENANT_ID` (default tenant V4)
- `TB_EXCHANGE` (default `bybit`)
- `TB_PRODUCT_ID` (default `SOL-USD`)
- `TB_EXECUTION_MODE` (`simulator` | `live`, default `simulator`)
- `TB_RESERVE_USD` (default `0`)
- `TB_SESSION_CAPITAL_USD` (default `100`)
- `TB_TICK_INTERVAL_MS` (default `1000`)
