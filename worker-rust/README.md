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
- `TB_MARKET_DATA_GAP_WARN_MS` (threshold de alerta para `market_data_gap_detected`)
- `TB_COMMAND_LAG_WARN_MS` (threshold de alerta para `command_lag_detected`)
- `TB_HEARTBEAT_LAG_WARN_MS` (threshold de alerta para `heartbeat_lag_detected`)
- `TB_CHAOS_REDIS_FAIL_EVERY_N` (simula fallo de Redis cada N operaciones)
- `TB_CHAOS_BYBIT_MARKET_FAIL_EVERY_N` (simula fallo de market data Bybit cada N requests)
- `TB_CHAOS_BYBIT_EXEC_FAIL_EVERY_N` (simula fallo de ejecucion Bybit cada N llamadas API)

## Smoke test de Chaos (staging V4 aislado)

Script:
```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\phase3_chaos_smoke.ps1
```

Que valida:
- Run A (`synthetic + live`): alertas `redis_*_failed`, alertas de `execution_*_failed`,
  y thresholds de `market_data_gap_detected` / `heartbeat_lag_detected`.
- Run B (`bybit_rest + simulator`): alerta `market_data_provider_error` con provider `bybit_rest`.

Defaults del script (solo V4):
- Postgres staging: `localhost:5443` / `tradingbotv4_staging`
- Redis staging: `localhost:6390/15`
- Contenedor Redis para flush: `tradingbotv4-staging-redis`

Notas:
- El script no toca V3.
- Si una validacion falla, sale con codigo `1` y deja rutas de logs para diagnostico.

## Shadow Diff Replay (Fase 4 - Bloque 1)

Script:
```powershell
.\.venv\Scripts\python.exe .\scripts\phase4_shadow_diff_replay.py
```

Modo dual recomendado:
```powershell
.\.venv\Scripts\python.exe .\scripts\phase4_shadow_diff_replay.py --mode both
```

Salida:
- genera replay (si no se pasa `--replay-path`)
- ejecuta worker Rust en modo `replay + simulator`
- evalua kernel Python sobre el mismo stream
- produce reporte de divergencias por ciclo:
  - `logs/phase4_shadow/shadow_diff_<timestamp>.json`
  - `logs/phase4_shadow/shadow_diff_<timestamp>.md`
- resumen dual:
  - `strict` -> paridad de `order_submitted` / `order_canceled`
  - `intent` -> paridad de `kernel_bootstrap_grid` / `kernel_rebalance_grid`
- gates informativos (ajustables):
  - `--strict-gate` (default `0.80`)
  - `--intent-gate` (default `0.95`)
