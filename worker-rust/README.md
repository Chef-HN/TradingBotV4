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

Replay baseline fijo (v1):
```powershell
.\.venv\Scripts\python.exe .\scripts\phase4_shadow_diff_replay.py --mode both --replay-path .\replay\fixtures\solusd_shadow_baseline_v1_20260508.jsonl
```

Perfiles Python disponibles:
- `--python-profile db` (default): usa parámetros reales de estrategia en BD.
- `--python-profile legacy`: perfil histórico del harness.
- `--python-profile rust_projection`: proyecta cardinalidad strict equivalente a Rust (2*grid_levels en bootstrap y rebalance).

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
- enforce opcional:
  - `--enforce-gates --gate-scope intent` (recomendado para Fase 4 actual)
  - `--enforce-gates --gate-scope both` (estricto)

Gate completo (strict+intent) con baseline fijo:
```powershell
.\.venv\Scripts\python.exe .\scripts\phase4_shadow_diff_replay.py --mode both --replay-path .\replay\fixtures\solusd_shadow_baseline_v1_20260508.jsonl --python-profile rust_projection --enforce-gates --gate-scope both
```

## Canary Smoke (Fase 4 - Bloque 6)

Script:
```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\phase4_canary_smoke.ps1
```

Que valida:
- Worker V4 emite `worker_boot` y `kernel_bootstrap_grid`.
- Se observan ciclos reales (`correlation_id` de ciclo) y `order_submitted` minimo.
- No aparecen fallos criticos (`redis_*_failed`, `execution_*_failed`, `market_data_provider_error`, `execution_reconciliation_*`).
- No aparecen alertas de lag/gap por encima del budget configurado.

Defaults del script (solo V4):
- Postgres staging: `localhost:5443` / `tradingbotv4_staging`
- Redis staging: `localhost:6390/15`
- Modo: `simulator`
- Provider: `synthetic`
- `MinCycles` dinamico:
  - `synthetic`: `5`
  - `bybit_rest`: `1` (ventanas estables pueden no generar muchos eventos)

Opcional (si quieres fallback automatico documentado al fallar):
```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\phase4_canary_smoke.ps1 -AutoRollbackOnFail
```

Nota para `bybit_rest`:
- El script ejecuta preflight HTTP a `api.bybit.com` antes del canary.
- Si el entorno no tiene salida de red, falla rapido con codigo `2`.
- Para forzar la corrida sin preflight (diagnostico), usar `-SkipBybitPreflight`.
- Diagnostico extendido de egress:
```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\phase4_bybit_egress_diagnostic.ps1
```

Notas:
- El script no toca V3.
- Si falla, sale con codigo `1` y deja logs en `logs/phase4_canary/`.
