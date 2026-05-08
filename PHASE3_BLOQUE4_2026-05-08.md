# Fase 3 - Bloque 4 (Chaos Smoke Operativo)

Fecha: 2026-05-08  
Repo: `C:\Users\Abraham\source\repos\TradingBotV4`

## Objetivo del bloque
Automatizar una corrida corta de chaos en staging V4 y validar alertas/umbrales de forma reproducible.

## Implementado
1. Script operativo:
   - `scripts/phase3_chaos_smoke.ps1`
2. Guía rápida en README:
   - `worker-rust/README.md`
3. Validaciones automáticas incluidas:
   - Run A (`synthetic + live`):
     - `redis_*_failed`
     - `execution_*_failed`
     - thresholds de `market_data_gap_detected` y `heartbeat_lag_detected`
   - Run B (`bybit_rest + simulator`):
     - `market_data_provider_error` con provider `bybit_rest`

## Evidencia de ejecución
Comando:
```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\phase3_chaos_smoke.ps1
```

Resultado:
- `CHAOS SMOKE: PASS`
- Run A: 129 eventos (incluye `execution_submit_failed`, `redis_*_failed`, `market_data_gap_detected`, `heartbeat_lag_detected`)
- Run B: 26 eventos (incluye `market_data_provider_error`)

Logs generados:
- `logs/phase3_chaos/runA_synthetic_live_20260508_115402.out.log`
- `logs/phase3_chaos/runB_bybit_market_chaos_20260508_115415.out.log`

## Notas de seguridad operativa
- El script usa por defecto solo el stack aislado de V4:
  - Postgres: `localhost:5443` (`tradingbotv4_staging`)
  - Redis: `localhost:6390/15` (`tradingbotv4-staging-redis`)
- No toca contenedores ni base de datos de V3.
