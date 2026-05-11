# Fase 4 - Bloque 7 (Pre-Cutover bybit_rest + Go/No-Go)

Fecha: 2026-05-11  
Repo: `C:\Users\Abraham\source\repos\TradingBotV4`

## Objetivo del bloque
Ejecutar canary pre-cutover con market data real (`bybit_rest`) en staging V4 y registrar decision operativa go/no-go con rollback.

## Implementado
1. Ejecucion de canary bybit_rest:
   - `powershell -ExecutionPolicy Bypass -File .\scripts\phase4_canary_smoke.ps1 -MarketDataProvider bybit_rest -DurationSeconds 60 -AutoRollbackOnFail`
2. Hardening de diagnostico:
   - `worker-rust/src/providers/bybit_market_data.rs`: errores de request/status ahora incluyen URL/category/symbol.
   - `worker-rust/src/runtime.rs`: `market_data_provider_error` publica error expandido.
3. Preflight de conectividad bybit:
   - `scripts/phase4_canary_smoke.ps1` agrega preflight HTTP a `api.bybit.com` para detectar bloqueo de red antes de correr canary.
   - `worker-rust/README.md` documenta preflight y opcion `-SkipBybitPreflight`.

## Resultado observado
### Corrida bybit_rest (antes de preflight)
- Estado: `FAIL`
- Eventos:
  - `worker_boot = 1`
  - `market_data_provider_error = 58`
  - `kernel_bootstrap_grid = 0`
  - `order_submitted = 0`
- Evidencia:
  - `logs/phase4_canary/canary_20260511_192321.out.log`
  - `logs/phase4_canary/rollback_plan_20260511_192421.md`

### Diagnostico de conectividad
- Prueba directa HTTP a Bybit desde este entorno:
  - `Invoke-WebRequest https://api.bybit.com/v5/market/tickers?category=spot&symbol=SOLUSDT`
  - Resultado: `Unable to connect to the remote server`
- Corrida con preflight:
  - `powershell -ExecutionPolicy Bypass -File .\scripts\phase4_canary_smoke.ps1 -MarketDataProvider bybit_rest -DurationSeconds 60`
  - Resultado: `FAIL` inmediato por falta de conectividad HTTP a `api.bybit.com`.

## Decision operativa
- **NO-GO** para cutover con `bybit_rest` en el entorno actual.

## Criterios para pasar a GO
1. Conectividad saliente estable a `https://api.bybit.com` (DNS + TCP + TLS).
2. Canary `bybit_rest` PASS sin `market_data_provider_error`.
3. `kernel_bootstrap_grid` y `order_submitted` presentes por encima de minimos del smoke.
4. Sin fallos criticos (`redis_*_failed`, `execution_*_failed`, `execution_reconciliation_*`).

## Rollback aplicado
- Se mantiene V4 fuera de canary activo con data real hasta resolver egress.
- V3 permanece como runtime principal sin cambios.
- Scope de rollback: solo V4.
