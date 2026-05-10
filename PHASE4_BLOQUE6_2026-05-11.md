# Fase 4 - Bloque 6 (Canary Smoke + Rollback Guardrail)

Fecha: 2026-05-11  
Repo: `C:\Users\Abraham\source\repos\TradingBotV4`

## Objetivo del bloque
Agregar un canary smoke automatizado para V4 que valide salud operativa del worker Rust en staging y deje fallback documentado en caso de fallo.

## Implementado
1. Script nuevo:
   - `scripts/phase4_canary_smoke.ps1`
2. Validaciones automaticas del canary:
   - presencia de `worker_boot` y `kernel_bootstrap_grid`
   - minimo de `order_submitted`
   - minimo de ciclos observados por `correlation_id`
   - budget de alertas (`market_data_gap_detected`, `heartbeat_lag_detected`, `command_lag_detected`)
   - budget de fallos criticos (`redis_*_failed`, `execution_*_failed`, `market_data_provider_error`, `execution_reconciliation_*`)
3. Guardrail de rollback:
   - opcion `-AutoRollbackOnFail` que genera `rollback_plan_<timestamp>.md` con acciones inmediatas (scope V4 only).
4. README actualizado:
   - `worker-rust/README.md` con seccion de uso del canary smoke.

## Validacion ejecutada
Comando:
```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\phase4_canary_smoke.ps1
```

Resultado:
- `CANARY SMOKE: PASS`
- Eventos totales: `313`
- Conteos principales:
  - `order_submitted = 144`
  - `order_canceled = 138`
  - `kernel_rebalance_grid = 23`
  - `kernel_fill_processed = 6`
  - `worker_boot = 1`
  - `kernel_bootstrap_grid = 1`
- Sin fallos criticos y sin alertas de lag/gap fuera de budget.

Evidencia:
- `logs/phase4_canary/canary_20260511_000027.out.log`
- `logs/phase4_canary/canary_20260511_000027.err.log`

## Nota operativa
- El bloque corre solo contra staging V4 (`5443`, `6390/15`).
- No toca contenedores ni base de datos de V3.
