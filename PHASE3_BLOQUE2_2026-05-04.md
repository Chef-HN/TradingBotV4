# Fase 3 - Bloque 2 (Latencias y Health Signals)

Fecha: 2026-05-04  
Repo: `C:\Users\Abraham\source\repos\TradingBotV4`

## Entregado
1. Metricas de latencia por etapa en estado runtime (`observability.latency_ms`):
   - `market_data`
   - `command_poll`
   - `command_apply`
   - `execution_on_tick`
   - `kernel_tick`
   - `fill_flush`
   - `fill_process`
   - `reconciliation`
   - `daily_close`
   - `state_publish`
   - `heartbeat_publish`

2. Eventos de salud:
   - `command_lag_detected`
   - `heartbeat_lag_detected`
   - `market_data_gap_detected` (ya existente en bloque anterior)
   - `execution_reconciliation_mismatch` (ya existente desde cierre Fase 2)

3. Nuevos umbrales por env:
   - `TB_COMMAND_LAG_WARN_MS`
   - `TB_HEARTBEAT_LAG_WARN_MS`
   - `TB_MARKET_DATA_GAP_WARN_MS` (conservado)

## Thresholds iniciales recomendados
1. `TB_MARKET_DATA_GAP_WARN_MS`: `5000`
2. `TB_COMMAND_LAG_WARN_MS`: `1500`
3. `TB_HEARTBEAT_LAG_WARN_MS`: `12000`
4. `execution_reconciliation_mismatch`: alertar en primer evento y escalar si se repite > 3 veces en 5 min

## Accion operativa sugerida
1. `market_data_gap_detected`:
   - revisar conectividad/API exchange
   - bajar carga local o aumentar `TB_TICK_INTERVAL_MS` si hay saturacion
2. `command_lag_detected`:
   - revisar backlog de comandos en Redis
   - validar latencia DB/Redis y CPU del host
3. `heartbeat_lag_detected`:
   - revisar bloqueo en loop principal (etapa con mayor `*_max`)
   - validar salud del proceso y reinicio controlado si persiste
4. `execution_reconciliation_mismatch`:
   - inspeccionar ordenes abiertas provider vs kernel
   - confirmar fills recientes y evaluar `reset` controlado si procede

## Validacion
Comando:
```powershell
cd C:\Users\Abraham\source\repos\TradingBotV4\worker-rust
& "$env:USERPROFILE\.cargo\bin\cargo.exe" test -q
```

Resultado:
- `29 passed`
- `0 failed`
