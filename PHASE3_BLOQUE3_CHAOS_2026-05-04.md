# Fase 3 - Bloque 3 (Chaos + Hardening)

Fecha: 2026-05-04  
Repo: `C:\Users\Abraham\source\repos\TradingBotV4`

## Objetivo del bloque
Simular fallos controlados de Redis/Bybit y validar que el worker emite alertas sin caerse.

## Implementado
1. Inyeccion de fallos por `every_n` (desactivado por defecto):
   - `TB_CHAOS_REDIS_FAIL_EVERY_N`
   - `TB_CHAOS_BYBIT_MARKET_FAIL_EVERY_N`
   - `TB_CHAOS_BYBIT_EXEC_FAIL_EVERY_N`

2. Hardening del loop runtime ante fallos:
   - No aborta por fallo transitorio de `pop_commands`, `publish_state`, `publish_heartbeat`.
   - No aborta por fallo de market data provider (excepto `replay_completed`).
   - No aborta por fallos de `submit/cancel/liquidate/flush/reconciliation`.
   - Emite eventos de error estructurados para cada caso.

3. Alertas y telemetria de salud:
   - `redis_command_poll_failed`
   - `redis_state_publish_failed`
   - `redis_heartbeat_publish_failed`
   - `market_data_provider_error`
   - `execution_on_tick_failed`
   - `execution_submit_failed`
   - `execution_cancel_failed`
   - `execution_liquidation_failed`
   - `execution_flush_fills_failed`
   - `execution_reconciliation_snapshot_failed`
   - `command_lag_detected`
   - `heartbeat_lag_detected`

4. Estado `observability` expandido:
   - contadores de fallos por subsistema (`redis_*_failures`, `market_data_failures`, `execution_failures`)
   - latencias por etapa (`latency_ms.*`)
   - umbrales activos de alertas

## Validacion ejecutada
Comando:
```powershell
cd C:\Users\Abraham\source\repos\TradingBotV4\worker-rust
& "$env:USERPROFILE\.cargo\bin\cargo.exe" test
```

Resultado:
- `34 passed`
- `0 failed`

Incluye tests de caos:
- `control_plane::tests::should_inject_every_n_matches_expected_sequence`
- `providers::bybit_market_data::tests::should_inject_every_n_matches_expected_sequence`
- `providers::bybit_live::tests::should_inject_every_n_matches_expected_sequence`
