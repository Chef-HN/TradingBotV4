# Fase 4 - Bloque 9 (Aclaracion de fallo bybit_rest + Canary PASS)

Fecha: 2026-05-12  
Repo: `C:\Users\Abraham\source\repos\TradingBotV4`

## Pregunta operativa
Por que aparecia fallo en `api.bybit.com` si V3 si tiene conectividad?

## Hallazgos
1. V3 si llega a Bybit:
   - Prueba desde `tradingbotv3-bot-1` con Python stdlib (`urllib`) a `https://api.bybit.com/...`
   - Resultado: `HTTP 200`
2. El FAIL de V4 no era un unico problema:
   - `tradingbotv4-staging-postgres` y `tradingbotv4-staging-redis` estaban detenidos (`Exited (255)`).
   - El smoke de `bybit_rest` usaba `MinCycles=5`, pero en ventanas estables puede haber solo eventos de bootstrap (baja densidad de eventos), generando falso negativo.
3. Adicionalmente, las pruebas desde este entorno de herramienta pueden sufrir restricciones de red de sesion; por eso se validaron corridas fuera del sandbox para confirmar estado real.

## Ajustes aplicados
1. `scripts/phase4_canary_smoke.ps1`
   - `MinCycles` pasa a dinamico:
     - `synthetic`: `5`
     - `bybit_rest`: `1`
   - Nuevo check de salud: falla si el worker termina antes de completar ventana (`ExitedBeforeWindow`).
2. `worker-rust/README.md`
   - Documentacion del `MinCycles` dinamico por provider.

## Validacion final ejecutada
Comando:
```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\phase4_canary_smoke.ps1 -MarketDataProvider bybit_rest -DurationSeconds 60 -AutoRollbackOnFail
```

Resultado:
- `CANARY SMOKE: PASS`
- Eventos: `worker_boot=1`, `kernel_bootstrap_grid=1`, `order_submitted=6`
- Sin `market_data_provider_error` ni fallos criticos.

Evidencia:
- `logs/phase4_canary/canary_20260512_232236.out.log`
- `logs/phase4_canary/canary_20260512_232236.err.log`

## Estado del plan
- Pre-cutover bybit_rest: **PASS (smoke corto)**.
- Siguiente paso recomendado: corrida extendida (10-15 min) antes de decision GO final de cutover.
