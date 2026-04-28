# SESSION HANDOFF V4 - 2026-04-29 01:05:09 +02:00

## Repo activo
- Ruta: `C:\Users\Abraham\source\repos\TradingBotV4`
- Rama: `master`
- Estado: limpio (sin cambios pendientes)

## Ultimos commits (mas recientes primero)
1. `da79c86` - v4 worker rust: implement bybit live execution provider with signed v5 requests
2. `2172833` - v4 worker rust: add bybit/rest market data provider and replay source
3. `71c217d` - v4 worker rust: disable reset commands by default behind env flag
4. `4fd06ee` - v4 worker rust: harden command parsing and scope tests
5. `811d9ef` - v4 worker rust: wire db+redis runtime and local daily close schedule commands

## Estado funcional actual (V4)
### Worker Rust
- Runtime con carga de estrategia desde BD (`tenant_pair_strategies`) y fail-fast.
- Control-plane Redis v4 para comandos/estado/heartbeat.
- Scheduler de daily close local con `next_cycle` e `immediate`.
- `reset` deshabilitado por defecto (solo con `TB_ALLOW_RESET_COMMAND=true`).
- `MarketDataProvider` configurable por env:
  - `bybit_rest`
  - `replay` (archivo JSON/JSONL con `TB_REPLAY_TICKS_PATH`)
  - `synthetic`
- `BybitLiveExecutionProvider` real implementado con firma V5 HMAC:
  - `/v5/order/create`
  - `/v5/order/cancel`
  - `/v5/order/cancel-all`
  - `/v5/execution/list`
  - `/v5/account/wallet-balance`

### Pruebas
- `cargo test` en `worker-rust`: 17 passed / 0 failed.

## Restricciones activas del proyecto (definidas por Abraham)
1. No tocar TradingBotV3 para estos cambios (trabajar en V4).
2. No borrar BD ni archivos.
3. No cambiar parametros de trading sin revision/aprobacion explicita.
4. Todo cambio importante debe quedar versionado en git con commits frecuentes.
5. Reset no debe estar expuesto a usuarios finales de produccion.

## Variables de entorno relevantes para live V4
- `TB_BYBIT_API_KEY` (o `BYBIT_API_KEY`)
- `TB_BYBIT_API_SECRET` (o `BYBIT_API_SECRET`)
- Opcionales:
  - `TB_BYBIT_REST_BASE_URL`
  - `TB_BYBIT_CATEGORY` (default `spot`)
  - `TB_BYBIT_RECV_WINDOW_MS`
  - `TB_BYBIT_HTTP_TIMEOUT_MS`
  - `TB_MARKET_DATA_PROVIDER` (`bybit_rest|replay|synthetic`)
  - `TB_REPLAY_TICKS_PATH` (si modo replay)

## Proximo bloque recomendado
1. Integrar `instruments-info` de Bybit para cuantizar `price`/`qty` a `tickSize` y `qtyStep` antes de enviar ordenes live.
2. Agregar pruebas unitarias de rounding/precision y errores de minQty/minNotional.
3. Ejecutar replay diferencial para validar paridad de decisiones kernel entre modos con mismo stream.

## Comandos de verificacion rapida al retomar
```powershell
cd C:\Users\Abraham\source\repos\TradingBotV4\worker-rust
& "$env:USERPROFILE\.cargo\bin\cargo.exe" test
```

```powershell
cd C:\Users\Abraham\source\repos\TradingBotV4
git log --oneline -8
git status --short
```
