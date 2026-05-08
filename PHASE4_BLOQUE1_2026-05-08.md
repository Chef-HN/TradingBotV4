# Fase 4 - Bloque 1 (Shadow Diff Replay)

Fecha: 2026-05-08  
Repo: `C:\Users\Abraham\source\repos\TradingBotV4`

## Objetivo del bloque
Ejecutar V3-kernel (Python) vs V4-worker (Rust) sobre el mismo stream replay y generar reporte de divergencias sin operar real.

## Implementado
1. Harness de replay y diff:
   - `scripts/phase4_shadow_diff_replay.py`
2. Flujo cubierto por el harness:
   - carga estrategia activa desde `tenant_pair_strategies` (staging V4)
   - genera replay sintético JSONL si no se pasa `--replay-path`
   - ejecuta Rust worker en `TB_MARKET_DATA_PROVIDER=replay` + `TB_EXECUTION_MODE=simulator`
   - ejecuta kernel Python sobre el mismo stream
   - compara por ciclo:
     - `order_submitted`
     - `order_canceled`
     - `kernel_bootstrap_grid`
     - `kernel_rebalance_grid`
3. Artefactos de salida:
   - reporte JSON completo
   - reporte Markdown resumido (top divergencias)

## Evidencia de ejecución
Comando:
```powershell
.\.venv\Scripts\python.exe .\scripts\phase4_shadow_diff_replay.py
```

Resultado (corrida inicial):
- Replay: `logs/phase4_shadow/shadow_replay_20260508_145247.jsonl`
- Reporte JSON: `logs/phase4_shadow/shadow_diff_20260508_145247.json`
- Reporte MD: `logs/phase4_shadow/shadow_diff_20260508_145247.md`
- Summary:
  - compared cycles: `180`
  - match cycles: `140`
  - divergence cycles: `40`
  - match ratio: `0.7778`

## Lectura rápida del resultado
- Las divergencias se concentran en ciclos donde Rust rebalancea y vuelve a publicar 6 bids + 6 asks.
- El kernel Python, con su lógica actual de rebalance incremental y filtros de stale/flip, no emite el mismo patrón de `place/cancel` en esos ciclos.
- Esto da una base concreta para priorizar los próximos ajustes de paridad en Fase 4.
