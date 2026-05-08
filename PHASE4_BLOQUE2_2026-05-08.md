# Fase 4 - Bloque 2 (Modo Dual Strict/Intent)

Fecha: 2026-05-08  
Repo: `C:\Users\Abraham\source\repos\TradingBotV4`

## Objetivo del bloque
Separar explícitamente:
1. divergencia de **decisión del kernel** (`intent`)
2. divergencia de **implementación de reemplazo/cancelación de órdenes** (`strict`)

## Implementado
1. `scripts/phase4_shadow_diff_replay.py` ahora soporta:
   - `--mode strict|intent|both` (default: `both`)
   - `--strict-gate` (default: `0.80`)
   - `--intent-gate` (default: `0.95`)
2. Reporte JSON y Markdown expandido:
   - summary por modo (`strict` e `intent`)
   - top divergences por modo
   - estado de gates (PASS/FAIL)
3. README actualizado con uso de modo dual.

## Ejecución de validación
Comando:
```powershell
.\.venv\Scripts\python.exe .\scripts\phase4_shadow_diff_replay.py --mode both
```

Resultado:
- `strict_match_ratio = 0.7778` -> `FAIL` contra gate `0.80`
- `intent_match_ratio = 1.0000` -> `PASS` contra gate `0.95`

Artefactos:
- `logs/phase4_shadow/shadow_diff_20260508_161005.json`
- `logs/phase4_shadow/shadow_diff_20260508_161005.md`

## Conclusión operativa
- La **intención de decisión** está alineada (100% en esta corrida).
- La brecha actual está en la **semántica de ejecución/rebalance** (conteo de place/cancel), no en el disparo de rebalance.
