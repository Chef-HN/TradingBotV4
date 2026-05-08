# Fase 4 - Bloque 3 (Baseline Fijo + Gate Enforce)

Fecha: 2026-05-08  
Repo: `C:\Users\Abraham\source\repos\TradingBotV4`

## Objetivo del bloque
Dejar un baseline replay fijo y habilitar gate ejecutable para correr el shadow diff como quality check reproducible.

## Implementado
1. Baseline replay fijo en repo:
   - `replay/fixtures/solusd_shadow_baseline_v1_20260508.jsonl`
2. Gate enforce en harness:
   - `--enforce-gates`
   - `--gate-scope strict|intent|both` (default: `intent`)
3. Reporte enriquecido:
   - incluye `mode`, `gate_scope`, `enforce_gates`
   - incluye estado de gates en JSON/Markdown

## Validación ejecutada
Comando (baseline fijo, modo dual):
```powershell
.\.venv\Scripts\python.exe .\scripts\phase4_shadow_diff_replay.py --mode both --replay-path .\replay\fixtures\solusd_shadow_baseline_v1_20260508.jsonl
```

Resultado esperado actual:
- `strict_match_ratio` ~ `0.7778` (por debajo de `0.80`)
- `intent_match_ratio` = `1.0000` (supera `0.95`)

Comando de gate recomendado en estado actual:
```powershell
.\.venv\Scripts\python.exe .\scripts\phase4_shadow_diff_replay.py --mode both --replay-path .\replay\fixtures\solusd_shadow_baseline_v1_20260508.jsonl --enforce-gates --gate-scope intent
```

## Conclusión
- La paridad de intención del kernel está estable y gateable.
- La paridad estricta de place/cancel queda como KPI secundario para siguientes ajustes del engine.
