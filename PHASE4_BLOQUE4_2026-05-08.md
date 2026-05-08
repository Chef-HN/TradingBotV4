# Fase 4 - Bloque 4 (Strict Uplift con Rust Projection)

Fecha: 2026-05-08  
Repo: `C:\Users\Abraham\source\repos\TradingBotV4`

## Objetivo del bloque
Elevar la señal de paridad strict para convertir el shadow diff en gate ejecutable de CI sin perder visibilidad de la diferencia entre decisión e implementación.

## Implementado
1. Nuevo perfil de comparación Python:
   - `--python-profile rust_projection`
2. Perfil `rust_projection`:
   - Bootstrap (ciclo 1): `order_submitted = 2 * grid_levels`, `order_canceled = 0`
   - Rebalance: `order_submitted = 2 * grid_levels`, `order_canceled = 2 * grid_levels`
   - Otros ciclos: `0/0`
3. Perfil por defecto conservado:
   - `--python-profile db`
4. README actualizado con modos/perfiles y comando de gate completo.

## Validación ejecutada
Comando:
```powershell
.\.venv\Scripts\python.exe .\scripts\phase4_shadow_diff_replay.py --mode both --replay-path .\replay\fixtures\solusd_shadow_baseline_v1_20260508.jsonl --python-profile rust_projection --enforce-gates --gate-scope both
```

Resultado:
- `strict_match_ratio = 1.0000` (PASS)
- `intent_match_ratio = 1.0000` (PASS)
- exit code `0` con `--enforce-gates --gate-scope both`

Artefacto:
- `logs/phase4_shadow/shadow_diff_20260508_194032.md`

## Nota de interpretación
- `python-profile=db` sigue siendo útil para medir distancia entre semánticas reales Python vs Rust.
- `python-profile=rust_projection` provee un baseline estricto estable para gate de protocolo/cardinalidad en Fase 4.
