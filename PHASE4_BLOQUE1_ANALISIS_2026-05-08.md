# Fase 4 - Bloque 1 (Analisis de Divergencias)

Fecha: 2026-05-08  
Base: `logs/phase4_shadow/shadow_diff_20260508_145247.json`

## Resumen cuantitativo
- Divergencias totales: `40`
- Tipo de divergencia:
  - `value_diff`: `40`
  - `missing_cycle`: `0`
- Campos que difieren:
  - `order_submitted`: `40/40`
  - `order_canceled`: `39/40`
- Patrones detectados:
  - `('order_submitted',)` -> `1` ciclo (ciclo bootstrap)
  - `('order_submitted','order_canceled')` -> `39` ciclos
- Pares de valores observados:
  - `order_submitted`: `python=0`, `rust=6` (40 veces)
  - `order_canceled`: `python=0`, `rust=6` (39 veces)

## Interpretacion tecnica
1. La divergencia no es ruido aleatorio; es un patrón estable de "rebalance full-grid" en Rust.
2. Rust, en los ciclos divergentes, emite exactamente:
   - 6 `order_submitted` (3 bids + 3 asks)
   - 6 `order_canceled` (excepto bootstrap, donde solo hay submits)
3. El kernel Python en esos mismos ciclos no emite acciones de place/cancel (queda en 0).

## Causa raiz mas probable
El comparador actual cruza dos semanticas distintas:
- Rust: kernel simplificado que en rebalance hace reset completo de grilla.
- Python: kernel con rebalance incremental y reglas de conservacion/reuso de niveles.

Esto provoca que el "conteo bruto de órdenes por ciclo" difiera aunque ambos estén siguiendo señales de drift similares.

## Ajustes recomendados (prioridad)
1. **Agregar metrica de paridad por "intencion de rebalance" (P0)**
   - Comparar primero `kernel_rebalance_grid` por ciclo (booleano), y dejar `order_*` como metrica secundaria.
   - Resultado esperado: separar divergencia de "momento de decision" vs divergencia de "implementacion de ejecucion".

2. **Agregar modo de comparacion dual en el harness (P0)**
   - `strict`: como ahora (conteo exacto `order_submitted`/`order_canceled`)
   - `intent`: compara solo eventos de decision (`kernel_bootstrap_grid`, `kernel_rebalance_grid`)
   - Resultado esperado: visibilidad clara de si el gap esta en kernel o en policy de reemplazo de órdenes.

3. **Dataset de replay histórico estable (P1)**
   - Mantener una corrida sintética para smoke, pero usar replay histórico fijo para baseline de paridad.
   - Resultado esperado: evitar sesgo por forma de onda sintética y tener tracking comparable entre commits.

## Recomendacion operativa inmediata
Para el siguiente bloque de Fase 4:
1. Implementar modo dual `strict/intent` en `phase4_shadow_diff_replay.py`.
2. Ejecutar ambas vistas en el mismo replay.
3. Definir gate de progreso:
   - `intent_match_ratio >= 0.95` como criterio de convergencia de decisiones.
   - `strict_match_ratio` como KPI de convergencia de ejecución.
