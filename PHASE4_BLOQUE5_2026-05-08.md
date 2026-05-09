# Fase 4 - Bloque 5 (CI Gate de Shadow Baseline)

Fecha: 2026-05-08  
Repo: `C:\Users\Abraham\source\repos\TradingBotV4`

## Objetivo del bloque
Llevar el gate del baseline de shadow diff a CI para bloquear regresiones de paridad protocolaria (strict + intent).

## Implementado
1. Workflow nuevo:
   - `.github/workflows/phase4-shadow-gate.yml`
2. Trigger del workflow:
   - `pull_request` y `push` sobre `main/master` (con paths relevantes)
   - `workflow_dispatch` manual
3. Runtime de CI:
   - `postgres:16-alpine` en `127.0.0.1:5443`
   - `redis:7-alpine` en `127.0.0.1:6390`
4. Pipeline del job:
   - checkout
   - setup Python 3.12
   - setup Rust estable + cache
   - `pip install -e .[dev]`
   - `python -m scripts.init_db` (migraciones V4)
   - gate ejecutable:
     - `scripts/phase4_shadow_diff_replay.py`
     - `--mode both`
     - baseline fijo `replay/fixtures/solusd_shadow_baseline_v1_20260508.jsonl`
     - `--python-profile rust_projection`
     - `--enforce-gates --gate-scope both`
5. Artefactos:
   - upload de `logs/phase4_shadow/` para diagnostico de divergencias en cada corrida.

## Resultado esperado en CI
- Si hay regresion en paridad strict o intent para el baseline fijo, el job falla.
- Si pasa, queda validada la paridad para el contrato de cardinalidad/decision de Fase 4.

## Nota operativa
- Esta integracion no toca V3.
- Usa puertos/DSN de staging V4 (`5443`, `6390`, `tradingbotv4_staging`).
