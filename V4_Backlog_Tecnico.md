BACKLOG TECNICO V4 - PYTHON CONTROL/API + RUST WORKER
Version: 1.0
Fecha: 2026-04-14 14:28:35
Autor: Codex

1) RESUMEN EJECUTIVO
- Objetivo: construir V4 con paridad funcional paper/live y separacion de planos: control en Python, ejecucion en Rust.
- Regla principal: algoritmo unico; solo cambia el proveedor de ejecucion (Bybit Live vs Bybit Simulator).
- Restriccion critica: V4 debe preservar 100% de estructura e informacion de BD V3 para analisis de performance, ejecucion y resiliencia.
- Estrategia de migracion: shadow mode, canary por simbolo, cutover gradual sin apagar V3 al inicio.

2) PRINCIPIOS DE DISENO
- Paridad algoritmica estricta entre paper y live.
- Fail-fast en configuracion: si falta un parametro requerido, el worker no inicia.
- Auditoria total de cambios de parametros (SCD2 + actor + timestamp + diff).
- Observabilidad first-class: eventos de orden, fill, market-data, reconexion y recovery.
- Idempotencia y resiliencia en comandos y reconciliacion de estado.

3) ALCANCE TECNOLOGICO V4
- Python (Control/API): FastAPI + SQLAlchemy + Redis + Auth + Dashboard + Governance de estrategias.
- Rust (Worker): Tokio + provider trait + kernel + risk + execution + daily close + state publisher.
- Datos: PostgreSQL para estado y auditoria; Redis Streams para comandos/estado en vivo.
- Integraciones: Bybit Live Provider y Bybit Simulator Provider con la misma interfaz.

4) INVENTARIO BD V3 (PARIDAD OBLIGATORIA EN V4)
- Total objetos (tablas/vistas) detectados en schema public: 14
- Politica: cada tabla/vista de V3 se migra o se mantiene con compatibilidad backwards para reporteria y auditoria.

4.bot_restarts) bot_restarts
Columnas:
- id :: bigint :: NOT NULL
- session_id :: uuid :: NOT NULL
- product_id :: character varying :: NOT NULL
- triggered_by :: character varying :: NOT NULL
- restarted_at :: timestamp with time zone :: NOT NULL
Indices:
- bot_restarts_pkey
- idx_bot_restarts_product_time
Accion V4:
- Migrar tabla 1:1 inicialmente. Solo cambios aditivos permitidos en V4.

4.equity_snapshots) equity_snapshots
Columnas:
- id :: bigint :: NOT NULL
- session_id :: uuid :: NOT NULL
- product_id :: character varying :: NOT NULL
- total_equity :: numeric :: NOT NULL
- quote_inventory :: numeric :: NOT NULL
- base_inventory :: numeric :: NOT NULL
- realized_pnl :: numeric :: NOT NULL
- unrealized_pnl :: numeric :: NOT NULL
- mid_anchor :: numeric :: NOT NULL
- mid_price :: numeric :: NOT NULL
- trigger :: character varying :: NOT NULL
- recorded_at :: timestamp with time zone :: NOT NULL
Indices:
- equity_snapshots_pkey
- idx_equity_product_time
- idx_equity_session_time
Accion V4:
- Migrar tabla 1:1 inicialmente. Solo cambios aditivos permitidos en V4.

4.exchange_credentials) exchange_credentials
Columnas:
- id :: character varying :: NOT NULL
- exchange_name :: character varying :: NOT NULL
- api_key_encrypted :: text :: NOT NULL
- api_secret_encrypted :: text :: NOT NULL
- api_passphrase_encrypted :: text :: NULL
- encryption_key_id :: character varying :: NOT NULL
- created_at :: timestamp with time zone :: NOT NULL
- updated_at :: timestamp with time zone :: NOT NULL
- created_by :: character varying :: NULL
- active :: boolean :: NOT NULL
Indices:
- exchange_credentials_pkey
- idx_exchange_credentials_active
- idx_exchange_credentials_exchange_name
- idx_exchange_credentials_unique_active
Accion V4:
- Migrar tabla 1:1 inicialmente. Solo cambios aditivos permitidos en V4.

4.exchange_strategies) exchange_strategies
Columnas:
- id :: integer :: NOT NULL
- name :: character varying :: NOT NULL
- exchange_name :: character varying :: NOT NULL
- is_active :: boolean :: NOT NULL
- spacing_bps :: numeric :: NOT NULL
- rebalance_threshold_bps :: numeric :: NOT NULL
- grid_levels :: integer :: NOT NULL
- level_size_quote :: numeric :: NOT NULL
- max_inventory_ratio :: numeric :: NOT NULL
- maker_fee_rate :: numeric :: NOT NULL
- stale_reprice_threshold_bps :: numeric :: NOT NULL
- stale_order_age_seconds :: integer :: NOT NULL
- created_at :: timestamp with time zone :: NOT NULL
- updated_at :: timestamp with time zone :: NOT NULL
- rebalance_defer_seconds :: integer :: NOT NULL
- rebalance_defer_max_drift_bps :: numeric :: NOT NULL
- symbols :: text :: NOT NULL
- paper_mode :: boolean :: NOT NULL
- total_wallet_usd :: numeric :: NOT NULL
- session_capital_usd :: numeric :: NOT NULL
- maker_only :: boolean :: NOT NULL
- symbol_overrides :: jsonb :: NULL
- updated_by :: character varying :: NOT NULL
- local_timezone_iana :: character varying :: NOT NULL
- daily_close_hour :: integer :: NOT NULL
- daily_close_minute :: integer :: NOT NULL
- spread_freeze_bps :: numeric :: NOT NULL
- regime_stress_spread_bps :: numeric :: NOT NULL
- regime_trend_slope_threshold :: numeric :: NOT NULL
- regime_mr_distance_threshold_bps :: numeric :: NOT NULL
- regime_hysteresis_bps :: numeric :: NOT NULL
- regime_rsi_bear_threshold :: numeric :: NOT NULL
- regime_rsi_bull_threshold :: numeric :: NOT NULL
- ws_retry_window_seconds :: integer :: NOT NULL
- ws_initial_retry_delay_seconds :: integer :: NOT NULL
- ws_max_retry_delay_seconds :: integer :: NOT NULL
- ws_message_timeout_seconds :: integer :: NOT NULL
- ws_heartbeat_timeout_seconds :: integer :: NOT NULL
Indices:
- exchange_strategies_name_key
- exchange_strategies_pkey
- idx_exchange_strategies_exchange
Accion V4:
- Migrar tabla 1:1 inicialmente. Solo cambios aditivos permitidos en V4.

4.fills) fills
Columnas:
- fill_id :: character varying :: NOT NULL
- order_id :: character varying :: NULL
- client_order_id :: character varying :: NOT NULL
- product_id :: character varying :: NOT NULL
- session_id :: uuid :: NULL
- side :: character varying :: NOT NULL
- price :: numeric :: NOT NULL
- size_base :: numeric :: NOT NULL
- quote_value :: numeric :: NOT NULL
- fee_quote :: numeric :: NOT NULL
- level_index :: integer :: NULL
- grid_side :: character varying :: NULL
- liquidity_indicator :: character varying :: NOT NULL
- trade_time :: timestamp with time zone :: NOT NULL
Indices:
- fills_pkey
- idx_fills_order_id
- idx_fills_product_id
- idx_fills_session_id
- idx_fills_trade_time
Accion V4:
- Migrar tabla 1:1 inicialmente. Solo cambios aditivos permitidos en V4.

4.grid_levels) grid_levels
Columnas:
- level_id :: uuid :: NOT NULL
- product_id :: character varying :: NOT NULL
- session_id :: uuid :: NOT NULL
- side :: character varying :: NOT NULL
- level_index :: integer :: NOT NULL
- price :: numeric :: NOT NULL
- size_base :: numeric :: NOT NULL
- size_quote :: numeric :: NOT NULL
- client_order_id :: character varying :: NULL
- order_id :: character varying :: NULL
- status :: character varying :: NOT NULL
- fill_price :: numeric :: NULL
- fill_fee_quote :: numeric :: NULL
- created_at :: timestamp with time zone :: NOT NULL
- updated_at :: timestamp with time zone :: NOT NULL
- filled_at :: timestamp with time zone :: NULL
- opened_at :: timestamp with time zone :: NULL
Indices:
- grid_levels_pkey
- idx_grid_levels_opened_at
- idx_grid_levels_order_id
- idx_grid_levels_product
- idx_grid_levels_session
- idx_grid_levels_status
Accion V4:
- Migrar tabla 1:1 inicialmente. Solo cambios aditivos permitidos en V4.

4.otp_codes) otp_codes
Columnas:
- id :: character varying :: NOT NULL
- email :: character varying :: NOT NULL
- code :: character varying :: NOT NULL
- created_at :: timestamp with time zone :: NULL
- expires_at :: timestamp with time zone :: NOT NULL
- used :: boolean :: NULL
Indices:
- ix_otp_codes_email
- otp_codes_pkey
Accion V4:
- Migrar tabla 1:1 inicialmente. Solo cambios aditivos permitidos en V4.

4.sessions) sessions
Columnas:
- session_id :: uuid :: NOT NULL
- product_id :: character varying :: NOT NULL
- mode :: character varying :: NOT NULL
- status :: character varying :: NOT NULL
- mid_anchor :: numeric :: NOT NULL
- spacing_bps :: numeric :: NOT NULL
- grid_levels :: integer :: NOT NULL
- realized_pnl_quote :: numeric :: NOT NULL
- total_fills :: integer :: NOT NULL
- started_at :: timestamp with time zone :: NOT NULL
- ended_at :: timestamp with time zone :: NULL
- updated_at :: timestamp with time zone :: NOT NULL
- reserve_usd :: numeric :: NOT NULL
- level_size_quote :: numeric :: NULL
- rebalance_threshold_bps :: numeric :: NULL
- max_inventory_ratio :: numeric :: NULL
- maker_fee_rate :: numeric :: NULL
- symbol_overrides :: jsonb :: NULL
- underfunded :: boolean :: NOT NULL
- underfunded_shortfall_usd :: numeric :: NOT NULL
Indices:
- idx_sessions_product_id
- idx_sessions_status
- sessions_pkey
Accion V4:
- Migrar tabla 1:1 inicialmente. Solo cambios aditivos permitidos en V4.

4.strategy_param_history) strategy_param_history
Columnas:
- history_id :: integer :: NOT NULL
- strategy_id :: integer :: NOT NULL
- strategy_name :: character varying :: NOT NULL
- exchange_name :: character varying :: NOT NULL
- spacing_bps :: numeric :: NOT NULL
- rebalance_threshold_bps :: numeric :: NOT NULL
- grid_levels :: integer :: NOT NULL
- level_size_quote :: numeric :: NOT NULL
- max_inventory_ratio :: numeric :: NOT NULL
- maker_fee_rate :: numeric :: NOT NULL
- stale_reprice_threshold_bps :: numeric :: NOT NULL
- stale_order_age_seconds :: integer :: NOT NULL
- rebalance_defer_seconds :: integer :: NOT NULL
- rebalance_defer_max_drift_bps :: numeric :: NOT NULL
- total_wallet_usd :: numeric :: NOT NULL
- session_capital_usd :: numeric :: NOT NULL
- maker_only :: boolean :: NOT NULL
- paper_mode :: boolean :: NOT NULL
- symbols :: character varying :: NOT NULL
- symbol_overrides :: jsonb :: NULL
- valid_from :: timestamp with time zone :: NOT NULL
- valid_to :: timestamp with time zone :: NULL
- updated_by :: character varying :: NOT NULL
- change_summary :: text :: NULL
- local_timezone_iana :: character varying :: NOT NULL
- daily_close_hour :: integer :: NOT NULL
- daily_close_minute :: integer :: NOT NULL
- spread_freeze_bps :: numeric :: NOT NULL
- regime_stress_spread_bps :: numeric :: NOT NULL
- regime_trend_slope_threshold :: numeric :: NOT NULL
- regime_mr_distance_threshold_bps :: numeric :: NOT NULL
- regime_hysteresis_bps :: numeric :: NOT NULL
- regime_rsi_bear_threshold :: numeric :: NOT NULL
- regime_rsi_bull_threshold :: numeric :: NOT NULL
- ws_retry_window_seconds :: integer :: NOT NULL
- ws_initial_retry_delay_seconds :: integer :: NOT NULL
- ws_max_retry_delay_seconds :: integer :: NOT NULL
- ws_message_timeout_seconds :: integer :: NOT NULL
- ws_heartbeat_timeout_seconds :: integer :: NOT NULL
Indices:
- idx_sph_current
- idx_sph_strategy_id
- idx_sph_valid_from
- strategy_param_history_pkey
Accion V4:
- Migrar tabla 1:1 inicialmente. Solo cambios aditivos permitidos en V4.

4.ticks) ticks
Columnas:
- id :: bigint :: NOT NULL
- product_id :: character varying :: NOT NULL
- bid :: numeric :: NOT NULL
- ask :: numeric :: NOT NULL
- mid :: numeric :: NOT NULL
- last_trade_price :: numeric :: NOT NULL
- event_time :: timestamp with time zone :: NOT NULL
Indices:
- idx_ticks_product_time
- ticks_pkey
Accion V4:
- Migrar tabla 1:1 inicialmente. Solo cambios aditivos permitidos en V4.

4.users) users
Columnas:
- id :: character varying :: NOT NULL
- email :: character varying :: NOT NULL
- password_hash :: character varying :: NOT NULL
- display_name :: character varying :: NOT NULL
- preferred_locale :: character varying :: NOT NULL
- created_at :: timestamp with time zone :: NOT NULL
Indices:
- ix_users_email
- users_pkey
Accion V4:
- Migrar tabla 1:1 inicialmente. Solo cambios aditivos permitidos en V4.

4.v_daily_pnl) v_daily_pnl
Columnas:
- trade_date :: timestamp without time zone :: NULL
- product_id :: character varying :: NULL
- fills :: bigint :: NULL
- net_flow_usd :: numeric :: NULL
- total_fees :: numeric :: NULL
- low_price :: numeric :: NULL
- high_price :: numeric :: NULL
- avg_price :: numeric :: NULL
Accion V4:
- Reimplementar vista con SQL equivalente y pruebas de regresion de resultados.

4.v_session_pnl) v_session_pnl
Columnas:
- session_id :: uuid :: NULL
- product_id :: character varying :: NULL
- mode :: character varying :: NULL
- status :: character varying :: NULL
- mid_anchor :: numeric :: NULL
- spacing_bps :: numeric :: NULL
- grid_levels :: integer :: NULL
- realized_pnl_quote :: numeric :: NULL
- total_fills :: integer :: NULL
- fill_count :: bigint :: NULL
- total_volume_usd :: numeric :: NULL
- total_fees_paid :: numeric :: NULL
- total_buy_value :: numeric :: NULL
- total_sell_value :: numeric :: NULL
- started_at :: timestamp with time zone :: NULL
- ended_at :: timestamp with time zone :: NULL
- duration_seconds :: numeric :: NULL
Accion V4:
- Reimplementar vista con SQL equivalente y pruebas de regresion de resultados.

4.worker_process_log) worker_process_log
Columnas:
- id :: bigint :: NOT NULL
- exchange :: character varying :: NOT NULL
- started_at :: timestamp with time zone :: NOT NULL
- pid :: integer :: NULL
- stopped_at :: timestamp with time zone :: NULL
- stop_reason :: character varying :: NULL
Indices:
- idx_worker_process_log_exchange
- worker_process_log_pkey
Accion V4:
- Migrar tabla 1:1 inicialmente. Solo cambios aditivos permitidos en V4.

5) TABLAS NUEVAS PROPUESTAS EN V4 (ADITIVAS, NO DESTRUCTIVAS)
- order_events: placed, acknowledged, cancelled, rejected, partially_filled, filled.
- execution_reconciliation_log: diferencias entre estado interno y exchange/simulador.
- worker_runtime_events: reconnect, heartbeat_stale, restart_reason, recovery_action.
- command_audit_log: comando recibido, dedupe key, resultado, latencia, actor.
- market_data_gap_log: ventanas sin ticks/heartbeats por simbolo.
- backtest_replay_runs: corrida, parametros, checksum input, checksum output.

6) BACKLOG POR EPICAS

E01 - Arquitectura Base V4 y Contratos (P0)
Objetivo: Definir contratos formales entre Python control-plane y Rust execution-plane.
Historias/Tareas:
- Definir schema de comandos y eventos (JSON schema/versionado).
- Definir trait ExecutionProvider en Rust (live/simulator).
- Definir MarketDataProvider y StatePublisher interfaces.
- Definir versionado de protocolo con compatibilidad retroactiva.
Definition of Done:
- Contrato publicado y aprobado.
- Tests de serializacion/deserializacion.
- Documento ADR de decisiones arquitectonicas.
Estimacion: 8-10 dias

E02 - Paridad BD V3 -> V4 (P0)
Objetivo: Garantizar que toda estructura e informacion de V3 exista en V4.
Historias/Tareas:
- Crear migraciones V4 para tablas actuales sin perdida de columnas/indices.
- Crear vistas v_daily_pnl y v_session_pnl compatibles.
- Agregar checks autom?ticos de paridad schema en CI.
- Agregar script de verificacion de conteos y checksums por tabla cr?tica.
Definition of Done:
- 100% tablas y columnas V3 presentes en V4.
- Indices criticos equivalentes o mejores.
- Reporte de paridad firmado antes de cutover.
Estimacion: 7-9 dias

E03 - Kernel Unico en Rust (P0)
Objetivo: Mover kernel de estrategia/riesgo a un flujo unico desacoplado de paper/live.
Historias/Tareas:
- Portar reglas grid + regime + risk + daily close al worker Rust.
- Agregar pruebas deterministas con replay de ticks.
- Crear golden tests paper/live con mismo stream de entrada.
Definition of Done:
- Mismo input => mismas decisiones del kernel.
- Cobertura de pruebas de reglas criticas >= 90% en modulo kernel.
Estimacion: 12-15 dias

E04 - Execution Providers (Bybit Live y Bybit Simulator) (P0)
Objetivo: Unificar execution path; solo cambia proveedor.
Historias/Tareas:
- Implementar BybitLiveExecutionProvider (place/cancel/sync/poll fills/balances).
- Implementar BybitSimulatorExecutionProvider con misma semantica de fills.
- Agregar reconciliacion de ordenes/fills en ambos providers.
Definition of Done:
- Worker sin if/else paper/live en path de ejecucion.
- Paridad funcional de eventos de orden/fill.
Estimacion: 10-14 dias

E05 - Daily Close Local + Caja/Reserva (P0)
Objetivo: Implementar cierre local 00:00 con continuidad de ciclo y opcion de cambio de zona.
Historias/Tareas:
- Implementar scheduler timezone-aware con IANA.
- Implementar modos next_cycle vs immediate en cambios de zona.
- Implementar reglas equity/session_capital/reserve/underfunded.
- Persistir eventos de cierre y resultado por simbolo.
Definition of Done:
- Pruebas multi-zona (Europe/Paris, Asia/Singapore, etc).
- Ejecucion exacta y auditable del cierre diario.
Estimacion: 6-8 dias

E06 - Control/API Python V4 (P1)
Objetivo: Modernizar control plane sin perder endpoints operativos actuales.
Historias/Tareas:
- Portar endpoints de estrategia, historial, analysis-window, timezone-drift.
- Aplicar validaciones estrictas de parametros por par.
- Implementar command queue robusta con dedupe y retries.
Definition of Done:
- Compatibilidad con dashboard y scripts operativos.
- Todos los cambios quedan auditados en strategy_param_history.
Estimacion: 8-10 dias

E07 - Observabilidad de Performance, Ejecucion y Resiliencia (P0)
Objetivo: Proveer telemetria completa para tuning y postmortems.
Historias/Tareas:
- Instrumentar m?tricas: fills/h, rebalance/fill, stale_cancel_ratio real, markout 30s/2m/5m.
- Logs estructurados con correlation_id por decision/order/fill.
- Dashboards de salud: ws reconnects, heartbeat lag, command lag, worker restarts.
- Alertas: underfunded, worker down, no ticks, no fills anormales.
Definition of Done:
- SLOs definidos y alertas en produccion.
- Runbooks de incidentes publicados.
Estimacion: 7-9 dias

E08 - Seguridad y Secrets (P1)
Objetivo: Fortalecer manejo de credenciales y trazabilidad de acceso.
Historias/Tareas:
- Mantener exchange_credentials con cifrado y rotacion de key.
- Eliminar dependencias de .env para claves en runtime normal.
- Auditar accesos a secrets y operaciones sensibles.
Definition of Done:
- No secrets en logs.
- Rotacion validada en entorno de staging.
Estimacion: 4-6 dias

E09 - Testing y QA de Paridad (P0)
Objetivo: Asegurar comportamiento consistente y evitar regresiones.
Historias/Tareas:
- Replay tests deterministas con datasets historicos.
- Differential tests V3 vs V4 por simbolo y sesion.
- Tests de chaos: reconexion WS, latencia DB/Redis, restart loop.
- Bloqueo CI si discovery recolecta 0 tests.
Definition of Done:
- Test suite verde en CI.
- Reporte de paridad decision-level firmado.
Estimacion: 8-12 dias

E10 - Migracion, Shadow Mode y Cutover (P0)
Objetivo: Transicionar a V4 sin downtime ni perdida de informacion.
Historias/Tareas:
- Shadow mode: V4 consume market data y compara decisiones contra V3.
- Canary: activar V4 por un par (DOGE), luego SOL.
- Plan de rollback instantaneo.
- Cutover final con checklist de validacion y ventanas de observacion.
Definition of Done:
- Cutover completado con KPIs dentro de tolerancias.
- Rollback probado previamente en simulacro.
Estimacion: 6-8 dias

7) MATRIZ DE PARIDAD DE DATOS (OBLIGATORIA)
- Regla: ninguna tabla de V3 se elimina antes de 2 ciclos completos de cierre diario en V4.
- Regla: migraciones V4 deben ser aditivas; cambios destructivos solo despues de deprecacion aprobada.
- Regla: datos criticos de analisis deben conservar granularidad original (ticks, fills, grid_levels, equity_snapshots, worker_process_log).
- Regla: strategy_param_history y exchange_strategies deben mantener trazabilidad SCD2 sin lagunas.
- Regla: sesiones y fills deben conservar llaves y correlacion para reconstruccion exacta de performance.

8) KPIS Y CRITERIOS DE ACEPTACION DE PROGRAMA
- Paridad de decisiones kernel (paper/live simulado): >= 99.9% en replay determinista.
- Integridad de datos post-migracion: 100% de tablas y columnas V3 presentes y accesibles.
- Trazabilidad: 100% de cambios de parametros auditados.
- Resiliencia: MTTR de worker < 2 min con guardia automatica.
- Cierre diario local: 100% ejecuciones correctas en timezone configurada.

9) PLAN DE FASES
- Fase 0 (1 semana): contratos + inventario + paridad schema + ADRs.
- Fase 1 (2-3 semanas): kernel Rust + simulator + persistencia.
- Fase 2 (2 semanas): provider live + reconciliacion + daily close local completo.
- Fase 3 (1-2 semanas): observabilidad + chaos + hardening.
- Fase 4 (1-2 semanas): shadow mode + canary + cutover.

10) RIESGOS Y MITIGACIONES
- Riesgo: divergencia paper/live por eventos de fill incompletos.
Mitigacion: event sourcing de orden/fill + reconciliacion periodica + tests diferenciales.
- Riesgo: perdida de trazabilidad por cambios de schema.
Mitigacion: estrategia aditiva + scripts de checksum/conteo + aprobacion de data parity gate.
- Riesgo: complejidad Rust para equipo.
Mitigacion: boundaries claros, codigo idiomatico, guias internas y pairing.

11) ENTREGABLES
- Documento de contratos (API/Events).
- Worker Rust operativo con providers live/simulator.
- API Python V4 con endpoints de control y auditoria.
- Reporte de paridad BD V3->V4 y runbooks de operacion.
- Dashboard de performance/ejecucion/resiliencia y alertas.
