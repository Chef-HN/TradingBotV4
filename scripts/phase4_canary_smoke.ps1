param(
    [string]$RepoRoot = (Split-Path -Parent $PSScriptRoot),
    [string]$WorkerRustDir = "",
    [string]$DbDsn = "postgresql://tradingbot:tradingbot@localhost:5443/tradingbotv4_staging",
    [string]$MarketDataDbDsn = "postgresql://tradingbot:tradingbot@localhost:5433/tradingbotv3",
    [string]$RedisUrl = "redis://localhost:6390/15",
    [string]$TenantId = "00000000-0000-0000-0000-000000000001",
    [string]$Exchange = "bybit",
    [string]$ProductId = "SOL-USD",
    [ValidateSet("simulator", "live")]
    [string]$ExecutionMode = "simulator",
    [ValidateSet("synthetic", "bybit_rest", "postgres_tail")]
    [string]$MarketDataProvider = "synthetic",
    [int]$DurationSeconds = 45,
    [int]$TickIntervalMs = 800,
    [int]$MarketDataGapWarnMs = 5000,
    [int]$HeartbeatLagWarnMs = 5000,
    [int]$CommandLagWarnMs = 5000,
    [int]$MinOrderSubmitted = 2,
    [int]$MinCycles = -1,
    [int]$MaxAllowedFailures = 0,
    [int]$MaxAllowedGapAlerts = 0,
    [int]$MaxAllowedHeartbeatAlerts = 0,
    [int]$MaxAllowedCommandLagAlerts = 0,
    [switch]$SkipBybitPreflight,
    [switch]$AutoRollbackOnFail
)

$ErrorActionPreference = "Stop"

if ([string]::IsNullOrWhiteSpace($WorkerRustDir)) {
    $WorkerRustDir = Join-Path $RepoRoot "worker-rust"
}

$repoLeaf = Split-Path -Leaf $RepoRoot
if ($repoLeaf -ne "TradingBotV4") {
    throw "Safety check failed: RepoRoot debe apuntar a TradingBotV4 (actual: $RepoRoot)."
}

$logsDir = Join-Path $RepoRoot "logs\phase4_canary"
if (-not (Test-Path $logsDir)) {
    New-Item -ItemType Directory -Path $logsDir | Out-Null
}

function Write-Info {
    param([string]$Message)
    Write-Host ("[{0}] {1}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $Message)
}

function Parse-Events {
    param([string]$Path)
    if (-not (Test-Path $Path)) { return @() }

    $events = @()
    Get-Content -Path $Path | ForEach-Object {
        $line = $_.Trim()
        if ([string]::IsNullOrWhiteSpace($line)) { return }
        try {
            $evt = $line | ConvertFrom-Json -ErrorAction Stop
            if ($null -ne $evt -and $null -ne $evt.state_type) {
                $events += ,$evt
            }
        }
        catch {
            # Ignore non-JSON output lines.
        }
    }
    return $events
}

function Get-CycleId {
    param([object]$Event)
    $payload = $Event.payload
    if ($null -eq $payload) { return $null }
    $cid = [string]$payload.correlation_id
    if ([string]::IsNullOrWhiteSpace($cid)) { return $null }
    if ($cid -notmatch "^cycle:.*:(\d+)$") { return $null }
    return [int]$Matches[1]
}

function Run-WorkerWindow {
    param(
        [hashtable]$EnvVars,
        [int]$Seconds
    )

    $stamp = Get-Date -Format "yyyyMMdd_HHmmss"
    $stdout = Join-Path $logsDir "canary_${stamp}.out.log"
    $stderr = Join-Path $logsDir "canary_${stamp}.err.log"

    $original = @{}
    foreach ($key in $EnvVars.Keys) {
        $original[$key] = [System.Environment]::GetEnvironmentVariable($key, "Process")
        [System.Environment]::SetEnvironmentVariable($key, [string]$EnvVars[$key], "Process")
    }

    $process = $null
    $exitedBeforeWindow = $false
    $exitCode = $null
    try {
        Write-Info "Iniciando canary por $Seconds s (provider=$MarketDataProvider mode=$ExecutionMode)..."
        $process = Start-Process cargo -ArgumentList @("run", "--quiet") -PassThru -WindowStyle Hidden -WorkingDirectory $WorkerRustDir -RedirectStandardOutput $stdout -RedirectStandardError $stderr
        Start-Sleep -Seconds $Seconds
        if ($null -ne $process -and $process.HasExited) {
            $exitedBeforeWindow = $true
            $exitCode = $process.ExitCode
            Write-Info "Canary finalizo antes de tiempo (ExitCode=$exitCode)."
        }
    }
    finally {
        if ($null -ne $process -and -not $process.HasExited) {
            Stop-Process -Id $process.Id -Force
            Write-Info "Canary detenido al completar ventana (PID=$($process.Id))."
        }

        foreach ($key in $EnvVars.Keys) {
            [System.Environment]::SetEnvironmentVariable($key, $original[$key], "Process")
        }
    }

    $events = Parse-Events -Path $stdout
    [PSCustomObject]@{
        Stdout = $stdout
        Stderr = $stderr
        Events = $events
        ExitedBeforeWindow = $exitedBeforeWindow
        ExitCode = $exitCode
    }
}

function Count-State {
    param(
        [object[]]$Events,
        [string]$StateType
    )
    return @($Events | Where-Object { $_.state_type -eq $StateType }).Count
}

function Test-BybitConnectivity {
    param(
        [string]$Url,
        [int]$TimeoutSeconds = 10
    )

    try {
        $resp = Invoke-WebRequest -UseBasicParsing -Uri $Url -TimeoutSec $TimeoutSeconds -Method Get
        return ($null -ne $resp -and $resp.StatusCode -ge 200 -and $resp.StatusCode -lt 500)
    }
    catch {
        return $false
    }
}

Write-Info "Phase4 canary smoke iniciado (solo V4)."
Write-Info "RepoRoot=$RepoRoot"
Write-Info "WorkerRustDir=$WorkerRustDir"
Write-Info "DB/Redis V4: $DbDsn | $RedisUrl"
if ($MarketDataProvider -eq "postgres_tail") {
    Write-Info "Market data source (read-only): $MarketDataDbDsn"
}

if ($MarketDataProvider -eq "bybit_rest" -and -not $SkipBybitPreflight) {
    $bybitUrl = "https://api.bybit.com/v5/market/tickers?category=spot&symbol=SOLUSDT"
    Write-Info "Preflight bybit_rest: comprobando conectividad a $bybitUrl"
    if (-not (Test-BybitConnectivity -Url $bybitUrl -TimeoutSeconds 10)) {
        Write-Host ""
        Write-Host "CANARY SMOKE: FAIL" -ForegroundColor Red
        Write-Host "- Preflight fallo: sin conectividad HTTP hacia api.bybit.com."
        Write-Host "- No se ejecuta canary bybit_rest hasta resolver egress DNS/red/firewall."
        exit 2
    }
}

$envVars = @{
    TB_DB_DSN = $DbDsn
    TB_REDIS_URL = $RedisUrl
    TB_TENANT_ID = $TenantId
    TB_EXCHANGE = $Exchange
    TB_PRODUCT_ID = $ProductId
    TB_EXECUTION_MODE = $ExecutionMode
    TB_MARKET_DATA_PROVIDER = $MarketDataProvider
    TB_TICK_INTERVAL_MS = $TickIntervalMs
    TB_MARKET_DATA_GAP_WARN_MS = $MarketDataGapWarnMs
    TB_HEARTBEAT_LAG_WARN_MS = $HeartbeatLagWarnMs
    TB_COMMAND_LAG_WARN_MS = $CommandLagWarnMs
    TB_CHAOS_REDIS_FAIL_EVERY_N = 0
    TB_CHAOS_BYBIT_MARKET_FAIL_EVERY_N = 0
    TB_CHAOS_BYBIT_EXEC_FAIL_EVERY_N = 0
}
if ($MarketDataProvider -eq "postgres_tail") {
    $envVars["TB_MARKET_DATA_DB_DSN"] = $MarketDataDbDsn
    $envVars["TB_MARKET_DATA_DB_START_MODE"] = "latest"
}

$run = Run-WorkerWindow -EnvVars $envVars -Seconds $DurationSeconds
$events = $run.Events
$counts = $events | Group-Object state_type | Sort-Object Count -Descending

$requiredMinCycles = $MinCycles
if ($requiredMinCycles -lt 0) {
    if ($MarketDataProvider -eq "bybit_rest") {
        # Bybit can have low event density in stable windows.
        $requiredMinCycles = 1
    }
    elseif ($MarketDataProvider -eq "postgres_tail") {
        # Tailing DB ticks can also have sparse event density in short windows.
        $requiredMinCycles = 1
    }
    else {
        $requiredMinCycles = 5
    }
}

Write-Info "Resumen de eventos: total=$($events.Count)"
foreach ($g in $counts) {
    Write-Host ("  {0,4}  {1}" -f $g.Count, $g.Name)
}

$failureTypes = @(
    "redis_command_poll_failed",
    "redis_state_publish_failed",
    "redis_heartbeat_publish_failed",
    "market_data_provider_error",
    "execution_on_tick_failed",
    "execution_submit_failed",
    "execution_cancel_failed",
    "execution_liquidation_failed",
    "execution_flush_fills_failed",
    "execution_reconciliation_snapshot_failed",
    "execution_reconciliation_mismatch"
)

$failureCount = @($events | Where-Object { $failureTypes -contains $_.state_type }).Count
$gapAlerts = Count-State -Events $events -StateType "market_data_gap_detected"
$heartbeatAlerts = Count-State -Events $events -StateType "heartbeat_lag_detected"
$commandLagAlerts = Count-State -Events $events -StateType "command_lag_detected"
$orderSubmitted = Count-State -Events $events -StateType "order_submitted"
$bootstrapCount = Count-State -Events $events -StateType "kernel_bootstrap_grid"
$bootCount = Count-State -Events $events -StateType "worker_boot"

$cycleIds = New-Object System.Collections.Generic.HashSet[int]
foreach ($evt in $events) {
    $cycleId = Get-CycleId -Event $evt
    if ($null -ne $cycleId) {
        [void]$cycleIds.Add($cycleId)
    }
}

$errors = New-Object System.Collections.Generic.List[string]

if ($bootCount -lt 1) {
    $errors.Add("No se detecto worker_boot.")
}
if ($bootstrapCount -lt 1) {
    $errors.Add("No se detecto kernel_bootstrap_grid.")
}
if ($orderSubmitted -lt $MinOrderSubmitted) {
    $errors.Add("order_submitted=$orderSubmitted por debajo del minimo esperado ($MinOrderSubmitted).")
}
if ($cycleIds.Count -lt $requiredMinCycles) {
    $errors.Add("Ciclos observados=$($cycleIds.Count) por debajo del minimo esperado ($requiredMinCycles).")
}
if ($run.ExitedBeforeWindow) {
    $errors.Add("El worker finalizo antes de completar la ventana (ExitCode=$($run.ExitCode)).")
}
if ($failureCount -gt $MaxAllowedFailures) {
    $errors.Add("Eventos de fallo=$failureCount exceden maximo permitido=$MaxAllowedFailures.")
}
if ($gapAlerts -gt $MaxAllowedGapAlerts) {
    $errors.Add("market_data_gap_detected=$gapAlerts excede maximo permitido=$MaxAllowedGapAlerts.")
}
if ($heartbeatAlerts -gt $MaxAllowedHeartbeatAlerts) {
    $errors.Add("heartbeat_lag_detected=$heartbeatAlerts excede maximo permitido=$MaxAllowedHeartbeatAlerts.")
}
if ($commandLagAlerts -gt $MaxAllowedCommandLagAlerts) {
    $errors.Add("command_lag_detected=$commandLagAlerts excede maximo permitido=$MaxAllowedCommandLagAlerts.")
}

if ($errors.Count -gt 0) {
    Write-Host ""
    Write-Host "CANARY SMOKE: FAIL" -ForegroundColor Red
    $errors | ForEach-Object { Write-Host ("- " + $_) }
    Write-Host ""
    Write-Host "Logs:"
    Write-Host "  Stdout: $($run.Stdout)"
    Write-Host "  Stderr: $($run.Stderr)"

    if ($AutoRollbackOnFail) {
        $stamp = Get-Date -Format "yyyyMMdd_HHmmss"
        $rollbackFile = Join-Path $logsDir "rollback_plan_${stamp}.md"
        $rollbackLines = @(
            "# Phase4 Canary Rollback Plan",
            "",
            "- Timestamp: $(Get-Date -Format "yyyy-MM-dd HH:mm:ss")",
            "- Scope: V4 only (no V3 changes).",
            "- Action: mantener V4 fuera de canary activo y conservar V3 como runtime principal.",
            "",
            "## Immediate checks",
            "- Verificar que no haya proceso local `cargo run` activo de V4.",
            "- Verificar contenedores V3 en estado `Up` (sin cambios aplicados).",
            "- Revisar logs canary para diagnostico y abrir correccion antes de reintentar.",
            "",
            "## Evidence",
            "- Stdout: $($run.Stdout)",
            "- Stderr: $($run.Stderr)"
        )
        Set-Content -LiteralPath $rollbackFile -Value $rollbackLines -Encoding UTF8
        Write-Host ""
        Write-Host "Rollback plan generado: $rollbackFile"
    }

    exit 1
}

Write-Host ""
Write-Host "CANARY SMOKE: PASS" -ForegroundColor Green
Write-Host "Validaciones OK:"
Write-Host "- worker_boot y kernel_bootstrap_grid presentes."
Write-Host "- order_submitted >= $MinOrderSubmitted."
Write-Host "- ciclos observados >= $requiredMinCycles."
Write-Host "- sin eventos de fallo y sin alertas por encima de umbral."
Write-Host ""
Write-Host "Logs:"
Write-Host "  Stdout: $($run.Stdout)"
Write-Host "  Stderr: $($run.Stderr)"
exit 0
