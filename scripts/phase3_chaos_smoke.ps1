param(
    [string]$RepoRoot = (Split-Path -Parent $PSScriptRoot),
    [string]$WorkerRustDir = "",
    [string]$RedisContainer = "tradingbotv4-staging-redis",
    [int]$RedisDb = 15,
    [string]$DbDsn = "postgresql://tradingbot:tradingbot@localhost:5443/tradingbotv4_staging",
    [string]$RedisUrl = "redis://localhost:6390/15",
    [string]$TenantId = "00000000-0000-0000-0000-000000000001",
    [string]$Exchange = "bybit",
    [string]$ProductId = "SOL-USD",
    [int]$DurationSeconds = 12,
    [int]$TickIntervalMs = 700,
    [int]$WarnThresholdMs = 300
)

$ErrorActionPreference = "Stop"

if ([string]::IsNullOrWhiteSpace($WorkerRustDir)) {
    $WorkerRustDir = Join-Path $RepoRoot "worker-rust"
}

$logsDir = Join-Path $RepoRoot "logs\phase3_chaos"
if (-not (Test-Path $logsDir)) {
    New-Item -ItemType Directory -Path $logsDir | Out-Null
}

function Write-Info {
    param([string]$Message)
    Write-Host ("[{0}] {1}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $Message)
}

function Flush-StagingRedis {
    param(
        [string]$ContainerName,
        [int]$DbIndex
    )

    try {
        $result = docker exec $ContainerName redis-cli -n $DbIndex FLUSHDB 2>&1
        Write-Info "Redis FLUSHDB ($ContainerName db=$DbIndex): $result"
    }
    catch {
        Write-Info "WARN: no se pudo hacer FLUSHDB en Redis staging: $($_.Exception.Message)"
    }
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
            # Ignore non-JSON lines.
        }
    }
    return $events
}

function Run-WorkerWindow {
    param(
        [string]$RunName,
        [hashtable]$EnvVars,
        [int]$Seconds
    )

    $stamp = Get-Date -Format "yyyyMMdd_HHmmss"
    $stdout = Join-Path $logsDir "${RunName}_${stamp}.out.log"
    $stderr = Join-Path $logsDir "${RunName}_${stamp}.err.log"

    $original = @{}
    foreach ($key in $EnvVars.Keys) {
        $original[$key] = [System.Environment]::GetEnvironmentVariable($key, "Process")
        [System.Environment]::SetEnvironmentVariable($key, [string]$EnvVars[$key], "Process")
    }

    try {
        Write-Info "Iniciando $RunName por $Seconds s..."
        $p = Start-Process cargo -ArgumentList @("run", "--quiet") -PassThru -WindowStyle Hidden -WorkingDirectory $WorkerRustDir -RedirectStandardOutput $stdout -RedirectStandardError $stderr
        Start-Sleep -Seconds $Seconds
        if (-not $p.HasExited) {
            Stop-Process -Id $p.Id -Force
            Write-Info "$RunName detenido al completar ventana de prueba (PID=$($p.Id))."
        } else {
            Write-Info "$RunName finalizo por si solo (PID=$($p.Id))."
        }
    }
    finally {
        foreach ($key in $EnvVars.Keys) {
            [System.Environment]::SetEnvironmentVariable($key, $original[$key], "Process")
        }
    }

    $events = Parse-Events -Path $stdout
    $counts = $events | Group-Object state_type | Sort-Object Count -Descending

    Write-Info "Resumen ${RunName}: eventos=$($events.Count)"
    foreach ($g in $counts) {
        Write-Host ("  {0,4}  {1}" -f $g.Count, $g.Name)
    }

    [PSCustomObject]@{
        Name = $RunName
        Stdout = $stdout
        Stderr = $stderr
        Events = $events
    }
}

function Any-StateType {
    param(
        [object[]]$Events,
        [string[]]$StateTypes
    )
    return @($Events | Where-Object { $StateTypes -contains $_.state_type }).Count -gt 0
}

function Check-Threshold {
    param(
        [object[]]$Events,
        [string]$StateType,
        [int]$ExpectedMs
    )
    $items = @($Events | Where-Object { $_.state_type -eq $StateType })
    if ($items.Count -eq 0) { return $false }
    $match = @($items | Where-Object { [int]$_.payload.threshold_ms -eq $ExpectedMs })
    return $match.Count -gt 0
}

Write-Info "Phase3 chaos smoke iniciado (solo stack V4 aislado)."
Write-Info "RepoRoot=$RepoRoot"
Write-Info "WorkerRustDir=$WorkerRustDir"

Flush-StagingRedis -ContainerName $RedisContainer -DbIndex $RedisDb

$commonEnv = @{
    TB_DB_DSN = $DbDsn
    TB_REDIS_URL = $RedisUrl
    TB_TENANT_ID = $TenantId
    TB_EXCHANGE = $Exchange
    TB_PRODUCT_ID = $ProductId
    TB_TICK_INTERVAL_MS = $TickIntervalMs
    TB_MARKET_DATA_GAP_WARN_MS = $WarnThresholdMs
    TB_HEARTBEAT_LAG_WARN_MS = $WarnThresholdMs
    TB_COMMAND_LAG_WARN_MS = $WarnThresholdMs
}

$runAEnv = @{}
$commonEnv.Keys | ForEach-Object { $runAEnv[$_] = $commonEnv[$_] }
$runAEnv["TB_EXECUTION_MODE"] = "live"
$runAEnv["TB_MARKET_DATA_PROVIDER"] = "synthetic"
$runAEnv["TB_CHAOS_REDIS_FAIL_EVERY_N"] = 2
$runAEnv["TB_CHAOS_BYBIT_EXEC_FAIL_EVERY_N"] = 1
$runAEnv["TB_CHAOS_BYBIT_MARKET_FAIL_EVERY_N"] = 0
$runAEnv["TB_BYBIT_API_KEY"] = "dummy_key"
$runAEnv["TB_BYBIT_API_SECRET"] = "dummy_secret"

$runA = Run-WorkerWindow -RunName "runA_synthetic_live" -EnvVars $runAEnv -Seconds $DurationSeconds

Flush-StagingRedis -ContainerName $RedisContainer -DbIndex $RedisDb

$runBEnv = @{}
$commonEnv.Keys | ForEach-Object { $runBEnv[$_] = $commonEnv[$_] }
$runBEnv["TB_EXECUTION_MODE"] = "simulator"
$runBEnv["TB_MARKET_DATA_PROVIDER"] = "bybit_rest"
$runBEnv["TB_CHAOS_REDIS_FAIL_EVERY_N"] = 2
$runBEnv["TB_CHAOS_BYBIT_EXEC_FAIL_EVERY_N"] = 0
$runBEnv["TB_CHAOS_BYBIT_MARKET_FAIL_EVERY_N"] = 1

$runB = Run-WorkerWindow -RunName "runB_bybit_market_chaos" -EnvVars $runBEnv -Seconds $DurationSeconds

$errors = New-Object System.Collections.Generic.List[string]

$redisErrorTypes = @(
    "redis_command_poll_failed",
    "redis_state_publish_failed",
    "redis_heartbeat_publish_failed"
)
$executionErrorTypes = @(
    "execution_on_tick_failed",
    "execution_submit_failed",
    "execution_cancel_failed",
    "execution_liquidation_failed",
    "execution_flush_fills_failed",
    "execution_reconciliation_snapshot_failed"
)

if (-not (Any-StateType -Events $runA.Events -StateTypes $redisErrorTypes)) {
    $errors.Add("Run A: no aparecieron alertas de fallo Redis esperadas.")
}
if (-not (Any-StateType -Events $runA.Events -StateTypes $executionErrorTypes)) {
    $errors.Add("Run A: no aparecieron alertas de fallo de ejecucion esperadas.")
}
if (-not (Check-Threshold -Events $runA.Events -StateType "heartbeat_lag_detected" -ExpectedMs $WarnThresholdMs)) {
    $errors.Add("Run A: no se observo heartbeat_lag_detected con threshold_ms=$WarnThresholdMs.")
}
if (-not (Check-Threshold -Events $runA.Events -StateType "market_data_gap_detected" -ExpectedMs $WarnThresholdMs)) {
    $errors.Add("Run A: no se observo market_data_gap_detected con threshold_ms=$WarnThresholdMs.")
}

$runBMarketErrors = @(
    $runB.Events | Where-Object {
        $_.state_type -eq "market_data_provider_error" -and
        [string]$_.payload.market_data_provider -eq "bybit_rest"
    }
)
if ($runBMarketErrors.Count -lt 1) {
    $errors.Add("Run B: no aparecio market_data_provider_error para provider bybit_rest.")
}

if ($errors.Count -gt 0) {
    Write-Host ""
    Write-Host "CHAOS SMOKE: FAIL" -ForegroundColor Red
    $errors | ForEach-Object { Write-Host ("- " + $_) }
    Write-Host ""
    Write-Host "Logs:"
    Write-Host "  Run A stdout: $($runA.Stdout)"
    Write-Host "  Run A stderr: $($runA.Stderr)"
    Write-Host "  Run B stdout: $($runB.Stdout)"
    Write-Host "  Run B stderr: $($runB.Stderr)"
    exit 1
}

Write-Host ""
Write-Host "CHAOS SMOKE: PASS" -ForegroundColor Green
Write-Host "Validaciones OK:"
Write-Host "- Run A emitio alertas Redis + ejecucion y thresholds esperados."
Write-Host "- Run B emitio market_data_provider_error (bybit_rest)."
Write-Host ""
Write-Host "Logs:"
Write-Host "  Run A stdout: $($runA.Stdout)"
Write-Host "  Run A stderr: $($runA.Stderr)"
Write-Host "  Run B stdout: $($runB.Stdout)"
Write-Host "  Run B stderr: $($runB.Stderr)"
exit 0
