param(
    [datetime]$EndAt = (Get-Date).Date.AddDays(1).AddHours(9),
    [int]$IntervalMinutes = 120,
    [string]$ContainerName = "tradingbotv3-bot-1",
    [string]$ProjectRoot = "C:\Users\Abraham\source\repos\TradingBotV3",
    [switch]$RunOnce
)

$ErrorActionPreference = "Stop"
$logDir = Join-Path $ProjectRoot "logs"
if (-not (Test-Path $logDir)) {
    New-Item -ItemType Directory -Path $logDir | Out-Null
}
$logFile = Join-Path $logDir ("worker_guardian_{0}.log" -f (Get-Date -Format "yyyyMMdd_HHmmss"))

function Write-Log {
    param([string]$Message)
    $line = "[{0}] {1}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $Message
    $line | Tee-Object -FilePath $logFile -Append
}

function Test-ContainerRunning {
    param([string]$Name)
    try {
        $status = docker inspect -f "{{.State.Running}}" $Name 2>$null
        return ($status -match "true")
    } catch {
        return $false
    }
}

function Get-WorkerStatus {
    param([string]$Name)
    try {
        $out = docker exec $Name supervisorctl status worker 2>&1
        return [string]$out
    } catch {
        return ""
    }
}

function Ensure-BotUp {
    param(
        [string]$Name,
        [string]$Root
    )
    if (-not (Test-ContainerRunning -Name $Name)) {
        Write-Log "Container '$Name' no esta corriendo. Ejecutando 'docker compose up -d bot'..."
        Push-Location $Root
        try {
            docker compose up -d bot | Out-Null
        } finally {
            Pop-Location
        }
        Start-Sleep -Seconds 5
    }
}

function Heal-Worker {
    param([string]$Name)
    $status = Get-WorkerStatus -Name $Name
    if ($status -match "RUNNING") {
        Write-Log "Worker OK: $status"
        return
    }

    Write-Log "Worker down/no RUNNING. Estado actual: $status"
    try {
        $restartOut = docker exec $Name supervisorctl restart worker 2>&1
        Write-Log "Intento restart worker: $restartOut"
    } catch {
        Write-Log "Error en restart worker: $($_.Exception.Message)"
    }

    Start-Sleep -Seconds 6
    $statusAfter = Get-WorkerStatus -Name $Name
    if ($statusAfter -match "RUNNING") {
        Write-Log "Worker recuperado via supervisorctl: $statusAfter"
        return
    }

    Write-Log "Worker sigue mal. Reiniciando contenedor bot..."
    docker restart $Name | Out-Null
    Start-Sleep -Seconds 8
    $statusFinal = Get-WorkerStatus -Name $Name
    Write-Log "Estado despues de restart contenedor: $statusFinal"
}

Write-Log "Guardian iniciado. EndAt local=$($EndAt.ToString('yyyy-MM-dd HH:mm:ss')) Interval=${IntervalMinutes}m"

if ($RunOnce) {
    Ensure-BotUp -Name $ContainerName -Root $ProjectRoot
    Heal-Worker -Name $ContainerName
    Write-Log "RunOnce finalizado."
    exit 0
}

while ((Get-Date) -lt $EndAt) {
    try {
        Ensure-BotUp -Name $ContainerName -Root $ProjectRoot
        Heal-Worker -Name $ContainerName
    } catch {
        Write-Log "Error en ciclo guardian: $($_.Exception.Message)"
    }

    $next = (Get-Date).AddMinutes($IntervalMinutes)
    if ($next -gt $EndAt) {
        $sleepSec = [int][Math]::Max(0, ($EndAt - (Get-Date)).TotalSeconds)
    } else {
        $sleepSec = $IntervalMinutes * 60
    }
    if ($sleepSec -le 0) { break }
    Write-Log "Proximo chequeo en $sleepSec segundos."
    Start-Sleep -Seconds $sleepSec
}

Write-Log "Guardian finalizado al llegar a EndAt."
