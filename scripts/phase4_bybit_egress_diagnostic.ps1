param(
    [string]$RepoRoot = (Split-Path -Parent $PSScriptRoot),
    [int]$TcpPort = 443,
    [int]$HttpTimeoutSeconds = 10
)

$ErrorActionPreference = "Stop"

$repoLeaf = Split-Path -Leaf $RepoRoot
if ($repoLeaf -ne "TradingBotV4") {
    throw "Safety check failed: RepoRoot must point to TradingBotV4 (actual: $RepoRoot)."
}

$logsDir = Join-Path $RepoRoot "logs\phase4_canary"
if (-not (Test-Path $logsDir)) {
    New-Item -ItemType Directory -Path $logsDir | Out-Null
}

$stamp = Get-Date -Format "yyyyMMdd_HHmmss"
$reportPath = Join-Path $logsDir "bybit_egress_diag_${stamp}.md"

$targets = @(
    [PSCustomObject]@{
        Host = "api.bybit.com"
        Url  = "https://api.bybit.com/v5/market/tickers?category=spot&symbol=SOLUSDT"
    },
    [PSCustomObject]@{
        Host = "api.bytick.com"
        Url  = "https://api.bytick.com/v5/market/tickers?category=spot&symbol=SOLUSDT"
    },
    [PSCustomObject]@{
        Host = "api-testnet.bybit.com"
        Url  = "https://api-testnet.bybit.com/v5/market/tickers?category=spot&symbol=SOLUSDT"
    }
)

function Resolve-HostIps {
    param([string]$HostName)
    try {
        $rows = Resolve-DnsName $HostName -ErrorAction Stop
        $ips = @($rows | Where-Object { $_.IPAddress } | Select-Object -ExpandProperty IPAddress -Unique)
        if ($ips.Count -eq 0) {
            return "RESOLVED_NO_IP"
        }
        return ($ips -join ",")
    }
    catch {
        return "DNS_FAIL: $($_.Exception.Message)"
    }
}

function Test-TcpPort {
    param(
        [string]$HostName,
        [int]$Port
    )
    try {
        $probe = Test-NetConnection -ComputerName $HostName -Port $Port -WarningAction SilentlyContinue
        return [PSCustomObject]@{
            Ok = [bool]$probe.TcpTestSucceeded
            RemoteAddress = [string]$probe.RemoteAddress
        }
    }
    catch {
        return [PSCustomObject]@{
            Ok = $false
            RemoteAddress = ""
            Error = "TCP_FAIL: $($_.Exception.Message)"
        }
    }
}

function Test-Http {
    param(
        [string]$Url,
        [int]$TimeoutSeconds
    )
    try {
        $resp = Invoke-WebRequest -UseBasicParsing -Uri $Url -TimeoutSec $TimeoutSeconds -Method Get
        return [PSCustomObject]@{
            Ok = $true
            Status = [int]$resp.StatusCode
            Error = ""
        }
    }
    catch {
        return [PSCustomObject]@{
            Ok = $false
            Status = 0
            Error = [string]$_.Exception.Message
        }
    }
}

$results = New-Object System.Collections.Generic.List[object]
foreach ($t in $targets) {
    $dns = Resolve-HostIps -HostName $t.Host
    $tcp = Test-TcpPort -HostName $t.Host -Port $TcpPort
    $http = Test-Http -Url $t.Url -TimeoutSeconds $HttpTimeoutSeconds
    $results.Add(
        [PSCustomObject]@{
            Host = $t.Host
            Url = $t.Url
            Dns = $dns
            TcpOk = [bool]$tcp.Ok
            TcpRemote = [string]$tcp.RemoteAddress
            TcpError = [string]$tcp.Error
            HttpOk = [bool]$http.Ok
            HttpStatus = [int]$http.Status
            HttpError = [string]$http.Error
        }
    )
}

$allTcpOk = @($results | Where-Object { $_.TcpOk -eq $true }).Count -eq $results.Count
$allHttpOk = @($results | Where-Object { $_.HttpOk -eq $true }).Count -eq $results.Count
$overallPass = $allTcpOk -and $allHttpOk

$lines = @()
$lines += "# Bybit Egress Diagnostic"
$lines += ""
$lines += "- Timestamp: $(Get-Date -Format "yyyy-MM-dd HH:mm:ss")"
$lines += "- Repo: $RepoRoot"
$lines += "- Overall: $([string]::new(@('F','A','I','L'),0,4))"
if ($overallPass) {
    $lines[$lines.Count - 1] = "- Overall: PASS"
}
$lines += ""
$lines += "## Results"
$lines += ""
foreach ($r in $results) {
    $lines += "- Host: $($r.Host)"
    $lines += "  URL: $($r.Url)"
    $lines += "  DNS: $($r.Dns)"
    $lines += "  TCP:${TcpPort}: $($r.TcpOk) (remote=$($r.TcpRemote))"
    if (-not [string]::IsNullOrWhiteSpace($r.TcpError)) {
        $lines += "  TCP Error: $($r.TcpError)"
    }
    $lines += "  HTTP: $($r.HttpOk) (status=$($r.HttpStatus))"
    if (-not [string]::IsNullOrWhiteSpace($r.HttpError)) {
        $lines += "  HTTP Error: $($r.HttpError)"
    }
    $lines += ""
}

$lines += "## Recommendation"
if ($overallPass) {
    $lines += "- Network egress is OK for Bybit endpoints."
    $lines += "- Re-run phase4 canary with bybit_rest and proceed with cutover go/no-go."
}
else {
    $lines += "- Keep V4 in NO-GO for bybit_rest cutover."
    $lines += "- Ask infra/network team to allow outbound TCP $TcpPort to Bybit endpoints."
    $lines += "- Re-run this diagnostic after firewall/proxy changes."
}

Set-Content -LiteralPath $reportPath -Value $lines -Encoding UTF8

Write-Host "Bybit egress diagnostic completed."
Write-Host "Overall: $([bool]$overallPass)"
Write-Host "Report: $reportPath"
foreach ($r in $results) {
    Write-Host ("- {0} | DNS={1} | TCP={2} | HTTP={3}" -f $r.Host, $r.Dns, $r.TcpOk, $r.HttpOk)
}

if (-not $overallPass) {
    exit 1
}

exit 0
