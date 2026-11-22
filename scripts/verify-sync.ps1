<#
.SYNOPSIS
    一条命令验证 PowerSync 双向同步：后台 → 本地（下载）、本地 → 后台（新增/修改/删除）。

.DESCRIPTION
    [1/4] 下载：新库全量同步 + 逐表校验（本地每一行都在后端且字段一致）+ 每表行数
    [2/4] 新增：本地 INSERT（离线可写）→ 自动上传 → REST 断言后端有这行
    [3/4] 修改：本地 UPDATE → 自动上传 → REST 断言后端字段已变
    [4/4] 删除：本地 DELETE → 自动上传 → REST 断言后端已无该行
    默认清掉探针行；-KeepRow 保留方便去后台管理页查看。任一步失败 → 退出码 1。

.EXAMPLE
    .\scripts\verify-sync.ps1
    .\scripts\verify-sync.ps1 -KeepRow
    .\scripts\verify-sync.ps1 -SkipDownload          # 只测上传
    .\scripts\verify-sync.ps1 -Database .\out\mine.db
#>
[CmdletBinding()]
param(
    [string]$Database = './out/verify-sync.db',
    [string]$Schema = './examples/todo/schema-cutter.json',
    [string]$Table = 'sys_error_report',

    [string]$Endpoint = $env:PS_URL,
    [string]$Rest = $env:REST_URL,
    [string]$Rpc = $env:RPC_URL,
    [string]$Box = $env:BOX_CODE,
    [string]$Org = $env:ORG_CODE,

    [switch]$KeepRow,
    [switch]$SkipDownload,
    [switch]$KeepDb
)

$ErrorActionPreference = 'Stop'

# 连接参数不在脚本里写死：必须用参数或环境变量提供
if (-not $Endpoint -or -not $Rest -or -not $Rpc -or -not $Box -or -not $Org) {
    Write-Host "连接参数未提供。请用参数或环境变量指定：" -ForegroundColor Yellow
    Write-Host '  $env:PS_URL="http://<host>:8080"' -ForegroundColor Yellow
    Write-Host '  $env:REST_URL="http://<host>:8089/rest"' -ForegroundColor Yellow
    Write-Host '  $env:RPC_URL="http://<host>:8089/rest/rpc/fetch_credentials"' -ForegroundColor Yellow
    Write-Host '  $env:BOX_CODE="<box_code>"; $env:ORG_CODE="<org_code>"' -ForegroundColor Yellow
    exit 1
}
try { [Console]::OutputEncoding = [System.Text.Encoding]::UTF8 } catch { }
$OutputEncoding = [System.Text.Encoding]::UTF8

$repo = Split-Path -Parent $PSScriptRoot
if (-not (Test-Path (Join-Path $repo 'go.mod'))) { $repo = (Get-Location).Path }
Push-Location $repo

$script:failed = @()
function Section([string]$t) { Write-Host "`n===== $t =====" -ForegroundColor Cyan }
function Ok([string]$m) { Write-Host "  ✅ $m" -ForegroundColor Green }
function Bad([string]$m) { Write-Host "  ❌ $m" -ForegroundColor Red; $script:failed += $m }
function Info([string]$m) { Write-Host "  $m" -ForegroundColor DarkGray }

# 跑 pstable（原生程序 stderr 不会变终止错误），输出直接打到控制台，只返回退出码
function Run-Ps([string[]]$psArgs) {
    $prev = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    & go run ./cmd/pstable @psArgs | Out-Host
    $code = $LASTEXITCODE
    $ErrorActionPreference = $prev
    return $code
}

# 跑 pstable 并拿到 stdout 文本（用于断言本地字段）
function Get-PsText([string[]]$psArgs) {
    $prev = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    $out = (& go run ./cmd/pstable @psArgs 2>$null | Out-String).Trim()
    $ErrorActionPreference = $prev
    return $out
}

function Get-GoVersion {
    $prev = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    $out = (& go version 2>&1 | Out-String).Trim()
    $code = $LASTEXITCODE
    $ErrorActionPreference = $prev
    return @{ Out = $out; Code = $code }
}

function Ensure-Go {
    $r = Get-GoVersion
    if ($r.Code -eq 0 -and $r.Out -notmatch 'cannot find GOROOT') { return $r.Out }
    foreach ($cand in @($env:GOROOT, 'D:\go', (Join-Path $env:LOCALAPPDATA 'Programs\Go'), 'C:\Go', 'C:\Program Files\Go')) {
        if ($cand -and (Test-Path (Join-Path $cand 'bin\go.exe')) -and (Test-Path (Join-Path $cand 'src\runtime'))) {
            $env:GOROOT = $cand
            $bin = Join-Path $cand 'bin'
            if (($env:PATH -split ';') -notcontains $bin) { $env:PATH = $bin + ';' + $env:PATH }
            Write-Host "  GOROOT 自动修正为 $cand" -ForegroundColor Yellow
            $r = Get-GoVersion
            if ($r.Code -eq 0) { return $r.Out }
        }
    }
    Write-Host "  go 不可用：$($r.Out)" -ForegroundColor Red
    Write-Host "  先执行：. .\scripts\go-env.ps1 -FixProfile" -ForegroundColor Red
    exit 1
}

# 取 token（给 REST 断言用）
function Get-Token {
    $body = @{ box_code = $Box; org_code = $Org } | ConvertTo-Json
    $resp = Invoke-RestMethod -Method Post -Uri $Rpc -ContentType 'application/json' -Body $body
    return $resp.token
}

# 从后端读一行（id 精确匹配）；不存在返回 $null
function Get-BackendRow([string]$id) {
    $uri = "$Rest/$Table" + '?id=eq.' + [uri]::EscapeDataString($id)
    $rows = Invoke-RestMethod -Method Get -Uri $uri -Headers @{ Authorization = "Bearer $script:token" }
    if ($null -eq $rows) { return $null }
    if ($rows -is [System.Array]) { if ($rows.Count -eq 0) { return $null } return $rows[0] }
    return $rows
}

# 等待本地达到预期（写入后服务端会回显，可能短暂用旧快照覆盖本地）
function Wait-Local([string]$sql, [string]$pattern, [int]$timeoutMs = 8000) {
    $deadline = (Get-Date).AddMilliseconds($timeoutMs)
    $last = ''
    while ((Get-Date) -lt $deadline) {
        $last = Get-PsText (@('-q', $sql, '-format', 'csv') + $common)
        if ($last -match $pattern) { return $true }
        Start-Sleep -Milliseconds 150
    }
    Write-Host ("    （最后一次读到：{0}）" -f ($last -replace "`r?`n", ' | ')) -ForegroundColor DarkGray
    return $false
}

$common = @('-db', $Database, '-schema', $Schema,
    '-endpoint', $Endpoint, '-rest', $Rest, '-rpc', $Rpc, '-box', $Box, '-org', $Org)

try {
    Section "0/4 环境检查"
    Ok (Ensure-Go)
    if (-not (Test-Path $Schema)) { throw "找不到 schema: $Schema" }
    Ok "schema: $Schema"
    $script:token = Get-Token
    Ok "已用 RPC 取到 token（后端断言用）"

    # ---------------------------------------------------------------- 1
    if (-not $SkipDownload) {
        Section "1/4 下载：后台 → 本地（全量同步 + 逐表校验）"
        if (-not $KeepDb) {
            Remove-Item -ErrorAction SilentlyContinue "$Database", "$Database-wal", "$Database-shm", "$Database.lock"
        }
        $code = Run-Ps (@('-sync', '-verify-all') + $common)
        if ($code -ne 0) { Bad "下载/校验失败（退出码 $code）" } else { Ok "下载完成（详见上面的逐表结果）" }
    } else {
        Section "1/4 下载（已跳过）"
    }

    # ---------------------------------------------------------------- 2
    Section "2/4 新增：本地 INSERT → 自动上传"
    $id = [guid]::NewGuid().ToString()
    $sql = "INSERT INTO ""$Table""(id, org_code, box_code, box_cell, box_type, user_id, user_name, error_date, error_desc, trans_type, op_status) " +
    "VALUES('$id', '$Org', '$Box', 'A1', '1', 'u1', 'verify-sync', '2026-01-01 00:00:00', '双向同步校验', 'PROBE', 'PROBE');"
    $code = Run-Ps (@('-upload', '-verify-table', $Table, '-exec', $sql) + $common)
    if ($code -ne 0) { Bad "新增上传失败" }
    $row = Get-BackendRow $id
    if ($row -and $row.error_desc -eq '双向同步校验') { Ok "后端已有该行（id=$id, error_desc=双向同步校验）" }
    else { Bad "后端查不到刚插入的行（id=$id）" }

    # ---------------------------------------------------------------- 3
    Section "3/4 修改：本地 UPDATE → 自动上传"
    $sql = "UPDATE ""$Table"" SET op_status='UPDATED', error_desc='双向同步校验-已改' WHERE id='$id';"
    $code = Run-Ps (@('-upload', '-verify-table', $Table, '-exec', $sql) + $common)
    if ($code -ne 0) { Bad "修改上传失败" }
    $row = Get-BackendRow $id
    if ($row -and $row.op_status -eq 'UPDATED') { Ok "后端字段已更新（op_status=UPDATED）" }
    else { Bad "后端字段没更新：$($row | ConvertTo-Json -Compress)" }

    # 本地也要一致（写后服务端会回显，等它收敛）
    $localSQL = "SELECT op_status FROM ""$Table"" WHERE id='$id'"
    if (Wait-Local $localSQL 'UPDATED') { Ok "本地也是 UPDATED" } else { Bad "本地字段没更新（等了 8s）" }

    # ---------------------------------------------------------------- 4
    if ($KeepRow) {
        Section "4/4 保留探针行（-KeepRow）"
        Info "后台应能看到 id=$id"
        Info "删除它：.\scripts\ps-write.ps1 -Database $Database -Action delete -Table $Table -Id $id"
    } else {
        Section "4/4 删除：本地 DELETE → 自动上传"
        $sql = "DELETE FROM ""$Table"" WHERE id='$id';"
        $code = Run-Ps (@('-upload', '-verify-table', $Table, '-exec', $sql) + $common)
        if ($code -ne 0) { Bad "删除上传失败" }
        if (Get-BackendRow $id) { Bad "后端仍有该行（删除没有同步）" } else { Ok "后端已无该行" }
        $cntSQL = "SELECT count(*) AS n FROM ""$Table"" WHERE id='$id'"
        if (Wait-Local $cntSQL '(?m)^0\s*$') { Ok "本地也已删除" } else { Bad "本地仍有该行（等 8s 后）" }
    }

    # ---------------------------------------------------------------- 汇总
    Section "结果"
    if ($script:failed.Count -eq 0) {
        Write-Host "  ✅ 双向同步校验通过：下载 / 新增 / 修改 / 删除" -ForegroundColor Green
        exit 0
    }
    Write-Host "  ❌ 有 $($script:failed.Count) 项失败：" -ForegroundColor Red
    $script:failed | ForEach-Object { Write-Host "     - $_" -ForegroundColor Red }
    exit 1
}
finally {
    Pop-Location
}
