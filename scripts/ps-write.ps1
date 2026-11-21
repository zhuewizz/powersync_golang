<#
.SYNOPSIS
    本地优先一键写操作：insert / update / delete / query / list / exec，
    自动上传到后端，并核对前后端行数。

.DESCRIPTION
    本脚本只是 cmd/pstable 的薄封装：
      本地 SQLite 立即生效（离线也能用）→ 写入进 ps_crud 上传队列 →
      带 -upload 时在退出前自动同步到 PostgREST → 打印前后端行数核对结果。

    所有参数都有默认值（指向测试环境），可用环境变量覆盖：
      PS_URL / REST_URL / RPC_URL / BOX_CODE / ORG_CODE / PS_DB

.EXAMPLE
    .\scripts\ps-write.ps1 -Action insert -Table sys_error_report `
        -Values @{ org_code='ORG001'; box_code='D1-2'; box_cell='A1';
                   user_id='u1'; user_name='ps-write'; error_date='2026-01-01 00:00:00';
                   error_desc='ps-write 插入演示'; trans_type='PROBE'; op_status='PROBE'; box_type='1' }

.EXAMPLE
    .\scripts\ps-write.ps1 -Action update -Table sys_error_report -Id $id -Values @{ error_desc='改过了' }

.EXAMPLE
    .\scripts\ps-write.ps1 -Action delete -Table sys_error_report -Id $id

.EXAMPLE
    .\scripts\ps-write.ps1 -Action query -Sql "select count(*) as n from cutter_info"

.EXAMPLE
    .\scripts\ps-write.ps1 -Action insert -Table sys_error_report -Values @{...} -NoUpload
#>
[CmdletBinding()]
param(
    [ValidateSet('insert', 'update', 'delete', 'query', 'list', 'exec')]
    [string]$Action = 'query',

    [string]$Table,
    [string]$Id,
    [object]$Values,
    [string]$Sql,

    [string]$Database = $(if ($env:PS_DB) { $env:PS_DB } else { './out/verify.db' }),

    [string]$Endpoint = $(if ($env:PS_URL) { $env:PS_URL } else { 'http://120.24.7.22:8080' }),
    [string]$Rest = $(if ($env:REST_URL) { $env:REST_URL } else { 'http://120.24.7.22:8089/rest' }),
    [string]$Rpc = $(if ($env:RPC_URL) { $env:RPC_URL } else { 'http://120.24.7.22:8089/rest/rpc/fetch_credentials' }),
    [string]$Box = $(if ($env:BOX_CODE) { $env:BOX_CODE } else { 'D1-2' }),
    [string]$Org = $(if ($env:ORG_CODE) { $env:ORG_CODE } else { 'ORG001' }),

    [switch]$NoUpload,
    [switch]$DryRun,
    [switch]$NoVerify,
    [switch]$ShowSql
)

$ErrorActionPreference = 'Stop'

# 控制台按 UTF-8 解码 go 的输出（否则 PS 5.1 下中文会变乱码）
try { [Console]::OutputEncoding = [System.Text.Encoding]::UTF8 } catch { }
$OutputEncoding = [System.Text.Encoding]::UTF8

# go 的 GOROOT 有时被版本管理器（如 g）改坏，这里做一次兜底自检/自动修复。
function Resolve-Go {
    $prev = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    $out = ''
    try { $out = (& go env GOROOT 2>&1 | Out-String).Trim() } catch { $out = '' }
    $code = $LASTEXITCODE
    $ErrorActionPreference = $prev
    if ($code -eq 0 -and $out -and (Test-Path $out)) { return }
    foreach ($cand in @('D:\go', 'C:\Go', 'D:\Program Files\Go', $env:GOROOT)) {
        if ($cand -and (Test-Path (Join-Path $cand 'bin\go.exe'))) {
            $env:GOROOT = $cand
            $env:PATH = (Join-Path $cand 'bin') + ';' + $env:PATH
            Write-Host "提示: 已自动设置 GOROOT=$cand" -ForegroundColor Yellow
            return
        }
    }
    throw ('go 不可用（GOROOT=' + $out + '）。请手动设置：$env:GOROOT="D:\go"; $env:PATH="$env:GOROOT\bin;$env:PATH"')
}

function Quote-Ident([string]$name) {
    return '"' + $name.Replace('"', '""') + '"'
}

# ConvertTo-ValuesTable 兼容两种传法：
#   -Values @{ org_code='ORG001'; box_type='1' }  （PowerShell 里直接写 hashtable，推荐）
#   -Values "org_code=ORG001;box_type=1"              （cmd / bash 传字符串，便于脚本化）
function ConvertTo-ValuesTable($v) {
    if ($null -eq $v) { return @{} }
    if ($v -is [System.Collections.IDictionary]) { return $v }
    $h = @{}
    foreach ($pair in ([string]$v -split '[;,]')) {
        if ([string]::IsNullOrWhiteSpace($pair)) { continue }
        $i = $pair.IndexOf('=')
        if ($i -lt 1) { throw "无法解析 -Values 项: $pair（应为 列=值）" }
        $k = $pair.Substring(0, $i).Trim()
        $val = $pair.Substring($i + 1).Trim()
        if ($val.Length -ge 2) {
            $fc = $val[0]; $lc = $val[$val.Length - 1]
            if (($fc -eq '"' -and $lc -eq '"') -or ($fc -eq "'" -and $lc -eq "'")) {
                $val = $val.Substring(1, $val.Length - 2)
            }
        }
        $h[$k] = $val
    }
    return $h
}

function Test-Key($h, [string]$k) {
    if ($h -is [System.Collections.Specialized.OrderedDictionary]) { return $h.Contains($k) }
    return $h.ContainsKey($k)
}

function ConvertTo-SqlLiteral($v) {
    if ($null -eq $v) { return 'NULL' }
    if ($v -is [bool]) { if ($v) { return '1' } else { return '0' } }
    if ($v -is [int] -or $v -is [long] -or $v -is [int16] -or $v -is [byte] -or
        $v -is [double] -or $v -is [single] -or $v -is [decimal]) {
        return [string]::Format([System.Globalization.CultureInfo]::InvariantCulture, '{0}', $v)
    }
    if ($v -is [datetime]) {
        return "'" + $v.ToString('yyyy-MM-dd HH:mm:ss') + "'"
    }
    return "'" + ([string]$v).Replace("'", "''") + "'"
}

$repo = Split-Path -Parent $PSScriptRoot
if (-not (Test-Path (Join-Path $repo 'go.mod'))) {
    # 允许从仓库根目录执行：.\scripts\ps-write.ps1
    $repo = (Get-Location).Path
}
Push-Location $repo
$Db = $Database
Resolve-Go
try {
    if ($Action -in @('insert', 'update', 'delete', 'exec') -and -not (Test-Path $Db)) {
        throw "本地库不存在: $Db（先跑一次 examples/todo，或用 -Database 指定）"
    }
    if ($Action -in @('insert', 'update', 'delete') -and [string]::IsNullOrWhiteSpace($Table)) {
        throw "-Action $Action 需要 -Table"
    }

    $Values = ConvertTo-ValuesTable $Values
    $statements = @()
    $insertId = $Id

    switch ($Action) {
        'insert' {
            if ($null -eq $Values -or $Values.Count -eq 0) { throw '-Action insert 需要 -Values @{列=值; ...}' }
            $cols = New-Object System.Collections.ArrayList
            $vals = New-Object System.Collections.ArrayList

            if ([string]::IsNullOrWhiteSpace($insertId) -and -not (Test-Key $Values 'id')) {
                $insertId = [guid]::NewGuid().ToString()
            }
            if (-not [string]::IsNullOrWhiteSpace($insertId) -and -not (Test-Key $Values 'id')) {
                [void]$cols.Add('id')
                [void]$vals.Add((ConvertTo-SqlLiteral $insertId))
            }
            foreach ($k in $Values.Keys) {
                [void]$cols.Add((Quote-Ident $k))
                [void]$vals.Add((ConvertTo-SqlLiteral $Values[$k]))
            }
            $statements += "INSERT INTO $(Quote-Ident $Table)($($cols -join ', ')) VALUES($($vals -join ', '));"
        }
        'update' {
            if (-not [string]::IsNullOrWhiteSpace($Sql)) {
                $statements += $Sql
                break
            }
            if ([string]::IsNullOrWhiteSpace($Id)) { throw '-Action update 需要 -Id（或用 -Sql 自己写 WHERE）' }
            if ($null -eq $Values -or $Values.Count -eq 0) { throw '-Action update 需要 -Values @{列=新值; ...}' }
            $sets = @()
            foreach ($k in $Values.Keys) { $sets += "$(Quote-Ident $k) = $(ConvertTo-SqlLiteral $Values[$k])" }
            $statements += "UPDATE $(Quote-Ident $Table) SET $($sets -join ', ') WHERE id = $(ConvertTo-SqlLiteral $Id);"
        }
        'delete' {
            if ([string]::IsNullOrWhiteSpace($Sql)) {
                if ([string]::IsNullOrWhiteSpace($Id)) { throw '-Action delete 需要 -Id（或 -Sql 自己写 WHERE）' }
                $statements += "DELETE FROM $(Quote-Ident $Table) WHERE id = $(ConvertTo-SqlLiteral $Id);"
            }
            else {
                $statements += $Sql
            }
        }
        'exec' {
            if ([string]::IsNullOrWhiteSpace($Sql)) { throw '-Action exec 需要 -Sql' }
            $statements += $Sql
        }
        'query' {
            if ([string]::IsNullOrWhiteSpace($Sql)) {
                if ([string]::IsNullOrWhiteSpace($Table)) { throw '-Action query 需要 -Sql 或 -Table' }
                $Sql = "SELECT * FROM $(Quote-Ident $Table) LIMIT 20;"
            }
            $statements += $Sql
        }
        'list' { }
    }

    $upload = (-not $NoUpload) -and (-not $DryRun) -and ($Action -in @('insert', 'update', 'delete', 'exec'))
    $psArgs = @('run', './cmd/pstable', '-db', $Db)
    if ($upload) {
        $psArgs += @('-upload', '-endpoint', $Endpoint, '-rest', $Rest, '-rpc', $Rpc, '-box', $Box, '-org', $Org)
        if ((-not $NoVerify) -and (-not [string]::IsNullOrWhiteSpace($Table))) {
            $psArgs += @('-verify-table', $Table)
        }
    }
    if ($DryRun) { $psArgs += '-dry-run' }

    switch ($Action) {
        'list' {
            $psArgs += '-ls'
            & go @psArgs
        }
        'query' {
            $psArgs += @('-q', $statements[0])
            & go @psArgs
        }
        default {
            $sql = ($statements -join "`n").TrimEnd()
            if (-not $sql.EndsWith(';')) { $sql += ';' }
            if ($ShowSql -or $DryRun) { Write-Host "SQL:`n$sql" -ForegroundColor DarkGray }
            $psArgs += '-i'
            $sql | & go @psArgs
        }
    }
    if ($LASTEXITCODE -ne 0) { throw "pstable 退出码 $LASTEXITCODE" }

    if ($Action -eq 'insert' -and -not [string]::IsNullOrWhiteSpace($insertId)) {
        Write-Host "新行 id: $insertId" -ForegroundColor Cyan
        if ($upload) {
            Write-Host "删除它: .\scripts\ps-write.ps1 -Action delete -Table $Table -Id $insertId" -ForegroundColor DarkGray
        }
        else {
            Write-Host "(仅写本地；联网后由 App 自动上传)" -ForegroundColor DarkYellow
        }
    }
}
finally {
    Pop-Location
}
