# 修正 PowerShell 里的 Go 环境（GOROOT / PATH），并可永久改写 profile。
#
# 典型症状：
#   go: cannot find GOROOT directory: C:\Users\<你>\.g\go
# 原因：PowerShell profile 里写了 $env:GOROOT="$HOME\.g\go"（g 版本管理器的目录），
# 而 g 被卸载/目录不存在，于是每个新窗口的 GOROOT 都是坏路径。
#
# 用法：
#   . .\scripts\go-env.ps1                 # 只在当前会话修好
#   . .\scripts\go-env.ps1 -FixProfile     # 永久修：备份并改写 PowerShell profile
#   go version
[CmdletBinding()]
param(
    # 永久修复 PowerShell profile（会先备份为 *.bak-时间戳）
    [switch]$FixProfile,
    # 一般不用传；测试/多 profile 时可指定
    [string]$ProfilePath = $PROFILE.CurrentUserCurrentHost
)

try { [Console]::OutputEncoding = [System.Text.Encoding]::UTF8 } catch { }
$OutputEncoding = [System.Text.Encoding]::UTF8

function Test-GoRoot([string]$p) {
    return $p -and (Test-Path (Join-Path $p 'bin\go.exe')) -and (Test-Path (Join-Path $p 'src\runtime'))
}

# ---------------------------------------------------------------- 1. 找可用的 GOROOT
$candidates = @(
    $env:GOROOT,
    'D:\go',
    (Join-Path $env:LOCALAPPDATA 'Programs\Go'),
    'C:\Go',
    'C:\Program Files\Go',
    'C:\Program Files (x86)\Go'
)

$found = $null
foreach ($c in $candidates) {
    if (Test-GoRoot $c) { $found = $c; break }
}

if (-not $found) {
    Write-Host "✗ 没找到可用的 Go 安装目录（试过：$($candidates -join ', ')）" -ForegroundColor Red
    Write-Host "  请手动指定：`$env:GOROOT='D:\go'; `$env:PATH=`"D:\go\bin;`$env:PATH`"" -ForegroundColor Yellow
    return
}

# ---------------------------------------------------------------- 2. 修当前会话
if ($env:GOROOT -ne $found) {
    Write-Host "GOROOT: $($env:GOROOT) -> $found" -ForegroundColor Yellow
} else {
    Write-Host "GOROOT: $found（本来就对）" -ForegroundColor DarkGray
}
$env:GOROOT = $found

# 先把指向已卸载 g 的 PATH 项摘掉（$HOME\.g\bin、E:\gvm），再补上真正的 go bin
$env:PATH = (($env:PATH -split ';') | Where-Object {
        $_ -and -not ($_ -match 'gvm' -or $_ -match '\.g[\\/]')
    }) -join ';'
$goBin = Join-Path $found 'bin'
if (($env:PATH -split ';') -notcontains $goBin) {
    $env:PATH = $goBin + ';' + $env:PATH
}

Write-Host ("go version: " + (& (Join-Path $goBin 'go.exe') version)) -ForegroundColor Green

# ---------------------------------------------------------------- 3. 可选：永久修 profile
if ($FixProfile) {
    if (-not (Test-Path $ProfilePath)) {
        Write-Host "✗ profile 不存在：$ProfilePath" -ForegroundColor Red
        return
    }
    $stamp = Get-Date -Format 'yyyyMMdd-HHmmss'
    $backup = "$ProfilePath.bak-$stamp"
    Copy-Item $ProfilePath $backup
    Write-Host "已备份 profile -> $backup" -ForegroundColor DarkGray

    $lines = Get-Content $ProfilePath
    $out = New-Object System.Collections.Generic.List[string]
    $changed = 0
    foreach ($line in $lines) {
        # 任何写死 g 目录的 GOROOT/PATH 行都注释掉
        if ($line -match '^\s*\$env:(GOROOT|Path)\s*=' -or $line -match '\.g\\go' -or $line -match '\.g\\bin') {
            $out.Add("# [go-env.ps1 $stamp 注释掉] " + $line)
            $changed++
        } else {
            $out.Add($line)
        }
    }
    $out.Add("")
    $out.Add("# --- go-env.ps1 $stamp：GOROOT 不可用时回退到能跑的安装 ---")
    $out.Add('if (-not $env:GOROOT -or -not (Test-Path "$env:GOROOT\src\runtime")) { $env:GOROOT = ''D:\go'' }')
    $out.Add('$env:Path = "$env:GOROOT\bin;$env:Path"')
    # PowerShell 5.1 读无 BOM 的 UTF-8 会乱码，所以写 UTF-8 BOM
    $utf8Bom = New-Object System.Text.UTF8Encoding($true)
    [System.IO.File]::WriteAllLines($ProfilePath, $out, $utf8Bom)
    Write-Host "已改写 profile（注释掉 $changed 行并追加回退逻辑）：$ProfilePath" -ForegroundColor Green
    Write-Host "重开一个 PowerShell 窗口（或 . `$PROFILE）后即生效。" -ForegroundColor DarkGray
} else {
    Write-Host "（只影响当前会话；永久修复请再执行一次：. .\scripts\go-env.ps1 -FixProfile）" -ForegroundColor DarkGray
}
