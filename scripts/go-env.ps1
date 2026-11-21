# 修正 PowerShell 里的 Go 环境（GOROOT / PATH）。
#
# 典型症状（本机就是这样）：
#   go: cannot find GOROOT directory: C:\Users\<你>\.g\go
# 原因：PowerShell 的 profile 里有一行
#   $env:GOROOT="$HOME\.g\go"
# 但 `g` 管理的那个目录并不存在（g 没装好/被删了），于是每个新窗口的 GOROOT 都是坏路径。
#
# 用法：
#   . .\scripts\go-env.ps1                 # 只在当前会话修好
#   . .\scripts\go-env.ps1 -FixProfile     # 永久修：备份并改写 PowerShell profile
#   go version
#
# 想自己手动改，把 profile 里那两行换成：
#   if (Test-Path "$HOME\.g\go\src\runtime") { $env:GOROOT = "$HOME\.g\go" }
#   if (-not $env:GOROOT -or -not (Test-Path "$env:GOROOT\src\runtime")) { $env:GOROOT = 'D:\go' }
#   $env:Path = "$env:GOROOT\bin;$env:Path"
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
$goBin = Join-Path $found 'bin'
if (($env:PATH -split ';') -notcontains $goBin) {
    $env:PATH = $goBin + ';' + $env:PATH
}
# 顺手把坏的 $HOME\.g\bin 之类从 PATH 里摘掉（它指向不存在的 go）
$env:PATH = (($env:PATH -split ';') | Where-Object { $_ -and -not ($_ -match '\\\.g\\') }) -join ';'

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
        if ($line -match '^\s*\$env:(GOROOT|Path)\s*=') {
            $out.Add("# [go-env.ps1 $stamp 注释掉] " + $line)
            $changed++
        } else {
            $out.Add($line)
        }
    }
    $out.Add("")
    $out.Add("# --- 由 scripts/go-env.ps1 修正（$stamp）：优先用能跑的 Go，g 的目录不存在时回退 ---")
    $out.Add('if (Test-Path "$HOME\.g\go\src\runtime") { $env:GOROOT = "$HOME\.g\go" }')
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
