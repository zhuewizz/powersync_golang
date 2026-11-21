# 在 PowerShell 里修正 go 的 GOROOT（应对被 `g`/安装器改坏的环境变量）。
#
# 用法（点号前缀才会作用于当前会话）：
#     . .\scripts\go-env.ps1
#     go version
#
# 如果 go 报 "cannot find GOROOT directory: C:\Users\<你>\.g\go"，
# 就跑这个脚本；只想永久修复的话执行 docs/README 里的
# [Environment]::SetEnvironmentVariable('GOROOT','D:\go','User') 然后重开终端。

try { [Console]::OutputEncoding = [System.Text.Encoding]::UTF8 } catch { }
$OutputEncoding = [System.Text.Encoding]::UTF8

$candidates = @(
    (Join-Path $env:LOCALAPPDATA 'Programs\Go'),
    'D:\go',
    'C:\Go',
    'C:\Program Files\Go',
    'C:\Program Files (x86)\Go',
    $env:GOROOT
)

function Test-GoRoot([string]$p) {
    return $p -and (Test-Path (Join-Path $p 'bin\go.exe')) -and (Test-Path (Join-Path $p 'src\runtime'))
}

$found = $null
foreach ($c in $candidates) {
    if (Test-GoRoot $c) { $found = $c; break }
}

if (-not $found) {
    Write-Host "没找到可用的 Go 安装目录（试过：$($candidates -join ', ')）" -ForegroundColor Red
    Write-Host "请手动指定：`$env:GOROOT='D:\go'; `$env:PATH=\\"D:\go\bin;`$env:PATH\\"" -ForegroundColor Yellow
    return
}

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

Write-Host ("go version: " + (& (Join-Path $goBin 'go.exe') version)) -ForegroundColor Green
Write-Host @'
提示：本脚本只影响当前 PowerShell 会话；要永久修复，执行
  [Environment]::SetEnvironmentVariable('GOROOT','D:\go','User')
然后重开终端（或删掉这个坏变量：SetEnvironmentVariable('GOROOT',$null,'User')）。
'@ -ForegroundColor DarkGray
