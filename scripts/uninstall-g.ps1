# 卸载 g（Go 版本管理器，E:\gvm\g.exe）。
#
# 本机现状（脚本会自己核对）：
#   - 可执行文件：E:\gvm\g.exe（在 Machine PATH 的 E:\gvm 里）
#   - 数据目录 ：C:\Users\<你>\.g（downloads / versions，空）
#   - 坏就坏在 g 把 GOROOT 指到 %USERPROFILE%\.g\go，而这个目录已经不存在了
#   - 真正在用的 Go 是 D:\go（User 环境变量 GOROOT=D:\go）
#
# 用法（建议以管理员身份运行，否则改不了 Machine PATH）：
#   powershell -ExecutionPolicy Bypass -File scripts\uninstall-g.ps1 -WhatIf   # 只预览
#   powershell -ExecutionPolicy Bypass -File scripts\uninstall-g.ps1           # 真删
[CmdletBinding(SupportsShouldProcess = $true)]
param(
    [string[]]$ExtraDirs = @(),   # 想额外删除的目录
    [switch]$KeepDirs             # 只清 PATH/profile，不删目录
)

$ErrorActionPreference = 'Continue'
try { [Console]::OutputEncoding = [System.Text.Encoding]::UTF8 } catch { }

$log = New-Object System.Collections.Generic.List[string]
function Say([string]$msg, [string]$color = 'Gray') {
    Write-Host $msg -ForegroundColor $color
    $script:log.Add($msg)
}

$stamp = Get-Date -Format 'yyyyMMdd-HHmmss'
$gDir = Join-Path $env:USERPROFILE '.g'
$gvmDir = 'E:\gvm'
$targets = @($gvmDir, $gDir) + $ExtraDirs

# ---------------------------------------------------------------- 0. 备份
$backupDir = Join-Path (Split-Path -Parent $PSScriptRoot) 'out'
New-Item -ItemType Directory -Force -Path $backupDir | Out-Null
$backup = Join-Path $backupDir "g-uninstall-backup-$stamp.txt"

$prof = $PROFILE.CurrentUserCurrentHost
$sb = New-Object System.Text.StringBuilder
[void]$sb.AppendLine("# g 卸载前的环境备份 $stamp")
[void]$sb.AppendLine("USER_PATH=$([Environment]::GetEnvironmentVariable('PATH','User'))")
[void]$sb.AppendLine("MACHINE_PATH=$([Environment]::GetEnvironmentVariable('PATH','Machine'))")
[void]$sb.AppendLine("GOROOT_USER=$([Environment]::GetEnvironmentVariable('GOROOT','User'))")
[void]$sb.AppendLine("GOPATH_USER=$([Environment]::GetEnvironmentVariable('GOPATH','User'))")
foreach ($d in $targets) {
    if (Test-Path $d) {
        [void]$sb.AppendLine("DIR $d : $((Get-ChildItem $d -Force -Recurse -ErrorAction SilentlyContinue | Measure-Object).Count) 项")
    }
}
if (Test-Path $prof) {
    [void]$sb.AppendLine("--- profile: $prof ---")
    [void]$sb.AppendLine((Get-Content $prof -Raw))
}
[System.IO.File]::WriteAllText($backup, $sb.ToString(), (New-Object System.Text.UTF8Encoding($true)))
Say "备份已写入: $backup" 'DarkGray'

# ---------------------------------------------------------------- 1. 清理 PATH
function Clean-Path([string]$scope) {
    $cur = [Environment]::GetEnvironmentVariable('PATH', $scope)
    if (-not $cur) { return }
    $items = $cur -split ';'
    $keep = @($items | Where-Object {
            $_ -and -not ($_ -match '(?i)\\gvm\\?$' -or $_ -match '(?i)\.g[\\/](bin|go)')
        })
    $removed = @($items | Where-Object { $_ -and ($keep -notcontains $_) })
    if ($removed.Count -eq 0) {
        Say "$scope PATH: 无需改动" 'DarkGray'
        return
    }
    Say "$scope PATH 将移除：" 'Yellow'
    $removed | ForEach-Object { Say "    $_" 'Yellow' }
    if ($PSCmdlet.ShouldProcess("$scope PATH", "移除 $($removed.Count) 项")) {
        try {
            [Environment]::SetEnvironmentVariable('PATH', ($keep -join ';'), $scope)
            Say "$scope PATH: 已更新（移除 $($removed.Count) 项）" 'Green'
        } catch {
            Say "$scope PATH: 修改失败（需要管理员权限）: $($_.Exception.Message)" 'Red'
            Say "  手动处理：系统属性 → 环境变量 → 编辑 PATH → 删除上列条目" 'Red'
        }
    }
}
Clean-Path 'User'
Clean-Path 'Machine'

# ---------------------------------------------------------------- 2. 清理 profile
if (Test-Path $prof) {
    $lines = Get-Content $prof
    $filtered = @($lines | Where-Object {
            -not ($_ -match '(?i)\.g\\go' -or $_ -match '(?i)\.g\\bin' -or
                $_ -match '(?i)GOROOT\s*=\s*"?\$HOME\\\.g' -or
                $_ -match 'go-env\.ps1 .*注释掉')
        })
    if ($filtered.Count -ne $lines.Count) {
        Say "profile: 移除 $($lines.Count - $filtered.Count) 行 g 相关配置" 'Yellow'
        if ($PSCmdlet.ShouldProcess($prof, '改写 profile')) {
            try {
                [System.IO.File]::WriteAllLines($prof, $filtered, (New-Object System.Text.UTF8Encoding($true)))
                Say "profile: 已更新 $prof" 'Green'
            } catch {
                Say "profile: 改写失败: $($_.Exception.Message)" 'Red'
            }
        }
    } else {
        Say "profile: 没有 g 相关行" 'DarkGray'
    }
}

# ---------------------------------------------------------------- 3. 删除目录
if (-not $KeepDirs) {
    foreach ($d in $targets) {
        if (-not (Test-Path $d)) { continue }
        if ($PSCmdlet.ShouldProcess($d, '删除目录')) {
            try {
                Remove-Item -LiteralPath $d -Recurse -Force -ErrorAction Stop
                Say "已删除目录: $d" 'Green'
            } catch {
                Say "删除失败 $d : $($_.Exception.Message)" 'Red'
                Say "  （g.exe 可能正在被占用；也可能需要管理员权限）" 'Red'
            }
        }
    }
} else {
    Say "-KeepDirs：保留目录未删" 'DarkGray'
}

# ---------------------------------------------------------------- 4. 校验
Say ""
Say "=== 卸载后校验（当前会话可能仍残留旧 PATH，重开窗口才是最终结果）===" 'Cyan'
foreach ($c in @('g', 'go')) {
    $cmd = Get-Command $c -ErrorAction SilentlyContinue
    if ($cmd) { Say ("  {0} -> {1}" -f $c, $cmd.Source) } else { Say ("  {0} -> (未找到)" -f $c) }
}
Say ("  GOROOT      = " + $env:GOROOT)
Say ("  go env GOROOT = " + (& go env GOROOT 2>&1))
Say ("  go version  = " + (& go version 2>&1))
Say ""
Say "提示：重开一个 PowerShell 窗口后再执行 go version 确认；PATH 里删掉 E:\gvm 后 g 命令就没了。" 'DarkGray'
Say "备份文件: $backup" 'DarkGray'
