# ==========================================================
# 01_functions.ps1
# 計測関数定義（リハーサルで動作確認済み、決定13対応版）
# ==========================================================
# 使い方: 00_setup.ps1 のあとにドットソース実行
#   . .\evidence\20260612\01_functions.ps1

function Remove-TargetSafe {
    param([string]$TargetPath)
    if (Test-Path $TargetPath) {
        Remove-Item -Recurse -Force $TargetPath -ErrorAction SilentlyContinue
    }
}

function Wait-ColdState {
    param([int]$Seconds = 70)
    Write-Host "  Waiting $Seconds sec for cold state..." -ForegroundColor DarkGray
    Start-Sleep -Seconds $Seconds
}

function Invoke-Measurement {
    param(
        [string]$Engine,           # "core" or "fusion"
        [string]$Command,          # "parse" / "compile" / "run" / "test" / "docs generate"
        [string]$State,            # "cold" or "warm"
        [int]$Iteration,
        [string]$TargetPath,       # "target_core" or "target_fusion"
        [string]$LogDir,
        [string]$CsvPath,
        [switch]$PreRunForTestCold # test cold 用: 計測前に run を1回（記録外）
    )

    $cmdExe = if ($Engine -eq "core") { "dbt" } else { "dbtf" }

    # ログファイル名（空白はアンダースコアに）
    $cmdSafe = $Command -replace " ", "_"
    $logFile = "{0}\{1}_{2}_{3}_iter{4}.log" -f $LogDir, $Engine, $cmdSafe, $State, $Iteration
    $timestampJst = Get-Date -Format "yyyy-MM-dd HH:mm:ss zzz"

    Write-Host ""
    Write-Host "=== [$Engine / $Command / $State / iter$Iteration] $timestampJst ===" -ForegroundColor Cyan

    # 決定13対応: test cold は run（記録外）→ test（記録対象）
    if ($PreRunForTestCold -and $Command -eq "test") {
        Write-Host "  Pre-run (not recorded) for test cold..." -ForegroundColor DarkGray
        & $cmdExe run --target-path $TargetPath *>&1 | Out-Null
    }

    # 計測対象コマンド実行
    $startTime = Get-Date
    if ($Command -eq "docs generate") {
        & $cmdExe docs generate --target-path $TargetPath *>&1 | Tee-Object -FilePath $logFile | Out-Null
    } else {
        & $cmdExe $Command --target-path $TargetPath *>&1 | Tee-Object -FilePath $logFile | Out-Null
    }
    $exitCode = $LASTEXITCODE
    $endTime = Get-Date
    $duration = ($endTime - $startTime).TotalSeconds
    $durationFormatted = [math]::Round($duration, 3)

    # CSV追記
    $csvLine = "{0},{1},{2},{3},{4},{5},{6},{7}" -f `
        $timestampJst, $Engine, $Command, $State, $Iteration, $durationFormatted, $exitCode, (Split-Path $logFile -Leaf)
    $csvLine | Add-Content -Path $CsvPath -Encoding UTF8

    $color = if ($exitCode -eq 0) { "Green" } else { "Red" }
    Write-Host ("  -> {0} sec / exit_code={1}" -f $durationFormatted, $exitCode) -ForegroundColor $color

    return [PSCustomObject]@{
        Duration = $duration
        ExitCode = $exitCode
        LogFile  = $logFile
    }
}

Write-Host "Functions loaded: Invoke-Measurement, Remove-TargetSafe, Wait-ColdState" -ForegroundColor Green
