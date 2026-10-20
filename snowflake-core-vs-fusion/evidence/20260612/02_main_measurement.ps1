# ==========================================================
# 02_main_measurement.ps1
# 本計測本体（86回、約1時間20分〜1時間40分）
# ==========================================================
# 使い方: 00_setup.ps1 と 01_functions.ps1 をドットソース後に実行
#   . .\evidence\20260612\00_setup.ps1
#   . .\evidence\20260612\01_functions.ps1
#   . .\evidence\20260612\02_main_measurement.ps1

$measurementStart = Get-Date
Write-Host ""
Write-Host "==============================================" -ForegroundColor Magenta
Write-Host "  MAIN MEASUREMENT START" -ForegroundColor Magenta
Write-Host ("  $($measurementStart.ToString('yyyy-MM-dd HH:mm:ss zzz'))") -ForegroundColor Magenta
Write-Host "==============================================" -ForegroundColor Magenta

# === 設定 ===
$commands = @("parse", "compile", "run", "test")
$targetCore = "target_core"
$targetFusion = "target_fusion"
$totalCount = 0

# === プリフライト: --target-path が両エンジンで動くか確認 ===
Write-Host ""
Write-Host "=== Pre-flight: --target-path support ===" -ForegroundColor Cyan
Remove-TargetSafe -TargetPath $targetCore
Remove-TargetSafe -TargetPath $targetFusion

Write-Host "  Testing dbt (core) parse --target-path target_core..." -ForegroundColor DarkGray
& dbt parse --target-path $targetCore *>&1 | Out-Null
$preCore = $LASTEXITCODE

Write-Host "  Testing dbtf (fusion) parse --target-path target_fusion..." -ForegroundColor DarkGray
& dbtf parse --target-path $targetFusion *>&1 | Out-Null
$preFusion = $LASTEXITCODE

if ($preCore -ne 0 -or $preFusion -ne 0) {
    Write-Host "Pre-flight FAILED: core=$preCore, fusion=$preFusion" -ForegroundColor Red
    Write-Host "Aborting before any recorded measurement." -ForegroundColor Red
    return
}
Write-Host "Pre-flight OK (both engines support --target-path)" -ForegroundColor Green

# === parse / compile / run / test の cold + warm ===
foreach ($cmd in $commands) {
    Write-Host ""
    Write-Host "######################################################" -ForegroundColor Yellow
    Write-Host "## Command: $cmd" -ForegroundColor Yellow
    Write-Host "######################################################" -ForegroundColor Yellow

    # ----- COLD phase: 5 iter, alternating Core/Fusion -----
    Write-Host ""
    Write-Host ">>> COLD phase ($cmd)" -ForegroundColor Cyan
    for ($iter = 1; $iter -le 5; $iter++) {
        # Core cold
        Remove-TargetSafe -TargetPath $targetCore
        Wait-ColdState -Seconds 70
        if ($cmd -eq "test") {
            Invoke-Measurement -Engine "core" -Command $cmd -State "cold" -Iteration $iter `
                -TargetPath $targetCore -LogDir $global:logDir -CsvPath $global:csvPath `
                -PreRunForTestCold | Out-Null
        } else {
            Invoke-Measurement -Engine "core" -Command $cmd -State "cold" -Iteration $iter `
                -TargetPath $targetCore -LogDir $global:logDir -CsvPath $global:csvPath | Out-Null
        }
        $totalCount++

        # Fusion cold
        Remove-TargetSafe -TargetPath $targetFusion
        Wait-ColdState -Seconds 70
        if ($cmd -eq "test") {
            Invoke-Measurement -Engine "fusion" -Command $cmd -State "cold" -Iteration $iter `
                -TargetPath $targetFusion -LogDir $global:logDir -CsvPath $global:csvPath `
                -PreRunForTestCold | Out-Null
        } else {
            Invoke-Measurement -Engine "fusion" -Command $cmd -State "cold" -Iteration $iter `
                -TargetPath $targetFusion -LogDir $global:logDir -CsvPath $global:csvPath | Out-Null
        }
        $totalCount++
    }

    # ----- Warmup (記録対象外): Core/Fusion両方を1回回す -----
    Write-Host ""
    Write-Host ">>> Warmup for $cmd (not recorded)" -ForegroundColor DarkGray
    if ($cmd -eq "test") {
        # test の warmup には run も必要
        & dbt run --target-path $targetCore *>&1 | Out-Null
        & dbtf run --target-path $targetFusion *>&1 | Out-Null
        & dbt test --target-path $targetCore *>&1 | Out-Null
        & dbtf test --target-path $targetFusion *>&1 | Out-Null
    } else {
        & dbt $cmd --target-path $targetCore *>&1 | Out-Null
        & dbtf $cmd --target-path $targetFusion *>&1 | Out-Null
    }

    # ----- WARM phase: 5 iter, alternating Core/Fusion -----
    Write-Host ""
    Write-Host ">>> WARM phase ($cmd)" -ForegroundColor Cyan
    for ($iter = 1; $iter -le 5; $iter++) {
        Invoke-Measurement -Engine "core" -Command $cmd -State "warm" -Iteration $iter `
            -TargetPath $targetCore -LogDir $global:logDir -CsvPath $global:csvPath | Out-Null
        $totalCount++
        Invoke-Measurement -Engine "fusion" -Command $cmd -State "warm" -Iteration $iter `
            -TargetPath $targetFusion -LogDir $global:logDir -CsvPath $global:csvPath | Out-Null
        $totalCount++
    }
}

# === docs generate: warm のみ 3 iter (決定14) ===
Write-Host ""
Write-Host "######################################################" -ForegroundColor Yellow
Write-Host "## Command: docs generate (warm only, 3 iter)" -ForegroundColor Yellow
Write-Host "######################################################" -ForegroundColor Yellow

# Warmup
Write-Host ">>> Warmup for docs generate (not recorded)" -ForegroundColor DarkGray
& dbt docs generate --target-path $targetCore *>&1 | Out-Null
& dbtf docs generate --target-path $targetFusion *>&1 | Out-Null

# WARM phase: 3 iter, alternating
Write-Host ""
Write-Host ">>> WARM phase (docs generate)" -ForegroundColor Cyan
for ($iter = 1; $iter -le 3; $iter++) {
    Invoke-Measurement -Engine "core" -Command "docs generate" -State "warm" -Iteration $iter `
        -TargetPath $targetCore -LogDir $global:logDir -CsvPath $global:csvPath | Out-Null
    $totalCount++
    Invoke-Measurement -Engine "fusion" -Command "docs generate" -State "warm" -Iteration $iter `
        -TargetPath $targetFusion -LogDir $global:logDir -CsvPath $global:csvPath | Out-Null
    $totalCount++
}

# === 完了サマリ ===
$measurementEnd = Get-Date
$elapsed = ($measurementEnd - $measurementStart).TotalMinutes

Write-Host ""
Write-Host "==============================================" -ForegroundColor Magenta
Write-Host "  MAIN MEASUREMENT COMPLETE" -ForegroundColor Magenta
Write-Host ("  End: $($measurementEnd.ToString('yyyy-MM-dd HH:mm:ss zzz'))") -ForegroundColor Magenta
Write-Host "  Total recorded measurements: $totalCount" -ForegroundColor Magenta
Write-Host ("  Elapsed: {0} minutes" -f [math]::Round($elapsed, 1)) -ForegroundColor Magenta
Write-Host "  CSV: $global:csvPath" -ForegroundColor Magenta
Write-Host "==============================================" -ForegroundColor Magenta
