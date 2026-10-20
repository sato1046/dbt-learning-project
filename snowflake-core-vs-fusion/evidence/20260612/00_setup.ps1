# ==========================================================
# 00_setup.ps1
# dbt Fusion x Snowflake 本計測 - 環境セットアップ
# ==========================================================
# 使い方: PowerShell でこのファイルをドットソース実行
#   . .\evidence\20260612\00_setup.ps1

# 1. dbt Fusion のエイリアス
Set-Alias dbtf "C:\Users\saori.sato\.local\bin\dbt.exe" -Scope Global

# 2. DBT_PROFILES_DIR 解除（BQ検証用の残骸対策）
Remove-Item Env:\DBT_PROFILES_DIR -ErrorAction SilentlyContinue

# 3. UTF-8 設定（決定6：必須）
$global:OutputEncoding = [System.Text.Encoding]::UTF8
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$env:PYTHONIOENCODING = "utf-8"

# 4. プロジェクトディレクトリへ移動
Set-Location "C:\Users\saori.sato\Documents\自主学習\dbt_learning_snowflake"

# 5. 本計測用ディレクトリ作成（実行日の日付）
$global:today = Get-Date -Format "yyyyMMdd"
$global:evidenceDir = ".\evidence\$global:today"
$global:logDir = "$global:evidenceDir\logs"
$global:csvPath = "$global:evidenceDir\results.csv"

New-Item -ItemType Directory -Force -Path $global:evidenceDir | Out-Null
New-Item -ItemType Directory -Force -Path $global:logDir | Out-Null

# 6. CSVヘッダ作成（既存なら上書きしない＝追記モード）
if (-not (Test-Path $global:csvPath)) {
    "timestamp_jst,engine,command,state,iteration,duration_sec,exit_code,log_file" |
        Out-File -FilePath $global:csvPath -Encoding UTF8
    Write-Host "CSV created: $global:csvPath" -ForegroundColor Green
} else {
    Write-Host "CSV exists: $global:csvPath (append mode)" -ForegroundColor Yellow
}

# 7. プリフライト確認
Write-Host ""
Write-Host "=== Pre-flight checks ===" -ForegroundColor Cyan
Write-Host "Today        : $global:today"
Write-Host "Evidence dir : $global:evidenceDir"
Write-Host "Log dir      : $global:logDir"
Write-Host "CSV path     : $global:csvPath"
Write-Host "PYTHONIOENCODING : $env:PYTHONIOENCODING"

if (Test-Path "dbt_project.yml") {
    Write-Host "dbt_project.yml : found" -ForegroundColor Green
} else {
    Write-Host "dbt_project.yml : NOT FOUND" -ForegroundColor Red
}

Write-Host ""
Write-Host "=== Setup complete ===" -ForegroundColor Green
Write-Host "Next: . .\evidence\20260612\01_functions.ps1"
