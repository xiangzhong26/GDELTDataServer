param([string]$Config = "$PSScriptRoot/config.local.json")
$ErrorActionPreference = "Stop"
Set-Location -LiteralPath $PSScriptRoot
if (-not (Test-Path -LiteralPath "$PSScriptRoot/.venv/Scripts/python.exe")) {
    python -m venv "$PSScriptRoot/.venv"
    if ($LASTEXITCODE -ne 0) { throw "创建Python环境失败" }
}
& "$PSScriptRoot/.venv/Scripts/python.exe" -m pip install -e .
if ($LASTEXITCODE -ne 0) { throw "安装依赖失败" }
if (-not (Test-Path -LiteralPath $Config)) {
    Copy-Item -LiteralPath "$PSScriptRoot/config.example.json" -Destination $Config
}
& "$PSScriptRoot/.venv/Scripts/python.exe" -m gdelt_server --config $Config serve
