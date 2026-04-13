param(
    [string]$ProjectDir = (Get-Location).Path,
    [string]$Goal = "Backend refactor task"
)

$ErrorActionPreference = "Stop"

$constitution = Join-Path $PSScriptRoot "backend-refactor-constitution.md"

python -m claude_orchestrator.cli do $Goal 
  -d $ProjectDir 
  --doc $constitution 
  --skip-gather 
  -y