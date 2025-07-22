#!/bin/zsh
DIR="$(cd "$(dirname "$0")" && pwd)"
FLAG="$DIR/pipeline_enabled.flag"
if [ -f "$FLAG" ]; then
    python3 "$DIR/pipeline_orchestrator.py"
fi