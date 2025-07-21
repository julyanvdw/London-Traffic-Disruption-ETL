#!/bin/zsh
DIR="$(cd "$(dirname "$0")" && pwd)"
FLAG="$DIR/pipeline_enabled.flag"
if [ -f "$FLAG" ]; then
    /Library/Frameworks/Python.framework/Versions/3.13/bin/python3 "$DIR/pipeline_orchestrator.py"
fi