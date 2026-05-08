@echo off
setlocal
cd /d "%~dp0"
python -m pipeline.run_small_batch_validation
endlocal
