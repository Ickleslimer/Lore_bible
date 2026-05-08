@echo off
setlocal
cd /d "%~dp0"
python -m pipeline.ui_review_app
endlocal
