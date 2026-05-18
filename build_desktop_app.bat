@echo off
setlocal
cd /d "%~dp0"
python -m PyInstaller --noconfirm --clean --onefile --windowed --name TheriacLoreDesktop --collect-submodules pipeline theriac_lore_desktop.py
endlocal
