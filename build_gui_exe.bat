@echo off
setlocal
cd /d "%~dp0"
python -m PyInstaller --noconfirm --clean --onefile --console --name TheriacLoreGUI --collect-submodules pipeline theriac_lore_gui.py
endlocal
