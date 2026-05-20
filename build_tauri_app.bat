@echo off
setlocal
cd /d "%~dp0"
where link.exe >nul 2>nul
if not errorlevel 1 goto build

set "VSWHERE=C:\Program Files (x86)\Microsoft Visual Studio\Installer\vswhere.exe"
if not exist "%VSWHERE%" set "VSWHERE=%ProgramFiles%\Microsoft Visual Studio\Installer\vswhere.exe"
if exist "%VSWHERE%" for /f "usebackq delims=" %%i in (`"%VSWHERE%" -latest -products * -requires Microsoft.VisualStudio.Component.VC.Tools.x86.x64 -property installationPath`) do set "VSINSTALL=%%i"
if not defined VSINSTALL if exist "C:\Program Files (x86)\Microsoft Visual Studio\2022\BuildTools\Common7\Tools\VsDevCmd.bat" set "VSINSTALL=C:\Program Files (x86)\Microsoft Visual Studio\2022\BuildTools"
if defined VSINSTALL if exist "%VSINSTALL%\Common7\Tools\VsDevCmd.bat" call "%VSINSTALL%\Common7\Tools\VsDevCmd.bat" -arch=x64 -host_arch=x64

where link.exe >nul 2>nul
if errorlevel 1 goto missing_link

:build
cd /d "%~dp0desktop-tauri"
call npm install
call npm run build
endlocal
exit /b %ERRORLEVEL%

:missing_link
echo Missing Microsoft C++ linker link.exe.
echo Install "Visual Studio Build Tools" with the "Desktop development with C++" workload, then rerun this script.
endlocal
exit /b 1
