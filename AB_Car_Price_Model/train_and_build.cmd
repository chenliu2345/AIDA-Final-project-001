@echo off
setlocal
set PYTHONUTF8=1
cd /d "%~dp0"
if not exist ".venv\Scripts\python.exe" (
    python -m venv .venv
    if errorlevel 1 goto failed
)
".venv\Scripts\python.exe" -m pip install -r requirements.txt
if errorlevel 1 goto failed
".venv\Scripts\python.exe" build.py %*
if errorlevel 1 goto failed
echo.
echo SUCCESS: dist\AB_Car_Price.exe
pause
exit /b 0
:failed
echo.
echo FAILED. See the error above.
pause
exit /b 1
