@echo off
REM === CONFIGURACIÓN ===
set PYTHON_PATH=C:\Python311\python.exe
set PROJECT_PATH=D:\source\repos\AlphaETL
set SCRIPT=%PROJECT_PATH%\run.py

REM === CREAR TAREA PROGRAMADA ===
schtasks /Create /TN "AlphaETL_FACTURAC" /SC DAILY /ST 01:30 ^
 /TR "\"%PYTHON_PATH%\" \"%SCRIPT%\" --entry FACTURAC" ^
 /RL HIGHEST /F /RU "%USERNAME%"

echo.
echo === Tarea FACTURAC  ===
pause