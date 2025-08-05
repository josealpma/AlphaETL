@echo off
REM === CONFIGURACIÓN ===
set PYTHON_PATH=C:\Python311\python.exe
set PROJECT_PATH=D:\source\repos\AlphaETL
set SCRIPT=%PROJECT_PATH%\run.py

REM === ELIMINAR TAREA SI YA EXISTE ===
schtasks /Delete /TN "AlphaETL_CLIENTES" /F >nul 2>&1

REM === CREAR TAREA CADA 5 MINUTOS ===
schtasks /Create /TN "AlphaETL_CLIENTES" /SC MINUTE /MO 2 ^
 /TR "\"%PYTHON_PATH%\" \"%SCRIPT%\" --entry CLIENTES" ^
 /RL HIGHEST /F /RU "%USERNAME%"

echo.
echo === Tarea CLIENTES  ===
pause
