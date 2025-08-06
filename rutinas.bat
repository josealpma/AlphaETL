@echo off
setlocal enabledelayedexpansion

REM === CONFIGURACION DEL ENTORNO VIRTUAL ===
set "PYTHON_PATH=C:\AlphaETL\venv\Scripts\python.exe" 
set "PROJECT_PATH=C:\AlphaETL\"
set "SCRIPT=%PROJECT_PATH%\run.py"

for %%D in (AGENTES PRODUCTO CLIENTES) do (
    schtasks /Delete /TN "AlphaETL_%%D" /F >nul 2>&1
    call :crear_tarea "AlphaETL_%%D" "HOURLY" "2" "%%D" "06:00" "22:00"
)
for %%D in (CREDITOS FACTURAC FACTURAD) do (
    schtasks /Delete /TN "AlphaETL_%%D" /F >nul 2>&1
    call :crear_tarea "AlphaETL_%%D" "MINUTE" "30" "%%D" "06:00" "22:00"
)

echo.
echo === Todas las tareas fueron creadas correctamente ===
pause
exit /b

REM === FUNCION: crear_tarea nombre frecuencia intervalo entry start end
:crear_tarea
  set "TASK_NAME=%~1"
  set "SCHEDULE=%~2"
  set "INTERVAL=%~3"
  set "ENTRY=%~4"
  set "START_TIME=%~5"
  set "END_TIME=%~6"

  schtasks /Create /TN "%TASK_NAME%" ^
    /SC %SCHEDULE% /MO %INTERVAL%       ^
    /ST %START_TIME% /ET %END_TIME%     ^
    /TR "%PYTHON_PATH% %SCRIPT% --entry %ENTRY%" ^
    /RL HIGHEST /F /RU %USERNAME%
exit /b
