@echo off
REM Activar entorno virtual

echo Iniciando entorno virtual...
call venv\Scripts\activate.bat

echo Ejecutando GUI...
python main.py

pause
