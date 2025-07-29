@echo off
REM Activar entorno virtual y correr GUI

echo Iniciando entorno virtual...
call venv\Scripts\activate.bat

echo Ejecutando GUI...
python main.py

pause
