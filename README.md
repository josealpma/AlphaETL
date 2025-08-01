# Visión general del proyecto

Este proyecto facilita la sincronización de datos desde archivos DBF (procedentes de sistemas heredados) hacia una base de datos MySQL. Su corazón es el módulo **`etl_core.py`**, que garantiza que solo se transfieran las filas nuevas o modificadas, ahorrando tiempo y evitando cargas completas innecesarias.

## Estructura del repositorio

```
├── config/
│   ├── config.json       # Parámetros globales (carpeta DBF, URI MySQL, tamaño de lote…)
│   └── schemas.json      # Definición de cada DBF: tabla destino, columnas, KEYS y HASHES
├── etl/
│   ├── etl_core.py       # Lógica central del ETL
│   └── control.py        # Funciones auxiliares (e.g. actualizar fecha de última sincronización)
├── gui/                  # Interfaz gráfica con PyQt5 
├── main.py               # Punto de arranque: selecciona DBF y lanza el ETL en hilo
└── requirements.txt      # Dependencias de Python
```

## Requisitos

- Python 3.8+  
- Paquetes (via `pip install -r requirements.txt`):  
  - `pandas`, `dbfread`, `sqlalchemy`, `psutil`, `PyQt5` (opcional), etc.  
- Servidor MySQL configurado y accesible.

## Configuración

1. **`config/config.json`**  
   ```jsonc
   {
     "DBF_DIR": "/ruta/a/archivos_dbf",
     "MYSQL_URI": "mysql+pymysql://user:pass@host:3306/bd",
     "CHUNK_SIZE": 1000
   }
   ```
2. **`config/schemas.json`**  
   Describe cada archivo DBF:
   ```jsonc
   {
     "ENTRIES": {
       "CATALOGS": [ … ],
       "TRANSACTIONAL": [
         {
           "DBF": "FACTURAC",
           "TARGET": {
             "TABLE": "tbl_facturas",
             "COLUMNS": [
               { "SOURCE": "no_fac", "TARGET": "no_fac" },
               …
             ],
             "KEYS": ["no_fac","ped_int"],
             "HASHES": [
               "status_fac","subt_fac","descuento",
               "total_fac","saldo_fac","mes","año","hora_fac"
             ]
           }
         }
       ]
     }
   }
   ```

## Flujo ETL (en `etl_core.py`)

1. **Carga de configuración**: lee `config.json` y `schemas.json`.  
2. **Lectura de DBF**: convierte el .DBF en un DataFrame de pandas.  
3. **Renombrado**: adapta nombres de columnas SOURCE→TARGET.  
4. **Hashing**: para cada fila, genera SHA-256 de las columnas de `HASHES`.  
5. **Detección de duplicados internos**: elimina filas repetidas en el mismo DBF.  
6. **Comparación con MySQL**:  
   - Carga en memoria el mapeo `(KEYS) → row_hash` de la tabla destino.  
   - Filtra solo filas cuyo hash difiera o no exista.  
7. **Upsert**:  
   - Inserta nuevas y actualiza modificadas con `ON DUPLICATE KEY UPDATE`.  
   - Incluye la actualización de `row_hash` para no volver a marcarlas en la siguiente ejecución.  
8. **Registro de log**: almacena en `tbl_sync_log` cuántas filas se procesaron, sincronizaron y cuánta memoria se consumió.  
9. **Actualización de fecha**: guarda la marca de tiempo de la última sincronización.

## Beneficios

- **Eficiencia**: solo mueve lo que cambió.  
- **Consistencia**: detecta con precisión modificaciones de cualquier columna relevante.  
- **Trazabilidad**: bitácora detallada de cada ejecución.  
- **Extensibilidad**: añadir nuevos DBF es tan sencillo como actualizar `schemas.json`.

## Uso

```bash
python main.py
```

O integrar `ejecutar_etl_con_progreso` en un cron o tarea programada.

## Distribucion

En linea de comandos, ubicar en la raiz del proyecto una linea de comandos y ejecutar la siguiente sentencia:

```bash
pyinstaller ^
  --onefile ^
  --name AlphaETL ^
  --add-data "config\config.json;config" ^
  --add-data "config\schemas.json;config" ^
  --add-data "ui;ui" ^
  --add-data "gui;gui" ^
  main.py
  ```

Esto generará un ejecutable `AlphaETL.exe` en la carpeta `dist/`.