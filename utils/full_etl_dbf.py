
#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import json
import argparse
from dbfread import DBF
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.dialects.mysql import VARCHAR, DECIMAL, FLOAT, DATETIME, BOOLEAN, TEXT

# Cargar configuración global desde config.json
def load_config():
    base = os.path.dirname(__file__)
    cfg_path = os.path.join(base, 'config.json')
    with open(cfg_path, 'r', encoding='utf-8') as f:
        cfg = json.load(f)
    return cfg['DBF_DIR'], cfg['MYSQL_URI']

# Leer DBF completo en DataFrame y retornar objeto DBF
def load_dbf(path):
    dbf = DBF(path, load=False, ignore_missing_memofile=True, char_decode_errors='ignore')
    records = []
    for rec in dbf:
        clean = {
            k: (v.decode('latin-1').strip() if isinstance(v, bytes) else v)
            for k, v in rec.items() if not k.startswith('_')
        }
        records.append(clean)
    df = pd.DataFrame(records)
    return df, dbf

# Mapear tipos DBF a SQLAlchemy
def map_dtypes(dbf):
    dtype_map = {}
    for field in dbf.fields:
        name = field.name
        ttype = field.type.upper()
        length = getattr(field, 'length', None)
        decs = getattr(field, 'decimal_count', 0)
        if ttype == 'C':  # Character
            if length and length <= 255:
                dtype_map[name] = VARCHAR(length, collation='utf8mb4_unicode_ci')
            else:
                dtype_map[name] = TEXT(collation='utf8mb4_unicode_ci')
        elif ttype == 'N':  # Numeric
            precision = length or 10
            scale = decs or 0
            dtype_map[name] = DECIMAL(precision, scale)
        elif ttype == 'F':  # Float
            dtype_map[name] = FLOAT()
        elif ttype == 'D':  # Date
            dtype_map[name] = DATETIME()
        elif ttype == 'L':  # Logical/Boolean
            dtype_map[name] = BOOLEAN()
        elif ttype == 'M':  # Memo/Text
            dtype_map[name] = TEXT(collation='utf8mb4_unicode_ci')
        else:
            dtype_map[name] = TEXT(collation='utf8mb4_unicode_ci')
    return dtype_map

# Función principal
def main():
    dbf_dir, mysql_uri = load_config()
    parser = argparse.ArgumentParser(description='ETL DBF→MySQL con tipos dinámicos')
    parser.add_argument(
        '--dbf', '-t', required=True,
        help='Nombre de archivo DBF (sin .dbf)'
    )
    args = parser.parse_args()

    filename = f"{args.dbf}.dbf"
    dbf_path = os.path.join(dbf_dir, filename)
    if not os.path.isfile(dbf_path):
        raise FileNotFoundError(f"No se encontró {dbf_path}")

    df, dbf = load_dbf(dbf_path)
    if df.empty:
        print(f"Advertencia: {filename} no contiene registros.")
        return

    # Mapear tipo de dato por columna
    dtypes = map_dtypes(dbf)

    engine = create_engine(mysql_uri)
    table_name = args.dbf.lower()

    # Volcar a MySQL con dtypes
    df.to_sql(
        name=table_name,
        con=engine,
        if_exists='replace',
        index=False,
        chunksize=1000,
        dtype=dtypes
    )

    # Mostrar conteo
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT COUNT(*) FROM `{table_name}`"))
        count = result.scalar()
    print(f"Carga completa: tabla '{table_name}' con {count} registros.")

if __name__ == '__main__':
    main()
