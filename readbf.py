import os
import json
from dbfread import DBF

def generar_arbol_dbf(dbf_dir):
    """
    Recorre todos los .dbf en dbf_dir y devuelve un dict:
    {
      "NOMBRE_TABLA": [
         {"name": "CAMPO1", "type": "C", "length": 30, "decimal_count": 0},
         {"name": "CAMPO2", "type": "N", "length": 10, "decimal_count": 2},
         ...
      ],
      ...
    }
    """
    arbol = {}
    for archivo in os.listdir(dbf_dir):
        if archivo.upper().endswith('.DBF'):
            ruta = os.path.join(dbf_dir, archivo)
            nombre_tabla = os.path.splitext(archivo)[0]
            tabla = DBF(ruta, load=False, ignorecase=True, recfactory=dict)
            campos = []
            for f in tabla.fields:
                campos.append({
                    "name":          f.name,
                    "type":          f.type,
                    "length":        f.length,
                    "decimal_count": f.decimal_count
                })
            arbol[nombre_tabla] = campos
    return arbol

if __name__ == "__main__":
    # Ruta de la carpeta donde están tus DBF
    DBF_DIR = r"C:\alpha\dbf"
    # Nombre del archivo de salida JSON
    OUTPUT_FILE = os.path.join(DBF_DIR, "estructura_dbf.json")

    # Generar el árbol y escribirlo en JSON
    resultado = generar_arbol_dbf(DBF_DIR)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(resultado, f, indent=2, ensure_ascii=False)

    # Mensaje opcional de confirmación
    print(f"Estructura guardada en: {OUTPUT_FILE}")
