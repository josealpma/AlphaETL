import argparse
import json
import logging
import os
import sys
from datetime import datetime

# ==== RUTAS BASE ====
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE_DIR)

# Importa tu core real
try:
    from etl.etl_core import ejecutar_etl_con_progreso  # (dbf_name, chunk_size, progress_callback)
except Exception as ex:
    print("[FATAL] No se pudo importar etl.etl_core.ejecutar_etl_con_progreso:", repr(ex))
    sys.exit(90)

CONFIG_PATH = os.path.join(BASE_DIR, "config", "config.json")
SCHEMA_PATH = os.path.join(BASE_DIR, "config", "schemas.json")
LOG_DIR     = os.path.join(BASE_DIR, "logs")


# ==== UTILIDADES ====
def cargar_json(path: str) -> dict:
    if not os.path.exists(path):
        print(f"[FATAL] No existe el archivo requerido: {path}")
        sys.exit(91)
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception as ex:
        print(f"[FATAL] Error al leer JSON {path}: {ex!r}")
        sys.exit(92)


def configurar_logger(nombre_entry: str, log_path: str | None = None) -> str:
    """
    Crea logger de archivo + consola.
    logs/<ENTRY>/etl_<ENTRY>_<YYYYMMDD>_<HHMM>.log
    """
    nombre_entry = nombre_entry.upper()

    # Asegura carpetas
    os.makedirs(LOG_DIR, exist_ok=True)

    if not log_path:
        ts = datetime.now().strftime("%Y%m%d_%H%M")
        entry_dir = os.path.join(LOG_DIR, nombre_entry)
        os.makedirs(entry_dir, exist_ok=True)
        log_path = os.path.join(entry_dir, f"etl_{nombre_entry}_{ts}.log")
    else:
        os.makedirs(os.path.dirname(log_path), exist_ok=True)

    # reset handlers
    for h in logging.root.handlers[:]:
        logging.root.removeHandler(h)

    logging.basicConfig(
        filename=log_path,
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s"
    )
    # espejo a consola
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.INFO)
    console.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logging.getLogger().addHandler(console)

    print(f"[RUN] Logger inicializado -> {log_path}")
    logging.info(f"Logger inicializado: {log_path}")
    return log_path


def resolver_entry(nombre_entry: str, schemas: dict) -> tuple[dict, bool]:
    """
    Retorna (entry, is_txn)
    - Valida estructura schemas.
    - Lista entries disponibles para diagnostico.
    """
    if "ENTRIES" not in schemas or not isinstance(schemas["ENTRIES"], dict):
        print("[FATAL] schemas.json no tiene la seccion 'ENTRIES' válida.")
        sys.exit(93)

    entries = schemas["ENTRIES"]
    cats = entries.get("CATALOGS", [])
    txns = entries.get("TRANSACTIONAL", [])

    if not isinstance(cats, list) or not isinstance(txns, list):
        print("[FATAL] 'CATALOGS' o 'TRANSACTIONAL' no son listas en schemas.json.")
        sys.exit(94)

    disponibles_cats = [e.get("DBF", "") for e in cats]
    disponibles_txns = [e.get("DBF", "") for e in txns]

    print(f"[DEBUG] CATALOGS disponibles: {disponibles_cats}")
    print(f"[DEBUG] TRANSACTIONAL disponibles: {disponibles_txns}")

    nombre_entry_up = nombre_entry.upper()
    entry = next((e for e in cats if e.get("DBF", "").upper() == nombre_entry_up), None)
    if entry:
        return entry, False

    entry = next((e for e in txns if e.get("DBF", "").upper() == nombre_entry_up), None)
    if entry:
        return entry, True

    print(f"[ERROR] No se encontro DBF='{nombre_entry_up}' en CATALOGS ni TRANSACTIONAL.")
    sys.exit(95)


# ==== MAIN CLI ====
def main():
    parser = argparse.ArgumentParser(description="Runner CLI para AlphaETL (ejecucion por DBF/ENTRY).")
    parser.add_argument("-e", "--entry", required=True, help="DBF a procesar (coincide con 'DBF' en schemas.json).")
    parser.add_argument("--chunk-size", type=int, default=1000, help="Tamaño de lote para upsert (default 1000).")
    parser.add_argument("--log", help="Ruta de log (opcional, si no se da se crea automatica).")
    parser.add_argument("--debug", action="store_true", help="Modo diagnostico (mas salida en consola).")
    args = parser.parse_args()

    entry_name = args.entry.upper()

    # Prints previos para saber rutas críticas
    print(f"[RUN] BASE_DIR={BASE_DIR}")
    print(f"[RUN] CONFIG_PATH={CONFIG_PATH}  exists={os.path.exists(CONFIG_PATH)}")
    print(f"[RUN] SCHEMA_PATH={SCHEMA_PATH}  exists={os.path.exists(SCHEMA_PATH)}")
    print(f"[RUN] ENTRY={entry_name}  CHUNK={args.chunk_size}")

    # Logger
    log_path = configurar_logger(entry_name, args.log)
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
        logging.debug("[DEBUG] Modo diagnostico activado")

    # Cargar config y schemas
    config = cargar_json(CONFIG_PATH)
    schemas = cargar_json(SCHEMA_PATH)

    # Resolver entry
    entry_cfg, is_txn = resolver_entry(entry_name, schemas)
    logging.info(f"Entry resuelto: DBF={entry_cfg.get('DBF')}  is_txn={is_txn}")

    # Callback progreso (0..100) con umbral para no spamear
    last_pct = -1
    def on_progress(pct: int) -> None:
        nonlocal last_pct
        if pct == 100 or pct - last_pct >= 5:
            last_pct = pct
            msg = f"Progreso: {pct}%"
            print("[RUN]", msg)
            logging.info(msg)

    # Ejecutar ETL
    try:
        logging.info("==== INICIO EJECUCION ETL ====")
        print("[RUN] Ejecutando ETL…")
        resumen = ejecutar_etl_con_progreso(
            dbf_name=entry_name,
            chunk_size=args.chunk_size,
            progress_callback=on_progress,
        )
        logging.info(resumen)
        print("[RUN] ETL OK ->", resumen)
        logging.info("==== ETL FINALIZADO EXITOSAMENTE ====")
        print(f"[RUN] Log en: {log_path}")
        sys.exit(0)

    except SystemExit:
        raise
    except Exception as ex:
        print("[RUN][ERROR]", repr(ex))
        logging.exception("Error durante la ejecucion del ETL:")
        print(f"[RUN] Revisa el log: {log_path}")
        sys.exit(1)


if __name__ == "__main__":
    main()
