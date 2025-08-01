# run_etl.py
import argparse
import json
import logging
import os
import sys

# Asegúrate de que tu proyecto esté en el path
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE_DIR)

# Importa tu función principal de ETL
from etl.main import ejecutar_etl  # o el nombre de tu módulo/función

def main():
    parser = argparse.ArgumentParser(
        description="Ejecuta el proceso ETL de AlphaERP desde línea de comandos"
    )
    parser.add_argument(
        "-c", "--config",
        default=os.path.join(BASE_DIR, "config", "config.json"),
        help="Ruta al archivo config.json"
    )
    parser.add_argument(
        "-l", "--log",
        default=os.path.join(BASE_DIR, "logs", "etl.log"),
        help="Ruta al archivo de log"
    )
    args = parser.parse_args()

    # Configurar logging
    os.makedirs(os.path.dirname(args.log), exist_ok=True)
    logging.basicConfig(
        filename=args.log,
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s"
    )
    logging.info("=== Inicio ejecución ETL ===")

    # Cargar configuración
    with open(args.config, encoding="utf-8") as f:
        cfg = json.load(f)

    # Llamar a tu función de carga
    try:
        ejecutar_etl(cfg)
        logging.info("=== ETL finalizado exitosamente ===")
    except Exception as e:
        logging.exception("Error durante la ejecución del ETL:")
        sys.exit(1)

if __name__ == "__main__":
    main()
