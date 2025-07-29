# gui/config_dialog.py

import os
import json
from PyQt5 import QtWidgets, uic

CONFIG_PATH = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.json')
UI_PATH     = os.path.join(os.path.dirname(__file__), '..','ui','config.ui')

def cargar_config():
    """Lee el config.json existente (o devuelve valores por defecto)."""
    if not os.path.exists(CONFIG_PATH):
        return {
            "DBF_DIR": "",
            "CHUNK_SIZE": 1000,
            "MYSQL": {
                "HOST": "localhost",
                "PORT": 3306,
                "USER": "",
                "PASSWORD": "",
                "DATABASE": ""
            },
            "ENTRIES": {}
        }
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

def guardar_config(cfg: dict):
    """Sobrescribe config.json con la nueva configuración."""
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=4)

def construir_mysql_uri(mysql_cfg: dict) -> str:
    return (
        f"mysql+pymysql://{mysql_cfg['USER']}:{mysql_cfg['PASSWORD']}"
        f"@{mysql_cfg['HOST']}:{mysql_cfg['PORT']}/{mysql_cfg['DATABASE']}"
    )

class ConfigDialog(QtWidgets.QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        # Carga el layout desde config.ui
        uic.loadUi(UI_PATH, self)

        # Leer config actual y poblar los campos
        self.cfg = cargar_config()
        mysql = self.cfg.get("MYSQL", {})

        self.rutaDbfTxt.setText(self.cfg.get("DBF_DIR", ""))
        self.hostTxt. setText(mysql.get("HOST", ""))
        self.portTxt. setText(str(mysql.get("PORT", 3306)))
        self.userTxt. setText(mysql.get("USER", ""))
        self.pwdTxt.  setText(mysql.get("PASSWORD", ""))
        self.databaseTxt.setText(mysql.get("DATABASE", ""))

        # Conectar los botones del QDialogButtonBox
        # Asume que en config.ui tu QDialogButtonBox se llama "buttonGroup"
        self.buttonGroup.accepted.connect(self._on_accept)
        self.buttonGroup.rejected.connect(self.reject)

    def _on_accept(self):
        # Validar y leer valores
        dbf_dir = self.rutaDbfTxt.text().strip()
        try:
            port = int(self.portTxt.text())
        except ValueError:
            QtWidgets.QMessageBox.warning(self, "Error", "El puerto debe ser un número entero.")
            return

        mysql_cfg = {
            "HOST":     self.hostTxt.text().strip(),
            "PORT":     port,
            "USER":     self.userTxt.text().strip(),
            "PASSWORD": self.pwdTxt.text(),
            "DATABASE": self.databaseTxt.text().strip()
        }

        # Reconstruir el config completo (preservando ENTRIES y CHUNK_SIZE)
        nueva_cfg = {
            "DBF_DIR":  dbf_dir,
            "CHUNK_SIZE": self.cfg.get("CHUNK_SIZE", 1000),
            "MYSQL":    mysql_cfg,
            "MYSQL_URI": construir_mysql_uri(mysql_cfg),
            "ENTRIES":  self.cfg.get("ENTRIES", {})
        }

        # Guardar y cerrar
        guardar_config(nueva_cfg)
        QtWidgets.QMessageBox.information(self, "Configuración", "Guardado exitoso.")
        self.accept()
