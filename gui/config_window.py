import tkinter as tk
from tkinter import filedialog, messagebox
import json
import os
import pymysql  # o mysql.connector si prefieres

CONFIG_PATH = "config/config.json"

# Ventana singleton
_config_window = None

def cargar_config():
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
            "ENTRIES": []
        }
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

def guardar_config(config):
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=4)

def construir_mysql_uri(mysql_cfg):
    return (
        f"mysql+pymysql://{mysql_cfg['USER']}:{mysql_cfg['PASSWORD']}"
        f"@{mysql_cfg['HOST']}:{mysql_cfg['PORT']}/{mysql_cfg['DATABASE']}"
    )

def abrir_ventana_config(parent):
    global _config_window
    # Si ya existe y sigue activa, solo la traemos al frente
    if _config_window and _config_window.winfo_exists():
        _config_window.lift()
        _config_window.focus_force()
        return

    # Crear nueva ventana
    ventana = tk.Toplevel(parent)
    _config_window = ventana
    ventana.title("Configuración general")
    ventana.resizable(False, False)

    # Centrar la ventana
    ventana.update_idletasks()
    w, h = 550, 360
    x = (ventana.winfo_screenwidth() // 2) - (w // 2)
    y = (ventana.winfo_screenheight() // 2) - (h // 2)
    ventana.geometry(f"{w}x{h}+{x}+{y}")

    # Al cerrar, limpiar la referencia
    def on_close():
        global _config_window
        _config_window = None
        ventana.destroy()
    ventana.protocol("WM_DELETE_WINDOW", on_close)

    # Cargar configuración actual
    config = cargar_config()
    mysql_cfg = config.get("MYSQL", {})

    # Variables Tkinter
    vars_dict = {
        "DBF_DIR": tk.StringVar(value=config.get("DBF_DIR", "")),
        "CHUNK_SIZE": tk.StringVar(value=str(config.get("CHUNK_SIZE", 1000))),
        "HOST": tk.StringVar(value=mysql_cfg.get("HOST", "localhost")),
        "PORT": tk.StringVar(value=str(mysql_cfg.get("PORT", 3306))),
        "USER": tk.StringVar(value=mysql_cfg.get("USER", "")),
        "PASSWORD": tk.StringVar(value=mysql_cfg.get("PASSWORD", "")),
        "DATABASE": tk.StringVar(value=mysql_cfg.get("DATABASE", ""))
    }

    # Etiquetas
    etiquetas = {
        "DBF_DIR": "Ruta DBF:",
        "CHUNK_SIZE": "Tamaño de lote:",
        "HOST": "MySQL Host:",
        "PORT": "Puerto:",
        "USER": "Usuario:",
        "PASSWORD": "Contraseña:",
        "DATABASE": "Base de datos:"
    }

    # Crear filas de widgets
    for i, (key, label_text) in enumerate(etiquetas.items()):
        tk.Label(ventana, text=label_text, anchor="e", width=18)\
          .grid(row=i, column=0, sticky="e", padx=8, pady=6)
        entry = tk.Entry(
            ventana,
            textvariable=vars_dict[key],
            width=40,
            show="*" if key == "PASSWORD" else None
        )
        entry.grid(row=i, column=1, padx=4, pady=6, sticky="w")

        # Botón para seleccionar carpeta DBF
        if key == "DBF_DIR":
            def seleccionar_ruta():
                path = filedialog.askdirectory()
                if path:
                    vars_dict["DBF_DIR"].set(path)
            tk.Button(ventana, text="Seleccionar", command=seleccionar_ruta)\
              .grid(row=i, column=2, padx=4)

    # Función para probar conexión MySQL
    def probar_conexion():
        try:
            conn = pymysql.connect(
                host=vars_dict["HOST"].get(),
                port=int(vars_dict["PORT"].get()),
                user=vars_dict["USER"].get(),
                password=vars_dict["PASSWORD"].get(),
                database=vars_dict["DATABASE"].get()
            )
            conn.close()
            messagebox.showinfo("Conexión exitosa", "MySQL OK.")
        except Exception as e:
            messagebox.showerror("Error de conexión", str(e))

    # Función para guardar configuración
    def guardar():
        try:
            chunk = int(vars_dict["CHUNK_SIZE"].get())
        except ValueError:
            messagebox.showerror("Error", "CHUNK_SIZE debe ser entero.")
            return

        mysql_new = {
            "HOST": vars_dict["HOST"].get(),
            "PORT": int(vars_dict["PORT"].get()),
            "USER": vars_dict["USER"].get(),
            "PASSWORD": vars_dict["PASSWORD"].get(),
            "DATABASE": vars_dict["DATABASE"].get()
        }

        nueva_cfg = {
            "DBF_DIR": vars_dict["DBF_DIR"].get(),
            "CHUNK_SIZE": chunk,
            "MYSQL": mysql_new,
            "MYSQL_URI": construir_mysql_uri(mysql_new),
            "ENTRIES": config.get("ENTRIES", [])
        }
        guardar_config(nueva_cfg)
        messagebox.showinfo("Éxito", "Configuración guardada.")
        on_close()

    # Botones de acción
    tk.Button(ventana, text="Guardar configuración", command=guardar, width=25)\
      .grid(row=len(etiquetas)+1, column=1, pady=(10,4))
    tk.Button(ventana, text="Probar conexión", command=probar_conexion, width=25)\
      .grid(row=len(etiquetas)+2, column=1, pady=(4,10))
