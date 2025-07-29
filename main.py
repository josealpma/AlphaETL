#!/usr/bin/env python3
# main.py

import sys, os

from datetime import datetime

from PyQt5 import QtWidgets, uic
from PyQt5.QtCore import QObject, QThread, pyqtSignal

from etl.etl_core import cargar_config, cargar_schemas, ejecutar_etl_con_progreso
from etl.control   import obtener_ultima_fecha, actualizar_fecha
from gui.config_dialog import ConfigDialog

from gui.history_dialog import HistoryDialog

if getattr(sys, "frozen", False):
    BASE_DIR = sys._MEIPASS
else:
    BASE_DIR = os.path.dirname(os.path.abspath(sys.argv[0]))

UI_PATH     = os.path.join(BASE_DIR, "ui", "main.ui")
SCHEMA_PATH = os.path.join(BASE_DIR, "config", "schemas.json")

class ETLWorker(QObject):
    finished = pyqtSignal(str, str)  # mensaje, dbf_name
    error    = pyqtSignal(str)
    progress = pyqtSignal(int)

    def __init__(self, dbf_name: str, chunk_size: int):
        super().__init__()
        self.dbf_name   = dbf_name
        self.chunk_size = chunk_size

    def run(self):
        try:
            msg = ejecutar_etl_con_progreso(
                self.dbf_name,
                chunk_size=self.chunk_size,
                progress_callback=lambda p: self.progress.emit(p)
            )
            self.finished.emit(msg, self.dbf_name)
        except Exception as e:
            self.error.emit(str(e))

class MainWindow(QtWidgets.QMainWindow):
    def __init__(self):
        super().__init__()
        uic.loadUi(UI_PATH, self)

        cfg     = cargar_config()
        schemas = cargar_schemas()
        cats    = [e["DBF"] for e in schemas["ENTRIES"]["CATALOGS"]]
        trs     = [e["DBF"] for e in schemas["ENTRIES"]["TRANSACTIONAL"]]

        # poblar comboboxes
        self.cmbCatalogDbf.addItems(cats)
        self.cmbTxnDbf.addItems(trs)
        if cats: self.cmbCatalogDbf.setCurrentIndex(0)
        if trs:  self.cmbTxnDbf.setCurrentIndex(0)

        # signals
        self.cmbCatalogDbf.currentTextChanged.connect(self.refresh_last_sync_catalogs)
        self.cmbTxnDbf.currentTextChanged.connect(self.refresh_last_sync_transactions)
        self.btnCatalogRun.clicked.connect(self.on_run_catalogs)
        self.btnTxnRun.clicked.connect(self.on_run_transactionals)
        self.btnCatalogConfig.clicked.connect(self.open_config)
        self.btnTxnConfig.clicked.connect(self.open_config)

        # initial state
        self.refresh_last_sync_catalogs(self.cmbCatalogDbf.currentText())
        self.refresh_last_sync_transactions(self.cmbTxnDbf.currentText())
        self.prgCatalogSync.setValue(0)
        self.prgTxnSync.setValue(0)

        # Conectar el nuevo botón de historial
        self.btnCatalogHistory.clicked.connect(self.show_catalog_history)
        self.btnTxnHistory.clicked.connect(self.show_transactional_history)

        # lock window size
        self.setFixedSize(self.size())

    def show_catalog_history(self):
        dbf = self.cmbCatalogDbf.currentText()
        cfg = cargar_config()
        dlg = HistoryDialog(cfg["MYSQL_URI"], dbf, parent=self)
        dlg.exec_()

    def show_transactional_history(self):
        dbf = self.cmbTxnDbf.currentText()
        cfg = cargar_config()
        dlg = HistoryDialog(cfg["MYSQL_URI"], dbf, parent=self)
        dlg.exec_()

    def refresh_last_sync_catalogs(self, dbf_name: str):
        fecha = obtener_ultima_fecha(dbf_name)
        self.lblCatalogLastSync_Data.setText(fecha or "Nunca")

    def on_run_catalogs(self):
        dbf   = self.cmbCatalogDbf.currentText()
        chunk = cargar_config().get("CHUNK_SIZE", 1000)
        self.btnCatalogRun.setEnabled(False)
        self.prgCatalogSync.setValue(0)

        self.thread_catalogs     = QThread(self)
        self.worker_catalogs     = ETLWorker(dbf, chunk)
        self.worker_catalogs.moveToThread(self.thread_catalogs)

        self.thread_catalogs.started.connect(self.worker_catalogs.run)
        self.worker_catalogs.progress.connect(self.prgCatalogSync.setValue)
        self.worker_catalogs.finished.connect(self._on_success_catalogs)
        self.worker_catalogs.error.connect(self._on_error_catalogs)

        self.worker_catalogs.finished.connect(self.thread_catalogs.quit)
        self.worker_catalogs.finished.connect(self.worker_catalogs.deleteLater)
        self.thread_catalogs.finished.connect(self.thread_catalogs.deleteLater)

        self.thread_catalogs.start()

    def _on_success_catalogs(self, mensaje: str, dbf_name: str):
        QtWidgets.QMessageBox.information(self, "Catálogos", mensaje)
        self.refresh_last_sync_catalogs(dbf_name)
        self.prgCatalogSync.setValue(100)
        self.btnCatalogRun.setEnabled(True)

    def _on_error_catalogs(self, error_msg: str):
        QtWidgets.QMessageBox.critical(self, "Error Catálogos", error_msg)
        self.btnCatalogRun.setEnabled(True)

    def refresh_last_sync_transactions(self, dbf_name: str):
        fecha = obtener_ultima_fecha(dbf_name)
        self.lblTxnLastSync_Data.setText(fecha or "Nunca")

    def on_run_transactionals(self):
        dbf   = self.cmbTxnDbf.currentText()
        chunk = cargar_config().get("CHUNK_SIZE", 1000)
        self.btnTxnRun.setEnabled(False)
        self.prgTxnSync.setValue(0)

        self.thread_txn   = QThread(self)
        self.worker_txn   = ETLWorker(dbf, chunk)
        self.worker_txn.moveToThread(self.thread_txn)

        self.thread_txn.started.connect(self.worker_txn.run)
        self.worker_txn.progress.connect(self.prgTxnSync.setValue)
        self.worker_txn.finished.connect(self._on_success_transactions)
        self.worker_txn.error.connect(self._on_error_transactions)

        self.worker_txn.finished.connect(self.thread_txn.quit)
        self.worker_txn.finished.connect(self.worker_txn.deleteLater)
        self.thread_txn.finished.connect(self.thread_txn.deleteLater)

        self.thread_txn.start()

    def _on_success_transactions(self, mensaje: str, dbf_name: str):
        QtWidgets.QMessageBox.information(self, "Transaccionales", mensaje)
        self.refresh_last_sync_transactions(dbf_name)
        self.prgTxnSync.setValue(100)
        self.btnTxnRun.setEnabled(True)

    def _on_error_transactions(self, error_msg: str):
        QtWidgets.QMessageBox.critical(self, "Error Transaccionales", error_msg)
        self.btnTxnRun.setEnabled(True)

    def open_config(self):
        dlg = ConfigDialog(self)
        if dlg.exec_() == QtWidgets.QDialog.Accepted:
            self.refresh_last_sync_catalogs(self.cmbCatalogDbf.currentText())
            self.refresh_last_sync_transactions(self.cmbTxnDbf.currentText())

if __name__ == "__main__":
    app = QtWidgets.QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec_())
