# gui/history_dialog.py

import sys
import pandas as pd
import psutil
from PyQt5 import QtWidgets, QtCore
from PyQt5.QtWidgets import QDialog, QVBoxLayout, QTableView, QPushButton
from sqlalchemy import create_engine, text

class HistoryDialog(QDialog):
    def __init__(self, mysql_uri: str, dbf_name: str, parent=None):
        super().__init__(parent)
        self.setWindowTitle(f"Historial de sincronización: {dbf_name}")
        self.resize(700, 400)

        layout = QVBoxLayout(self)

        # Table view
        self.table = QTableView(self)
        layout.addWidget(self.table)

        # Close button
        btn_close = QPushButton("Cerrar", self)
        btn_close.clicked.connect(self.accept)
        layout.addWidget(btn_close)

        # Load and show data
        self.load_data(mysql_uri, dbf_name)

    def load_data(self, mysql_uri: str, dbf_name: str):
        # Build engine and query
        engine = create_engine(mysql_uri)
        query = text("""
            SELECT
              sync_time    AS Fecha,
              rows_processed AS Procesadas,
              rows_inserted  AS Conciliaciones,
              time_elapsed   AS Duración_s,
              chunk_size     AS Chunk,
              mem_used_mb    AS Memoria_MB
            FROM etl_sync_log
            WHERE dbf_name = :dbf
            ORDER BY sync_time DESC
        """)
        # Execute
        df = pd.read_sql(query, engine, params={"dbf": dbf_name})

        # Set up model
        model = PandasModel(df)
        self.table.setModel(model)
        self.table.resizeColumnsToContents()


class PandasModel(QtCore.QAbstractTableModel):
    def __init__(self, df: pd.DataFrame, parent=None):
        super().__init__(parent)
        self._df = df

    def rowCount(self, parent=QtCore.QModelIndex()):
        return len(self._df)

    def columnCount(self, parent=QtCore.QModelIndex()):
        return len(self._df.columns)

    def data(self, index, role=QtCore.Qt.DisplayRole):
        if role == QtCore.Qt.DisplayRole:
            value = self._df.iat[index.row(), index.column()]
            return str(value)
        return None

    def headerData(self, section, orientation, role=QtCore.Qt.DisplayRole):
        if role != QtCore.Qt.DisplayRole:
            return None
        if orientation == QtCore.Qt.Horizontal:
            return self._df.columns[section]
        else:
            return section + 1
