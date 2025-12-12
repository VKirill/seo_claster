"""
Стили для GUI приложения
"""

APP_STYLE = """
QMainWindow {
    background-color: #f5f5f5;
}

QTreeView {
    background-color: white;
    border: 1px solid #ddd;
    border-radius: 4px;
    padding: 4px;
}

QTreeView::item {
    padding: 4px;
    border-bottom: 1px solid #eee;
}

QTreeView::item:hover {
    background-color: #e3f2fd;
}

QTreeView::item:selected {
    background-color: #2196f3;
    color: white;
}

QTableView {
    background-color: white;
    border: 1px solid #ddd;
    border-radius: 4px;
    gridline-color: #e0e0e0;
    selection-background-color: #2196f3;
    selection-color: white;
}

QTableView::item {
    padding: 4px;
}

QTableView::item:alternate {
    background-color: #f9f9f9;
}

QHeaderView::section {
    background-color: #f0f0f0;
    padding: 6px;
    border: 1px solid #ddd;
    font-weight: bold;
}

QPushButton {
    background-color: #2196f3;
    color: white;
    border: none;
    padding: 6px 12px;
    border-radius: 4px;
    font-weight: bold;
}

QPushButton:hover {
    background-color: #1976d2;
}

QPushButton:pressed {
    background-color: #0d47a1;
}

QLineEdit, QComboBox {
    border: 1px solid #ddd;
    border-radius: 4px;
    padding: 4px;
    background-color: white;
}

QLineEdit:focus, QComboBox:focus {
    border: 2px solid #2196f3;
}

QLabel {
    color: #333;
}

QStatusBar {
    background-color: #e0e0e0;
    color: #333;
}

QProgressBar {
    border: 1px solid #ddd;
    border-radius: 4px;
    text-align: center;
    background-color: #f0f0f0;
}

QProgressBar::chunk {
    background-color: #4caf50;
    border-radius: 3px;
}
"""






