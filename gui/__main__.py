"""
Точка входа для запуска GUI приложения
"""

import sys
from PyQt6.QtWidgets import QApplication
from .main_window import MainWindow
from .styles import APP_STYLE


def main():
    """Главная функция запуска"""
    app = QApplication(sys.argv)
    
    # Устанавливаем стиль приложения
    app.setStyle('Fusion')
    app.setStyleSheet(APP_STYLE)
    
    window = MainWindow()
    window.show()
    
    sys.exit(app.exec())


if __name__ == '__main__':
    main()

