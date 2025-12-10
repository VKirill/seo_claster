"""
Диалог настроек кластеризации
"""

from PyQt6.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QLabel,
    QSpinBox, QComboBox, QPushButton, QProgressBar,
    QLineEdit, QGroupBox
)
from PyQt6.QtCore import Qt, pyqtSignal


class ClusteringDialog(QDialog):
    """Диалог настроек кластеризации"""
    
    clustering_started = pyqtSignal(dict)  # Сигнал запуска с параметрами
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Настройки кластеризации")
        self.setMinimumWidth(500)
        self._init_ui()
    
    def _init_ui(self):
        """Инициализация UI"""
        layout = QVBoxLayout(self)
        
        # Параметры кластеризации
        clustering_group = QGroupBox("Параметры кластеризации")
        clustering_layout = QVBoxLayout()
        
        # min_common_urls
        url_layout = QHBoxLayout()
        url_layout.addWidget(QLabel("Минимум общих URL:"))
        self.min_urls_spin = QSpinBox()
        self.min_urls_spin.setMinimum(1)
        self.min_urls_spin.setMaximum(20)
        self.min_urls_spin.setValue(8)
        url_layout.addWidget(self.min_urls_spin)
        clustering_layout.addLayout(url_layout)
        
        # max_cluster_size
        size_layout = QHBoxLayout()
        size_layout.addWidget(QLabel("Макс. размер кластера:"))
        self.max_size_spin = QSpinBox()
        self.max_size_spin.setMinimum(1)
        self.max_size_spin.setMaximum(1000)
        self.max_size_spin.setValue(100)
        self.max_size_spin.setSpecialValueText("Без лимита")
        size_layout.addWidget(self.max_size_spin)
        clustering_layout.addLayout(size_layout)
        
        # mode
        mode_layout = QHBoxLayout()
        mode_layout.addWidget(QLabel("Режим:"))
        self.mode_combo = QComboBox()
        self.mode_combo.addItems(["strict", "balanced", "soft"])
        self.mode_combo.setCurrentText("balanced")
        mode_layout.addWidget(self.mode_combo)
        clustering_layout.addLayout(mode_layout)
        
        clustering_group.setLayout(clustering_layout)
        layout.addWidget(clustering_group)
        
        # Параметры SERP
        serp_group = QGroupBox("Параметры SERP (XMLStock)")
        serp_layout = QVBoxLayout()
        
        # Регион (lr)
        region_layout = QHBoxLayout()
        region_layout.addWidget(QLabel("Регион (ID):"))
        self.region_spin = QSpinBox()
        self.region_spin.setMinimum(1)
        self.region_spin.setMaximum(999999)
        self.region_spin.setValue(213)  # Москва по умолчанию
        self.region_spin.setToolTip("213 = Москва, 2 = Санкт-Петербург, 225 = Россия")
        region_layout.addWidget(self.region_spin)
        serp_layout.addLayout(region_layout)
        
        # Устройство (device)
        device_layout = QHBoxLayout()
        device_layout.addWidget(QLabel("Устройство:"))
        self.device_combo = QComboBox()
        self.device_combo.addItems(["desktop", "mobile", "tablet", "iphone", "android"])
        self.device_combo.setCurrentText("desktop")
        self.device_combo.setToolTip("С какого устройства осуществляется поиск")
        device_layout.addWidget(self.device_combo)
        serp_layout.addLayout(device_layout)
        
        # Домен (site)
        site_layout = QHBoxLayout()
        site_layout.addWidget(QLabel("Домен (site:):"))
        self.site_edit = QLineEdit()
        self.site_edit.setPlaceholderText("domain.ru (без site:)")
        self.site_edit.setToolTip("Если указан, к запросам добавляется site:domain.ru")
        site_layout.addWidget(self.site_edit)
        serp_layout.addLayout(site_layout)
        
        serp_group.setLayout(serp_layout)
        layout.addWidget(serp_group)
        
        # Прогресс
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        layout.addWidget(self.progress_bar)
        
        # Кнопки
        buttons_layout = QHBoxLayout()
        self.btn_start = QPushButton("Запустить")
        self.btn_cancel = QPushButton("Отмена")
        buttons_layout.addWidget(self.btn_start)
        buttons_layout.addWidget(self.btn_cancel)
        layout.addLayout(buttons_layout)
        
        # Подключение сигналов
        self.btn_start.clicked.connect(self._on_start)
        self.btn_cancel.clicked.connect(self.reject)
    
    def _on_start(self):
        """Запуск кластеризации"""
        site_value = self.site_edit.text().strip()
        params = {
            'min_common_urls': self.min_urls_spin.value(),
            'max_cluster_size': self.max_size_spin.value() if self.max_size_spin.value() > 0 else None,
            'mode': self.mode_combo.currentText(),
            'serp_region': self.region_spin.value(),
            'serp_device': self.device_combo.currentText(),
            'serp_site': site_value if site_value else None
        }
        self.clustering_started.emit(params)
        self.accept()
    
    def set_progress(self, value: int, maximum: int = 100):
        """Установить прогресс"""
        self.progress_bar.setMaximum(maximum)
        self.progress_bar.setValue(value)
        self.progress_bar.setVisible(True)

