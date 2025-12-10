"""
Диалог управления сохраненными кластеризациями
"""

import json
from pathlib import Path
from typing import Dict, Any, Optional, List
from PyQt6.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QLabel,
    QListWidget, QPushButton, QMessageBox,
    QFileDialog, QGroupBox, QTextEdit
)
from PyQt6.QtCore import Qt, pyqtSignal


class ClusteringManagerDialog(QDialog):
    """Диалог управления сохраненными кластеризациями"""
    
    recluster_requested = pyqtSignal(dict)  # Сигнал перекластеризации с параметрами
    
    def __init__(self, parent=None, output_dir: Path = None):
        super().__init__(parent)
        self.setWindowTitle("Управление кластеризациями")
        self.setMinimumWidth(700)
        self.setMinimumHeight(500)
        self.output_dir = output_dir or Path("output")
        self.clusterings: List[Dict[str, Any]] = []
        self._init_ui()
        self._load_clusterings()
    
    def _init_ui(self):
        """Инициализация UI"""
        layout = QVBoxLayout(self)
        
        # Список кластеризаций
        list_group = QGroupBox("Сохраненные кластеризации")
        list_layout = QVBoxLayout()
        
        self.clustering_list = QListWidget()
        self.clustering_list.itemSelectionChanged.connect(self._on_selection_changed)
        list_layout.addWidget(self.clustering_list)
        
        list_group.setLayout(list_layout)
        layout.addWidget(list_group)
        
        # Информация о выбранной кластеризации
        info_group = QGroupBox("Параметры кластеризации")
        info_layout = QVBoxLayout()
        
        self.params_text = QTextEdit()
        self.params_text.setReadOnly(True)
        self.params_text.setMaximumHeight(150)
        info_layout.addWidget(self.params_text)
        
        info_group.setLayout(info_layout)
        layout.addWidget(info_group)
        
        # Кнопки действий
        buttons_layout = QHBoxLayout()
        
        self.btn_recluster = QPushButton("Перекластеризовать")
        self.btn_recluster.setEnabled(False)
        self.btn_recluster.clicked.connect(self._on_recluster)
        buttons_layout.addWidget(self.btn_recluster)
        
        self.btn_open_excel = QPushButton("Открыть Excel")
        self.btn_open_excel.setEnabled(False)
        self.btn_open_excel.clicked.connect(self._on_open_excel)
        buttons_layout.addWidget(self.btn_open_excel)
        
        self.btn_save_as = QPushButton("Сохранить как...")
        self.btn_save_as.setEnabled(False)
        self.btn_save_as.clicked.connect(self._on_save_as)
        buttons_layout.addWidget(self.btn_save_as)
        
        self.btn_refresh = QPushButton("Обновить")
        self.btn_refresh.clicked.connect(self._load_clusterings)
        buttons_layout.addWidget(self.btn_refresh)
        
        self.btn_close = QPushButton("Закрыть")
        self.btn_close.clicked.connect(self.accept)
        buttons_layout.addWidget(self.btn_close)
        
        layout.addLayout(buttons_layout)
    
    def _load_clusterings(self):
        """Загрузить список сохраненных кластеризаций"""
        self.clustering_list.clear()
        self.clusterings = []
        
        # Ищем JSON файлы с кластеризациями
        json_files = list(self.output_dir.glob("**/seo_analysis_hierarchy*.json"))
        
        for json_file in json_files:
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    
                # Извлекаем параметры кластеризации
                params = data.get('clustering_params', {})
                group_name = params.get('group_name', json_file.parent.name)
                
                # Формируем название
                name = f"{group_name}"
                if params.get('serp_region'):
                    name += f" | Регион: {params.get('serp_region')}"
                if params.get('serp_device'):
                    name += f" | Устройство: {params.get('serp_device')}"
                if params.get('serp_site'):
                    name += f" | Site: {params.get('serp_site')}"
                
                clustering_info = {
                    'name': name,
                    'json_path': json_file,
                    'params': params,
                    'data': data
                }
                
                self.clusterings.append(clustering_info)
                self.clustering_list.addItem(name)
                
            except Exception as e:
                print(f"Ошибка загрузки {json_file}: {e}")
        
        if not self.clusterings:
            self.clustering_list.addItem("Нет сохраненных кластеризаций")
    
    def _on_selection_changed(self):
        """Обработка изменения выбора"""
        current_item = self.clustering_list.currentItem()
        if not current_item or current_item.text() == "Нет сохраненных кластеризаций":
            self.btn_recluster.setEnabled(False)
            self.btn_open_excel.setEnabled(False)
            self.btn_save_as.setEnabled(False)
            self.params_text.clear()
            return
        
        index = self.clustering_list.currentRow()
        if 0 <= index < len(self.clusterings):
            clustering = self.clusterings[index]
            params = clustering.get('params', {})
            
            # Форматируем параметры для отображения
            params_text = "Параметры кластеризации:\n"
            params_text += f"  Группа: {params.get('group_name', 'N/A')}\n"
            params_text += f"  Минимум общих URL: {params.get('min_common_urls', 'N/A')}\n"
            params_text += f"  Макс. размер кластера: {params.get('max_cluster_size', 'N/A')}\n"
            params_text += f"  Режим: {params.get('mode', 'N/A')}\n"
            params_text += f"  Регион: {params.get('serp_region', 'N/A')}\n"
            params_text += f"  Устройство: {params.get('serp_device', 'N/A')}\n"
            if params.get('serp_site'):
                params_text += f"  Домен: {params.get('serp_site')}\n"
            
            self.params_text.setPlainText(params_text)
            self.btn_recluster.setEnabled(True)
            self.btn_open_excel.setEnabled(True)
            self.btn_save_as.setEnabled(True)
    
    def _on_recluster(self):
        """Перекластеризовать с сохраненными параметрами"""
        index = self.clustering_list.currentRow()
        if 0 <= index < len(self.clusterings):
            clustering = self.clusterings[index]
            params = clustering.get('params', {})
            
            # Удаляем group_name из params для передачи в контроллер
            clustering_params = {k: v for k, v in params.items() if k != 'group_name'}
            
            self.recluster_requested.emit(clustering_params)
            self.accept()
    
    def _on_open_excel(self):
        """Открыть Excel файл"""
        index = self.clustering_list.currentRow()
        if 0 <= index < len(self.clusterings):
            clustering = self.clusterings[index]
            json_path = clustering['json_path']
            
            # Ищем соответствующий Excel файл
            excel_path = json_path.parent / json_path.name.replace('hierarchy', '').replace('.json', '.xlsx')
            if not excel_path.exists():
                # Пробуем найти любой Excel файл в той же папке
                excel_files = list(json_path.parent.glob("*.xlsx"))
                if excel_files:
                    excel_path = excel_files[0]
                else:
                    QMessageBox.warning(self, "Ошибка", f"Excel файл не найден в {json_path.parent}")
                    return
            
            # Открываем файл
            import os
            import platform
            if platform.system() == 'Windows':
                os.startfile(excel_path)
            elif platform.system() == 'Darwin':  # macOS
                os.system(f'open "{excel_path}"')
            else:  # Linux
                os.system(f'xdg-open "{excel_path}"')
    
    def _on_save_as(self):
        """Сохранить кластеризацию в другое место"""
        index = self.clustering_list.currentRow()
        if 0 <= index < len(self.clusterings):
            clustering = self.clusterings[index]
            json_path = clustering['json_path']
            
            # Выбираем место сохранения
            file_path, _ = QFileDialog.getSaveFileName(
                self,
                "Сохранить кластеризацию как",
                str(json_path),
                "JSON Files (*.json)"
            )
            
            if file_path:
                try:
                    import shutil
                    shutil.copy2(json_path, file_path)
                    QMessageBox.information(self, "Успех", f"Кластеризация сохранена в {file_path}")
                except Exception as e:
                    QMessageBox.critical(self, "Ошибка", f"Не удалось сохранить: {e}")

