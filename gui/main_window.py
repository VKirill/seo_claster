"""
Главное окно приложения
"""

from PyQt6.QtWidgets import (
    QMainWindow, QWidget, QHBoxLayout, QVBoxLayout,
    QSplitter, QMenuBar, QStatusBar, QMessageBox
)
from PyQt6.QtCore import Qt, pyqtSlot
from PyQt6.QtGui import QAction, QKeySequence

from .views import ProjectPanel, QueryTableView, ClusterTreeView, StatsPanel
from .views.dialogs import ClusteringDialog, ExportDialog, ClusteringManagerDialog
from .controllers import ProjectController, ClusteringController, ExportController


class MainWindow(QMainWindow):
    """Главное окно приложения"""
    
    def __init__(self):
        super().__init__()
        self.setWindowTitle("SEO Cluster - Desktop GUI")
        self.setGeometry(100, 100, 1400, 800)
        
        # Контроллеры
        self.project_controller = ProjectController()
        self.clustering_controller = ClusteringController()
        self.export_controller = ExportController()
        
        # Текущее состояние
        self.current_group: str = None
        
        self._init_ui()
        self._init_menu()
        self._connect_signals()
    
    def _init_ui(self):
        """Инициализация UI"""
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        
        main_layout = QHBoxLayout(central_widget)
        
        # Сплиттер для разделения панелей
        splitter = QSplitter(Qt.Orientation.Horizontal)
        
        # Левая панель - проекты
        self.project_panel = ProjectPanel()
        splitter.addWidget(self.project_panel)
        
        # Центральная панель - запросы
        self.query_table = QueryTableView()
        splitter.addWidget(self.query_table)
        
        # Правая панель - кластеры и статистика
        right_panel = QWidget()
        right_layout = QVBoxLayout(right_panel)
        
        self.cluster_tree = ClusterTreeView()
        right_layout.addWidget(self.cluster_tree)
        
        self.stats_panel = StatsPanel()
        right_layout.addWidget(self.stats_panel)
        
        splitter.addWidget(right_panel)
        
        # Устанавливаем пропорции
        splitter.setSizes([200, 700, 300])
        
        main_layout.addWidget(splitter)
        
        # Статус-бар
        self.statusBar().showMessage("Готово")
    
    def _init_menu(self):
        """Инициализация меню"""
        menubar = self.menuBar()
        
        # Файл
        file_menu = menubar.addMenu("Файл")
        
        import_action = QAction("Импорт проекта...", self)
        import_action.setShortcut(QKeySequence("Ctrl+I"))
        import_action.triggered.connect(self._on_import_project)
        file_menu.addAction(import_action)
        
        export_action = QAction("Экспорт...", self)
        export_action.setShortcut(QKeySequence("Ctrl+E"))
        export_action.triggered.connect(self._on_export)
        file_menu.addAction(export_action)
        
        file_menu.addSeparator()
        
        exit_action = QAction("Выход", self)
        exit_action.setShortcut(QKeySequence("Ctrl+Q"))
        exit_action.triggered.connect(self.close)
        file_menu.addAction(exit_action)
        
        # Анализ
        analysis_menu = menubar.addMenu("Анализ")
        
        cluster_action = QAction("Кластеризовать...", self)
        cluster_action.setShortcut(QKeySequence("F5"))
        cluster_action.triggered.connect(self._on_cluster)
        analysis_menu.addAction(cluster_action)
        
        manage_clusterings_action = QAction("Управление кластеризациями...", self)
        manage_clusterings_action.triggered.connect(self._on_manage_clusterings)
        analysis_menu.addAction(manage_clusterings_action)
        
        # Справка
        help_menu = menubar.addMenu("Справка")
        
        about_action = QAction("О программе", self)
        about_action.triggered.connect(self._on_about)
        help_menu.addAction(about_action)
    
    def _connect_signals(self):
        """Подключение сигналов"""
        self.project_panel.project_selected.connect(self._on_project_selected)
        self.query_table.query_selected.connect(self._on_query_selected)
        self.cluster_tree.cluster_selected.connect(self._on_cluster_selected)
    
    @pyqtSlot(str)
    def _on_project_selected(self, group_name: str):
        """Обработка выбора проекта"""
        self.current_group = group_name
        self.statusBar().showMessage(f"Загружен проект: {group_name}")
        
        # Загружаем запросы
        self.query_table.load_group(group_name)
        
        # Обновляем статистику
        stats = self.project_controller.get_group_stats(group_name)
        if stats:
            self.stats_panel.update_stats(stats)
        
            # Загружаем кластеры
            try:
                from seo_analyzer.core.cache.master_query_db import MasterQueryDatabase
                master_db = MasterQueryDatabase()
                df = master_db.load_queries(group_name, include_serp_urls=False)
                if df is not None:
                    self.cluster_tree.load_data(df)
            except Exception as e:
                print(f"Ошибка загрузки кластеров: {e}")
    
    @pyqtSlot(int)
    def _on_query_selected(self, row: int):
        """Обработка выбора запроса"""
        query_data = self.query_table.model.get_query_data(row)
        if query_data:
            keyword = query_data.get('keyword', '')
            self.statusBar().showMessage(f"Выбран запрос: {keyword}")
    
    @pyqtSlot(int)
    def _on_cluster_selected(self, cluster_id: int):
        """Обработка выбора кластера"""
        # Фильтруем таблицу по кластеру
        queries = self.cluster_tree.model.get_cluster_queries(cluster_id)
        self.statusBar().showMessage(f"Выбран кластер {cluster_id}: {len(queries)} запросов")
    
    def _on_cluster(self):
        """Запуск кластеризации"""
        if not self.current_group:
            QMessageBox.warning(self, "Внимание", "Выберите проект для кластеризации")
            return
        
        dialog = ClusteringDialog(self)
        if dialog.exec():
            params = {
                'min_common_urls': dialog.min_urls_spin.value(),
                'max_cluster_size': dialog.max_size_spin.value() if dialog.max_size_spin.value() > 0 else None,
                'mode': dialog.mode_combo.currentText(),
                'serp_region': dialog.region_spin.value(),
                'serp_device': dialog.device_combo.currentText(),
                'serp_site': dialog.site_edit.text().strip() if dialog.site_edit.text().strip() else None
            }
            
            # Запускаем кластеризацию
            self.statusBar().showMessage("Запуск кластеризации...")
            self.clustering_controller.start_clustering(
                self.current_group,
                params,
                self._on_clustering_progress,
                self._on_clustering_finished
            )
    
    def _on_clustering_progress(self, message: str, current: int, total: int):
        """Обновление прогресса кластеризации"""
        self.statusBar().showMessage(f"{message} ({current}/{total})")
    
    def _on_clustering_finished(self, success: bool, message: str):
        """Завершение кластеризации"""
        if success:
            QMessageBox.information(self, "Успех", message)
            # Обновляем данные
            if self.current_group:
                self._on_project_selected(self.current_group)
        else:
            QMessageBox.critical(self, "Ошибка", message)
        self.statusBar().showMessage("Готово")
    
    def _on_export(self):
        """Экспорт результатов"""
        if not self.current_group:
            QMessageBox.warning(self, "Внимание", "Выберите проект для экспорта")
            return
        
        dialog = ExportDialog(self)
        if dialog.exec():
            from PyQt6.QtWidgets import QFileDialog
            format_name = dialog.format_combo.currentText().lower()
            default_ext = {
                'excel': '.xlsx',
                'csv': '.csv',
                'json': '.json',
                'html': '.html'
            }.get(format_name, '.xlsx')
            
            file_path, _ = QFileDialog.getSaveFileName(
                self,
                "Сохранить как",
                f"{self.current_group}{default_ext}",
                f"{format_name.upper()} Files (*{default_ext})"
            )
            
            if file_path:
                from pathlib import Path
                success = self.export_controller.export_group(
                    self.current_group,
                    format_name,
                    Path(file_path),
                    {
                        'include_charts': dialog.chk_charts.isChecked(),
                        'include_pivot': dialog.chk_pivot.isChecked()
                    }
                )
                
                if success:
                    QMessageBox.information(self, "Успех", "Экспорт завершен")
                else:
                    QMessageBox.warning(self, "Ошибка", "Не удалось экспортировать данные")
    
    def _on_import_project(self):
        """Импорт проекта"""
        self.project_panel._on_import_clicked()
    
    def _on_manage_clusterings(self):
        """Управление сохраненными кластеризациями"""
        from pathlib import Path
        output_dir = Path("output")
        if self.current_group:
            output_dir = output_dir / "groups" / self.current_group
        
        dialog = ClusteringManagerDialog(self, output_dir)
        dialog.recluster_requested.connect(self._on_recluster_requested)
        dialog.exec()
    
    def _on_recluster_requested(self, params: dict):
        """Обработка запроса на перекластеризацию"""
        if not self.current_group:
            QMessageBox.warning(self, "Внимание", "Выберите проект для перекластеризации")
            return
        
        # Запускаем кластеризацию с сохраненными параметрами
        self.statusBar().showMessage("Запуск перекластеризации...")
        self.clustering_controller.start_clustering(
            self.current_group,
            params,
            self._on_clustering_progress,
            self._on_clustering_finished
        )
    
    def _on_about(self):
        """О программе"""
        QMessageBox.about(
            self,
            "О программе",
            "SEO Cluster Desktop GUI\n\n"
            "Версия 1.0.0\n"
            "Desktop приложение для управления проектами SEO кластеризации"
        )

