"""
Контроллер кластеризации
"""

from typing import Dict, Any
from PyQt6.QtCore import QThread, pyqtSignal

from pipeline.analyzer import SEOAnalyzer
from pipeline.args_builder import create_argument_parser
from seo_analyzer.core.config import RUN_CONFIG


class ClusteringWorker(QThread):
    """Воркер для запуска кластеризации в отдельном потоке"""
    
    progress = pyqtSignal(str, int, int)  # Сигнал прогресса (сообщение, текущее, всего)
    finished = pyqtSignal(bool, str)  # Сигнал завершения (успех, сообщение)
    
    def __init__(self, group_name: str, params: Dict[str, Any], parent=None):
        super().__init__(parent)
        self.group_name = group_name
        self.params = params
    
    def run(self):
        """Запуск кластеризации"""
        try:
            # Создаем парсер и объект args
            parser = create_argument_parser()
            
            # Создаем args с дефолтными значениями
            args = parser.parse_args([f"semantika/{self.group_name}.csv"])
            
            # Устанавливаем параметры кластеризации
            args.serp_similarity_threshold = self.params.get('min_common_urls', 8)
            max_size = self.params.get('max_cluster_size', 100)
            if max_size is None:
                args.max_cluster_size = 999999  # Без лимита
            else:
                args.max_cluster_size = max_size
            args.serp_mode = self.params.get('mode', 'balanced')
            args.group = self.group_name
            
            # Параметры SERP
            args.serp_region = self.params.get('serp_region', 213)
            args.serp_device = self.params.get('serp_device', 'desktop')
            args.serp_site = self.params.get('serp_site', None)
            
            # Сохраняем параметры для экспорта в JSON
            args.clustering_params = {
                'min_common_urls': args.serp_similarity_threshold,
                'max_cluster_size': max_size,
                'mode': args.serp_mode,
                'serp_region': args.serp_region,
                'serp_device': args.serp_device,
                'serp_site': args.serp_site,
                'group_name': self.group_name
            }
            
            # Устанавливаем дефолтные значения из RUN_CONFIG
            for key, value in RUN_CONFIG.items():
                if not hasattr(args, key):
                    setattr(args, key, value)
            
            # Создаем анализатор
            analyzer = SEOAnalyzer(args)
            
            # Запускаем pipeline
            import asyncio
            asyncio.run(analyzer.run())
            
            self.finished.emit(True, "Кластеризация завершена успешно")
            
        except Exception as e:
            import traceback
            error_msg = f"Ошибка: {str(e)}\n{traceback.format_exc()}"
            self.finished.emit(False, error_msg)


class ClusteringController:
    """Контроллер управления кластеризацией"""
    
    def __init__(self):
        self.worker: ClusteringWorker = None
    
    def start_clustering(self, group_name: str, params: Dict[str, Any], progress_callback, finished_callback):
        """
        Запустить кластеризацию
        
        Args:
            group_name: Название группы
            params: Параметры кластеризации
            progress_callback: Функция для обновления прогресса
            finished_callback: Функция при завершении
        """
        if self.worker and self.worker.isRunning():
            return False  # Уже запущено
        
        self.worker = ClusteringWorker(group_name, params)
        self.worker.progress.connect(progress_callback)
        self.worker.finished.connect(finished_callback)
        self.worker.start()
        return True
    
    def stop_clustering(self):
        """Остановить кластеризацию"""
        if self.worker and self.worker.isRunning():
            self.worker.terminate()
            self.worker.wait()

