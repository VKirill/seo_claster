"""Основной класс SEO анализатора (оркестратор)"""

import asyncio
from seo_analyzer.core.config import get_output_dir
from seo_analyzer.core.output_manager import OutputManager

from .stages import (
    load_data_stage,
    preprocessing_stage,
    classification_stage,
    analyze_serp_stage,
    calculate_metrics_stage,
    preload_yandex_direct_stage,
    enrich_with_yandex_direct_stage,
    clustering_stage,
    postprocess_clusters_stage,
    aggregate_cluster_lsi_stage,
    generate_forms_stage,
    export_results_stage,
)


class SEOAnalyzer:
    """Главный класс SEO анализатора"""
    
    def __init__(self, args):
        """Инициализация анализатора"""
        self.args = args
        
        # Система групп
        self.current_group = None
        self.group_manager = None
        
        # Output manager (будет обновлен после загрузки данных)
        self.output_manager = OutputManager()
        self.output_dir = self.output_manager.output_dir
        
        # Данные
        self.df = None
        self.keyword_dicts = None
        self.geo_dicts = None
        self.stopwords = None
        self.intent_weights = None
        
        # Компоненты
        self.normalizer = None
        self.intent_classifier = None
        self.brand_detector = None
        self.funnel_classifier = None
        self.page_mapper = None
        self.forms_generator = None
        
        # Кластеризаторы
        self.deduplicator = None
        self.semantic_clusterer = None
        self.structure_clusterer = None
        self.topic_modeler = None
        self.hierarchical_clusterer = None
        self.difficulty_scorer = None
        self.graph_builder = None
        
        # SERP и метрики
        self.serp_analyzer = None
        self.metrics_calculator = None
        self.lsi_aggregator = None
        
        # Экспортеры
        self.csv_exporter = None
        self.json_exporter = None
        self.graph_exporter = None
        self.html_visualizer = None
        self.excel_exporter = None
        
        # Асинхронные задачи
        self.yandex_direct_task = None  # Фоновая задача сбора данных Direct
    
    async def run(self):
        """Запуск полного pipeline"""
        print("=" * 80)
        print("🚀 SEO ANALYZER - Запуск анализа")
        print("=" * 80)
        print()
        
        try:
            # Этап 1: Загрузка данных
            await load_data_stage(self.args, self)
            
            # Обновляем output manager после загрузки группы
            if hasattr(self, 'current_group') and self.current_group:
                self.output_manager = OutputManager(self.current_group)
                self.output_dir = self.output_manager.output_dir
            
            # Этап 2: Предобработка
            await preprocessing_stage(self.args, self)
            
            # 🚀 ЗАПУСК ФОНОВОЙ ЗАДАЧИ: Предзагрузка Yandex Direct (асинхронно)
            # Запускаем сразу после preprocessing - данные загрузятся в кэш
            # пока идут SERP анализ, кластеризация и метрики
            try:
                from pipeline.stages.yandex_direct_preloader import _should_run_yandex_direct
                should_run, _ = _should_run_yandex_direct()
                
                if should_run:
                    self.yandex_direct_task = asyncio.create_task(
                        preload_yandex_direct_stage(self.args, self)
                    )
                    print("🚀 Yandex Direct предзагрузка запущена в фоне")
                    print()
            except Exception:
                # Если не удалось проверить - просто пропускаем
                pass
            
            # Этап 3: SERP анализ (ПЕРВЫМ! База для всего остального)
            await analyze_serp_stage(self.args, self)
            
            # ⚡ ПАРАЛЛЕЛЬНАЯ ОБРАБОТКА: Этапы 4-7
            # Кластеризация, классификация и метрики независимы друг от друга
            print("⚡ Запуск параллельной обработки (кластеризация + классификация + метрики)...")
            print()
            
            # Группа A: Параллельные задачи после SERP
            clustering_task = asyncio.create_task(self._run_clustering_pipeline())
            classification_task = asyncio.create_task(classification_stage(self.args, self))
            metrics_task = asyncio.create_task(calculate_metrics_stage(self.args, self))
            
            # Ждем завершения всех задач
            await asyncio.gather(
                clustering_task,
                classification_task,
                metrics_task,
                return_exceptions=False
            )
            
            print()
            print("✅ Параллельная обработка завершена")
            
            # ========================================
            # СОХРАНЕНИЕ В MASTER DB (ПОСЛЕ всех этапов)
            # ========================================
            # Сохраняем ВСЕ данные в единую таблицу для быстрых экспериментов
            # Важно: сохраняем ПОСЛЕ классификации и метрик, чтобы все поля были заполнены
            await self._save_to_master_db()
            
            # Этап 7.5: Ожидание предзагрузки и обогащение Yandex Direct
            if self.yandex_direct_task:
                print("⏳ Ожидание завершения предзагрузки Yandex Direct...")
                await self.yandex_direct_task
                print("✓ Предзагрузка завершена, данные в кэше")
                print()
                
                # Теперь обогащаем DataFrame данными из кэша (мгновенно)
                await enrich_with_yandex_direct_stage(self.args, self)
            
            # Этап 8: Генерация форм (только для топ-запросов)
            if not self.args.skip_forms:
                await generate_forms_stage(self.args, self)
            
            # Этап 9: Экспорт результатов
            await export_results_stage(self.args, self)
            
            print()
            print("=" * 80)
            print("✅ АНАЛИЗ ЗАВЕРШЕН!")
            print("=" * 80)
            print(f"📂 Результаты сохранены в: {self.output_dir}")
        
        finally:
            # Всегда закрываем SERP анализатор (даже при ошибке)
            if self.serp_analyzer:
                await self.serp_analyzer.close()
    
    async def _run_clustering_pipeline(self):
        """
        Полный пайплайн кластеризации (последовательный внутри себя)
        
        Этапы:
        1. Кластеризация
        2. Пост-обработка кластеров
        3. Агрегация LSI (если доступна)
        """
        # Этап 4: Кластеризация (на основе SERP данных)
        await clustering_stage(self.args, self)
        
        # Этап 4.1: Пост-обработка кластеров
        await postprocess_clusters_stage(self.args, self)
        
        # Этап 5: Агрегация LSI по кластерам
        if 'lsi_phrases' in self.df.columns and 'semantic_cluster_id' in self.df.columns:
            await aggregate_cluster_lsi_stage(self.args, self)
    
    async def _save_to_master_db(self):
        """Сохранение всех данных в Master Query Database"""
        try:
            from seo_analyzer.core.cache.master_query_db import MasterQueryDatabase
            import hashlib
            from pathlib import Path
            
            # Определяем группу
            if hasattr(self, 'current_group') and self.current_group:
                group_name = self.current_group.name
                csv_path = self.current_group.input_file
            else:
                group_name = "default"
                csv_path = Path(self.args.input_file) if hasattr(self.args, 'input_file') else None
            
            # Вычисляем hash CSV
            csv_hash = None
            if csv_path and csv_path.exists():
                with open(csv_path, 'rb') as f:
                    csv_hash = hashlib.md5(f.read()).hexdigest()
            
            print()
            print("=" * 80)
            print(f"💾 Сохранение в Master Query Database")
            print("=" * 80)
            print(f"  Группа: {group_name}")
            print(f"  Запросов: {len(self.df)}")
            print()
            
            # Инициализируем Master DB
            master_db = MasterQueryDatabase()
            
            # Сохраняем
            master_db.save_queries(
                group_name=group_name,
                df=self.df,
                csv_path=csv_path,
                csv_hash=csv_hash
            )
            
            # Статистика
            stats = master_db.get_statistics(group_name)
            print()
            print("📊 Статистика сохранения:")
            print(f"  ✓ Всего запросов: {stats['total_queries']:,}")
            print(f"  ✓ С интентом: {stats['with_intent']:,}")
            print(f"  ✓ С SERP данными: {stats['with_serp']:,}")
            print(f"  ✓ С Yandex Direct: {stats['with_direct']:,}")
            print(f"  ✓ Средний KEI: {stats['avg_kei']:.2f}")
            print()
            print("💡 Теперь при изменении параметров кластеризации (min_common_urls и т.д.)")
            print("   данные будут загружаться из Master DB мгновенно!")
            print()
            
        except Exception as e:
            print(f"⚠️  Ошибка сохранения в Master DB: {e}")
            print("   Продолжаем без сохранения...")

