"""
Multi-Group Runner
Запуск анализа для нескольких групп запросов
"""

import asyncio
from pathlib import Path
from typing import List

from .analyzer import SEOAnalyzer
from seo_analyzer.core.query_groups import QueryGroupManager


class MultiGroupRunner:
    """Запуск анализа для нескольких групп"""
    
    def __init__(self, args):
        """
        Инициализация runner'а
        
        Args:
            args: Аргументы командной строки
        """
        self.args = args
        self.group_manager = QueryGroupManager()
        self.group_manager.discover_groups()
    
    async def run_all_groups(self, parallel: bool = True, unified_serp: bool = True):
        """
        Запуск анализа для всех групп
        
        Args:
            parallel: True для параллельной обработки (рекомендуется)
            unified_serp: True для объединенной обработки SERP всех групп (все запросы вместе)
        """
        groups = self.group_manager.groups
        
        if not groups:
            print("⚠️  Группы не найдены в semantika/")
            return
        
        print("=" * 80)
        print(f"🚀 MULTI-GROUP ANALYSIS - Обработка {len(groups)} групп")
        if unified_serp and len(groups) > 1:
            print("⚡ Режим: ОБЪЕДИНЕННАЯ обработка SERP (все запросы из всех групп вместе)")
            print("   Все запросы распределяются по прокси и обрабатываются одновременно")
        elif parallel and len(groups) > 1:
            print("⚡ Режим: ПАРАЛЛЕЛЬНАЯ обработка (быстрее благодаря SERP кэшу)")
        else:
            print("⚡ Режим: ПОСЛЕДОВАТЕЛЬНАЯ обработка")
        print("=" * 80)
        print()
        
        if unified_serp and len(groups) > 1:
            # Объединенная обработка SERP - все запросы из всех групп вместе
            await self._run_groups_unified_serp(groups)
        elif parallel and len(groups) > 1:
            # Параллельная обработка
            await self._run_groups_parallel(groups)
        else:
            # Последовательная обработка
            await self._run_groups_sequential(groups)
        
        print("\n" + "=" * 80)
        print("✅ ВСЕ ГРУППЫ ОБРАБОТАНЫ")
        print("=" * 80)
        
        # Финальная статистика
        self._print_summary()
    
    async def _run_groups_sequential(self, groups):
        """Последовательная обработка групп"""
        for i, group in enumerate(groups, 1):
            print(f"\n{'=' * 80}")
            print(f"📊 ГРУППА {i}/{len(groups)}: {group.name}")
            print(f"{'=' * 80}\n")
            
            try:
                await self._run_single_group(group)
                print(f"\n✅ Группа '{group.name}' обработана успешно")
                
            except Exception as e:
                print(f"\n❌ Ошибка обработки группы '{group.name}': {e}")
                continue
    
    async def _run_groups_parallel(self, groups):
        """
        Параллельная обработка групп
        
        Преимущества:
        - Если SERP данные уже в БД, группа обрабатывается сразу
        - Пока одна группа ждет API, другие используют кэш
        - Значительно быстрее при наличии кэша
        """
        # Создаем задачи для всех групп
        tasks = []
        for i, group in enumerate(groups, 1):
            task = asyncio.create_task(
                self._run_single_group_safe(group, i, len(groups))
            )
            tasks.append(task)
        
        # Запускаем все задачи параллельно
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Обрабатываем результаты
        for i, (group, result) in enumerate(zip(groups, results), 1):
            if isinstance(result, Exception):
                print(f"\n❌ Группа '{group.name}': Ошибка - {result}")
            else:
                print(f"✅ Группа '{group.name}': Завершена")
    
    async def _run_single_group(self, group):
        """Запуск анализа для одной группы"""
        # Создаем копию аргументов для группы
        group_args = self._prepare_group_args(group)
        
        # Запускаем анализатор
        analyzer = SEOAnalyzer(group_args)
        analyzer.current_group = group
        analyzer.group_manager = self.group_manager
        
        await analyzer.run()
    
    async def _run_single_group_safe(self, group, index: int, total: int):
        """
        Безопасный запуск анализа группы (для параллельной обработки)
        
        Args:
            group: QueryGroup
            index: Номер группы
            total: Всего групп
        """
        print(f"\n{'=' * 80}")
        print(f"📊 ГРУППА {index}/{total}: {group.name} - СТАРТ")
        print(f"{'=' * 80}\n")
        
        try:
            await self._run_single_group(group)
            print(f"\n✅ Группа '{group.name}' обработана успешно")
            return True
            
        except Exception as e:
            print(f"\n❌ Ошибка обработки группы '{group.name}': {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def _prepare_group_args(self, group):
        """Подготовка аргументов для группы"""
        import copy
        group_args = copy.copy(self.args)
        
        # Устанавливаем путь к файлу группы
        group_args.input_file = str(group.input_file)
        group_args.group = group.name
        
        return group_args
    
    async def _run_groups_unified_serp(self, groups):
        """
        Объединенная обработка SERP для всех групп
        
        Все запросы из всех групп собираются в один список и обрабатываются вместе,
        распределяясь по прокси. Это позволяет максимально эффективно использовать
        все доступные прокси и обрабатывать запросы параллельно.
        """
        print("=" * 80)
        print("🔄 ОБЪЕДИНЕННАЯ ОБРАБОТКА SERP - Сбор всех запросов из всех групп")
        print("=" * 80)
        print()
        
        # Собираем все запросы из всех групп
        all_queries = []
        query_to_group_map = {}  # Словарь для сохранения group_name при сохранении
        
        for group in groups:
            print(f"📁 Загружаем группу: {group.name}")
            try:
                # Загружаем данные группы
                group_args = self._prepare_group_args(group)
                analyzer = SEOAnalyzer(group_args)
                analyzer.current_group = group
                analyzer.group_manager = self.group_manager
                
                # Загружаем данные (только загрузка, без обработки)
                from pipeline.stages.data_loader import load_data_stage
                await load_data_stage(group_args, analyzer)
                
                # Собираем запросы из группы
                group_queries = analyzer.df['keyword'].tolist() if not analyzer.df.empty else []
                all_queries.extend(group_queries)
                
                # Сохраняем соответствие запрос -> группа
                for query in group_queries:
                    query_to_group_map[query] = group.name
                
                print(f"   ✓ Загружено {len(group_queries)} запросов из группы '{group.name}'")
                
            except Exception as e:
                print(f"   ❌ Ошибка загрузки группы '{group.name}': {e}")
                continue
        
        if not all_queries:
            print("⚠️  Не найдено запросов для обработки")
            return
        
        print()
        print(f"📊 ВСЕГО ЗАПРОСОВ ДЛЯ ОБРАБОТКИ: {len(all_queries)}")
        print(f"   Из {len(groups)} групп")
        print()
        
        # Обрабатываем все запросы вместе через первую группу (для получения настроек)
        first_group = groups[0]
        group_args = self._prepare_group_args(first_group)
        
        # Создаем временный анализатор для обработки SERP
        analyzer = SEOAnalyzer(group_args)
        analyzer.current_group = first_group
        analyzer.group_manager = self.group_manager
        
        # Загружаем данные первой группы (для получения настроек)
        # НО: пропускаем загрузку из CSV, так как мы уже собрали все запросы
        # Просто создаем DataFrame со всеми запросами
        import pandas as pd
        analyzer.df = pd.DataFrame({'keyword': all_queries})
        
        # Устанавливаем флаги чтобы не пытаться загружать из CSV
        analyzer.loaded_from_cache = False
        analyzer.loaded_from_master_db = False
        
        # Загружаем только словари и настройки (без данных)
        from seo_analyzer.core.helpers import load_all_data, load_intent_weights
        analyzer.keyword_dicts, analyzer.geo_dicts, analyzer.stopwords = await load_all_data()
        analyzer.intent_weights = await load_intent_weights()
        
        # Обрабатываем SERP для всех запросов вместе
        from pipeline.stages.serp_analyzer import analyze_serp_stage
        
        # Передаем query_to_group_map через analyzer
        analyzer.query_to_group_map = query_to_group_map
        
        print("=" * 80)
        print("🚀 НАЧАЛО ОБЪЕДИНЕННОЙ ОБРАБОТКИ SERP")
        print("=" * 80)
        print()
        
        await analyze_serp_stage(group_args, analyzer)
        
        # После обработки SERP запускаем остальные этапы для каждой группы отдельно
        print()
        print("=" * 80)
        print("🔄 ЗАВЕРШЕНИЕ ОБРАБОТКИ ОСТАЛЬНЫХ ЭТАПОВ ДЛЯ КАЖДОЙ ГРУППЫ")
        print("=" * 80)
        print()
        
        for i, group in enumerate(groups, 1):
            print(f"\n{'=' * 80}")
            print(f"📊 ГРУППА {i}/{len(groups)}: {group.name} - ЗАВЕРШЕНИЕ")
            print(f"{'=' * 80}\n")
            
            try:
                await self._run_single_group(group)
                print(f"\n✅ Группа '{group.name}' обработана успешно")
                
            except Exception as e:
                print(f"\n❌ Ошибка обработки группы '{group.name}': {e}")
                continue
    
    def _print_summary(self):
        """Вывод финальной статистики"""
        print(f"\n📊 Финальная статистика:")
        print(f"  Всего групп: {len(self.group_manager.groups)}")
        
        for group in self.group_manager.groups:
            # Проверяем наличие output директории вместо БД (БД теперь общая)
            status = "✅" if group.output_dir.exists() else "❌"
            print(f"  {status} {group.name}: {group.output_dir}")

