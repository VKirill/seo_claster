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
    
    async def run_all_groups(self, parallel: bool = True):
        """
        Запуск анализа для всех групп
        
        Args:
            parallel: True для параллельной обработки (рекомендуется)
        """
        groups = self.group_manager.groups
        
        if not groups:
            print("⚠️  Группы не найдены в semantika/")
            return
        
        print("=" * 80)
        print(f"🚀 MULTI-GROUP ANALYSIS - Обработка {len(groups)} групп")
        if parallel and len(groups) > 1:
            print("⚡ Режим: ПАРАЛЛЕЛЬНАЯ обработка (быстрее благодаря SERP кэшу)")
        else:
            print("⚡ Режим: ПОСЛЕДОВАТЕЛЬНАЯ обработка")
        print("=" * 80)
        print()
        
        if parallel and len(groups) > 1:
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
    
    def _print_summary(self):
        """Вывод финальной статистики"""
        print(f"\n📊 Финальная статистика:")
        print(f"  Всего групп: {len(self.group_manager.groups)}")
        
        for group in self.group_manager.groups:
            # Проверяем наличие output директории вместо БД (БД теперь общая)
            status = "✅" if group.output_dir.exists() else "❌"
            print(f"  {status} {group.name}: {group.output_dir}")

