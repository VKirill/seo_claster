"""CLI интерфейс для SEO анализатора (делегирован в args_builder)."""

import asyncio
import sys
from .analyzer import SEOAnalyzer
from .args_builder import create_argument_parser
from .multi_group_runner import MultiGroupRunner
from seo_analyzer.core.query_groups import QueryGroupManager


async def main():
    """Главная функция запуска."""
    # Обрабатываем команду "maxmin" для итеративной кластеризации
    # Формат: "скуд maxmin" или "maxmin"
    # Ищем "maxmin" в аргументах (может быть как отдельный аргумент, так и вместе с группой)
    maxmin_indices = [i for i, arg in enumerate(sys.argv) if arg.lower() == "maxmin"]
    if maxmin_indices:
        # Устанавливаем флаг --maxmin если его еще нет
        if "--maxmin" not in sys.argv:
            # Вставляем --maxmin перед первым найденным "maxmin"
            sys.argv.insert(maxmin_indices[0], "--maxmin")
            # Обновляем индексы после вставки
            maxmin_indices = [i + 1 for i in maxmin_indices]
        
        # Удаляем все вхождения "maxmin" (в обратном порядке, чтобы индексы не сдвигались)
        for idx in reversed(maxmin_indices):
            sys.argv.pop(idx)
    
    # Обрабатываем числовые параметры кластеризации из командной строки
    # Формат: "николай_чудотворец 6 0" -> min_common_urls=6, max_cluster_size=0 (без лимита)
    # Формат: "николай_чудотворец 6" -> min_common_urls=6, max_cluster_size=дефолт
    if len(sys.argv) >= 3:
        try:
            # Проверяем, является ли последний аргумент числом
            last_arg = int(sys.argv[-1])
            # Если предпоследний тоже число - два параметра
            if len(sys.argv) >= 4:
                try:
                    second_last = int(sys.argv[-2])
                    # Два числовых параметра: "группа число1 число2"
                    sys.argv[-3] = f"{sys.argv[-3]} {sys.argv[-2]} {sys.argv[-1]}"
                    sys.argv.pop()  # Удаляем последний
                    sys.argv.pop()  # Удаляем предпоследний
                except ValueError:
                    # Только последний - число: "группа число"
                    sys.argv[-2] = f"{sys.argv[-2]} {sys.argv[-1]}"
                    sys.argv.pop()  # Удаляем последний аргумент (число)
            else:
                # Только последний - число: "группа число"
                sys.argv[-2] = f"{sys.argv[-2]} {sys.argv[-1]}"
                sys.argv.pop()  # Удаляем последний аргумент (число)
        except ValueError:
            # Последний аргумент не число - ничего не делаем
            pass
    
    parser = create_argument_parser()
    args = parser.parse_args()
    
    # Режим: показать список групп
    if hasattr(args, 'list_groups') and args.list_groups:
        _list_groups()
        return
    
    # Определяем режим работы
    input_file = args.input_file
    single_file_mode = hasattr(args, 'single_file') and args.single_file
    
    # Извлекаем числовые параметры из конца input_file
    # Формат: "николай_чудотворец 6 0" -> min_common_urls=6, max_cluster_size=0
    # Формат: "николай_чудотворец 6" -> min_common_urls=6, max_cluster_size=дефолт
    clustering_threshold = None
    max_cluster_size_param = None
    
    if input_file:
        parts = input_file.rsplit(' ', 2)  # Разделяем по последним пробелам
        if len(parts) == 3:
            # Два числовых параметра
            try:
                clustering_threshold = int(parts[1])
                max_cluster_size_param = int(parts[2])
                input_file = parts[0]  # Обновляем input_file без чисел
                args.input_file = input_file
                args.serp_similarity_threshold = clustering_threshold
                # Если max_cluster_size = 0, устанавливаем очень большое значение (без лимита)
                if max_cluster_size_param == 0:
                    args.max_cluster_size = 999999  # Практически без лимита
                    print(f"📊 Параметры кластеризации: min_common_urls = {clustering_threshold}, max_cluster_size = без лимита")
                else:
                    args.max_cluster_size = max_cluster_size_param
                    print(f"📊 Параметры кластеризации: min_common_urls = {clustering_threshold}, max_cluster_size = {max_cluster_size_param}")
            except ValueError:
                # Не числа - пробуем один параметр
                parts = input_file.rsplit(' ', 1)
                if len(parts) == 2:
                    try:
                        clustering_threshold = int(parts[1])
                        input_file = parts[0]
                        args.input_file = input_file
                        args.serp_similarity_threshold = clustering_threshold
                        print(f"📊 Параметр кластеризации: min_common_urls = {clustering_threshold}")
                    except ValueError:
                        pass
        elif len(parts) == 2:
            # Один числовой параметр
            try:
                clustering_threshold = int(parts[1])
                input_file = parts[0]
                args.input_file = input_file
                args.serp_similarity_threshold = clustering_threshold
                print(f"📊 Параметр кластеризации: min_common_urls = {clustering_threshold}")
            except ValueError:
                pass
    
    # Сохраняем параметры для использования в именах файлов
    if clustering_threshold is None:
        clustering_threshold = getattr(args, 'serp_similarity_threshold', None)
    args.clustering_threshold = clustering_threshold
    
    # Сохраняем max_cluster_size для суффикса файлов
    if max_cluster_size_param is not None:
        args.max_cluster_size_param = max_cluster_size_param
    else:
        args.max_cluster_size_param = None
    
    # Если не указан файл и не режим одного файла - обрабатываем все группы
    if input_file is None and not single_file_mode:
        print("📁 Режим: Обработка всех групп из semantika/")
        print("   (для одного файла используйте: python main.py <имя_группы>)")
        print()
        
        runner = MultiGroupRunner(args)
        # По умолчанию последовательная обработка групп
        await runner.run_all_groups(parallel=False)
        return
    
    # Если указан файл - проверяем, это группа или путь
    if input_file:
        # Проверяем, есть ли такая группа
        manager = QueryGroupManager()
        groups = manager.discover_groups()
        
        # Ищем группу по имени (без расширения)
        group_name = input_file.replace('.csv', '')
        group = manager.get_group(group_name)
        
        if group and group.input_file.exists():
            # Это группа - обрабатываем её
            print(f"📁 Режим: Обработка группы '{group_name}'")
            print()
            args.group = group_name
            analyzer = SEOAnalyzer(args)
            await analyzer.run()
            return
    
    # Обычный режим: один файл (обратная совместимость)
    if input_file is None:
        # Если не указан файл, берем из конфига
        from seo_analyzer.core.config import RUN_CONFIG
        input_file = RUN_CONFIG["input_file"]
        args.input_file = input_file
    
    print(f"📁 Режим: Обработка файла '{input_file}'")
    print()
    analyzer = SEOAnalyzer(args)
    await analyzer.run()


def _list_groups():
    """Вывод списка доступных групп"""
    manager = QueryGroupManager()
    groups = manager.discover_groups()
    
    if not groups:
        print("⚠️  Группы не найдены в semantika/")
        return
    
    print("=" * 80)
    print(f"📁 Доступные группы запросов ({len(groups)}):")
    print("=" * 80)
    print()
    
    groups_info = manager.list_groups()
    
    for info in groups_info:
        status = "✅" if info['file_exists'] else "❌"
        db_status = "💾" if info['db_exists'] else "  "
        queries = info['queries_count'] or "?"
        
        print(f"{status} {db_status} {info['name']:<20} ({queries} запросов)")
        print(f"      Файл: {info['input_file']}")
        print(f"      Output: {info['output_dir']}")
        print()
    
    print("\nИспользование:")
    print("  python main.py                           # Обработать ВСЕ группы последовательно (по умолчанию)")
    print("  python main.py скуд                      # Обработать только группу 'скуд'")
    print("  python main.py --list-groups             # Показать список групп")
    print()


__all__ = ["create_argument_parser", "main"]


if __name__ == "__main__":
    asyncio.run(main())

