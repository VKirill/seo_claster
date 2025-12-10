"""
Обработка кэширования и синхронизации CSV
"""

from pathlib import Path
from seo_analyzer.export.csv import save_filtered_queries


class CacheHandler:
    """Обработчик кэширования"""
    
    @staticmethod
    def sync_csv_from_cache_if_needed(args, analyzer, print_stage):
        """
        Синхронизирует CSV файл с данными из кэша если это требуется
        
        Args:
            args: Аргументы командной строки
            analyzer: Экземпляр SEOAnalyzer
            print_stage: Функция для логирования
        """
        input_file = None
        
        if hasattr(analyzer, 'current_group') and analyzer.current_group:
            input_file = analyzer.current_group.input_file
        elif hasattr(args, 'input_file') and args.input_file:
            input_file = Path(args.input_file)
            if not input_file.is_absolute():
                input_file = Path.cwd() / input_file
        
        if not input_file or not input_file.exists():
            return
        
        try:
            import pandas as pd
            original_df = pd.read_csv(input_file, encoding='utf-8-sig')
            original_count = len(original_df)
            cached_count = len(analyzer.df)
            
            if original_count > cached_count:
                duplicates_in_csv = original_count - cached_count
                
                print_stage(f"\n⚠️  Обнаружено расхождение:")
                print_stage(f"   📄 В CSV файле: {original_count} запросов")
                print_stage(f"   💾 В кэше: {cached_count} запросов (без дублей)")
                print_stage(f"   🗑️  Дубликатов в CSV: {duplicates_in_csv}")
                print_stage(f"\n💾 Перезапись CSV с очищенными данными...")
                
                success = save_filtered_queries(
                    analyzer.df,
                    input_file,
                    backup=True
                )
                
                if success:
                    print_stage(f"✓ CSV файл синхронизирован с кэшем")
                    print_stage(f"✓ Удалено {duplicates_in_csv} дублей из исходного файла")
                else:
                    print_stage(f"❌ Не удалось синхронизировать CSV файл")
        
        except Exception:
            pass
    
    @staticmethod
    def save_to_cache(analyzer, total_duplicates, print_stage):
        """
        Сохранить данные в кэш
        
        Args:
            analyzer: Экземпляр SEOAnalyzer
            total_duplicates: Общее количество удаленных дублей
            print_stage: Функция для логирования
        """
        if hasattr(analyzer, 'query_cache') and hasattr(analyzer, 'current_group') and analyzer.current_group:
            print_stage("\n💾 Сохранение обработанных запросов в кэш...")
            
            analyzer.query_cache.save_queries(
                group_name=analyzer.current_group.name,
                csv_path=analyzer.current_group.input_file,
                df=analyzer.df,
                duplicates_removed=total_duplicates
            )
            
            print_stage(f"  ⚡ Следующий запуск будет мгновенным (без предобработки)")
            print_stage(f"  💡 CSV обновлён, кэш синхронизирован")
    
    @staticmethod
    def save_filtered_to_csv(args, analyzer, print_stage):
        """
        Сохранить отфильтрованные запросы в CSV
        
        Args:
            args: Аргументы командной строки
            analyzer: Экземпляр SEOAnalyzer
            print_stage: Функция для логирования
        """
        input_file = None
        
        if hasattr(analyzer, 'current_group') and analyzer.current_group:
            input_file = analyzer.current_group.input_file
        elif hasattr(args, 'input_file') and args.input_file:
            input_file = Path(args.input_file)
            if not input_file.is_absolute():
                input_file = Path.cwd() / input_file
        
        if input_file:
            success = save_filtered_queries(
                analyzer.df,
                input_file,
                backup=True
            )
            
            if success:
                print_stage(f"✓ Исходный файл обновлен (без дублей, только целевые запросы)")
            else:
                print_stage(f"⚠️  Не удалось обновить исходный файл")
        else:
            print_stage(f"⚠️  Не удалось определить путь к исходному файлу")

