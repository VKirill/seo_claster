"""
Скрипт для дособора LSI фраз из имеющихся URL данных

Находит запросы с заполненными serp_top_urls, но пустыми serp_lsi_phrases.
- Если URL в формате словарей с title/snippet/passages - извлекает LSI из них
- Если URL только строки и есть serp_req_id - запрашивает данные через API

Использование:
    python recover_missing_lsi.py [имя_группы]
    
    Если группа не указана - обрабатывает все группы в базе данных.
    Если указана - обрабатывает только указанную группу.
"""

import sys
from pathlib import Path

# Добавляем корневую директорию в путь
root_dir = Path(__file__).parent.parent.parent
sys.path.insert(0, str(root_dir))

import os
from pathlib import Path
from seo_analyzer.analysis.serp.analyzer import SERPAnalyzer
from seo_analyzer.core.config import SERP_CONFIG
from seo_analyzer.core.config_paths import OUTPUT_DIR


def get_api_key():
    """
    Получает API ключ из разных источников
    
    Returns:
        API ключ или None
    """
    api_key = None
    
    # 1. Пробуем config_local.py
    try:
        import config_local
        api_key = getattr(config_local, 'XMLSTOCK_API_KEY', None)
        if api_key:
            print("✓ API ключ загружен из config_local.py")
            return api_key
    except ImportError:
        pass
    
    # 2. Пробуем переменную окружения
    api_key = os.getenv('XMLSTOCK_API_KEY')
    if api_key:
        print("✓ API ключ загружен из переменной окружения")
        return api_key
    
    return None


def main():
    # Группа опциональна - если не указана, обрабатываем все группы
    group_name = sys.argv[1] if len(sys.argv) >= 2 else None
    
    print("=" * 80)
    if group_name:
        print(f"🔄 ДОСОБОР LSI ФРАЗ ДЛЯ ГРУППЫ: {group_name}")
    else:
        print("🔄 ДОСОБОР LSI ФРАЗ ДЛЯ ВСЕХ ГРУПП")
    print("=" * 80)
    print()
    
    # Проверяем наличие базы данных
    db_path = OUTPUT_DIR / "master_queries.db"
    if not db_path.exists():
        print(f"❌ База данных не найдена: {db_path}")
        print(f"   Убедитесь, что файл существует в папке output/")
        sys.exit(1)
    
    print(f"📂 База данных: {db_path}")
    print()
    
    # Получаем API ключ
    api_key = get_api_key()
    if not api_key:
        print("❌ API ключ xmlstock не найден!")
        print("   Способ 1: создайте config_local.py (см. config_local.py.example)")
        print("   Способ 2: export XMLSTOCK_API_KEY=user:key")
        sys.exit(1)
    
    # Инициализируем SERP Analyzer (группа может быть None)
    # SERPAnalyzer сам создаст MasterQueryDatabase внутри, если use_master_db=True
    analyzer = SERPAnalyzer(
        api_key=api_key,
        lr=SERP_CONFIG.get('api', {}).get('lr', 213),
        query_group=group_name,  # Может быть None для обработки всех групп
        use_master_db=True
    )
    
    # Запускаем дособор LSI
    print()
    import asyncio
    updated_count = asyncio.run(analyzer.recover_missing_lsi_from_urls(group_name=group_name))
    
    print()
    print("=" * 80)
    if updated_count > 0:
        print(f"✅ Успешно обновлено {updated_count} запросов")
        print()
        if group_name:
            print("📋 Следующие шаги:")
            print(f"   1. Пересоздать экспорты: python scripts/utils/rebuild_exports.py {group_name}")
            print("   2. Проверить результаты в Excel файле")
        else:
            print("📋 Следующие шаги:")
            print("   1. Пересоздать экспорты для нужных групп:")
            print("      python scripts/utils/rebuild_exports.py <имя_группы>")
            print("   2. Проверить результаты в Excel файлах")
    else:
        print("ℹ️  Нет запросов для обновления")
        print("   Возможные причины:")
        print("   - Все запросы уже имеют LSI фразы")
        print("   - У запросов нет URL данных")
        print("   - У запросов нет req_id для повторного запроса")
    print("=" * 80)
    print()


if __name__ == '__main__':
    main()

