"""
Скрипт для очистки базы данных SERP
Используется при необходимости пересобрать данные с обновленным кодом

⚠️ DEPRECATED: Этот скрипт использует устаревшую БД serp_data.db
Все данные теперь хранятся в master_queries.db (MasterQueryDatabase)
Для очистки данных используйте MasterQueryDatabase напрямую.
"""

import sys
from pathlib import Path

# SERPDatabase удалён - используйте MasterQueryDatabase
# from seo_analyzer.core.serp_database import SERPDatabase

print("⚠️  ВНИМАНИЕ: Этот скрипт устарел!")
print("   serp_data.db больше не используется.")
print("   Все данные теперь в master_queries.db")
print("   Используйте MasterQueryDatabase для управления данными.")
sys.exit(1)


def clear_database(force: bool = False):
    """Очистка базы данных SERP"""
    
    db_path = Path("output/serp_data.db")
    
    if not db_path.exists():
        print("✓ База данных не найдена, создание не требуется")
        return
    
    print("=" * 80)
    print("ОЧИСТКА БАЗЫ ДАННЫХ SERP")
    print("=" * 80)
    print()
    
    # Показываем статистику перед очисткой
    db = SERPDatabase()
    stats = db.get_statistics()
    
    print(f"📊 Текущая статистика БД:")
    print(f"   Всего запросов: {stats.get('total_queries', 0)}")
    print(f"   Всего документов: {stats.get('total_documents', 0)}")
    print(f"   Размер БД: {stats.get('db_size_mb', 0):.2f} MB")
    print()
    
    if not force:
        print("⚠️  ВНИМАНИЕ: Все данные SERP будут удалены!")
        print("   Для повторного сбора данных потребуются новые API запросы к xmlstock")
        print()
        
        response = input("   Продолжить? (yes/no): ").strip().lower()
        if response not in ['yes', 'y', 'да']:
            print("\n✗ Очистка отменена")
            return
        print()
    
    # Очищаем все данные
    deleted = db.clear_old_data(days=0)  # days=0 удаляет все записи
    
    print(f"✓ Удалено {deleted} записей из БД")
    
    # Показываем статистику после очистки
    stats = db.get_statistics()
    print(f"✓ Новый размер БД: {stats.get('db_size_mb', 0):.2f} MB")
    print()
    
    print("=" * 80)
    print("БАЗА ДАННЫХ ОЧИЩЕНА")
    print("=" * 80)
    print()
    print("Следующий запуск main.py с --enable-serp пересоберет данные")
    print("с использованием обновленного кода извлечения текста.")


if __name__ == '__main__':
    force = '--force' in sys.argv or '-f' in sys.argv
    clear_database(force)

