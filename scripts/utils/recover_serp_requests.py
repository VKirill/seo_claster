"""
Восстановление незавершённых SERP запросов
Используется после падения скрипта для докачки данных
"""

from pathlib import Path
from seo_analyzer.core.cache.master_query_db import MasterQueryDatabase


def main():
    """Восстановление незавершённых SERP запросов"""
    print("=" * 80)
    print("Восстановление незавершённых SERP запросов")
    print("=" * 80)
    
    master_db_path = Path("output/master_queries.db")
    
    if not master_db_path.exists():
        print("\n❌ Master DB не найдена: output/master_queries.db")
        print("   Запустите сначала анализ для создания БД")
        return
    
    master_db = MasterQueryDatabase(master_db_path)
    
    # Получаем список групп
    import sqlite3
    conn = sqlite3.connect(master_db_path)
    cursor = conn.execute("SELECT DISTINCT group_name FROM master_queries ORDER BY group_name")
    groups = [row[0] for row in cursor.fetchall()]
    conn.close()
    
    if not groups:
        print("\n❌ Группы не найдены в Master DB")
        return
    
    print(f"\nНайдено групп: {len(groups)}")
    
    # Проверяем каждую группу
    total_pending = 0
    groups_with_pending = []
    
    for group in groups:
        stats = master_db.get_serp_statistics(group)
        pending_count = stats['pending'] + stats['processing']
        
        if pending_count > 0:
            groups_with_pending.append((group, pending_count, stats))
            total_pending += pending_count
    
    if not groups_with_pending:
        print("\n✅ Все SERP запросы завершены!")
        print("\nСтатистика по группам:")
        for group in groups:
            stats = master_db.get_serp_statistics(group)
            print(f"\n{group}:")
            print(f"  ✓ Всего: {stats['total']:,}")
            print(f"  ✓ Завершено: {stats['completed']:,} ({stats['completion_rate']:.1%})")
            print(f"  ✓ С данными: {stats['with_data']:,}")
            if stats['error'] > 0:
                print(f"  ⚠️  Ошибок: {stats['error']:,}")
        return
    
    # Показываем группы с незавершёнными запросами
    print(f"\n⚠️  Найдено {total_pending:,} незавершённых SERP запросов в {len(groups_with_pending)} группах:")
    print()
    
    for i, (group, pending_count, stats) in enumerate(groups_with_pending, 1):
        print(f"{i}. {group}")
        print(f"   • Всего запросов: {stats['total']:,}")
        print(f"   • Завершено: {stats['completed']:,} ({stats['completion_rate']:.1%})")
        print(f"   • Pending: {stats['pending']:,}")
        print(f"   • Processing: {stats['processing']:,}")
        if stats['error'] > 0:
            print(f"   • Ошибок: {stats['error']:,}")
        print()
    
    print("Что делать дальше:")
    print()
    print("1️⃣  Показать список незавершённых запросов")
    print("2️⃣  Сбросить статус pending → completed (если данные уже есть)")
    print("3️⃣  Запустить докачку через main.py --resume-serp")
    print("0️⃣  Выход")
    
    choice = input("\nВаш выбор: ").strip()
    
    if choice == "1":
        # Показать список
        print("\nВыберите группу:")
        for i, (group, _, _) in enumerate(groups_with_pending, 1):
            print(f"  {i}. {group}")
        
        idx = input("\nНомер группы: ").strip()
        try:
            group_idx = int(idx) - 1
            if 0 <= group_idx < len(groups_with_pending):
                selected_group = groups_with_pending[group_idx][0]
                
                pending = master_db.get_pending_serp_queries(selected_group)
                
                print(f"\nНезавершённые запросы ({len(pending)}):")
                print("=" * 80)
                
                for item in pending[:50]:  # Первые 50
                    status_icon = "⏳" if item['serp_status'] == 'pending' else "🔄"
                    req_id = f" (req_id: {item['serp_req_id']})" if item['serp_req_id'] else ""
                    error = f" - {item['serp_error_message']}" if item['serp_error_message'] else ""
                    
                    print(f"{status_icon} {item['keyword']}{req_id}{error}")
                
                if len(pending) > 50:
                    print(f"\n... и ещё {len(pending) - 50} запросов")
            else:
                print("❌ Неверный номер")
        except ValueError:
            print("❌ Введите число")
    
    elif choice == "2":
        # Сбросить статусы
        print("\nВыберите группу:")
        for i, (group, _, _) in enumerate(groups_with_pending, 1):
            print(f"  {i}. {group}")
        
        idx = input("\nНомер группы: ").strip()
        try:
            group_idx = int(idx) - 1
            if 0 <= group_idx < len(groups_with_pending):
                selected_group = groups_with_pending[group_idx][0]
                
                print(f"\n⚠️  ВНИМАНИЕ: Это сбросит статус pending/processing → completed")
                print("   Используйте только если данные УЖЕ загружены вручную")
                
                confirm = input("\nПродолжить? (yes/no): ").strip().lower()
                
                if confirm == 'yes':
                    import sqlite3
                    conn = sqlite3.connect(master_db_path)
                    cursor = conn.cursor()
                    
                    cursor.execute('''
                        UPDATE master_queries
                        SET serp_status = 'completed',
                            serp_updated_at = CURRENT_TIMESTAMP
                        WHERE group_name = ? 
                          AND serp_status IN ('pending', 'processing')
                          AND serp_found_docs IS NOT NULL
                    ''', (selected_group,))
                    
                    updated = cursor.rowcount
                    conn.commit()
                    conn.close()
                    
                    print(f"\n✓ Обновлено {updated} запросов")
                else:
                    print("\nОтменено")
            else:
                print("❌ Неверный номер")
        except ValueError:
            print("❌ Введите число")
    
    elif choice == "3":
        # Инструкция по докачке
        print("\n📋 Инструкция по докачке:")
        print()
        print("1. Запустите анализ с флагом --force-serp:")
        print()
        print("   python main.py --force-serp")
        print()
        print("2. Скрипт автоматически найдёт незавершённые запросы")
        print("   и докачает их из xmlstock")
        print()
        print("3. Используйте --resume-pending для загрузки ТОЛЬКО pending запросов:")
        print()
        print("   python main.py --resume-pending")
        print()
    
    elif choice == "0":
        print("\nВыход...")
        return
    
    else:
        print("\n❌ Неверный выбор")


if __name__ == "__main__":
    main()

