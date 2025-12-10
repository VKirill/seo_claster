"""
Округление float значений в существующих БД до 3 знаков после запятой

Обновляет:
- domain_stats.offer_info_ratio, confidence
- domain_group_stats (нет float полей, только INTEGER)
- domain_global_stats.commercial_ratio, confidence_score
"""

import sqlite3
from pathlib import Path


def round_float(value: float) -> float:
    """Округление до 3 знаков"""
    if value is None:
        return None
    return round(value, 3)


def round_serp_data_db():
    """Округлить float в output/serp_data.db"""
    db_path = Path("output/serp_data.db")
    
    if not db_path.exists():
        print(f"⚠️  БД не найдена: {db_path}")
        return
    
    print("=" * 80)
    print("🔄 ОКРУГЛЕНИЕ FLOAT В БД")
    print("=" * 80)
    print()
    print(f"БД: {db_path}")
    print()
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # 1. Таблица: domain_stats (offer_info_ratio, confidence)
        print("📊 Обновление domain_stats...")
        cursor.execute("""
            SELECT id, offer_info_ratio, confidence 
            FROM domain_stats
            WHERE offer_info_ratio IS NOT NULL OR confidence IS NOT NULL
        """)
        
        rows = cursor.fetchall()
        print(f"   Найдено записей: {len(rows)}")
        
        updated = 0
        for row_id, ratio, conf in rows:
            new_ratio = round_float(ratio) if ratio is not None else None
            new_conf = round_float(conf) if conf is not None else None
            
            cursor.execute("""
                UPDATE domain_stats 
                SET offer_info_ratio = ?, confidence = ?
                WHERE id = ?
            """, (new_ratio, new_conf, row_id))
            updated += 1
        
        print(f"   ✓ Обновлено: {updated} записей")
        print()
        
        # 2. Таблица: domain_global_stats (commercial_ratio, confidence_score)
        print("📊 Обновление domain_global_stats...")
        
        # Проверяем существование таблицы
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='domain_global_stats'
        """)
        
        if cursor.fetchone():
            cursor.execute("""
                SELECT domain, commercial_ratio, confidence_score 
                FROM domain_global_stats
                WHERE commercial_ratio IS NOT NULL OR confidence_score IS NOT NULL
            """)
            
            rows = cursor.fetchall()
            print(f"   Найдено записей: {len(rows)}")
            
            updated = 0
            for domain, ratio, conf in rows:
                new_ratio = round_float(ratio) if ratio is not None else None
                new_conf = round_float(conf) if conf is not None else None
                
                cursor.execute("""
                    UPDATE domain_global_stats 
                    SET commercial_ratio = ?, confidence_score = ?
                    WHERE domain = ?
                """, (new_ratio, new_conf, domain))
                updated += 1
            
            print(f"   ✓ Обновлено: {updated} записей")
        else:
            print(f"   ⚠️  Таблица domain_global_stats не найдена (это нормально для старых БД)")
        
        print()
        
        # Коммит изменений
        conn.commit()
        
        print("=" * 80)
        print("✅ ОКРУГЛЕНИЕ ЗАВЕРШЕНО")
        print("=" * 80)
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()
        conn.rollback()
    
    finally:
        conn.close()


def round_group_databases():
    """Округлить float во всех БД групп"""
    groups_dir = Path("output/groups")
    
    if not groups_dir.exists():
        print("⚠️  Директория групп не найдена")
        return
    
    group_dbs = list(groups_dir.glob("*/serp_data.db"))
    
    if not group_dbs:
        print("⚠️  БД групп не найдены")
        return
    
    print()
    print("=" * 80)
    print(f"🔄 ОКРУГЛЕНИЕ FLOAT В БД ГРУПП ({len(group_dbs)} БД)")
    print("=" * 80)
    print()
    
    for db_path in group_dbs:
        group_name = db_path.parent.name
        print(f"📁 Группа: {group_name}")
        
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        try:
            # domain_stats в БД группы (если есть)
            cursor.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name='domain_stats'
            """)
            
            if cursor.fetchone():
                cursor.execute("""
                    SELECT id, offer_info_ratio, confidence 
                    FROM domain_stats
                    WHERE offer_info_ratio IS NOT NULL OR confidence IS NOT NULL
                """)
                
                rows = cursor.fetchall()
                
                if rows:
                    for row_id, ratio, conf in rows:
                        new_ratio = round_float(ratio) if ratio is not None else None
                        new_conf = round_float(conf) if conf is not None else None
                        
                        cursor.execute("""
                            UPDATE domain_stats 
                            SET offer_info_ratio = ?, confidence = ?
                            WHERE id = ?
                        """, (new_ratio, new_conf, row_id))
                    
                    print(f"   ✓ Обновлено: {len(rows)} записей")
            
            conn.commit()
            
        except Exception as e:
            print(f"   ⚠️  Ошибка: {e}")
            conn.rollback()
        
        finally:
            conn.close()
        
        print()


def main():
    """Главная функция"""
    print()
    print("🔢 Округление float значений в БД до 3 знаков после запятой")
    print()
    
    # Основная БД
    round_serp_data_db()
    
    # БД групп
    round_group_databases()
    
    print()
    print("✅ Все БД обновлены!")
    print()
    print("💡 Теперь все новые данные будут автоматически округляться при записи")
    print()


if __name__ == "__main__":
    main()

