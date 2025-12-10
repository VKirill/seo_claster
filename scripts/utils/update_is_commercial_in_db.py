"""
Обновляет флаг is_commercial для всех документов в БД
используя новую логику определения коммерческих доменов
"""

import sqlite3
from pathlib import Path
from seo_analyzer.core.serp_enricher.document_extractor import is_commercial_domain

def update_commercial_flags():
    """Обновляет флаги is_commercial в БД"""
    
    db_path = "output/serp_data.db"
    if not Path(db_path).exists():
        print(f"❌ База данных не найдена: {db_path}")
        return
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Получаем все documents
    cursor.execute("SELECT id, domain, is_commercial FROM serp_documents")
    documents = cursor.fetchall()
    
    print(f"📊 Найдено документов в БД: {len(documents)}")
    
    updated = 0
    changed_to_commercial = 0
    changed_to_info = 0
    
    for doc_id, domain, old_is_comm in documents:
        # Определяем новое значение
        new_is_comm = is_commercial_domain(domain or '')
        
        # Если изменилось - обновляем
        if old_is_comm != new_is_comm:
            cursor.execute(
                "UPDATE serp_documents SET is_commercial = ? WHERE id = ?",
                (new_is_comm, doc_id)
            )
            updated += 1
            
            if new_is_comm:
                changed_to_commercial += 1
            else:
                changed_to_info += 1
    
    conn.commit()
    
    print(f"✅ Обновлено флагов: {updated}")
    print(f"  → Стали коммерческими: {changed_to_commercial}")
    print(f"  → Стали информационными: {changed_to_info}")
    
    # Теперь обновляем метрики в serp_results
    print(f"\n📊 Обновление метрик в serp_results...")
    
    cursor.execute("SELECT id FROM serp_results")
    serp_ids = cursor.fetchall()
    
    for (serp_id,) in serp_ids:
        # Считаем коммерческие домены
        cursor.execute("""
            SELECT COUNT(*) FROM serp_documents
            WHERE serp_result_id = ? AND is_commercial = 1
        """, (serp_id,))
        comm_count = cursor.fetchone()[0]
        
        # Считаем информационные домены
        cursor.execute("""
            SELECT COUNT(*) FROM serp_documents
            WHERE serp_result_id = ? AND is_commercial = 0
        """, (serp_id,))
        info_count = cursor.fetchone()[0]
        
        # Обновляем метрики
        cursor.execute("""
            UPDATE serp_results
            SET commercial_domains = ?, info_domains = ?
            WHERE id = ?
        """, (comm_count, info_count, serp_id))
    
    conn.commit()
    conn.close()
    
    print(f"✅ Метрики обновлены для {len(serp_ids)} запросов")

if __name__ == "__main__":
    update_commercial_flags()

