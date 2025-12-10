"""
Создаёт таблицу domain_stats в БД с автоматической классификацией доменов.

Анализирует offer_info из XML и создаёт статистику по доменам.
"""
import sqlite3
import re
import json
from collections import Counter, defaultdict
import sys
from pathlib import Path

# Добавляем путь к модулям проекта
sys.path.insert(0, str(Path(__file__).parent))

from seo_analyzer.core.number_formatter import round_float


def parse_offer_info_from_xml(xml_text: str) -> int:
    """Подсчитывает количество offer_info в XML"""
    if not xml_text or 'offer_info' not in xml_text:
        return 0
    
    offers = re.findall(r'<offer_info>(.*?)</offer_info>', xml_text, re.DOTALL)
    return len(offers)


def extract_domain_from_url(url: str) -> str:
    """Извлекает домен из URL"""
    if not url:
        return ""
    
    url = re.sub(r'^https?://', '', url)
    domain = url.split('/')[0]
    domain = re.sub(r'^www\.', '', domain)
    
    return domain.lower()


def create_domain_stats_table(db_path: str = 'output/master_queries.db'):
    """Создаёт и заполняет таблицу domain_stats"""
    
    print("=" * 80)
    print("🗄️  СОЗДАНИЕ ТАБЛИЦЫ DOMAIN_STATS")
    print("=" * 80)
    print()
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Создаём таблицу
    print("📋 Создание таблицы domain_stats...")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS domain_stats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            domain TEXT NOT NULL UNIQUE,
            total_documents INTEGER DEFAULT 0,
            offer_info_count INTEGER DEFAULT 0,
            offer_info_ratio REAL DEFAULT 0.0,
            classification TEXT DEFAULT 'unknown',
            confidence REAL DEFAULT 0.0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_domain_classification ON domain_stats(classification)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_domain_offers ON domain_stats(offer_info_count)")
    
    print("✓ Таблица создана")
    print()
    
    # Анализируем домены
    print("📊 Анализ доменов из serp_documents...")
    
    cursor.execute("""
        SELECT domain, COUNT(*) as total
        FROM serp_documents
        WHERE domain IS NOT NULL AND domain != ''
        GROUP BY domain
    """)
    
    domain_stats = {}
    for domain, total in cursor.fetchall():
        domain_clean = re.sub(r'^www\.', '', domain.lower())
        domain_stats[domain_clean] = {'total': total, 'offers': 0}
    
    print(f"✓ Найдено {len(domain_stats)} уникальных доменов")
    print()
    
    # Подсчитываем offer_info
    print("🔍 Подсчёт offer_info...")
    
    cursor.execute("SELECT xml_response FROM serp_results WHERE xml_response IS NOT NULL")
    
    xml_processed = 0
    for (xml,) in cursor.fetchall():
        xml_processed += 1
        
        if xml_processed % 1000 == 0:
            print(f"  Обработано XML: {xml_processed}")
        
        if not xml or 'offer_info' not in xml:
            continue
        
        # Извлекаем URL и offer_info
        doc_urls = re.findall(r'<url>(.*?)</url>', xml)
        offer_count = parse_offer_info_from_xml(xml)
        
        if offer_count == 0:
            continue
        
        # Распределяем offer_info по доменам пропорционально
        for url in doc_urls[:30]:
            domain = extract_domain_from_url(url)
            if domain and domain in domain_stats:
                domain_stats[domain]['offers'] += 1
    
    print(f"✓ Обработано {xml_processed} XML")
    print()
    
    # Классификация доменов
    print("🏷️  Классификация доменов...")
    
    COMMERCIAL_RATIO_THRESHOLD = 0.3  # 30%+ документов с offer_info = коммерческий
    MIN_OFFERS_ABSOLUTE = 50  # Или 50+ offers в абсолютном значении
    MIN_DOCS = 50  # Минимум документов для классификации
    
    # Информационные паттерны (более строгие - только явные)
    info_patterns = [
        r'wiki', r'blog', r'forum', r'otvet', r'answer',
        r'habr', r'dzen', r'vc\.ru',
        r'youtube', r'rutube', r'vk\.com', r'ok\.ru',
        r'docs\.', r'doc\.', r'support\.', r'help\.',
        r'news', r'media'
    ]
    
    commercial_count = 0
    informational_count = 0
    unknown_count = 0
    
    # Очищаем таблицу
    cursor.execute("DELETE FROM domain_stats")
    
    for domain, stats in domain_stats.items():
        total = stats['total']
        offers = stats['offers']
        
        # Пропускаем домены с малым количеством документов
        if total < MIN_DOCS:
            continue
        
        ratio = round_float(offers / total if total > 0 else 0)
        
        # Классификация по СТРОГИМ правилам
        # 1. Высокое соотношение offer_info (>30%) ИЛИ много offers (>50)
        if ratio >= COMMERCIAL_RATIO_THRESHOLD or offers >= MIN_OFFERS_ABSOLUTE:
            # Проверяем что это НЕ информационный паттерн
            if any(re.search(pattern, domain.lower()) for pattern in info_patterns):
                # Информационный домен даже если есть offers (реклама/партнёрки)
                classification = 'informational'
                confidence = 0.9
                informational_count += 1
            else:
                # Настоящий коммерческий
                classification = 'commercial'
                confidence = round_float(min(ratio * 3, 1.0))
                commercial_count += 1
        # 2. Информационные паттерны (почти без offers)
        elif any(re.search(pattern, domain.lower()) for pattern in info_patterns):
            classification = 'informational'
            confidence = round_float(0.8)
            informational_count += 1
        # 3. Неизвестные
        else:
            classification = 'unknown'
            confidence = round_float(0.5)
            unknown_count += 1
        
        # Вставляем в БД
        cursor.execute("""
            INSERT INTO domain_stats 
            (domain, total_documents, offer_info_count, offer_info_ratio, classification, confidence)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (domain, total, offers, ratio, classification, confidence))
    
    conn.commit()
    
    print(f"✓ Классифицировано доменов:")
    print(f"  Коммерческих: {commercial_count}")
    print(f"  Информационных: {informational_count}")
    print(f"  Неизвестных: {unknown_count}")
    print()
    
    # Показываем статистику
    print("📊 СТАТИСТИКА ПО ТИПАМ:")
    print("-" * 80)
    
    for classification in ['commercial', 'informational']:
        cursor.execute("""
            SELECT domain, offer_info_count, total_documents, offer_info_ratio
            FROM domain_stats
            WHERE classification = ?
            ORDER BY offer_info_count DESC
            LIMIT 10
        """, (classification,))
        
        rows = cursor.fetchall()
        
        print(f"\n{classification.upper()}:")
        for domain, offers, total, ratio in rows:
            print(f"  {domain:40s} {offers:4d} offers / {total:5d} docs ({ratio*100:5.1f}%)")
    
    conn.close()
    
    print()
    print("=" * 80)
    print("✅ Таблица domain_stats создана и заполнена!")
    print()
    print("📝 Теперь классификация доменов берётся из БД!")


if __name__ == "__main__":
    create_domain_stats_table()

