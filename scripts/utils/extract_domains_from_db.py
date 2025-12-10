"""
Извлечение доменов из SERP базы данных для классификации по интенту.

Анализирует текущую БД и извлекает топ-домены для коммерческих
и информационных запросов.
"""
import sqlite3
from collections import Counter
from urllib.parse import urlparse
import re


def extract_domain(url: str) -> str:
    """
    Извлекает домен из URL.
    
    Удаляет www. префикс, оставляет поддомены (market.yandex.ru)
    """
    try:
        parsed = urlparse(url)
        domain = parsed.netloc or parsed.path
        
        # Убираем www. префикс
        domain = re.sub(r'^www\.', '', domain)
        
        return domain.lower()
    except:
        return ""


def analyze_serp_domains(db_path: str = 'output/serp_data.db'):
    """Анализирует домены в SERP базе данных"""
    
    print("=" * 80)
    print("🔍 АНАЛИЗ ДОМЕНОВ ИЗ SERP БД")
    print("=" * 80)
    print()
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Получаем домены из serp_documents с флагом is_commercial
        cursor.execute("""
            SELECT domain, is_commercial, COUNT(*) as cnt
            FROM serp_documents
            WHERE domain IS NOT NULL AND domain != ''
            GROUP BY domain, is_commercial
        """)
        
        rows = cursor.fetchall()
        print(f"📊 Найдено записей: {len(rows)}")
        print()
        
        # Счетчики доменов
        commercial_domains = Counter()
        informational_domains = Counter()
        all_domains = Counter()
        
        for domain, is_commercial, count in rows:
            domain = domain.lower()
            all_domains[domain] += count
            
            if is_commercial == 1 or is_commercial == True:
                commercial_domains[domain] += count
            else:
                informational_domains[domain] += count
        
        conn.close()
        
        # Выводим статистику
        print(f"🌐 Всего уникальных доменов: {len(all_domains)}")
        print(f"🛒 Коммерческих запросов: {sum(commercial_domains.values())}")
        print(f"📚 Информационных запросов: {sum(informational_domains.values())}")
        print()
        
        # Топ-30 коммерческих доменов
        print("🛒 ТОП-30 КОММЕРЧЕСКИХ ДОМЕНОВ:")
        print("-" * 80)
        for domain, count in commercial_domains.most_common(30):
            print(f"  {domain:40s} ({count} раз)")
        
        print()
        
        # Топ-30 информационных доменов
        print("📚 ТОП-30 ИНФОРМАЦИОННЫХ ДОМЕНОВ:")
        print("-" * 80)
        for domain, count in informational_domains.most_common(30):
            print(f"  {domain:40s} ({count} раз)")
        
        print()
        print("=" * 80)
        
        # Возвращаем для сохранения
        return {
            'commercial': commercial_domains.most_common(50),
            'informational': informational_domains.most_common(50),
            'all': all_domains.most_common(100)
        }
        
    except sqlite3.Error as e:
        print(f"❌ Ошибка при работе с БД: {e}")
        return None
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        return None


def save_domains_to_files(domains_data: dict):
    """
    Сохраняет домены в txt файлы.
    
    Фильтрует только крупные домены (мастодонтов), убирает www.
    """
    if not domains_data:
        print("⚠️ Нет данных для сохранения")
        return
    
    # Минимальное количество упоминаний для "мастодонта"
    MIN_OCCURRENCES = 50
    
    print()
    print("💾 СОХРАНЕНИЕ ДОМЕНОВ В ФАЙЛЫ (только мастодонты)...")
    print("-" * 80)
    
    # Коммерческие домены - фильтруем
    commercial_filtered = [
        (domain, count) for domain, count in domains_data['commercial']
        if count >= MIN_OCCURRENCES
    ]
    
    commercial_file = 'keywords_settings/commercial_domains.txt'
    with open(commercial_file, 'w', encoding='utf-8') as f:
        f.write("# Коммерческие домены (маркетплейсы, магазины, крупные сервисы)\n")
        f.write("# Только мастодонты (>50 упоминаний в SERP)\n")
        f.write("# Добавляйте по одному домену на строку\n")
        f.write("# Комментарии начинаются с #\n")
        f.write("# Формат: domain.ru (без www, с поддоменами если важны)\n\n")
        
        for domain, count in commercial_filtered:
            # Убираем www. если есть
            domain_clean = re.sub(r'^www\.', '', domain)
            f.write(f"{domain_clean}\n")
    
    print(f"✓ Сохранено {len(commercial_filtered)} коммерческих доменов (>{MIN_OCCURRENCES} упоминаний): {commercial_file}")
    
    # Информационные домены - фильтруем
    info_filtered = [
        (domain, count) for domain, count in domains_data['informational']
        if count >= MIN_OCCURRENCES
    ]
    
    info_file = 'keywords_settings/informational_domains.txt'
    with open(info_file, 'w', encoding='utf-8') as f:
        f.write("# Информационные домены (википедия, блоги, форумы, видео)\n")
        f.write("# Только мастодонты (>50 упоминаний в SERP)\n")
        f.write("# Добавляйте по одному домену на строку\n")
        f.write("# Комментарии начинаются с #\n")
        f.write("# Формат: domain.ru (без www, с поддоменами если важны)\n\n")
        
        for domain, count in info_filtered:
            # Убираем www. если есть
            domain_clean = re.sub(r'^www\.', '', domain)
            f.write(f"{domain_clean}\n")
    
    print(f"✓ Сохранено {len(info_filtered)} информационных доменов (>{MIN_OCCURRENCES} упоминаний): {info_file}")
    
    # Все домены (для справки)
    all_file = 'keywords_settings/all_domains_stats.txt'
    with open(all_file, 'w', encoding='utf-8') as f:
        f.write("# Статистика всех доменов из SERP БД\n")
        f.write("# Формат: домен (количество_упоминаний)\n\n")
        
        for domain, count in domains_data['all']:
            f.write(f"{domain} ({count})\n")
    
    print(f"✓ Сохранена статистика всех доменов: {all_file}")
    print()
    print("=" * 80)


if __name__ == "__main__":
    # Анализируем БД
    domains_data = analyze_serp_domains()
    
    # Сохраняем в файлы
    if domains_data:
        save_domains_to_files(domains_data)
        
        print()
        print("✅ ГОТОВО!")
        print()
        print("📝 Следующие шаги:")
        print("   1. Проверьте файлы в keywords_settings/")
        print("   2. Отредактируйте списки доменов вручную при необходимости")
        print("   3. Добавьте свои домены")
        print("   4. Запустите основной анализ - домены будут учитываться автоматически")

