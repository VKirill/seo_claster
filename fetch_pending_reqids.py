"""
Получение данных по pending req_id
Если данных нет (ошибка 203) - сброс для повторной отправки
"""

import sqlite3
import requests
import time
import sys
import re
from typing import Dict, Any
from xml.etree import ElementTree as ET

# Конфиг XMLStock
USER = "11396"
KEY = "b3c2f28ec0a90b44e486af55c2f6b270"
URL = "https://xmlstock.com/yandex/xml/"

def fetch_by_req_id(req_id: str) -> Dict[str, Any]:
    """
    Получить данные по req_id
    
    Returns:
        {'status': 'completed', 'xml_response': '...'} - успех
        {'status': 'pending'} - ещё не готово (202)
        {'status': 'expired'} - req_id истёк (203)
        {'status': 'error', 'error': '...'} - другая ошибка
    """
    params = {
        'user': USER,
        'key': KEY,
        'req_id': req_id
    }
    
    try:
        response = requests.get(URL, params=params, timeout=30)
        
        if response.status_code != 200:
            return {
                'status': 'error',
                'error': f"HTTP {response.status_code}"
            }
        
        xml_text = response.text
        
        # Проверяем на ошибки
        if '<error' in xml_text:
            # Извлекаем код и текст ошибки
            error_match = re.search(r'<error[^>]*code="([^"]*)"[^>]*>([^<]+)</error>', xml_text)
            if error_match:
                code = error_match.group(1)
                msg = error_match.group(2)
                
                # Код 202 - ещё не готово
                if code == '202':
                    return {'status': 'pending'}
                
                # Код 203 - req_id истёк или не существует
                elif code == '203':
                    return {'status': 'expired', 'error': msg}
                
                # Другие ошибки
                else:
                    return {'status': 'error', 'error': f"API error (code={code}): {msg}"}
            else:
                return {'status': 'error', 'error': xml_text[:200]}
        
        # Успешный ответ
        return {
            'status': 'completed',
            'xml_response': xml_text
        }
    
    except Exception as e:
        return {
            'status': 'error',
            'error': f"{type(e).__name__}: {str(e)}"
        }

def extract_serp_data(xml_text: str) -> Dict[str, Any]:
    """Простое извлечение TOP URLs из XML"""
    try:
        root = ET.fromstring(xml_text)
        
        # Извлекаем TOP URLs
        top_urls = []
        for idx, group in enumerate(root.findall('.//group'), 1):
            doc = group.find('.//doc')
            if doc is not None:
                url_elem = doc.find('.//url')
                domain_elem = doc.find('.//domain')
                title_elem = doc.find('.//title')
                
                if url_elem is not None and url_elem.text:
                    top_urls.append({
                        'url': url_elem.text,
                        'domain': domain_elem.text if domain_elem is not None else '',
                        'position': idx,
                        'title': title_elem.text if title_elem is not None else ''
                    })
        
        return {
            'top_urls': top_urls,
            'found_docs': len(top_urls)
        }
    
    except Exception as e:
        return {'top_urls': [], 'found_docs': 0}

def process_pending_reqids(group_name: str, max_requests: int = 1000, delay: float = 0.5):
    """
    Обработать pending req_id
    
    Args:
        group_name: Название группы
        max_requests: Максимум запросов (чтобы не превысить лимиты)
        delay: Задержка между запросами (сек)
    """
    
    db_path = "output/master_queries.db"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    print("="*80)
    print("🔍 ПОЛУЧЕНИЕ ДАННЫХ ПО PENDING REQ_ID")
    print("="*80)
    print(f"Группа: {group_name}")
    print(f"Задержка между запросами: {delay} сек")
    print(f"Максимум запросов: {max_requests}")
    print()
    
    # Получаем pending запросы с req_id
    cursor.execute('''
        SELECT keyword, serp_req_id
        FROM master_queries 
        WHERE group_name = ? 
          AND serp_status = 'pending'
          AND serp_req_id IS NOT NULL
          AND serp_req_id != ''
        LIMIT ?
    ''', (group_name, max_requests))
    
    pending_list = cursor.fetchall()
    total = len(pending_list)
    
    if total == 0:
        print("✅ Нет pending запросов с req_id")
        conn.close()
        return
    
    print(f"📋 Найдено pending с req_id: {total}")
    print()
    
    # Счётчики
    completed = 0
    still_pending = 0
    expired = 0
    errors = 0
    
    for idx, (keyword, req_id) in enumerate(pending_list, 1):
        keyword_short = keyword[:50] + "..." if len(keyword) > 50 else keyword
        
        # Прогресс каждые 50 запросов
        if idx % 50 == 0 or idx == 1:
            print(f"\n📊 Прогресс: {idx}/{total}")
            print("-"*80)
        
        # Получаем данные
        result = fetch_by_req_id(req_id)
        
        if result['status'] == 'completed':
            # Успех! Извлекаем данные и сохраняем
            serp_data = extract_serp_data(result['xml_response'])
            
            import json
            top_urls_json = json.dumps(serp_data['top_urls'], ensure_ascii=False)
            
            cursor.execute('''
                UPDATE master_queries
                SET 
                    serp_status = 'completed',
                    serp_found_docs = ?,
                    serp_top_urls = ?,
                    serp_updated_at = CURRENT_TIMESTAMP
                WHERE group_name = ? AND keyword = ?
            ''', (serp_data['found_docs'], top_urls_json, group_name, keyword))
            
            completed += 1
            
            if idx % 50 == 0 or idx <= 5:
                print(f"  ✅ {keyword_short:55} | {serp_data['found_docs']} URLs")
        
        elif result['status'] == 'pending':
            # Ещё не готово (202) - оставляем как есть
            still_pending += 1
            
            if idx <= 3:
                print(f"  ⏳ {keyword_short:55} | ещё не готово (202)")
        
        elif result['status'] == 'expired':
            # req_id истёк (203) - СБРАСЫВАЕМ
            cursor.execute('''
                UPDATE master_queries
                SET 
                    serp_status = 'pending',
                    serp_req_id = NULL,
                    serp_error_message = 'req_id expired (203)',
                    serp_updated_at = CURRENT_TIMESTAMP
                WHERE group_name = ? AND keyword = ?
            ''', (group_name, keyword))
            
            expired += 1
            
            if idx <= 3:
                print(f"  ⏰ {keyword_short:55} | req_id истёк → сброс")
        
        else:
            # Ошибка
            error_msg = result.get('error', 'Unknown')
            
            cursor.execute('''
                UPDATE master_queries
                SET 
                    serp_status = 'failed',
                    serp_error_message = ?,
                    serp_updated_at = CURRENT_TIMESTAMP
                WHERE group_name = ? AND keyword = ?
            ''', (error_msg[:500], group_name, keyword))
            
            errors += 1
            
            if idx <= 3:
                print(f"  ❌ {keyword_short:55} | {error_msg[:40]}")
        
        # Commit каждые 100 запросов
        if idx % 100 == 0:
            conn.commit()
        
        # Задержка для rate limit
        time.sleep(delay)
    
    # Финальный commit
    conn.commit()
    
    print()
    print("="*80)
    print("✅ ОБРАБОТКА ЗАВЕРШЕНА")
    print("="*80)
    print(f"  ✅ Успешно получено:  {completed:6} ({completed/total*100:.1f}%)")
    print(f"  ⏳ Ещё обрабатывается: {still_pending:6} ({still_pending/total*100:.1f}%)")
    print(f"  ⏰ req_id истекли:     {expired:6} ({expired/total*100:.1f}%) → сброшены")
    print(f"  ❌ Ошибки:            {errors:6} ({errors/total*100:.1f}%)")
    print("-"*80)
    print(f"  📝 ВСЕГО:            {total:6}")
    print("="*80)
    print()
    
    # Итоговая статистика группы
    cursor.execute('''
        SELECT 
            serp_status,
            COUNT(*) as count
        FROM master_queries 
        WHERE group_name = ?
        GROUP BY serp_status
        ORDER BY count DESC
    ''', (group_name,))
    
    print("📊 СТАТИСТИКА ГРУППЫ ПОСЛЕ ОБРАБОТКИ:")
    print("-"*80)
    for status, count in cursor.fetchall():
        status_display = status if status else 'pending'
        
        if status == 'completed':
            icon = "✅"
        elif status == 'pending':
            icon = "⏸️"
        elif status == 'processing':
            icon = "⏳"
        elif status == 'failed':
            icon = "❌"
        else:
            icon = "❓"
        
        print(f"  {icon} {status_display:15} {count:6}")
    
    # Без URLs
    cursor.execute('''
        SELECT COUNT(*) 
        FROM master_queries 
        WHERE group_name = ? 
          AND (serp_top_urls IS NULL OR serp_top_urls = '' OR serp_top_urls = '[]')
    ''', (group_name,))
    
    without_urls = cursor.fetchone()[0]
    
    print("-"*80)
    print(f"  📋 БЕЗ SERP URL:    {without_urls:6}")
    print("="*80)
    
    conn.close()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Использование: python fetch_pending_reqids.py <группа> [макс_запросов] [задержка]")
        print()
        print("Примеры:")
        print("  python fetch_pending_reqids.py николай_чудотворец")
        print("  python fetch_pending_reqids.py николай_чудотворец 500")
        print("  python fetch_pending_reqids.py николай_чудотворец 500 0.3")
        sys.exit(1)
    
    group_name = sys.argv[1]
    max_requests = int(sys.argv[2]) if len(sys.argv) > 2 else 1000
    delay = float(sys.argv[3]) if len(sys.argv) > 3 else 0.5
    
    process_pending_reqids(group_name, max_requests, delay)





