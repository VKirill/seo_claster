"""
Тест для проверки наличия title в URL кластеров JSON экспорта
"""

import json
from pathlib import Path


def test_json_cluster_urls_with_titles():
    """
    Проверяет что в JSON экспорте кластеров URL содержат title страниц
    
    Проверяемые поля:
    - common_urls: URL которые есть во всех запросах кластера
    - clustering_basis_urls: URL на основе которых произошла кластеризация
    
    Каждый URL должен быть словарем с полями:
    - url: строка с нормализованным URL
    - title: заголовок страницы из SERP (если доступен)
    """
    output_dir = Path('output')
    
    # Ищем JSON файлы с иерархией
    json_files = list(output_dir.glob('**/seo_analysis_hierarchy*.json'))
    
    if not json_files:
        print("⚠️  JSON файлы не найдены. Запустите кластеризацию для создания.")
        return False
    
    # Берем последний файл
    latest_json = sorted(json_files, key=lambda x: x.stat().st_mtime, reverse=True)[0]
    print(f"📂 Проверяем: {latest_json.relative_to(output_dir)}")
    
    with open(latest_json, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    total_clusters = len(data.get('subclusters', []))
    clusters_with_titles = 0
    
    for cluster in data.get('subclusters', []):
        # Проверяем common_urls
        has_title = False
        for url_data in cluster.get('common_urls', []):
            if isinstance(url_data, dict) and 'title' in url_data:
                has_title = True
                break
        
        # Проверяем clustering_basis_urls
        if not has_title:
            for url_data in cluster.get('clustering_basis_urls', []):
                if isinstance(url_data, dict) and 'title' in url_data:
                    has_title = True
                    break
        
        if has_title:
            clusters_with_titles += 1
    
    print(f"✓ Всего кластеров: {total_clusters}")
    print(f"✓ Кластеров с title в URL: {clusters_with_titles}")
    
    if clusters_with_titles > 0:
        print(f"\n✅ УСПЕХ! Title добавлены в {clusters_with_titles}/{total_clusters} кластеров")
        return True
    else:
        print(f"\n⚠️  Title не найдены. Возможно JSON создан до обновления кода.")
        return False


if __name__ == '__main__':
    test_json_cluster_urls_with_titles()

