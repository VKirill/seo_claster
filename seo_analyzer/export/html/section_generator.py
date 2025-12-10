"""Генерация HTML секций для дашборда"""

from typing import Dict, List


def generate_intent_section(stats: Dict) -> str:
    """
    Генерирует секцию распределения по интентам
    
    Args:
        stats: Словарь со статистикой
        
    Returns:
        HTML код секции интентов
    """
    if 'intent_dist' not in stats:
        return ""
    
    intent_names = {
        'commercial': 'Коммерческий',
        'informational': 'Информационный',
        'navigational': 'Навигационный',
        'transactional': 'Транзакционный'
    }
    
    total = sum(stats['intent_dist'].values())
    
    html = '<div class="section"><h2 class="section-title">Распределение по интентам</h2><div class="distribution">'
    
    for intent, count in sorted(stats['intent_dist'].items(), key=lambda x: x[1], reverse=True):
        percent = (count / total) * 100 if total > 0 else 0
        name = intent_names.get(intent, intent)
        html += f'''
            <div class="dist-item">
                <div class="dist-label">{name}</div>
                <div class="dist-bar">
                    <div class="dist-fill" style="width: {percent}%">
                        <span class="dist-value">{count} ({percent:.1f}%)</span>
                    </div>
                </div>
            </div>
            '''
    
    html += '</div></div>'
    return html


def generate_funnel_section(stats: Dict) -> str:
    """
    Генерирует секцию распределения по воронке продаж
    
    Args:
        stats: Словарь со статистикой
        
    Returns:
        HTML код секции воронки
    """
    if 'funnel_dist' not in stats:
        return ""
    
    funnel_names = {
        'Awareness': 'Осведомленность',
        'Interest': 'Интерес',
        'Consideration': 'Рассмотрение',
        'Decision': 'Решение',
        'Purchase': 'Покупка'
    }
    
    funnel_order = ['Awareness', 'Interest', 'Consideration', 'Decision', 'Purchase']
    total = sum(stats['funnel_dist'].values())
    
    html = '<div class="section"><h2 class="section-title">Распределение по воронке продаж</h2><div class="distribution">'
    
    for stage in funnel_order:
        if stage in stats['funnel_dist']:
            count = stats['funnel_dist'][stage]
            percent = (count / total) * 100 if total > 0 else 0
            name = funnel_names.get(stage, stage)
            html += f'''
                <div class="dist-item">
                    <div class="dist-label">{name}</div>
                    <div class="dist-bar">
                        <div class="dist-fill" style="width: {percent}%">
                            <span class="dist-value">{count} ({percent:.1f}%)</span>
                        </div>
                    </div>
                </div>
                '''
    
    html += '</div></div>'
    return html


def generate_clusters_section(clusters_data: List[Dict]) -> str:
    """
    Генерирует секцию кластеров
    
    Args:
        clusters_data: Список словарей с данными кластеров
        
    Returns:
        HTML код секции кластеров
    """
    if not clusters_data:
        return ""
    
    intent_names = {
        'commercial': 'Коммерческий',
        'informational': 'Информационный',
        'navigational': 'Навигационный',
        'transactional': 'Транзакционный',
        'unknown': 'Неизвестно'
    }
    
    funnel_names = {
        'Awareness': 'Осведомленность',
        'Interest': 'Интерес',
        'Consideration': 'Рассмотрение',
        'Decision': 'Решение',
        'Purchase': 'Покупка',
        'unknown': 'Неизвестно'
    }
    
    html = '<div class="section">'
    html += '<h2 class="section-title">Сформированные группы запросов</h2>'
    html += '<input type="text" class="search-box" id="clusterSearch" onkeyup="searchClusters()" placeholder="🔍 Поиск по группам и запросам...">'
    
    for cluster in clusters_data:
        intent_label = intent_names.get(cluster.get('main_intent', 'unknown'), 'Неизвестно')
        funnel_label = funnel_names.get(cluster.get('funnel_stage', 'unknown'), 'Неизвестно')
        
        html += f'''
            <div class="cluster-card">
                <div class="cluster-header">
                    <div class="cluster-name">📁 {cluster['name']}</div>
                    <div class="cluster-stats">
                        <div class="cluster-stat">
                            <div class="cluster-stat-value">{cluster['size']}</div>
                            <div class="cluster-stat-label">Запросов</div>
                        </div>
                        <div class="cluster-stat">
                            <div class="cluster-stat-value">{cluster['total_freq']:,}</div>
                            <div class="cluster-stat-label">Частотность</div>
                        </div>
                    </div>
                </div>
                
                <div class="cluster-meta">
                    <span class="badge badge-intent">Интент: {intent_label}</span>
                    <span class="badge badge-funnel">Воронка: {funnel_label}</span>
            '''
        
        if cluster.get('suggested_url'):
            html += f'<span class="badge badge-url">→ {cluster["suggested_url"]}</span>'
        
        html += '</div>'
        
        if cluster.get('top_queries'):
            html += f'''
                <button class="toggle-btn" onclick="toggleQueries({cluster['id']})">Показать запросы</button>
                <div id="queries-{cluster['id']}" class="queries-list queries-hidden">
                '''
            
            for query in cluster['top_queries']:
                keyword = query.get('keyword', '')
                freq = query.get('frequency_world', 0)
                html += f'''
                    <div class="query-item">
                        <span class="query-text">{keyword}</span>
                        <span class="query-freq">{freq:,}</span>
                    </div>
                    '''
            
            html += '</div>'
        
        html += '</div>'
    
    html += '</div>'
    return html

