"""HTML шаблоны и JavaScript для дашборда"""

from typing import Dict


def get_javascript() -> str:
    """
    Возвращает JavaScript код для интерактивности
    
    Returns:
        Строка с JavaScript кодом
    """
    return """
    <script>
        // Поиск по кластерам
        function searchClusters() {
            const input = document.getElementById('clusterSearch');
            const filter = input.value.toLowerCase();
            const clusters = document.getElementsByClassName('cluster-card');
            
            for (let i = 0; i < clusters.length; i++) {
                const name = clusters[i].querySelector('.cluster-name').textContent.toLowerCase();
                const queries = clusters[i].querySelector('.queries-list').textContent.toLowerCase();
                
                if (name.includes(filter) || queries.includes(filter)) {
                    clusters[i].classList.remove('hidden');
                } else {
                    clusters[i].classList.add('hidden');
                }
            }
        }
        
        // Переключение видимости запросов
        function toggleQueries(clusterId) {
            const queriesList = document.getElementById('queries-' + clusterId);
            const btn = event.target;
            
            if (queriesList.classList.contains('queries-hidden')) {
                queriesList.classList.remove('queries-hidden');
                btn.textContent = 'Скрыть запросы';
            } else {
                queriesList.classList.add('queries-hidden');
                btn.textContent = 'Показать запросы';
            }
        }
        
        // Анимация чисел при загрузке
        window.addEventListener('load', function() {
            const statValues = document.querySelectorAll('.stat-value');
            statValues.forEach(stat => {
                stat.style.opacity = '0';
                stat.style.transform = 'translateY(20px)';
                setTimeout(() => {
                    stat.style.transition = 'all 0.5s ease';
                    stat.style.opacity = '1';
                    stat.style.transform = 'translateY(0)';
                }, 100);
            });
        });
    </script>
    """


def generate_html_template(
    stats: Dict,
    intent_section: str,
    funnel_section: str,
    clusters_section: str,
    css_styles: str,
    javascript: str
) -> str:
    """
    Генерирует основной HTML шаблон дашборда
    
    Args:
        stats: Статистика анализа
        intent_section: HTML секции интентов
        funnel_section: HTML секции воронки
        clusters_section: HTML секции кластеров
        css_styles: CSS стили
        javascript: JavaScript код
        
    Returns:
        Полный HTML код дашборда
    """
    return f"""
<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>SEO Analyzer - Дашборд результатов</title>
    <style>
        {css_styles}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🚀 SEO Analyzer - Результаты анализа</h1>
            <p>Кластеризованная семантика с детальной аналитикой</p>
        </div>
        
        <div class="stats-grid">
            <div class="stat-card">
                <div class="stat-label">Всего запросов</div>
                <div class="stat-value">{stats['total_queries']:,}</div>
            </div>
            
            <div class="stat-card">
                <div class="stat-label">Общая частотность</div>
                <div class="stat-value">{stats['total_frequency']:,}</div>
                <div class="stat-subtext">показов в месяц</div>
            </div>
            
            <div class="stat-card">
                <div class="stat-label">Средняя частотность</div>
                <div class="stat-value">{stats['avg_frequency']}</div>
                <div class="stat-subtext">на запрос</div>
            </div>
            
            <div class="stat-card">
                <div class="stat-label">Групп (кластеров)</div>
                <div class="stat-value">{stats.get('n_clusters', 0)}</div>
            </div>
        </div>
        
        {intent_section}
        
        {funnel_section}
        
        {clusters_section}
    </div>
    
    {javascript}
</body>
</html>
"""

