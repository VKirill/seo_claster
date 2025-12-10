"""
Этап: Предварительная загрузка данных Yandex Direct в фоне.

Запускается сразу после preprocessing, загружает данные для всех запросов
пока выполняются другие этапы (SERP, кластеризация, метрики).
"""

from seo_analyzer.analysis import YandexDirectPreloader
from seo_analyzer.core.yandex_direct_auto_auth import ensure_yandex_direct_token


def _should_run_yandex_direct() -> tuple[bool, dict]:
    """
    Проверить нужно ли запускать Yandex Direct
    
    Returns:
        (should_run, config_dict)
    """
    try:
        from config_local import (
            YANDEX_DIRECT_ENABLED,
            YANDEX_DIRECT_TOKEN,
            YANDEX_DIRECT_CLIENT_ID,
            YANDEX_DIRECT_CLIENT_SECRET,
            YANDEX_DIRECT_USE_SANDBOX
        )
        
        # GEO_ID с дефолтным значением
        try:
            from config_local import YANDEX_DIRECT_GEO_ID
        except ImportError:
            YANDEX_DIRECT_GEO_ID = 213
            
        if not YANDEX_DIRECT_ENABLED:
            return False, {}
            
        if not YANDEX_DIRECT_CLIENT_ID or not YANDEX_DIRECT_CLIENT_SECRET:
            return False, {}
            
        return True, {
            'token': YANDEX_DIRECT_TOKEN,
            'client_id': YANDEX_DIRECT_CLIENT_ID,
            'client_secret': YANDEX_DIRECT_CLIENT_SECRET,
            'use_sandbox': YANDEX_DIRECT_USE_SANDBOX,
            'geo_id': YANDEX_DIRECT_GEO_ID
        }
    except ImportError:
        return False, {}


async def preload_yandex_direct_stage(args, analyzer):
    """
    Предварительная загрузка данных Yandex Direct.
    
    Загружает данные для запросов, которых нет в кэше,
    пока выполняются другие этапы анализа.
    
    Args:
        args: Аргументы командной строки
        analyzer: Экземпляр SEOAnalyzer
        
    Returns:
        None (данные сохраняются в кэш)
    """
    # Проверяем нужно ли запускать
    should_run, config = _should_run_yandex_direct()
    if not should_run:
        return
    
    # Автоматическая проверка и получение токена если нужно
    token = ensure_yandex_direct_token(
        client_id=config['client_id'],
        client_secret=config['client_secret'],
        current_token=config['token']
    )
    
    if not token:
        # Пользователь отказался - тихо пропускаем
        return
    
    print("🚀 ФОНОВАЯ ЗАДАЧА: Предзагрузка данных Yandex Direct")
    print("-" * 80)
    
    # Определяем название региона
    region_names = {
        213: "Москва",
        1: "Москва и область",
        2: "Санкт-Петербург",
        225: "Россия",
        187: "Украина",
        149: "Беларусь",
        159: "Казахстан"
    }
    region_name = region_names.get(config['geo_id'], f"GeoID {config['geo_id']}")
    
    print(f"📍 Регион: {region_name} (GeoID: {config['geo_id']})")
    print(f"🔧 Режим: {'Sandbox' if config['use_sandbox'] else 'Production'}")
    
    # Инициализация preloader
    preloader = YandexDirectPreloader(
        token=token,
        use_sandbox=config['use_sandbox'],
        geo_id=config['geo_id']
    )
    
    # Получаем список запросов
    if analyzer.df is None or analyzer.df.empty:
        print("⚠️  DataFrame пустой, предзагрузка пропущена")
        return
    
    if 'keyword' not in analyzer.df.columns:
        print("⚠️  Колонка 'keyword' не найдена, предзагрузка пропущена")
        return
    
    queries = analyzer.df['keyword'].unique().tolist()
    total_queries = len(queries)
    
    print(f"📦 Всего запросов: {total_queries}")
    
    # Проверяем сколько уже в кэше
    missing = preloader.get_missing_queries(queries)
    cached_count = total_queries - len(missing)
    
    if not missing:
        print(f"✅ Все запросы уже в кэше ({cached_count}), загрузка не требуется")
        print()
        return
    
    print(f"  ✓ В кэше: {cached_count}")
    print(f"  📥 Загружаем: {len(missing)}")
    print()
    
    # Загружаем данные
    stats = preloader.preload_queries(missing, show_progress=True)
    
    print()
    print(f"✅ Предзагрузка завершена:")
    print(f"  • Загружено: {stats['loaded']}")
    print(f"  • Из кэша: {stats['from_cache']}")
    if stats.get('skipped_long', 0) > 0:
        print(f"  • Пропущено (>6 слов): {stats['skipped_long']}")
    if stats['failed'] > 0:
        print(f"  • Ошибки: {stats['failed']}")
    print()

