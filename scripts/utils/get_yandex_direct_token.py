"""
Скрипт для получения OAuth токена Yandex Direct.

Использование:
    python get_yandex_direct_token.py

Токен автоматически сохраняется в config_local.py
"""

import sys
from pathlib import Path

# Добавляем путь к модулям
sys.path.insert(0, str(Path(__file__).parent))

from seo_analyzer.core.yandex_oauth_helper import YandexOAuthHelper, save_token_to_config


def main():
    """Основная функция."""
    
    print("\n" + "="*70)
    print("ПОЛУЧЕНИЕ OAUTH ТОКЕНА ДЛЯ YANDEX DIRECT API")
    print("="*70)
    
    # Пытаемся загрузить client_id и client_secret из конфига
    try:
        from config_local import (
            YANDEX_DIRECT_CLIENT_ID,
            YANDEX_DIRECT_CLIENT_SECRET
        )
        
        print("\n✅ Найдены данные приложения в config_local.py")
        print(f"   Client ID: {YANDEX_DIRECT_CLIENT_ID}")
        
    except ImportError:
        print("\n⚠️  Не найдены YANDEX_DIRECT_CLIENT_ID и YANDEX_DIRECT_CLIENT_SECRET")
        print("\nДобавьте в config_local.py:")
        print("""
YANDEX_DIRECT_CLIENT_ID = "ваш_client_id"
YANDEX_DIRECT_CLIENT_SECRET = "ваш_client_secret"
        """)
        
        # Запрашиваем вручную
        print("\nИли введите их сейчас:")
        YANDEX_DIRECT_CLIENT_ID = input("Client ID (ID приложения): ").strip()
        YANDEX_DIRECT_CLIENT_SECRET = input("Client Secret (Пароль): ").strip()
        
        if not YANDEX_DIRECT_CLIENT_ID or not YANDEX_DIRECT_CLIENT_SECRET:
            print("\n❌ Client ID и Secret обязательны!")
            return
    
    # Создаем helper
    oauth = YandexOAuthHelper(
        client_id=YANDEX_DIRECT_CLIENT_ID,
        client_secret=YANDEX_DIRECT_CLIENT_SECRET
    )
    
    # Интерактивная авторизация
    token = oauth.interactive_auth()
    
    if token:
        # Сохраняем в конфиг
        save_token_to_config(token)
        
        print("\n" + "="*70)
        print("✅ ГОТОВО!")
        print("="*70)
        print("\nТеперь можете использовать интеграцию Yandex Direct:")
        print("1. Проверьте config_local.py - токен сохранен")
        print("2. Запустите тест: python test_yandex_direct_integration.py")
        print("3. Используйте в своем коде")
        print()
    else:
        print("\n❌ Не удалось получить токен")
        print("Проверьте Client ID и Secret")


if __name__ == "__main__":
    main()

