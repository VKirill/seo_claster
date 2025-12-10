"""
Автоматическая проверка и получение токена Yandex Direct при запуске.

Если токен не указан - предлагает получить его интерактивно.
"""

from .yandex_oauth_helper import YandexOAuthHelper, save_token_to_config


def ensure_yandex_direct_token(client_id: str, client_secret: str, 
                                current_token: str = "") -> str:
    """
    Проверяет наличие токена, если нет - получает интерактивно.
    
    Args:
        client_id: ID приложения Yandex
        client_secret: Пароль приложения
        current_token: Текущий токен (может быть пустым)
        
    Returns:
        OAuth токен (существующий или новый)
    """
    # Если токен уже есть - возвращаем его
    if current_token and current_token.strip():
        return current_token.strip()
    
    # Токена нет - предлагаем получить
    print("\n" + "="*70)
    print("⚠️  YANDEX DIRECT: Токен не найден")
    print("="*70)
    print("\nДля получения данных из Yandex Direct API нужен OAuth токен.")
    print("\nВарианты:")
    print("  1. Получить токен сейчас (откроется браузер)")
    print("  2. Пропустить (Yandex Direct будет отключен)")
    print("  3. Получить позже (запустите: python get_yandex_direct_token.py)")
    
    choice = input("\nВыберите (1/2/3): ").strip()
    
    if choice == "1":
        print("\n🔄 Получение токена...")
        oauth = YandexOAuthHelper(client_id, client_secret)
        token = oauth.interactive_auth()
        
        if token:
            # Сохраняем в config_local.py
            save_token_to_config(token, "config_local.py")
            print("\n✅ Токен получен и сохранен в config_local.py")
            return token
        else:
            print("\n❌ Не удалось получить токен")
            return ""
    elif choice == "3":
        print("\nℹ️  Запустите позже: python get_yandex_direct_token.py")
        return ""
    else:
        print("\nℹ️  Yandex Direct отключен для этого запуска")
        return ""

