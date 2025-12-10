"""
Помощник для получения OAuth токена Yandex Direct.

Автоматизирует процесс получения токена по client_id и client_secret.
"""

import requests
import webbrowser
from typing import Optional, Tuple
from urllib.parse import urlencode


class YandexOAuthHelper:
    """Получение OAuth токена для Yandex Direct API."""
    
    AUTH_URL = "https://oauth.yandex.ru/authorize"
    TOKEN_URL = "https://oauth.yandex.ru/token"
    
    def __init__(self, client_id: str, client_secret: str):
        """
        Инициализация.
        
        Args:
            client_id: ID приложения (из https://oauth.yandex.ru/)
            client_secret: Пароль приложения
        """
        self.client_id = client_id
        self.client_secret = client_secret
        
    def get_auth_url(self) -> str:
        """
        Получить URL для авторизации.
        
        Returns:
            URL для открытия в браузере
        """
        params = {
            'response_type': 'code',
            'client_id': self.client_id,
            'display': 'popup',
            'scope': 'direct:api'
        }
        
        return f"{self.AUTH_URL}?{urlencode(params)}"
        
    def get_token_from_code(self, auth_code: str) -> Optional[str]:
        """
        Обменять код авторизации на токен.
        
        Args:
            auth_code: Код из URL после авторизации
            
        Returns:
            OAuth токен или None при ошибке
        """
        data = {
            'grant_type': 'authorization_code',
            'code': auth_code,
            'client_id': self.client_id,
            'client_secret': self.client_secret
        }
        
        try:
            response = requests.post(self.TOKEN_URL, data=data, timeout=30)
            
            if response.status_code == 200:
                token_data = response.json()
                return token_data.get('access_token')
            else:
                print(f"Ошибка получения токена: {response.status_code}")
                print(f"Ответ: {response.text}")
                return None
                
        except Exception as e:
            print(f"Ошибка запроса: {e}")
            return None
            
    def interactive_auth(self) -> Optional[str]:
        """
        Интерактивная авторизация через браузер.
        
        Returns:
            OAuth токен или None
        """
        auth_url = self.get_auth_url()
        
        print("\n" + "="*70)
        print("АВТОРИЗАЦИЯ В YANDEX DIRECT")
        print("="*70)
        print(f"\n1. Сейчас откроется браузер с URL авторизации")
        print(f"   Если не откроется, скопируйте эту ссылку:")
        print(f"\n   {auth_url}\n")
        
        # Открываем браузер
        try:
            webbrowser.open(auth_url)
        except:
            pass
            
        print("2. Авторизуйтесь и разрешите доступ к API")
        print("3. Скопируйте КОД из адресной строки после ?code=")
        print("   Пример: https://oauth.yandex.ru/?code=1234567")
        print("   Нужно скопировать: 1234567")
        
        auth_code = input("\nВведите код: ").strip()
        
        if not auth_code:
            print("Код не введен!")
            return None
            
        print("\n⏳ Получение токена...")
        token = self.get_token_from_code(auth_code)
        
        if token:
            print("✅ Токен успешно получен!")
            print(f"\nВаш токен:\n{token}\n")
            return token
        else:
            print("❌ Не удалось получить токен")
            return None


def save_token_to_config(token: str, config_path: str = "config_local.py"):
    """
    Сохранить токен в конфиг файл.
    
    Args:
        token: OAuth токен
        config_path: Путь к файлу конфига
    """
    try:
        # Читаем существующий конфиг
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config_lines = f.readlines()
        except FileNotFoundError:
            # Создаем новый если нет
            config_lines = []
            
        # Ищем строку с YANDEX_DIRECT_TOKEN
        token_line_idx = None
        for i, line in enumerate(config_lines):
            if line.strip().startswith('YANDEX_DIRECT_TOKEN'):
                token_line_idx = i
                break
                
        new_token_line = f'YANDEX_DIRECT_TOKEN = "{token}"\n'
        
        if token_line_idx is not None:
            # Обновляем существующую строку
            config_lines[token_line_idx] = new_token_line
        else:
            # Добавляем новую строку
            config_lines.append(new_token_line)
            
        # Сохраняем
        with open(config_path, 'w', encoding='utf-8') as f:
            f.writelines(config_lines)
            
        print(f"✅ Токен сохранен в {config_path}")
        
    except Exception as e:
        print(f"❌ Ошибка сохранения токена: {e}")


