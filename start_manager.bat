@echo off
chcp 65001 > nul
cls
color 0A

echo.
echo    ╔══════════════════════════════════════════════════════════════╗
echo    ║         🔍 SEO Cluster Manager - Запуск сервера              ║
echo    ╚══════════════════════════════════════════════════════════════╝
echo.
echo    📋 Проверка системы...
echo.

REM Проверка PHP
php --version > nul 2>&1
if errorlevel 1 (
    color 0C
    echo    ❌ ОШИБКА: PHP не найден!
    echo.
    echo    Установите PHP 7.4+ и добавьте в PATH
    echo    Или укажите полный путь к php.exe
    echo.
    pause
    exit /b 1
)

echo    ✅ PHP установлен
echo.

REM Проверка Python
python --version > nul 2>&1
if errorlevel 1 (
    echo    ⚠️  ВНИМАНИЕ: Python не найден (нужен для кластеризации^)
    echo.
) else (
    echo    ✅ Python установлен
    echo.
)

echo    ══════════════════════════════════════════════════════════════
echo.
echo    🚀 Запуск PHP сервера на http://localhost:8080
echo    (Порт 8080 выбран чтобы не конфликтовать с Openfire на 9090)
echo.
echo    После запуска:
echo       • Откроется браузер с интерфейсом
echo       • Интерфейс будет доступен на http://localhost:8080
echo       • Для остановки сервера нажмите Ctrl+C
echo.
echo    ══════════════════════════════════════════════════════════════
echo.

REM Ждём 2 секунды перед открытием браузера
timeout /t 2 > nul

REM Открываем браузер
start http://localhost:8080

REM Запускаем сервер из папки web с указанием корня проекта
cd web
php -S localhost:8080 -t . router.php
cd ..

echo.
echo    ══════════════════════════════════════════════════════════════
echo    Сервер остановлен
echo    ══════════════════════════════════════════════════════════════
echo.
pause

