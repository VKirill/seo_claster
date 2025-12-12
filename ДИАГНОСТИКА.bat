@echo off
chcp 65001 > nul
cls
color 0B

echo.
echo    ╔══════════════════════════════════════════════════════════════════╗
echo    ║              🔍 ДИАГНОСТИКА ПРОБЛЕМЫ                             ║
echo    ╚══════════════════════════════════════════════════════════════════╝
echo.

REM Проверка 1: PHP
echo    [1/5] Проверка PHP...
php --version > nul 2>&1
if errorlevel 1 (
    echo    ❌ PHP не найден!
    echo       Установите PHP 7.4+ и добавьте в PATH
    goto :error
) else (
    php --version | findstr /C:"PHP"
    echo    ✅ PHP установлен
)
echo.

REM Проверка 2: Файлы
echo    [2/5] Проверка файлов...
if not exist "index.html" (
    echo    ❌ index.html не найден!
    goto :error
)
if not exist "api.php" (
    echo    ❌ api.php не найден!
    goto :error
)
echo    ✅ Файлы на месте (index.html, api.php)
echo.

REM Проверка 3: Порт 8000
echo    [3/5] Проверка порта 8000...
netstat -an | find ":8000" > nul 2>&1
if errorlevel 1 (
    echo    ⚠️  Порт 8000 свободен (сервер НЕ запущен)
    echo       Это нормально, сейчас запустим
) else (
    echo    ✅ Порт 8000 занят (сервер, возможно, уже запущен)
)
echo.

REM Проверка 4: API синтаксис
echo    [4/5] Проверка синтаксиса API...
php -l api.php > nul 2>&1
if errorlevel 1 (
    echo    ❌ Ошибка синтаксиса в api.php!
    php -l api.php
    goto :error
)
echo    ✅ Синтаксис API корректен
echo.

REM Проверка 5: Папки
echo    [5/5] Проверка папок...
if not exist "semantika" (
    echo    ⚠️  Папка semantika не найдена, создаю...
    mkdir semantika
)
if not exist "output" (
    echo    ⚠️  Папка output не найдена, создаю...
    mkdir output
)
echo    ✅ Папки созданы (semantika, output)
echo.

echo    ══════════════════════════════════════════════════════════════════
echo    ✅ ВСЕ ПРОВЕРКИ ПРОЙДЕНЫ!
echo    ══════════════════════════════════════════════════════════════════
echo.
echo    Теперь можно запустить сервер:
echo       1. Запустите start_manager.bat
echo       2. Откройте http://localhost:8000
echo.
echo    Или нажмите любую клавишу для автоматического запуска...
echo.
pause > nul

cls
echo.
echo    🚀 Запускаю сервер...
echo.
timeout /t 2 > nul
start http://localhost:8000
php -S localhost:8000 index.html
goto :end

:error
color 0C
echo.
echo    ══════════════════════════════════════════════════════════════════
echo    ❌ ОБНАРУЖЕНЫ ПРОБЛЕМЫ!
echo    ══════════════════════════════════════════════════════════════════
echo.
echo    Смотрите ошибки выше и исправьте их
echo.
pause
exit /b 1

:end
echo.
echo    Сервер остановлен
pause





