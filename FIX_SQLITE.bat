@echo off
chcp 65001 > nul
cls
color 0E

echo.
echo    ╔══════════════════════════════════════════════════════════════════╗
echo    ║              🔧 ИСПРАВЛЕНИЕ SQLite В PHP                         ║
echo    ╚══════════════════════════════════════════════════════════════════╝
echo.
echo    Проблема: SQLite драйвер не включен в PHP
echo    Решение: Добавим extension=pdo_sqlite и extension=sqlite3
echo.

REM Найти php.ini
for /f "tokens=*" %%i in ('php --ini ^| findstr "Loaded Configuration File"') do set PHP_INI_LINE=%%i
set PHP_INI=%PHP_INI_LINE:*: =%

if "%PHP_INI%"=="" (
    echo    ❌ Файл php.ini не найден!
    echo    Используем php.ini-development
    set PHP_INI=C:\php83\php.ini-development
)

echo    📁 Файл конфигурации: %PHP_INI%
echo.

if not exist "%PHP_INI%" (
    if exist "C:\php83\php.ini" (
        set PHP_INI=C:\php83\php.ini
    ) else if exist "C:\php83\php.ini-development" (
        copy /Y "C:\php83\php.ini-development" "C:\php83\php.ini" > nul
        set PHP_INI=C:\php83\php.ini
        echo    ✅ Создан php.ini из php.ini-development
    ) else (
        echo    ❌ Не найден php.ini в C:\php83\
        pause
        exit /b 1
    )
)

echo    📝 Проверяем текущие расширения...
echo.

REM Проверяем, включены ли уже
findstr /C:"extension=pdo_sqlite" "%PHP_INI%" > nul 2>&1
if %errorlevel% equ 0 (
    echo    ℹ️  extension=pdo_sqlite уже есть в php.ini
) else (
    echo    ➕ Добавляем extension=pdo_sqlite
    echo extension=pdo_sqlite >> "%PHP_INI%"
)

findstr /C:"extension=sqlite3" "%PHP_INI%" > nul 2>&1
if %errorlevel% equ 0 (
    echo    ℹ️  extension=sqlite3 уже есть в php.ini
) else (
    echo    ➕ Добавляем extension=sqlite3
    echo extension=sqlite3 >> "%PHP_INI%"
)

echo.
echo    ══════════════════════════════════════════════════════════════════
echo    ✅ Расширения добавлены в php.ini
echo    ══════════════════════════════════════════════════════════════════
echo.
echo    🔍 Проверяем...
echo.

php -m | findstr -i sqlite
if %errorlevel% equ 0 (
    color 0A
    echo.
    echo    ══════════════════════════════════════════════════════════════════
    echo    ✅ SQLite успешно включен!
    echo    ══════════════════════════════════════════════════════════════════
    echo.
    echo    Теперь можно запускать:
    echo       start_manager.bat
    echo.
) else (
    color 0C
    echo.
    echo    ══════════════════════════════════════════════════════════════════
    echo    ⚠️  SQLite все еще не активен
    echo    ══════════════════════════════════════════════════════════════════
    echo.
    echo    Возможные причины:
    echo       1. Нет файлов php_sqlite3.dll и php_pdo_sqlite.dll
    echo       2. Закомментированы точкой с запятой ;extension=...
    echo.
    echo    Ручное решение:
    echo       1. Откройте: %PHP_INI%
    echo       2. Найдите строки:
    echo          ;extension=pdo_sqlite
    echo          ;extension=sqlite3
    echo       3. Удалите ; в начале строк
    echo       4. Сохраните файл
    echo.
)

pause





