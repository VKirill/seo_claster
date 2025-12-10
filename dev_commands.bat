@echo off
REM ========================================
REM  Полезные команды для разработки
REM ========================================

:menu
cls
echo.
echo ╔═══════════════════════════════════════════════════════════╗
echo ║        SEO CLUSTER - Команды разработчика                 ║
echo ╚═══════════════════════════════════════════════════════════╝
echo.
echo  1. Проверить размеры файлов (все)
echo  2. Проверить только большие файлы (^>150 строк)
echo  3. Показать топ-10 самых больших файлов
echo  4. Проверить конкретную директорию
echo  5. Показать структуру проекта
echo  6. Открыть правила кода
echo  7. Открыть архитектуру проекта
echo  0. Выход
echo.
set /p choice="Выберите действие (0-7): "

if "%choice%"=="1" goto check_all
if "%choice%"=="2" goto check_big
if "%choice%"=="3" goto check_top
if "%choice%"=="4" goto check_dir
if "%choice%"=="5" goto show_structure
if "%choice%"=="6" goto show_rules
if "%choice%"=="7" goto show_architecture
if "%choice%"=="0" goto end
goto menu

:check_all
cls
echo Проверка всех файлов в seo_analyzer...
echo.
python check_file_sizes.py
echo.
pause
goto menu

:check_big
cls
echo Файлы превышающие 150 строк:
echo.
powershell -Command "Get-ChildItem -Path 'seo_analyzer' -Recurse -Filter '*.py' | Where-Object { $_.Length -gt 0 } | ForEach-Object { $lines = (Get-Content $_.FullName | Measure-Object -Line).Lines; [PSCustomObject]@{ File = $_.Name; Lines = $lines; Path = $_.DirectoryName } } | Where-Object { $_.Lines -gt 150 } | Sort-Object -Property Lines -Descending | Format-Table -AutoSize"
echo.
pause
goto menu

:check_top
cls
echo Топ-10 самых больших файлов:
echo.
powershell -Command "Get-ChildItem -Path 'seo_analyzer' -Recurse -Filter '*.py' | Where-Object { $_.Length -gt 0 } | ForEach-Object { $lines = (Get-Content $_.FullName | Measure-Object -Line).Lines; [PSCustomObject]@{ File = $_.Name; Lines = $lines } } | Sort-Object -Property Lines -Descending | Select-Object -First 10 | Format-Table -AutoSize"
echo.
pause
goto menu

:check_dir
cls
set /p dir="Введите путь к директории (например, clustering): "
echo.
echo Проверка директории: %dir%
python check_file_sizes.py %dir%
echo.
pause
goto menu

:show_structure
cls
echo Структура проекта seo_analyzer:
echo.
tree /F seo_analyzer
echo.
pause
goto menu

:show_rules
cls
type ПРАВИЛА_КОДА.txt
echo.
pause
goto menu

:show_architecture
cls
echo Открываю АРХИТЕКТУРА_ПРОЕКТА.md...
start АРХИТЕКТУРА_ПРОЕКТА.md
goto menu

:end
exit



