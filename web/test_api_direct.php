<?php
error_reporting(E_ALL);
ini_set('display_errors', 1);

echo "=== Тест API напрямую ===\n\n";

$_GET['action'] = 'get_cache_stats';

echo "Запуск api.php...\n\n";

include 'api.php';

