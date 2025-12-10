<?php
/**
 * Router для PHP встроенного сервера
 * Обрабатывает запросы к api.php и index.html
 */

$requestUri = $_SERVER['REQUEST_URI'];
$requestPath = parse_url($requestUri, PHP_URL_PATH);

// Убираем начальный слэш
$requestPath = ltrim($requestPath, '/');

// Если запрос к api.php (с параметрами или без)
if ($requestPath === 'api.php' || strpos($requestPath, 'api.php') === 0) {
    require __DIR__ . '/api.php';
    exit;
}

// Если запрос к корню или index.html
if ($requestPath === '' || $requestPath === '/' || $requestPath === 'index.html') {
    require __DIR__ . '/index.html';
    exit;
}

// Если файл существует в текущей директории, отдаём его
$filePath = __DIR__ . '/' . $requestPath;
if (file_exists($filePath) && is_file($filePath)) {
    return false; // Отдаём файл как есть
}

// По умолчанию отдаём index.html
require __DIR__ . '/index.html';
exit;

