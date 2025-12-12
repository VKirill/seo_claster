<?php
/**
 * Пример использования переводчика названий задач в таблице AI Usage Logs
 * 
 * Этот файл показывает, как использовать translateTaskName() для перевода
 * технических названий задач на русский язык в таблице
 */

// Подключаем переводчик
require_once __DIR__ . '/task_translator.php';

/**
 * Пример функции для получения данных из БД и отображения таблицы
 */
function displayUsageLogsTable() {
    // Пример данных из БД (замените на реальный запрос)
    $logs = [
        ['id' => 1, 'task' => 'image_generation_photoshoot', 'user' => 'user1', 'timestamp' => '2024-01-01 12:00:00'],
        ['id' => 2, 'task' => 'avatar_analysis_result', 'user' => 'user2', 'timestamp' => '2024-01-01 13:00:00'],
        ['id' => 3, 'task' => 'text_generation', 'user' => 'user3', 'timestamp' => '2024-01-01 14:00:00'],
    ];
    
    echo '<table class="usage-logs-table">';
    echo '<thead>';
    echo '<tr>';
    echo '<th>ID</th>';
    echo '<th>Задача</th>';  // Колонка "Задача"
    echo '<th>Пользователь</th>';
    echo '<th>Время</th>';
    echo '</tr>';
    echo '</thead>';
    echo '<tbody>';
    
    foreach ($logs as $log) {
        echo '<tr>';
        echo '<td>' . htmlspecialchars($log['id']) . '</td>';
        // Применяем перевод названия задачи
        echo '<td class="task-name">' . htmlspecialchars(translateTaskName($log['task'])) . '</td>';
        echo '<td>' . htmlspecialchars($log['user']) . '</td>';
        echo '<td>' . htmlspecialchars($log['timestamp']) . '</td>';
        echo '</tr>';
    }
    
    echo '</tbody>';
    echo '</table>';
}

/**
 * Пример функции для API endpoint, который возвращает JSON
 */
function getUsageLogsJson() {
    // Пример данных из БД
    $logs = [
        ['id' => 1, 'task' => 'image_generation_photoshoot', 'user' => 'user1', 'timestamp' => '2024-01-01 12:00:00'],
        ['id' => 2, 'task' => 'avatar_analysis_result', 'user' => 'user2', 'timestamp' => '2024-01-01 13:00:00'],
    ];
    
    // Применяем перевод к каждой записи
    foreach ($logs as &$log) {
        $log['task'] = translateTaskName($log['task']);
        $log['task_original'] = $log['task']; // Сохраняем оригинальное название, если нужно
    }
    
    header('Content-Type: application/json; charset=utf-8');
    echo json_encode($logs, JSON_UNESCAPED_UNICODE);
}

/**
 * Пример использования в существующем коде
 * 
 * Если у вас уже есть код, который отображает таблицу, просто замените:
 * 
 * БЫЛО:
 * echo '<td>' . $row['task'] . '</td>';
 * 
 * СТАЛО:
 * echo '<td>' . translateTaskName($row['task']) . '</td>';
 * 
 * Или для JSON:
 * 
 * БЫЛО:
 * $result['task'] = $row['task'];
 * 
 * СТАЛО:
 * $result['task'] = translateTaskName($row['task']);
 */
