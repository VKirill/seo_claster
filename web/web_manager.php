<?php
/**
 * SEO Cluster Manager - Web Interface
 * Управление группами, кешами и кластеризацией через веб-интерфейс
 */

// Настройки
$PROJECT_DIR = dirname(__DIR__);
$SEMANTIKA_DIR = $PROJECT_DIR . '/semantika';
$OUTPUT_DIR = $PROJECT_DIR . '/output';
$CACHE_DB = $PROJECT_DIR . '/output/master_queries.db';
$PYTHON_EXE = 'python'; // или полный путь к python.exe

// Создаем необходимые директории
if (!file_exists($SEMANTIKA_DIR)) {
    mkdir($SEMANTIKA_DIR, 0755, true);
}
if (!file_exists($OUTPUT_DIR)) {
    mkdir($OUTPUT_DIR, 0755, true);
}

// Обработка AJAX запросов
if (isset($_GET['action'])) {
    header('Content-Type: application/json; charset=utf-8');
    
    switch ($_GET['action']) {
        case 'list_groups':
            echo json_encode(getGroups());
            break;
        
        case 'get_cache_stats':
            echo json_encode(getCacheStats());
            break;
        
        case 'run_clustering':
            $group = $_POST['group'] ?? 'all';
            echo json_encode(runClustering($group));
            break;
        
        case 'upload_file':
            echo json_encode(handleFileUpload());
            break;
        
        case 'delete_file':
            $filename = $_POST['filename'] ?? '';
            echo json_encode(deleteFile($filename));
            break;
        
        case 'clear_cache':
            echo json_encode(clearCache());
            break;
        
        case 'get_group_info':
            $group = $_GET['group'] ?? '';
            echo json_encode(getGroupInfo($group));
            break;
            
        default:
            echo json_encode(['error' => 'Unknown action']);
    }
    exit;
}

/**
 * Получить список групп
 */
function getGroups() {
    global $SEMANTIKA_DIR;
    
    $groups = [];
    $files = glob($SEMANTIKA_DIR . '/*.csv');
    
    foreach ($files as $file) {
        $basename = basename($file);
        
        // Пропускаем служебные файлы
        if (strpos($basename, '_backup') !== false || 
            strpos($basename, '~') === 0 || 
            strpos($basename, '.') === 0) {
            continue;
        }
        
        $name = pathinfo($basename, PATHINFO_FILENAME);
        $size = filesize($file);
        $modified = filemtime($file);
        
        // Подсчитываем количество строк
        $lines = 0;
        if ($fp = fopen($file, 'r')) {
            while (!feof($fp)) {
                if (fgets($fp)) $lines++;
            }
            fclose($fp);
            $lines = max(0, $lines - 1); // минус заголовок
        }
        
        $groups[] = [
            'name' => $name,
            'file' => $basename,
            'size' => $size,
            'size_human' => formatBytes($size),
            'queries' => $lines,
            'modified' => $modified,
            'modified_human' => date('d.m.Y H:i', $modified)
        ];
    }
    
    // Сортируем по дате изменения (новые первые)
    usort($groups, function($a, $b) {
        return $b['modified'] - $a['modified'];
    });
    
    return $groups;
}

/**
 * Получить статистику кеша (master_queries.db)
 */
function getCacheStats() {
    global $CACHE_DB;
    
    if (!file_exists($CACHE_DB)) {
        return [
            'exists' => false,
            'size' => 0,
            'queries' => 0,
            'groups' => 0,
            'serp_queries' => 0,
            'with_intent' => 0
        ];
    }
    
    try {
        $db = new PDO('sqlite:' . $CACHE_DB);
        $db->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        
        // Проверяем наличие таблицы master_queries
        $tables = $db->query("SELECT name FROM sqlite_master WHERE type='table'")->fetchAll(PDO::FETCH_COLUMN);
        
        if (!in_array('master_queries', $tables)) {
            return [
                'exists' => true,
                'size' => filesize($CACHE_DB),
                'size_human' => formatBytes(filesize($CACHE_DB)),
                'queries' => 0,
                'groups' => 0,
                'serp_queries' => 0,
                'with_intent' => 0,
                'error' => 'Таблица master_queries не найдена'
            ];
        }
        
        // Общее количество запросов
        $queries = $db->query("SELECT COUNT(*) FROM master_queries")->fetchColumn();
        
        // Количество групп
        $groups = $db->query("SELECT COUNT(DISTINCT group_name) FROM master_queries")->fetchColumn();
        
        // Запросы с SERP данными
        $serp_queries = $db->query("SELECT COUNT(*) FROM master_queries WHERE serp_status = 'completed'")->fetchColumn();
        
        // Запросы с классификацией интента
        $with_intent = $db->query("SELECT COUNT(*) FROM master_queries WHERE main_intent IS NOT NULL AND main_intent != ''")->fetchColumn();
        
        return [
            'exists' => true,
            'size' => filesize($CACHE_DB),
            'size_human' => formatBytes(filesize($CACHE_DB)),
            'queries' => (int)$queries,
            'groups' => (int)$groups,
            'serp_queries' => (int)$serp_queries,
            'with_intent' => (int)$with_intent,
            'modified' => date('d.m.Y H:i', filemtime($CACHE_DB))
        ];
    } catch (Exception $e) {
        return [
            'exists' => true,
            'error' => $e->getMessage()
        ];
    }
}

/**
 * Получить информацию о конкретной группе (master_queries.db)
 */
function getGroupInfo($groupName) {
    global $CACHE_DB, $OUTPUT_DIR;
    
    if (!file_exists($CACHE_DB)) {
        return ['error' => 'База данных не найдена'];
    }
    
    try {
        $db = new PDO('sqlite:' . $CACHE_DB);
        $db->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        
        // Проверяем наличие таблицы master_queries
        $tables = $db->query("SELECT name FROM sqlite_master WHERE type='table'")->fetchAll(PDO::FETCH_COLUMN);
        
        if (!in_array('master_queries', $tables)) {
            return [
                'name' => $groupName,
                'cached_queries' => 0,
                'has_results' => false,
                'result_files' => [],
                'error' => 'Таблица master_queries не найдена в БД'
            ];
        }
        
        // Подсчитываем запросы в группе из master_queries
        $stmt = $db->prepare("SELECT COUNT(*) FROM master_queries WHERE group_name = ?");
        $stmt->execute([$groupName]);
        $cached_queries = (int)$stmt->fetchColumn();
        
        // Подсчитываем запросы с SERP данными
        $stmt = $db->prepare("SELECT COUNT(*) FROM master_queries WHERE group_name = ? AND serp_status = 'completed'");
        $stmt->execute([$groupName]);
        $serp_queries = (int)$stmt->fetchColumn();
        
        // Подсчитываем запросы с интентом
        $stmt = $db->prepare("SELECT COUNT(*) FROM master_queries WHERE group_name = ? AND main_intent IS NOT NULL AND main_intent != ''");
        $stmt->execute([$groupName]);
        $intent_queries = (int)$stmt->fetchColumn();
        
        // Проверяем наличие результатов
        $output_group_dir = $OUTPUT_DIR . '/groups/' . $groupName;
        $has_results = file_exists($output_group_dir);
        
        $result_files = [];
        if ($has_results && is_dir($output_group_dir)) {
            $files = glob($output_group_dir . '/*');
            foreach ($files as $file) {
                if (is_file($file)) {
                    $result_files[] = [
                        'name' => basename($file),
                        'size' => formatBytes(filesize($file)),
                        'modified' => date('d.m.Y H:i', filemtime($file))
                    ];
                }
            }
        }
        
        return [
            'name' => $groupName,
            'cached_queries' => $cached_queries,
            'serp_queries' => $serp_queries,
            'intent_queries' => $intent_queries,
            'has_results' => $has_results,
            'result_files' => $result_files
        ];
    } catch (Exception $e) {
        return ['error' => $e->getMessage()];
    }
}

/**
 * Запустить кластеризацию
 */
function runClustering($group) {
    global $PYTHON_EXE, $PROJECT_DIR;
    
    $command = $group === 'all' 
        ? "$PYTHON_EXE main.py" 
        : "$PYTHON_EXE main.py --group " . escapeshellarg($group);
    
    $descriptorspec = [
        0 => ["pipe", "r"],
        1 => ["pipe", "w"],
        2 => ["pipe", "w"]
    ];
    
    $process = proc_open($command, $descriptorspec, $pipes, $PROJECT_DIR);
    
    if (!is_resource($process)) {
        return ['success' => false, 'error' => 'Не удалось запустить процесс'];
    }
    
    // Закрываем stdin
    fclose($pipes[0]);
    
    // Читаем вывод (с таймаутом)
    stream_set_timeout($pipes[1], 2);
    $output = stream_get_contents($pipes[1]);
    fclose($pipes[1]);
    
    $errors = stream_get_contents($pipes[2]);
    fclose($pipes[2]);
    
    $return_value = proc_close($process);
    
    return [
        'success' => $return_value === 0,
        'output' => $output,
        'errors' => $errors,
        'message' => $return_value === 0 
            ? "Кластеризация запущена для группы: $group" 
            : "Ошибка при запуске кластеризации"
    ];
}

/**
 * Обработка загрузки файла
 */
function handleFileUpload() {
    global $SEMANTIKA_DIR;
    
    if (!isset($_FILES['file'])) {
        return ['success' => false, 'error' => 'Файл не выбран'];
    }
    
    $file = $_FILES['file'];
    
    // Проверяем расширение
    $ext = strtolower(pathinfo($file['name'], PATHINFO_EXTENSION));
    if ($ext !== 'csv') {
        return ['success' => false, 'error' => 'Разрешены только CSV файлы'];
    }
    
    // Проверяем размер (макс 50MB)
    if ($file['size'] > 50 * 1024 * 1024) {
        return ['success' => false, 'error' => 'Файл слишком большой (макс 50MB)'];
    }
    
    // Формируем имя файла
    $filename = pathinfo($file['name'], PATHINFO_FILENAME);
    $filename = preg_replace('/[^a-zA-Z0-9_-]/', '_', $filename);
    $target_path = $SEMANTIKA_DIR . '/' . $filename . '.csv';
    
    // Проверяем, существует ли уже файл
    if (file_exists($target_path)) {
        // Создаем бэкап
        $backup_path = $SEMANTIKA_DIR . '/' . $filename . '_backup_' . date('YmdHis') . '.csv';
        copy($target_path, $backup_path);
    }
    
    if (move_uploaded_file($file['tmp_name'], $target_path)) {
        return [
            'success' => true,
            'filename' => basename($target_path),
            'message' => 'Файл успешно загружен'
        ];
    } else {
        return ['success' => false, 'error' => 'Ошибка при сохранении файла'];
    }
}

/**
 * Удалить файл группы
 */
function deleteFile($filename) {
    global $SEMANTIKA_DIR;
    
    $filepath = $SEMANTIKA_DIR . '/' . basename($filename);
    
    if (!file_exists($filepath)) {
        return ['success' => false, 'error' => 'Файл не найден'];
    }
    
    // Создаем бэкап перед удалением
    $backup_path = $SEMANTIKA_DIR . '/' . pathinfo($filename, PATHINFO_FILENAME) . '_deleted_' . date('YmdHis') . '.csv';
    copy($filepath, $backup_path);
    
    if (unlink($filepath)) {
        return [
            'success' => true,
            'message' => 'Файл удален (создан бэкап)',
            'backup' => basename($backup_path)
        ];
    } else {
        return ['success' => false, 'error' => 'Ошибка при удалении файла'];
    }
}

/**
 * Очистить кеш
 */
function clearCache() {
    global $CACHE_DB;
    
    if (!file_exists($CACHE_DB)) {
        return ['success' => false, 'error' => 'Кеш не найден'];
    }
    
    // Создаем бэкап
    $backup = $CACHE_DB . '.backup_' . date('YmdHis');
    if (!copy($CACHE_DB, $backup)) {
        return ['success' => false, 'error' => 'Не удалось создать бэкап'];
    }
    
    if (unlink($CACHE_DB)) {
        return [
            'success' => true,
            'message' => 'Кеш очищен (создан бэкап)',
            'backup' => basename($backup)
        ];
    } else {
        return ['success' => false, 'error' => 'Ошибка при удалении кеша'];
    }
}

/**
 * Форматирование размера файла
 */
function formatBytes($bytes, $precision = 2) {
    $units = ['B', 'KB', 'MB', 'GB'];
    $bytes = max($bytes, 0);
    $pow = floor(($bytes ? log($bytes) : 0) / log(1024));
    $pow = min($pow, count($units) - 1);
    $bytes /= pow(1024, $pow);
    return round($bytes, $precision) . ' ' . $units[$pow];
}

?>
<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>SEO Cluster Manager</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
        }
        
        .container {
            max-width: 1400px;
            margin: 0 auto;
        }
        
        .header {
            background: white;
            padding: 30px;
            border-radius: 15px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.2);
            margin-bottom: 30px;
        }
        
        .header h1 {
            color: #667eea;
            font-size: 32px;
            margin-bottom: 10px;
        }
        
        .header p {
            color: #666;
            font-size: 16px;
        }
        
        .grid {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 20px;
            margin-bottom: 20px;
        }
        
        @media (max-width: 1024px) {
            .grid { grid-template-columns: 1fr; }
        }
        
        .card {
            background: white;
            padding: 25px;
            border-radius: 15px;
            box-shadow: 0 5px 20px rgba(0,0,0,0.1);
        }
        
        .card h2 {
            color: #333;
            font-size: 22px;
            margin-bottom: 20px;
            padding-bottom: 10px;
            border-bottom: 2px solid #f0f0f0;
        }
        
        .stat-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
            gap: 15px;
            margin-top: 15px;
        }
        
        .stat-item {
            background: #f8f9fa;
            padding: 15px;
            border-radius: 10px;
            text-align: center;
        }
        
        .stat-value {
            font-size: 28px;
            font-weight: bold;
            color: #667eea;
            margin-bottom: 5px;
        }
        
        .stat-label {
            font-size: 13px;
            color: #666;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }
        
        .group-list {
            max-height: 500px;
            overflow-y: auto;
        }
        
        .group-item {
            background: #f8f9fa;
            padding: 15px;
            margin-bottom: 10px;
            border-radius: 10px;
            display: flex;
            justify-content: space-between;
            align-items: center;
            transition: all 0.3s;
        }
        
        .group-item:hover {
            background: #e9ecef;
            transform: translateX(5px);
        }
        
        .group-info {
            flex: 1;
        }
        
        .group-name {
            font-size: 18px;
            font-weight: bold;
            color: #333;
            margin-bottom: 5px;
        }
        
        .group-meta {
            font-size: 13px;
            color: #666;
        }
        
        .group-actions {
            display: flex;
            gap: 10px;
        }
        
        .btn {
            padding: 10px 20px;
            border: none;
            border-radius: 8px;
            font-size: 14px;
            font-weight: 600;
            cursor: pointer;
            transition: all 0.3s;
            text-decoration: none;
            display: inline-block;
        }
        
        .btn:hover {
            transform: translateY(-2px);
            box-shadow: 0 5px 15px rgba(0,0,0,0.2);
        }
        
        .btn-primary {
            background: #667eea;
            color: white;
        }
        
        .btn-success {
            background: #28a745;
            color: white;
        }
        
        .btn-danger {
            background: #dc3545;
            color: white;
        }
        
        .btn-warning {
            background: #ffc107;
            color: #333;
        }
        
        .btn-info {
            background: #17a2b8;
            color: white;
        }
        
        .btn-sm {
            padding: 8px 15px;
            font-size: 12px;
        }
        
        .upload-area {
            border: 3px dashed #ddd;
            border-radius: 10px;
            padding: 40px;
            text-align: center;
            cursor: pointer;
            transition: all 0.3s;
            margin-bottom: 20px;
        }
        
        .upload-area:hover {
            border-color: #667eea;
            background: #f8f9fa;
        }
        
        .upload-area.dragover {
            border-color: #667eea;
            background: #e7f1ff;
        }
        
        .upload-icon {
            font-size: 48px;
            color: #667eea;
            margin-bottom: 15px;
        }
        
        #file-input {
            display: none;
        }
        
        .alert {
            padding: 15px 20px;
            border-radius: 10px;
            margin-bottom: 20px;
            display: none;
        }
        
        .alert.show {
            display: block;
            animation: slideIn 0.3s;
        }
        
        @keyframes slideIn {
            from { opacity: 0; transform: translateY(-10px); }
            to { opacity: 1; transform: translateY(0); }
        }
        
        .alert-success {
            background: #d4edda;
            color: #155724;
            border-left: 4px solid #28a745;
        }
        
        .alert-error {
            background: #f8d7da;
            color: #721c24;
            border-left: 4px solid #dc3545;
        }
        
        .alert-info {
            background: #d1ecf1;
            color: #0c5460;
            border-left: 4px solid #17a2b8;
        }
        
        .loading {
            display: inline-block;
            width: 20px;
            height: 20px;
            border: 3px solid #f3f3f3;
            border-top: 3px solid #667eea;
            border-radius: 50%;
            animation: spin 1s linear infinite;
        }
        
        @keyframes spin {
            0% { transform: rotate(0deg); }
            100% { transform: rotate(360deg); }
        }
        
        .modal {
            display: none;
            position: fixed;
            top: 0;
            left: 0;
            width: 100%;
            height: 100%;
            background: rgba(0,0,0,0.7);
            z-index: 1000;
            align-items: center;
            justify-content: center;
        }
        
        .modal.show {
            display: flex;
        }
        
        .modal-content {
            background: white;
            padding: 30px;
            border-radius: 15px;
            max-width: 600px;
            width: 90%;
            max-height: 80vh;
            overflow-y: auto;
        }
        
        .modal-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 20px;
            padding-bottom: 15px;
            border-bottom: 2px solid #f0f0f0;
        }
        
        .modal-close {
            font-size: 28px;
            cursor: pointer;
            color: #999;
        }
        
        .modal-close:hover {
            color: #333;
        }
        
        pre {
            background: #f8f9fa;
            padding: 15px;
            border-radius: 8px;
            overflow-x: auto;
            font-size: 13px;
            line-height: 1.5;
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🔍 SEO Cluster Manager</h1>
            <p>Управление группами запросов, кешами и кластеризацией</p>
        </div>
        
        <div id="alert-container"></div>
        
        <div class="grid">
            <!-- Статистика кеша -->
            <div class="card">
                <h2>💾 Статистика кеша</h2>
                <div id="cache-stats" class="stat-grid">
                    <div class="loading"></div>
                </div>
                <div style="margin-top: 20px; text-align: center;">
                    <button class="btn btn-warning" onclick="clearCache()">🗑️ Очистить кеш</button>
                </div>
            </div>
            
            <!-- Загрузка файлов -->
            <div class="card">
                <h2>📤 Загрузка файлов</h2>
                <div class="upload-area" id="upload-area" onclick="document.getElementById('file-input').click()">
                    <div class="upload-icon">📁</div>
                    <div style="font-size: 16px; color: #333; margin-bottom: 10px;">
                        <strong>Перетащите CSV файл сюда</strong>
                    </div>
                    <div style="font-size: 14px; color: #666;">
                        или нажмите для выбора файла
                    </div>
                </div>
                <input type="file" id="file-input" accept=".csv" onchange="handleFileSelect(event)">
                
                <div style="text-align: center; margin-top: 20px;">
                    <button class="btn btn-success" onclick="runAllClustering()">
                        🚀 Запустить кластеризацию всех групп
                    </button>
                </div>
            </div>
        </div>
        
        <!-- Список групп -->
        <div class="card">
            <h2>📊 Группы запросов</h2>
            <div id="groups-list" class="group-list">
                <div class="loading"></div>
            </div>
        </div>
    </div>
    
    <!-- Модальное окно с информацией о группе -->
    <div id="group-modal" class="modal">
        <div class="modal-content">
            <div class="modal-header">
                <h2 id="modal-title">Информация о группе</h2>
                <span class="modal-close" onclick="closeModal()">&times;</span>
            </div>
            <div id="modal-body"></div>
        </div>
    </div>
    
    <script>
        // Загрузка данных при старте
        loadCacheStats();
        loadGroups();
        
        // Обновление каждые 10 секунд
        setInterval(() => {
            loadCacheStats();
            loadGroups();
        }, 10000);
        
        // Загрузка статистики кеша
        async function loadCacheStats() {
            try {
                const response = await fetch('?action=get_cache_stats');
                const data = await response.json();
                
                let html = '';
                
                if (!data.exists) {
                    html = '<div class="stat-item"><div class="stat-label">Кеш не найден</div></div>';
                } else if (data.error) {
                    html = `<div class="stat-item"><div class="stat-label">Ошибка: ${data.error}</div></div>`;
                } else {
                    html = `
                        <div class="stat-item">
                            <div class="stat-value">${data.queries}</div>
                            <div class="stat-label">Запросов</div>
                        </div>
                        <div class="stat-item">
                            <div class="stat-value">${data.documents}</div>
                            <div class="stat-label">Документов</div>
                        </div>
                        <div class="stat-item">
                            <div class="stat-value">${data.lsi_terms}</div>
                            <div class="stat-label">LSI терминов</div>
                        </div>
                        <div class="stat-item">
                            <div class="stat-value">${data.groups}</div>
                            <div class="stat-label">Групп</div>
                        </div>
                        <div class="stat-item">
                            <div class="stat-value">${data.size_human}</div>
                            <div class="stat-label">Размер БД</div>
                        </div>
                        <div class="stat-item">
                            <div class="stat-label">Обновлено</div>
                            <div style="font-size: 12px; color: #666; margin-top: 5px;">${data.modified}</div>
                        </div>
                    `;
                }
                
                document.getElementById('cache-stats').innerHTML = html;
            } catch (error) {
                console.error('Ошибка загрузки статистики кеша:', error);
            }
        }
        
        // Загрузка списка групп
        async function loadGroups() {
            try {
                const response = await fetch('?action=list_groups');
                const groups = await response.json();
                
                let html = '';
                
                if (groups.length === 0) {
                    html = '<div style="text-align: center; padding: 40px; color: #666;">Нет доступных групп</div>';
                } else {
                    groups.forEach(group => {
                        html += `
                            <div class="group-item">
                                <div class="group-info">
                                    <div class="group-name">📁 ${group.name}</div>
                                    <div class="group-meta">
                                        ${group.queries} запросов · 
                                        ${group.size_human} · 
                                        ${group.modified_human}
                                    </div>
                                </div>
                                <div class="group-actions">
                                    <button class="btn btn-info btn-sm" onclick="showGroupInfo('${group.name}')">
                                        ℹ️ Инфо
                                    </button>
                                    <button class="btn btn-primary btn-sm" onclick="runGroupClustering('${group.name}')">
                                        ▶️ Запустить
                                    </button>
                                    <button class="btn btn-danger btn-sm" onclick="deleteGroup('${group.file}')">
                                        🗑️
                                    </button>
                                </div>
                            </div>
                        `;
                    });
                }
                
                document.getElementById('groups-list').innerHTML = html;
            } catch (error) {
                console.error('Ошибка загрузки групп:', error);
            }
        }
        
        // Показать информацию о группе
        async function showGroupInfo(groupName) {
            try {
                const response = await fetch(`?action=get_group_info&group=${encodeURIComponent(groupName)}`);
                const data = await response.json();
                
                let html = '';
                
                if (data.error) {
                    html = `<div class="alert alert-error show">${data.error}</div>`;
                } else {
                    html = `
                        <div style="margin-bottom: 20px;">
                            <strong>Группа:</strong> ${data.name}<br>
                            <strong>Закешировано запросов:</strong> ${data.cached_queries}<br>
                            <strong>Результаты кластеризации:</strong> ${data.has_results ? 'Да ✅' : 'Нет ❌'}
                        </div>
                    `;
                    
                    if (data.result_files && data.result_files.length > 0) {
                        html += '<h3 style="margin-bottom: 15px;">Файлы результатов:</h3>';
                        html += '<div style="background: #f8f9fa; padding: 15px; border-radius: 8px;">';
                        data.result_files.forEach(file => {
                            html += `
                                <div style="padding: 8px 0; border-bottom: 1px solid #e0e0e0;">
                                    📄 ${file.name} <span style="color: #666;">(${file.size}, ${file.modified})</span>
                                </div>
                            `;
                        });
                        html += '</div>';
                    }
                }
                
                document.getElementById('modal-title').textContent = `Информация о группе: ${groupName}`;
                document.getElementById('modal-body').innerHTML = html;
                document.getElementById('group-modal').classList.add('show');
            } catch (error) {
                showAlert('error', 'Ошибка загрузки информации о группе');
            }
        }
        
        // Закрыть модальное окно
        function closeModal() {
            document.getElementById('group-modal').classList.remove('show');
        }
        
        // Запуск кластеризации для группы
        async function runGroupClustering(groupName) {
            if (!confirm(`Запустить кластеризацию для группы "${groupName}"?`)) {
                return;
            }
            
            showAlert('info', `Запуск кластеризации для группы "${groupName}"...`);
            
            try {
                const formData = new FormData();
                formData.append('group', groupName);
                
                const response = await fetch('?action=run_clustering', {
                    method: 'POST',
                    body: formData
                });
                
                const data = await response.json();
                
                if (data.success) {
                    showAlert('success', data.message);
                } else {
                    showAlert('error', data.error || data.message);
                }
            } catch (error) {
                showAlert('error', 'Ошибка при запуске кластеризации');
            }
        }
        
        // Запуск кластеризации для всех групп
        async function runAllClustering() {
            if (!confirm('Запустить кластеризацию для ВСЕХ групп?')) {
                return;
            }
            
            showAlert('info', 'Запуск кластеризации для всех групп...');
            
            try {
                const formData = new FormData();
                formData.append('group', 'all');
                
                const response = await fetch('?action=run_clustering', {
                    method: 'POST',
                    body: formData
                });
                
                const data = await response.json();
                
                if (data.success) {
                    showAlert('success', 'Кластеризация запущена для всех групп');
                } else {
                    showAlert('error', data.error || data.message);
                }
            } catch (error) {
                showAlert('error', 'Ошибка при запуске кластеризации');
            }
        }
        
        // Обработка выбора файла
        async function handleFileSelect(event) {
            const file = event.target.files[0];
            if (!file) return;
            
            await uploadFile(file);
            event.target.value = ''; // Сброс input
        }
        
        // Загрузка файла
        async function uploadFile(file) {
            showAlert('info', 'Загрузка файла...');
            
            try {
                const formData = new FormData();
                formData.append('file', file);
                
                const response = await fetch('?action=upload_file', {
                    method: 'POST',
                    body: formData
                });
                
                const data = await response.json();
                
                if (data.success) {
                    showAlert('success', data.message + ': ' + data.filename);
                    loadGroups();
                } else {
                    showAlert('error', data.error);
                }
            } catch (error) {
                showAlert('error', 'Ошибка при загрузке файла');
            }
        }
        
        // Удаление группы
        async function deleteGroup(filename) {
            if (!confirm(`Удалить файл "${filename}"?\n\n(Будет создан бэкап)`)) {
                return;
            }
            
            try {
                const formData = new FormData();
                formData.append('filename', filename);
                
                const response = await fetch('?action=delete_file', {
                    method: 'POST',
                    body: formData
                });
                
                const data = await response.json();
                
                if (data.success) {
                    showAlert('success', data.message);
                    loadGroups();
                } else {
                    showAlert('error', data.error);
                }
            } catch (error) {
                showAlert('error', 'Ошибка при удалении файла');
            }
        }
        
        // Очистка кеша
        async function clearCache() {
            if (!confirm('Очистить весь кеш?\n\n(Будет создан бэкап БД)')) {
                return;
            }
            
            try {
                const response = await fetch('?action=clear_cache', { method: 'POST' });
                const data = await response.json();
                
                if (data.success) {
                    showAlert('success', data.message);
                    loadCacheStats();
                } else {
                    showAlert('error', data.error);
                }
            } catch (error) {
                showAlert('error', 'Ошибка при очистке кеша');
            }
        }
        
        // Показать уведомление
        function showAlert(type, message) {
            const container = document.getElementById('alert-container');
            const alert = document.createElement('div');
            alert.className = `alert alert-${type} show`;
            alert.textContent = message;
            
            container.innerHTML = '';
            container.appendChild(alert);
            
            setTimeout(() => {
                alert.remove();
            }, 5000);
        }
        
        // Drag & Drop
        const uploadArea = document.getElementById('upload-area');
        
        ['dragenter', 'dragover', 'dragleave', 'drop'].forEach(eventName => {
            uploadArea.addEventListener(eventName, preventDefaults, false);
        });
        
        function preventDefaults(e) {
            e.preventDefault();
            e.stopPropagation();
        }
        
        ['dragenter', 'dragover'].forEach(eventName => {
            uploadArea.addEventListener(eventName, () => {
                uploadArea.classList.add('dragover');
            }, false);
        });
        
        ['dragleave', 'drop'].forEach(eventName => {
            uploadArea.addEventListener(eventName, () => {
                uploadArea.classList.remove('dragover');
            }, false);
        });
        
        uploadArea.addEventListener('drop', (e) => {
            const file = e.dataTransfer.files[0];
            if (file) {
                uploadFile(file);
            }
        }, false);
    </script>
</body>
</html>

