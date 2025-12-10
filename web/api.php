<?php
/**
 * SEO Cluster Manager - API Backend
 * REST API для управления группами и кластеризацией
 */

// CORS headers (если нужно)
header('Access-Control-Allow-Origin: *');
header('Access-Control-Allow-Methods: GET, POST');
header('Access-Control-Allow-Headers: Content-Type');

// Для загрузки файлов не устанавливаем Content-Type заранее
if ($_SERVER['REQUEST_METHOD'] !== 'POST' || !isset($_GET['action']) || $_GET['action'] !== 'upload_file') {
    header('Content-Type: application/json; charset=utf-8');
}

// Настройки
define('PROJECT_DIR', dirname(__DIR__));
define('SEMANTIKA_DIR', PROJECT_DIR . '/semantika');
define('OUTPUT_DIR', PROJECT_DIR . '/output');
define('CACHE_DB', PROJECT_DIR . '/output/master_queries.db');
define('PYTHON_EXE', 'python'); // или полный путь к python.exe

// Создаем необходимые директории
if (!file_exists(SEMANTIKA_DIR)) {
    mkdir(SEMANTIKA_DIR, 0755, true);
}
if (!file_exists(OUTPUT_DIR)) {
    mkdir(OUTPUT_DIR, 0755, true);
}

// Роутинг
$action = $_GET['action'] ?? 'unknown';

try {
    switch ($action) {
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
            // Устанавливаем Content-Type для ответа
            header('Content-Type: application/json; charset=utf-8');
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
            http_response_code(400);
            echo json_encode(['error' => 'Unknown action: ' . $action]);
    }
} catch (Exception $e) {
    http_response_code(500);
    echo json_encode([
        'error' => 'Server error',
        'message' => $e->getMessage()
    ]);
}

/**
 * Получить список групп
 */
function getGroups() {
    $groups = [];
    $files = glob(SEMANTIKA_DIR . '/*.csv');
    
    if (!$files) {
        return [];
    }
    
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
        if ($fp = @fopen($file, 'r')) {
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
    if (!file_exists(CACHE_DB)) {
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
        $db = new PDO('sqlite:' . CACHE_DB);
        $db->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        
        // Проверяем наличие таблицы master_queries
        $tables = $db->query("SELECT name FROM sqlite_master WHERE type='table'")->fetchAll(PDO::FETCH_COLUMN);
        
        if (!in_array('master_queries', $tables)) {
            return [
                'exists' => true,
                'size' => filesize(CACHE_DB),
                'size_human' => formatBytes(filesize(CACHE_DB)),
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
        
        // Запросы с SERP данными (где serp_status = 'completed')
        $serp_queries = $db->query("SELECT COUNT(*) FROM master_queries WHERE serp_status = 'completed'")->fetchColumn();
        
        // Запросы с классификацией интента
        $with_intent = $db->query("SELECT COUNT(*) FROM master_queries WHERE main_intent IS NOT NULL AND main_intent != ''")->fetchColumn();
        
        return [
            'exists' => true,
            'size' => filesize(CACHE_DB),
            'size_human' => formatBytes(filesize(CACHE_DB)),
            'queries' => (int)$queries,
            'groups' => (int)$groups,
            'serp_queries' => (int)$serp_queries,
            'with_intent' => (int)$with_intent,
            'modified' => date('d.m.Y H:i', filemtime(CACHE_DB))
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
    if (empty($groupName)) {
        return ['error' => 'Группа не указана'];
    }
    
    if (!file_exists(CACHE_DB)) {
        return [
            'name' => $groupName,
            'cached_queries' => 0,
            'has_results' => false,
            'result_files' => []
        ];
    }
    
    try {
        $db = new PDO('sqlite:' . CACHE_DB);
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
        $output_group_dir = OUTPUT_DIR . '/groups/' . $groupName;
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
    $command = $group === 'all' 
        ? PYTHON_EXE . ' main.py' 
        : PYTHON_EXE . ' main.py --group ' . escapeshellarg($group);
    
    // Запускаем в фоновом режиме
    if (strtoupper(substr(PHP_OS, 0, 3)) === 'WIN') {
        // Windows
        $command = 'start /B ' . $command . ' > NUL 2>&1';
        pclose(popen($command, 'r'));
    } else {
        // Linux/Mac
        $command .= ' > /dev/null 2>&1 &';
        exec($command);
    }
    
    return [
        'success' => true,
        'message' => $group === 'all' 
            ? 'Кластеризация запущена для всех групп' 
            : "Кластеризация запущена для группы: $group"
    ];
}

/**
 * Обработка загрузки файла
 */
function handleFileUpload() {
    try {
        if (!isset($_FILES['file'])) {
            return ['success' => false, 'error' => 'Файл не выбран'];
        }
        
        $file = $_FILES['file'];
        
        // Проверяем ошибки загрузки
        if ($file['error'] !== UPLOAD_ERR_OK) {
            $errorMessages = [
                UPLOAD_ERR_INI_SIZE => 'Файл превышает максимальный размер (php.ini)',
                UPLOAD_ERR_FORM_SIZE => 'Файл превышает максимальный размер формы',
                UPLOAD_ERR_PARTIAL => 'Файл загружен частично',
                UPLOAD_ERR_NO_FILE => 'Файл не был загружен',
                UPLOAD_ERR_NO_TMP_DIR => 'Отсутствует временная папка',
                UPLOAD_ERR_CANT_WRITE => 'Ошибка записи на диск',
                UPLOAD_ERR_EXTENSION => 'Загрузка остановлена расширением PHP'
            ];
            $errorMsg = $errorMessages[$file['error']] ?? 'Неизвестная ошибка загрузки (код: ' . $file['error'] . ')';
            return ['success' => false, 'error' => $errorMsg];
        }
        
        // Проверяем расширение
        $ext = strtolower(pathinfo($file['name'], PATHINFO_EXTENSION));
        if ($ext !== 'csv') {
            return ['success' => false, 'error' => 'Разрешены только CSV файлы (получен: .' . $ext . ')'];
        }
        
        // Проверяем размер (макс 50MB)
        if ($file['size'] > 50 * 1024 * 1024) {
            return ['success' => false, 'error' => 'Файл слишком большой (макс 50MB, получен: ' . formatBytes($file['size']) . ')'];
        }
        
        // Проверяем существование папки semantika
        if (!file_exists(SEMANTIKA_DIR)) {
            if (!mkdir(SEMANTIKA_DIR, 0755, true)) {
                return ['success' => false, 'error' => 'Не удалось создать папку: ' . SEMANTIKA_DIR];
            }
        }
        
        // Проверяем права на запись
        if (!is_writable(SEMANTIKA_DIR)) {
            return ['success' => false, 'error' => 'Нет прав на запись в папку: ' . SEMANTIKA_DIR];
        }
        
        // Формируем имя файла
        $filename = pathinfo($file['name'], PATHINFO_FILENAME);
        $filename = preg_replace('/[^a-zA-Z0-9_-]/', '_', $filename);
        
        // Проверяем, что имя файла не пустое
        if (empty($filename)) {
            $filename = 'uploaded_' . date('YmdHis');
        }
        
        $target_path = SEMANTIKA_DIR . '/' . $filename . '.csv';
        
        // Проверяем, существует ли уже файл
        if (file_exists($target_path)) {
            // Создаем бэкап
            $backup_path = SEMANTIKA_DIR . '/' . $filename . '_backup_' . date('YmdHis') . '.csv';
            if (!@copy($target_path, $backup_path)) {
                return ['success' => false, 'error' => 'Не удалось создать бэкап существующего файла'];
            }
        }
        
        // Перемещаем загруженный файл
        if (!move_uploaded_file($file['tmp_name'], $target_path)) {
            $lastError = error_get_last();
            $errorMsg = 'Ошибка при сохранении файла';
            if ($lastError) {
                $errorMsg .= ': ' . $lastError['message'];
            }
            return ['success' => false, 'error' => $errorMsg];
        }
        
        return [
            'success' => true,
            'filename' => basename($target_path),
            'message' => 'Файл успешно загружен: ' . basename($target_path)
        ];
    } catch (Exception $e) {
        return ['success' => false, 'error' => 'Исключение: ' . $e->getMessage()];
    }
}

/**
 * Удалить файл группы
 */
function deleteFile($filename) {
    if (empty($filename)) {
        return ['success' => false, 'error' => 'Имя файла не указано'];
    }
    
    $filepath = SEMANTIKA_DIR . '/' . basename($filename);
    
    if (!file_exists($filepath)) {
        return ['success' => false, 'error' => 'Файл не найден'];
    }
    
    // Создаем бэкап перед удалением
    $backup_path = SEMANTIKA_DIR . '/' . pathinfo($filename, PATHINFO_FILENAME) . '_deleted_' . date('YmdHis') . '.csv';
    if (!@copy($filepath, $backup_path)) {
        return ['success' => false, 'error' => 'Не удалось создать бэкап'];
    }
    
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
    if (!file_exists(CACHE_DB)) {
        return ['success' => false, 'error' => 'Кеш не найден'];
    }
    
    // Создаем бэкап
    $backup = CACHE_DB . '.backup_' . date('YmdHis');
    if (!@copy(CACHE_DB, $backup)) {
        return ['success' => false, 'error' => 'Не удалось создать бэкап'];
    }
    
    if (unlink(CACHE_DB)) {
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

