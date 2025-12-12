<?php
/**
 * Переводчик названий задач на русский язык
 * Используется для отображения технических названий задач в понятном виде
 */

/**
 * Маппинг технических названий задач на русские
 */
function getTaskTranslations() {
    return [
        // Генерация изображений
        'image_generation_photoshoot' => 'Генерация изображений для фотосессии',
        'image_generation' => 'Генерация изображений',
        'photoshoot_generation' => 'Генерация фотосессии',
        
        // Анализ аватаров
        'avatar_analysis_result' => 'Результат анализа аватара',
        'avatar_analysis' => 'Анализ аватара',
        'avatar_result' => 'Результат аватара',
        
        // Другие возможные задачи (можно расширить)
        'text_generation' => 'Генерация текста',
        'image_processing' => 'Обработка изображения',
        'content_analysis' => 'Анализ контента',
        'data_processing' => 'Обработка данных',
        'query_analysis' => 'Анализ запросов',
        'seo_analysis' => 'SEO анализ',
        'clustering' => 'Кластеризация',
        'classification' => 'Классификация',
        'intent_detection' => 'Определение интента',
        'serp_analysis' => 'SERP анализ',
        'direct_analysis' => 'Yandex Direct анализ',
    ];
}

/**
 * Переводит техническое название задачи на русский язык
 * 
 * @param string $taskName Техническое название задачи (например, 'image_generation_photoshoot')
 * @return string Русское название задачи или исходное название, если перевод не найден
 */
function translateTaskName($taskName) {
    if (empty($taskName)) {
        return $taskName;
    }
    
    $translations = getTaskTranslations();
    
    // Прямой поиск в словаре
    if (isset($translations[$taskName])) {
        return $translations[$taskName];
    }
    
    // Попытка найти частичное совпадение (для составных названий)
    $taskLower = mb_strtolower($taskName, 'UTF-8');
    foreach ($translations as $key => $translation) {
        $keyLower = mb_strtolower($key, 'UTF-8');
        if (mb_strpos($taskLower, $keyLower) !== false || mb_strpos($keyLower, $taskLower) !== false) {
            return $translation;
        }
    }
    
    // Если перевод не найден, возвращаем исходное название
    return $taskName;
}

/**
 * Переводит массив названий задач
 * 
 * @param array $taskNames Массив технических названий задач
 * @return array Массив русских названий задач
 */
function translateTaskNames($taskNames) {
    if (!is_array($taskNames)) {
        return [];
    }
    
    return array_map('translateTaskName', $taskNames);
}
