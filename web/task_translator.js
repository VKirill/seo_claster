/**
 * Переводчик названий задач на русский язык
 * Используется для отображения технических названий задач в понятном виде
 */

// Маппинг технических названий задач на русские
const TASK_TRANSLATIONS = {
    // Генерация изображений
    'image_generation_photoshoot': 'Генерация изображений для фотосессии',
    'image_generation': 'Генерация изображений',
    'photoshoot_generation': 'Генерация фотосессии',

    // Анализ аватаров
    'avatar_analysis_result': 'Результат анализа аватара',
    'avatar_analysis': 'Анализ аватара',
    'avatar_result': 'Результат аватара',

    // Другие возможные задачи (можно расширить)
    'text_generation': 'Генерация текста',
    'image_processing': 'Обработка изображения',
    'content_analysis': 'Анализ контента',
    'data_processing': 'Обработка данных',
    'query_analysis': 'Анализ запросов',
    'seo_analysis': 'SEO анализ',
    'clustering': 'Кластеризация',
    'classification': 'Классификация',
    'intent_detection': 'Определение интента',
    'serp_analysis': 'SERP анализ',
    'direct_analysis': 'Yandex Direct анализ',
};

/**
 * Переводит техническое название задачи на русский язык
 * 
 * @param {string} taskName Техническое название задачи (например, 'image_generation_photoshoot')
 * @returns {string} Русское название задачи или исходное название, если перевод не найден
 */
function translateTaskName(taskName) {
    if (!taskName) {
        return taskName;
    }

    // Прямой поиск в словаре
    if (TASK_TRANSLATIONS[taskName]) {
        return TASK_TRANSLATIONS[taskName];
    }

    // Попытка найти частичное совпадение (для составных названий)
    const taskLower = taskName.toLowerCase();
    for (const [key, translation] of Object.entries(TASK_TRANSLATIONS)) {
        const keyLower = key.toLowerCase();
        if (taskLower.includes(keyLower) || keyLower.includes(taskLower)) {
            return translation;
        }
    }

    // Если перевод не найден, возвращаем исходное название
    return taskName;
}

/**
 * Переводит массив названий задач
 * 
 * @param {string[]} taskNames Массив технических названий задач
 * @returns {string[]} Массив русских названий задач
 */
function translateTaskNames(taskNames) {
    if (!Array.isArray(taskNames)) {
        return [];
    }

    return taskNames.map(translateTaskName);
}

/**
 * Применяет перевод к ячейкам таблицы с задачами
 * Ищет все ячейки с классом 'task-name' или атрибутом data-task и переводит их содержимое
 */
function applyTaskTranslationsToTable() {
    // Вариант 1: Поиск по классу
    const taskCells = document.querySelectorAll('.task-name, [data-task]');
    taskCells.forEach(cell => {
        const taskName = cell.textContent.trim() || cell.getAttribute('data-task');
        if (taskName) {
            const translated = translateTaskName(taskName);
            if (translated !== taskName) {
                cell.textContent = translated;
            }
        }
    });

    // Вариант 2: Поиск в колонке "Задача" (если таблица имеет заголовки)
    const tables = document.querySelectorAll('table');
    tables.forEach(table => {
        const headers = Array.from(table.querySelectorAll('th, thead td'));
        const taskColumnIndex = headers.findIndex(th =>
            th.textContent.trim().toLowerCase().includes('задача')
        );

        if (taskColumnIndex !== -1) {
            const rows = table.querySelectorAll('tbody tr');
            rows.forEach(row => {
                const cells = row.querySelectorAll('td');
                if (cells[taskColumnIndex]) {
                    const taskName = cells[taskColumnIndex].textContent.trim();
                    if (taskName && TASK_TRANSLATIONS[taskName]) {
                        cells[taskColumnIndex].textContent = translateTaskName(taskName);
                    }
                }
            });
        }
    });
}

// Автоматическое применение переводов при загрузке страницы
if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', applyTaskTranslationsToTable);
} else {
    applyTaskTranslationsToTable();
}

// Экспорт для использования в других модулях
if (typeof module !== 'undefined' && module.exports) {
    module.exports = {
        translateTaskName,
        translateTaskNames,
        applyTaskTranslationsToTable,
        TASK_TRANSLATIONS
    };
}
