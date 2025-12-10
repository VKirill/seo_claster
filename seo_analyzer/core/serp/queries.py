"""SQL запросы для работы с SERP базой данных"""


def get_serp_query() -> str:
    """
    Получить SQL запрос для выборки SERP данных
    
    Returns:
        SQL запрос строка
    """
    return """
        SELECT * FROM serp_results
        WHERE query_hash = ? AND lr = ? AND created_at > ?
        ORDER BY created_at DESC
        LIMIT 1
    """


def get_documents_query() -> str:
    """
    Получить SQL запрос для выборки документов
    
    Returns:
        SQL запрос строка
    """
    return """
        SELECT * FROM serp_documents
        WHERE serp_result_id = ?
        ORDER BY position
    """


def get_lsi_query() -> str:
    """
    Получить SQL запрос для выборки LSI фраз
    
    Returns:
        SQL запрос строка
    """
    return """
        SELECT up.phrase, m.frequency, m.source 
        FROM serp_lsi_mapping m
        JOIN unique_lsi_phrases up ON m.phrase_id = up.id
        WHERE m.serp_result_id = ?
        ORDER BY m.frequency DESC
    """

