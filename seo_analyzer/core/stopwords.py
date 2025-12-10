"""Фильтрация стоп-слов и нежелательного контента"""

from typing import Set, List, Tuple
import pandas as pd
import re
try:
    import pymorphy3
    MORPH = pymorphy3.MorphAnalyzer()
except ImportError:
    try:
        import pymorphy2
        MORPH = pymorphy2.MorphAnalyzer()
    except ImportError:
        MORPH = None


class StopwordsFilter:
    """Фильтр стоп-слов для запросов"""
    
    def __init__(self, stopwords: Set[str]):
        """
        Инициализация фильтра
        
        Args:
            stopwords: Множество стоп-слов
        """
        self.stopwords = stopwords
        self.blocked_count = 0
        self.blocked_queries = []  # Список заблокированных запросов
        
        # Создаем расширенный набор со склонениями
        self.stopwords_expanded = self._expand_stopwords(stopwords)
    
    def _expand_stopwords(self, stopwords: Set[str]) -> Set[str]:
        """
        Расширяет стоп-слова всеми падежными формами
        
        Args:
            stopwords: Исходные стоп-слова
            
        Returns:
            Расширенный набор со всеми формами
        """
        if not MORPH:
            return stopwords
        
        expanded = set(stopwords)
        
        for word in stopwords:
            word_clean = word.strip().lower()
            
            # Пропускаем очень короткие (менее 3 символов)
            if len(word_clean) < 3:
                continue
            
            try:
                # Парсим слово
                parsed = MORPH.parse(word_clean)[0]
                
                # Получаем все формы слова
                lexeme = parsed.lexeme
                
                for form in lexeme:
                    form_word = form.word.lower()
                    if len(form_word) >= 3:  # Только формы длиной >= 3
                        expanded.add(form_word)
            except:
                # Если не удалось просклонять, оставляем исходное
                pass
        
        return expanded
    
    def contains_stopword(self, text: str) -> tuple:
        """
        Проверяет, содержит ли текст стоп-слово (только ЦЕЛЫЕ СЛОВА)
        
        Args:
            text: Текст для проверки
            
        Returns:
            (True/False, найденное стоп-слово или None)
        """
        text_lower = text.lower()
        
        # Разбиваем текст на слова (по пробелам и знакам препинания)
        words_in_text = re.findall(r'\b[а-яёa-z]+\b', text_lower)
        
        # Проверяем каждое слово из текста
        for word in words_in_text:
            # Сначала проверяем исходные стоп-слова
            if word in self.stopwords:
                return True, word
            
            # Затем проверяем расширенный набор (со склонениями)
            if word in self.stopwords_expanded:
                return True, word
        
        return False, None
    
    def filter_queries(self, queries: List[str]) -> List[str]:
        """
        Фильтрует список запросов от стоп-слов
        
        Args:
            queries: Список запросов
            
        Returns:
            Отфильтрованный список
        """
        filtered = []
        
        for query in queries:
            if not self.contains_stopword(query):
                filtered.append(query)
            else:
                self.blocked_count += 1
        
        return filtered
    
    def filter_dataframe(self, df: pd.DataFrame, column: str = 'keyword') -> pd.DataFrame:
        """
        Фильтрует DataFrame от запросов со стоп-словами
        
        Args:
            df: DataFrame с запросами
            column: Название колонки с запросами
            
        Returns:
            Отфильтрованный DataFrame
        """
        # Проверяем каждый запрос и сохраняем информацию о блокировке
        stopword_info = df[column].apply(self.contains_stopword)
        
        # Разделяем на флаг и стоп-слово
        is_blocked = stopword_info.apply(lambda x: x[0])
        found_stopword = stopword_info.apply(lambda x: x[1])
        
        # Сохраняем заблокированные запросы
        blocked_df = df[is_blocked].copy()
        blocked_df['stopword_found'] = found_stopword[is_blocked]
        self.blocked_queries = blocked_df.to_dict('records')
        self.blocked_count = len(blocked_df)
        
        # Возвращаем только не заблокированные
        return df[~is_blocked].reset_index(drop=True)
    
    def get_stats(self) -> dict:
        """Возвращает статистику фильтрации"""
        return {
            "stopwords_count": len(self.stopwords),
            "blocked_queries": self.blocked_count,
        }


def create_additional_stopwords() -> Set[str]:
    """
    Создает дополнительный набор технических стоп-слов
    
    Returns:
        Множество стоп-слов
    """
    return {
        # Технические символы
        "test", "тест", "asdf", "qwerty",
        # Спам
        "spam", "спам", "реклама",
        # Короткие запросы (меньше 2 символов будем фильтровать отдельно)
    }


def filter_short_queries(df: pd.DataFrame, column: str = 'keyword', min_length: int = 2) -> pd.DataFrame:
    """
    Фильтрует слишком короткие запросы
    
    Args:
        df: DataFrame с запросами
        column: Колонка с запросами
        min_length: Минимальная длина запроса
        
    Returns:
        Отфильтрованный DataFrame
    """
    mask = df[column].str.len() >= min_length
    return df[mask].reset_index(drop=True)


def filter_low_frequency(
    df: pd.DataFrame,
    freq_column: str = 'frequency_world',
    min_freq: int = 0
) -> pd.DataFrame:
    """
    Фильтрует запросы с низкой частотностью
    
    Args:
        df: DataFrame с запросами
        freq_column: Колонка с частотностью
        min_freq: Минимальная частотность
        
    Returns:
        Отфильтрованный DataFrame
    """
    if freq_column not in df.columns:
        return df
    
    mask = df[freq_column] >= min_freq
    return df[mask].reset_index(drop=True)


def filter_by_frequency_ratio(
    df: pd.DataFrame,
    freq_world_column: str = 'frequency_world',
    freq_exact_column: str = 'frequency_exact',
    max_ratio: float = 51.0
) -> pd.DataFrame:
    """
    Фильтрует запросы по соотношению частотности / точной частотности
    
    Запросы с высоким соотношением (> max_ratio) обычно являются "мусорными":
    - Слишком общие запросы
    - Запросы с опечатками
    - Нерелевантные фразы
    
    Args:
        df: DataFrame с запросами
        freq_world_column: Колонка с общей частотностью
        freq_exact_column: Колонка с точной частотностью ("!")
        max_ratio: Максимальное соотношение (по умолчанию 51)
        
    Returns:
        Отфильтрованный DataFrame
        
    Examples:
        >>> # Запрос: "купить телефон"
        >>> # frequency_world = 1000, frequency_exact = 500
        >>> # Соотношение: 1000/500 = 2 (хороший запрос)
        
        >>> # Запрос: "телефон" (слишком общий)
        >>> # frequency_world = 10000, frequency_exact = 100
        >>> # Соотношение: 10000/100 = 100 (плохой запрос, будет удален)
    """
    if freq_world_column not in df.columns or freq_exact_column not in df.columns:
        return df
    
    # Создаем маску для фильтрации
    # Условия:
    # 1. Точная частотность > 0 (чтобы избежать деления на ноль)
    # 2. Соотношение <= max_ratio
    # 3. Если frequency_exact = 0, то соотношение = бесконечность → УДАЛЯЕМ такие запросы
    mask = (
        (df[freq_exact_column] > 0) & 
        (df[freq_world_column] / df[freq_exact_column] <= max_ratio)
    )
    
    return df[mask].reset_index(drop=True)
