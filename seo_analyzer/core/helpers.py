"""Вспомогательные функции для загрузки словарей и данных"""

import asyncio
from pathlib import Path
from typing import Dict, List, Set
import aiofiles
import pandas as pd
from .config import (
    KEYWORD_GROUP_DIR,
    KEYWORDS_STOP_DIR,
    KEYWORD_DICTIONARIES,
    GEO_DICTIONARIES,
)


async def load_text_file_async(file_path: Path) -> Set[str]:
    """
    Асинхронная загрузка текстового файла в множество строк
    
    Args:
        file_path: Путь к файлу
        
    Returns:
        Множество строк из файла (lowercase, stripped, without BOM)
    """
    try:
        async with aiofiles.open(file_path, 'r', encoding='utf-8-sig') as f:  # utf-8-sig автоматически удаляет BOM
            content = await f.read()
            return {line.strip().lower() for line in content.split('\n') if line.strip()}
    except FileNotFoundError:
        print(f"⚠️ Файл не найден: {file_path}")
        return set()
    except Exception as e:
        print(f"⚠️ Ошибка загрузки {file_path}: {e}")
        return set()


def load_text_file_sync(file_path: Path) -> Set[str]:
    """
    Синхронная загрузка текстового файла
    
    Args:
        file_path: Путь к файлу
        
    Returns:
        Множество строк из файла (без BOM)
    """
    try:
        with open(file_path, 'r', encoding='utf-8-sig') as f:  # utf-8-sig автоматически удаляет BOM
            return {line.strip().lower() for line in f if line.strip()}
    except FileNotFoundError:
        print(f"⚠️ Файл не найден: {file_path}")
        return set()
    except Exception as e:
        print(f"⚠️ Ошибка загрузки {file_path}: {e}")
        return set()


async def load_intent_weights() -> Dict[str, float]:
    """
    Загрузка весов интентов из файла intent_weights.txt
    
    Returns:
        Словарь {intent_type: weight}
    """
    weights_file = KEYWORD_GROUP_DIR / "intent_weights.txt"
    weights = {}
    
    if not weights_file.exists():
        # Значения по умолчанию
        return {
            "commercial": 3.0,
            "transactional": 3.0,
            "informational": 4.0,
            "navigational": 4.0,
        }
    
    try:
        async with aiofiles.open(weights_file, 'r', encoding='utf-8') as f:
            content = await f.read()
            
        for line in content.splitlines():
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            
            if ':' in line:
                intent_type, weight_str = line.split(':', 1)
                try:
                    weights[intent_type.strip()] = float(weight_str.strip())
                except ValueError:
                    continue
        
        return weights
    except Exception as e:
        print(f"⚠️ Ошибка загрузки весов интентов: {e}")
        # Возвращаем значения по умолчанию
        return {
            "commercial": 3.0,
            "transactional": 3.0,
            "informational": 4.0,
            "navigational": 4.0,
        }


async def load_all_keyword_dictionaries() -> Dict[str, Dict[str, any]]:
    """
    Асинхронная загрузка всех словарей из keyword_group
    
    Returns:
        Словарь с загруженными данными
    """
    result = {}
    tasks = []
    
    for dict_name, dict_info in KEYWORD_DICTIONARIES.items():
        file_path = KEYWORD_GROUP_DIR / dict_info["file"]
        tasks.append((dict_name, dict_info, load_text_file_async(file_path)))
    
    # Загружаем все словари параллельно
    for dict_name, dict_info, task in tasks:
        words = await task
        result[dict_name] = {
            "words": words,
            "weight": dict_info["weight"],
            "flag": dict_info["flag"],
        }
    
    return result


async def load_all_geo_dictionaries() -> Dict[str, Set[str]]:
    """
    Асинхронная загрузка всех географических словарей
    
    Returns:
        Словарь с гео-словарями
    """
    result = {}
    tasks = []
    
    for geo_name, filename in GEO_DICTIONARIES.items():
        file_path = KEYWORD_GROUP_DIR / filename
        tasks.append((geo_name, load_text_file_async(file_path)))
    
    # Загружаем параллельно
    for geo_name, task in tasks:
        result[geo_name] = await task
    
    return result


async def load_stopwords_async() -> Set[str]:
    """
    Загрузка стоп-слов (Adult контент)
    
    Returns:
        Множество стоп-слов
    """
    stopwords_file = KEYWORDS_STOP_DIR / "Adult_hard.txt"
    return await load_text_file_async(stopwords_file)


def load_csv_data(file_path: Path) -> pd.DataFrame:
    """
    Загрузка CSV файла с семантикой
    
    Args:
        file_path: Путь к CSV файлу
        
    Returns:
        DataFrame с данными
    """
    try:
        # Пробуем разные кодировки и разделители
        for encoding in ['utf-8', 'utf-8-sig', 'cp1251', 'windows-1251']:
            for sep in [';', ',', '\t']:
                try:
                    df = pd.read_csv(file_path, encoding=encoding, sep=sep)
                    # Проверяем, что загрузилось адекватно (больше 1 колонки)
                    if len(df.columns) > 1:
                        print(f"✓ Загружено {len(df)} запросов из {file_path.name} ({encoding}, sep='{sep}')")
                        return df
                except (UnicodeDecodeError, pd.errors.ParserError):
                    continue
        
        # Если ничего не подошло
        raise ValueError(f"Не удалось определить формат файла {file_path}")
        
    except FileNotFoundError:
        print(f"⚠️ Файл не найден: {file_path}")
        return pd.DataFrame()
    except Exception as e:
        print(f"⚠️ Ошибка загрузки CSV {file_path}: {e}")
        return pd.DataFrame()


def detect_csv_columns(df: pd.DataFrame) -> Dict[str, str]:
    """
    Автоопределение колонок в CSV
    
    Args:
        df: DataFrame
        
    Returns:
        Словарь с именами колонок
    """
    columns = {}
    
    # Варианты названий для каждого типа колонок
    keyword_variants = ['keyword', 'запрос', 'query', 'ключевое слово', 'фраза', 'ключевоеслово']
    freq_world_variants = ['frequency_world', 'частотность', 'freq', 'показы', 'impressions', 
                           'max частотность весь мир', 'maxчастотностьвесьмир', 'весьмир']
    freq_exact_variants = ['frequency_exact', 'точная частотность', 'exact', '"!"', 
                          '!max частотность !весь !мир', '"!maxчастотность!весь!мир"',
                          '!maxчастотность!весь!мир',  # Без внешних кавычек
                          '!весь!мир', 'maxчастотность!весь!мир']
    
    df_columns_lower = {col.lower().replace(' ', '').replace('"', ''): col for col in df.columns}
    
    # Поиск колонки с ключевыми словами
    for variant in keyword_variants:
        variant_clean = variant.replace(' ', '').replace('"', '')
        if variant_clean in df_columns_lower:
            columns['keyword'] = df_columns_lower[variant_clean]
            break
    
    # Если не нашли по названию, берем первую текстовую колонку
    if 'keyword' not in columns:
        for col in df.columns:
            if df[col].dtype == 'object':
                columns['keyword'] = col
                break
    
    # Поиск колонки с частотностью
    for variant in freq_world_variants:
        variant_clean = variant.replace(' ', '').replace('"', '')
        if variant_clean in df_columns_lower:
            columns['frequency_world'] = df_columns_lower[variant_clean]
            break
    
    # Точная частотность
    # НЕ удаляем ! при поиске, так как это часть названия колонки
    for variant in freq_exact_variants:
        variant_clean = variant.replace(' ', '').replace('"', '')
        if variant_clean in df_columns_lower:
            columns['frequency_exact'] = df_columns_lower[variant_clean]
            break
    
    return columns


def normalize_dataframe_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Нормализация колонок DataFrame
    
    Args:
        df: Исходный DataFrame
        
    Returns:
        DataFrame с нормализованными колонками
    """
    detected_cols = detect_csv_columns(df)
    
    # Создаем новый DataFrame с нужными колонками
    result_df = pd.DataFrame()
    
    if 'keyword' in detected_cols:
        result_df['keyword'] = df[detected_cols['keyword']].astype(str)
    
    if 'frequency_world' in detected_cols:
        # Обрабатываем частоты: удаляем пробелы и другие разделители тысяч из чисел
        # Например: "3 000" -> "3000", "3,000" -> "3000", "3.000" -> "3000"
        # ВАЖНО: обрабатываем только колонку частот, не keyword!
        freq_world_col = df[detected_cols['frequency_world']]
        
        # Проверяем тип данных исходной колонки
        if freq_world_col.dtype == 'object':
            # Если это строки, преобразуем и удаляем разделители тысяч
            freq_world_series = freq_world_col.astype(str)
            # Удаляем пробелы, запятые и точки (разделители тысяч)
            freq_world_series = freq_world_series.str.replace(r'[\s,\.]', '', regex=True)
        else:
            # Если уже число - используем как есть
            freq_world_series = freq_world_col
        
        result_df['frequency_world'] = pd.to_numeric(
            freq_world_series, errors='coerce'
        ).fillna(0).astype(int)
    else:
        result_df['frequency_world'] = 0
    
    if 'frequency_exact' in detected_cols:
        # Обрабатываем частоты: удаляем пробелы и другие разделители тысяч из чисел
        # Например: "3 000" -> "3000", "3,000" -> "3000", "3.000" -> "3000"
        # ВАЖНО: обрабатываем только колонку частот, не keyword!
        freq_exact_col = df[detected_cols['frequency_exact']]
        
        # Проверяем тип данных исходной колонки
        if freq_exact_col.dtype == 'object':
            # Если это строки, преобразуем и удаляем разделители тысяч
            freq_exact_series = freq_exact_col.astype(str)
            # Удаляем пробелы, запятые и точки (разделители тысяч)
            freq_exact_series = freq_exact_series.str.replace(r'[\s,\.]', '', regex=True)
        else:
            # Если уже число - используем как есть
            freq_exact_series = freq_exact_col
        
        result_df['frequency_exact'] = pd.to_numeric(
            freq_exact_series, errors='coerce'
        ).fillna(0).astype(int)
    else:
        result_df['frequency_exact'] = 0
    
    return result_df


async def load_all_data():
    """
    Загрузка всех необходимых данных
    
    Returns:
        Tuple с данными (keyword_dicts, geo_dicts, stopwords)
    """
    print("📚 Загрузка словарей...")
    
    keyword_dicts, geo_dicts, stopwords = await asyncio.gather(
        load_all_keyword_dictionaries(),
        load_all_geo_dictionaries(),
        load_stopwords_async(),
    )
    
    print(f"✓ Загружено {len(keyword_dicts)} словарей классификации")
    print(f"✓ Загружено {len(geo_dicts)} географических словарей")
    print(f"✓ Загружено {len(stopwords)} стоп-слов")
    
    return keyword_dicts, geo_dicts, stopwords

