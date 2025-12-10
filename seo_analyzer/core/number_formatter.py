"""
Number Formatter
Форматирование чисел для записи в БД (округление до 3 знаков)
"""


def round_float(value: float, decimals: int = 3) -> float:
    """
    Округляет float до N знаков после запятой
    
    Args:
        value: Число для округления
        decimals: Количество знаков после запятой (по умолчанию 3)
        
    Returns:
        Округленное число
        
    Examples:
        >>> round_float(0.0560344827586207)
        0.056
        >>> round_float(0.885964912280702)
        0.886
        >>> round_float(0.714285714285714)
        0.714
    """
    if value is None:
        return None
    
    return round(value, decimals)


def round_dict_floats(data: dict, decimals: int = 3) -> dict:
    """
    Округляет все float значения в словаре
    
    Args:
        data: Словарь с данными
        decimals: Количество знаков после запятой
        
    Returns:
        Словарь с округленными значениями
    """
    result = {}
    for key, value in data.items():
        if isinstance(value, float):
            result[key] = round_float(value, decimals)
        elif isinstance(value, dict):
            result[key] = round_dict_floats(value, decimals)
        elif isinstance(value, list):
            result[key] = [
                round_dict_floats(item, decimals) if isinstance(item, dict)
                else round_float(item, decimals) if isinstance(item, float)
                else item
                for item in value
            ]
        else:
            result[key] = value
    return result

