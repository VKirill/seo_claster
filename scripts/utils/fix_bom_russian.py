"""
Удаление BOM из файла Russian.txt
"""

file_path = 'keyword_group/Russian.txt'

# Читаем с учётом BOM (utf-8-sig автоматически удаляет BOM при чтении)
with open(file_path, 'r', encoding='utf-8-sig') as f:
    content = f.read()

# Записываем без BOM (utf-8 не добавляет BOM)
with open(file_path, 'w', encoding='utf-8') as f:
    f.write(content)

print(f"✅ BOM удалён из {file_path}")

# Проверяем
with open(file_path, 'rb') as f:
    first_bytes = f.read(3)
    has_bom = (first_bytes == b'\xef\xbb\xbf')
    print(f"Проверка: BOM {'найден ❌' if has_bom else 'отсутствует ✅'}")

