"""
Transform blob data type from mysql to mysql

Args:
    - metadata_dict(dictionary): dictionary of column metadata

Returns:
    target_script(string): string for the column ddl
"""

def default(metadata_dict: dict[str: str]) -> str:
    char_max_size = metadata_dict['char_max_size']
    target_script = f'blob({char_max_size})'
    return target_script