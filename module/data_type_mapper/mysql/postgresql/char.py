"""
Transform char data type from mysql to postgresql

Args:
    - metadata_dict(dictionary): dictionary of column metadata

Returns:
    target_script(string): string for the column ddl
"""

def default(metadata_dict: dict[str: str]) -> str:
    char_max_length = metadata_dict['char_max_length']
    target_script = f'char({char_max_length})'
    return target_script