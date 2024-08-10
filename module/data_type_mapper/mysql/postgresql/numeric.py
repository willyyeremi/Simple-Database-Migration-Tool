"""
Transform numeric data type from mysql to postgresql

Args:
    - metadata_dict(dictionary): dictionary of column metadata

Returns:
    target_script(string): string for the column ddl
"""

def default(metadata_dict: dict[str: str]) -> str:
    numeric_precision = metadata_dict['numeric_precision']
    numeric_scale= metadata_dict['numeric_scale']
    target_script = f'numeric({numeric_precision, numeric_scale})'
    return target_script