"""
Transform datetime data type from mysql to mysql

Args:
    - metadata_dict(dictionary): dictionary of column metadata

Returns:
    target_script(string): string for the column ddl
"""

def default(metadata_dict: dict[str: str]) -> str:
    time_precision = metadata_dict['time_precision']
    if time_precision is not None:
        time_precision = f'({time_precision})'
    else:
        time_precision = ''
    target_script = f'datetime{time_precision}'
    return target_script