"""
Transform tinyblob data type from mysql to postgresql

Args:
    - metadata_dict(dictionary): dictionary of column metadata

Returns:
    target_script(string): string for the column ddl
"""

def default(metadata_dict: dict[str: str]) -> str:
    target_script = f'bytea'
    return target_script