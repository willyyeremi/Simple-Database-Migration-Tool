"""
Transform bigint data type from mysql to postgresql

Args:
    - metadata_dict(dictionary): dictionary of column metadata

Returns:
    target_script(string): string for the column ddl
"""

def default(metadata_dict: dict[str: str]) -> str:
    from re import match
    is_nullable = metadata_dict['is_nullable']
    integer_type_attribute = metadata_dict['integer_type_attribute']
    extra = metadata_dict['extra']
    if is_nullable == 'NOT NULL' and match("unsigned.*", integer_type_attribute) and match(".*auto_increment.*", extra):
        target_script = f'serial8'
    else:
        target_script = f'int8'
    return target_script