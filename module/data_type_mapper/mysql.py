def tinyint(product_target: str, metadata_dict: dict[str: str]) -> str:
    """
    Transform tinyint data type from mysql
    
    Args:
        - product_target(string): the product name of destination
        - number_type_attribute(string): tinyint attibutes argument
    
    Returns:
        target_script(string): string for the column ddl
    """
    number_type_attribute = metadata_dict[number_type_attribute]
    if product_target == 'mysql':
        target_script = f'tinyint {number_type_attribute}'
    elif product_target == 'postgresql':
        target_script = f'smallint'
    elif product_target == 'mariadb':
        target_script = f'tinyint {number_type_attribute}'
    return target_script

def smallint(product_target: str, metadata_dict: dict[str: str]) -> str:
    """
    Transform smallint data type from mysql
    
    Args:
        - product_target(string): the product name of destination
        - number_type_attribute(string): smallint attibutes argument
    
    Returns:
        target_script(string): string for the column ddl
    """
    number_type_attribute = metadata_dict[number_type_attribute]
    if product_target == 'mysql':
        target_script = f'smallint {number_type_attribute}'
    elif product_target == 'postgresql':
        target_script = f'smallint'
    elif product_target == 'mariadb':
        target_script = f'smallint {number_type_attribute}'
    return target_script

def mediumint(product_target: str, metadata_dict: dict[str: str]) -> str:
    """
    Transform mediumint data type from mysql
    
    Args:
        - product_target(string): the product name of destination
        - number_type_attribute(string): mediumint attibutes argument
    
    Returns:
        target_script(string): string for the column ddl
    """
    number_type_attribute = metadata_dict[number_type_attribute]
    if product_target == 'mysql':
        target_script = f'mediumint {number_type_attribute}'
    elif product_target == 'postgresql':
        target_script = f'integer'
    elif product_target == 'mariadb':
        target_script = f'mediumint {number_type_attribute}'
    return target_script

def int(product_target: str, metadata_dict: dict[str: str]) -> str:
    """
    Transform int data type from mysql
    
    Args:
        - product_target(string): the product name of destination
        - number_type_attribute(string): int attibutes argument
    
    Returns:
        target_script(string): string for the column ddl
    """
    number_type_attribute = metadata_dict[number_type_attribute]
    if product_target == 'mysql':
        target_script = f'int {number_type_attribute}'
    elif product_target == 'postgresql':
        target_script = f'integer'
    elif product_target == 'mariadb':
        target_script = f'int {number_type_attribute}'
    return target_script

def bigint(product_target: str, metadata_dict: dict[str: str]) -> str:
    """
    Transform bigint data type from mysql
    
    Args:
        - product_target(string): the product name of destination
        - number_type_attribute(string): bigint attibutes argument
    
    Returns:
        target_script(string): string for the column ddl
    """
    from re import match
    is_nullable = metadata_dict[is_nullable]
    number_type_attribute = metadata_dict[number_type_attribute]
    extra = metadata_dict[extra]
    if is_nullable == 'NOT NULL' and match("unsigned.+", number_type_attribute) and match(".+auto_increment.+", extra):
        if product_target == 'mysql':
            target_script = f'bigint {number_type_attribute} auto_increment'
        elif product_target == 'postgresql':
            target_script = f'bigserial'
        elif product_target == 'mariadb':
            target_script = f'bigint {number_type_attribute} auto_increment'
    else:
        if product_target == 'mysql':
            target_script = f'bigint {number_type_attribute}'
        elif product_target == 'postgresql':
            target_script = f'bigint'
        elif product_target == 'mariadb':
            target_script = f'bigint {number_type_attribute}'
    return target_script

def float(product_target: str, metadata_dict: dict[str: str]) -> str:
    """
    Transform float data type from mysql
    
    Args:
        - product_target(string): the product name of destination
        - number_type_attribute(string): float attibutes argument
    
    Returns:
        target_script(string): string for the column ddl
    """
    if product_target == 'mysql':
        target_script = f'float'
    elif product_target == 'postgresql':
        target_script = f'float'
    elif product_target == 'mariadb':
        target_script = f'float'
    return target_script

def double(product_target: str, metadata_dict: dict[str: str]) -> str:
    """
    Transform double data type from mysql
    
    Args:
        - product_target(string): the product name of destination
        - number_type_attribute(string): double attibutes argument
    
    Returns:
        target_script(string): string for the column ddl
    """
    
    if product_target == 'mysql':
        target_script = f'double'
    elif product_target == 'postgresql':
        target_script = f'double precision'
    elif product_target == 'mariadb':
        target_script = f'double'
    return target_script

def numeric(product_target: str, metadata_dict: dict[str: str]) -> str:
    """
    Transform numeric data type from mysql
    
    Args:
        - product_target(string): the product name of destination
        - number_type_attribute(string): numeric attibutes argument
    
    Returns:
        target_script(string): string for the column ddl
    """
    numeric_precision = metadata_dict[numeric_precision]
    numeric_scale= metadata_dict[numeric_scale]
    if product_target == 'mysql':
        target_script = f'numeric({numeric_precision, numeric_scale})'
    elif product_target == 'postgresql':
        target_script = f'numeric({numeric_precision, numeric_scale})'
    elif product_target == 'mariadb':
        target_script = f'numeric({numeric_precision, numeric_scale})'
    return target_script