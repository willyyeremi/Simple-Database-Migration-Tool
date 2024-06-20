def tinyint(product_target: str, metadata_dict: dict[str: str]) -> str:
    """
    Transform tinyint data type from mysql
    
    Args:
        - product_target(string): the product name of destination
        - metadata_dict(dictionary): dictionary of column metadata
    
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
        - metadata_dict(dictionary): dictionary of column metadata
    
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
        - metadata_dict(dictionary): dictionary of column metadata
    
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
        - metadata_dict(dictionary): dictionary of column metadata
    
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
        - metadata_dict(dictionary): dictionary of column metadata
    
    Returns:
        target_script(string): string for the column ddl
    """
    from re import match
    is_nullable = metadata_dict[is_nullable]
    number_type_attribute = metadata_dict[number_type_attribute]
    extra = metadata_dict[extra]
    if is_nullable == 'NOT NULL' and match("unsigned.*", number_type_attribute) and match(".*auto_increment.*", extra):
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
        - metadata_dict(dictionary): dictionary of column metadata
    
    Returns:
        target_script(string): string for the column ddl
    """
    if product_target == 'mysql':
        target_script = f'float'
    elif product_target == 'postgresql':
        target_script = f'real'
    elif product_target == 'mariadb':
        target_script = f'float'
    return target_script

def double(product_target: str, metadata_dict: dict[str: str]) -> str:
    """
    Transform double data type from mysql
    
    Args:
        - product_target(string): the product name of destination
        - metadata_dict(dictionary): dictionary of column metadata
    
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
        - metadata_dict(dictionary): dictionary of column metadata
    
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

def char(product_target: str, metadata_dict: dict[str: str]) -> str:
    """
    Transform char data type from mysql
    
    Args:
        - product_target(string): the product name of destination
        - metadata_dict(dictionary): dictionary of column metadata
    
    Returns:
        target_script(string): string for the column ddl
    """
    char_max_length = metadata_dict[char_max_length]
    char_set = metadata_dict[char_set]
    char_collation = metadata_dict[char_collation]
    if product_target == 'mysql':
        target_script = f'char({char_max_length}) set {char_set} collation {char_collation}'
    elif product_target == 'postgresql':
        target_script = f'char({char_max_length})'
    elif product_target == 'mariadb':
        target_script = f'char({char_max_length}) set {char_set} collation {char_collation}'
    return target_script

def varchar(product_target: str, metadata_dict: dict[str: str]) -> str:
    """
    Transform varchar data type from mysql
    
    Args:
        - product_target(string): the product name of destination
        - metadata_dict(dictionary): dictionary of column metadata
    
    Returns:
        target_script(string): string for the column ddl
    """
    char_max_length = metadata_dict[char_max_length]
    char_set = metadata_dict[char_set]
    char_collation = metadata_dict[char_collation]
    if product_target == 'mysql':
        target_script = f'varchar({char_max_length}) set {char_set} collation {char_collation}'
    elif product_target == 'postgresql':
        target_script = f'varchar({char_max_length})'
    elif product_target == 'mariadb':
        target_script = f'varchar({char_max_length}) set {char_set} collation {char_collation}'
    return target_script

def tinytext(product_target: str, metadata_dict: dict[str: str]) -> str:
    """
    Transform tinytext data type from mysql
    
    Args:
        - product_target(string): the product name of destination
        - metadata_dict(dictionary): dictionary of column metadata
    
    Returns:
        target_script(string): string for the column ddl
    """
    char_max_length = metadata_dict[char_max_length]
    char_set = metadata_dict[char_set]
    char_collation = metadata_dict[char_collation]
    if product_target == 'mysql':
        target_script = f'tinytext({char_max_length}) set {char_set} collation {char_collation}'
    elif product_target == 'postgresql':
        target_script = f'text'
    elif product_target == 'mariadb':
        target_script = f'tinytext({char_max_length}) set {char_set} collation {char_collation}'
    return target_script

def text(product_target: str, metadata_dict: dict[str: str]) -> str:
    """
    Transform text data type from mysql
    
    Args:
        - product_target(string): the product name of destination
        - metadata_dict(dictionary): dictionary of column metadata
    
    Returns:
        target_script(string): string for the column ddl
    """
    char_max_length = metadata_dict[char_max_length]
    char_set = metadata_dict[char_set]
    char_collation = metadata_dict[char_collation]
    if product_target == 'mysql':
        target_script = f'text({char_max_length}) set {char_set} collation {char_collation}'
    elif product_target == 'postgresql':
        target_script = f'text'
    elif product_target == 'mariadb':
        target_script = f'text({char_max_length}) set {char_set} collation {char_collation}'
    return target_script

def mediumtext(product_target: str, metadata_dict: dict[str: str]) -> str:
    """
    Transform mediumtext data type from mysql
    
    Args:
        - product_target(string): the product name of destination
        - metadata_dict(dictionary): dictionary of column metadata
    
    Returns:
        target_script(string): string for the column ddl
    """
    char_max_length = metadata_dict[char_max_length]
    char_set = metadata_dict[char_set]
    char_collation = metadata_dict[char_collation]
    if product_target == 'mysql':
        target_script = f'mediumtext({char_max_length}) set {char_set} collation {char_collation}'
    elif product_target == 'postgresql':
        target_script = f'text'
    elif product_target == 'mariadb':
        target_script = f'mediumtext({char_max_length}) set {char_set} collation {char_collation}'
    return target_script

def longtext(product_target: str, metadata_dict: dict[str: str]) -> str:
    """
    Transform longtext data type from mysql
    
    Args:
        - product_target(string): the product name of destination
        - metadata_dict(dictionary): dictionary of column metadata
    
    Returns:
        target_script(string): string for the column ddl
    """
    char_max_length = metadata_dict[char_max_length]
    char_set = metadata_dict[char_set]
    char_collation = metadata_dict[char_collation]
    if product_target == 'mysql':
        target_script = f'longtext({char_max_length}) set {char_set} collation {char_collation}'
    elif product_target == 'postgresql':
        target_script = f'text'
    elif product_target == 'mariadb':
        target_script = f'longtext({char_max_length}) set {char_set} collation {char_collation}'
    return target_script

def binary(product_target: str, metadata_dict: dict[str: str]) -> str:
    """
    Transform binary data type from mysql
    
    Args:
        - product_target(string): the product name of destination
        - metadata_dict(dictionary): dictionary of column metadata
    
    Returns:
        target_script(string): string for the column ddl
    """
    char_max_size = metadata_dict[char_max_size]
    char_set = metadata_dict[char_set]
    char_collation = metadata_dict[char_collation]
    if product_target == 'mysql':
        target_script = f'binary({char_max_size})'
    elif product_target == 'postgresql':
        target_script = f'bytea'
    elif product_target == 'mariadb':
        target_script = f'binary({char_max_size})'
    return target_script

def tinyblob(product_target: str, metadata_dict: dict[str: str]) -> str:
    """
    Transform tinyblob data type from mysql
    
    Args:
        - product_target(string): the product name of destination
        - metadata_dict(dictionary): dictionary of column metadata
    
    Returns:
        target_script(string): string for the column ddl
    """
    char_max_size = metadata_dict[char_max_size]
    if product_target == 'mysql':
        target_script = f'tinyblob'
    elif product_target == 'postgresql':
        target_script = f'bytea'
    elif product_target == 'mariadb':
        target_script = f'tinyblob'
    return target_script

def blob(product_target: str, metadata_dict: dict[str: str]) -> str:
    """
    Transform blob data type from mysql
    
    Args:
        - product_target(string): the product name of destination
        - metadata_dict(dictionary): dictionary of column metadata
    
    Returns:
        target_script(string): string for the column ddl
    """
    char_max_size = metadata_dict[char_max_size]
    if product_target == 'mysql':
        target_script = f'blob({char_max_size})'
    elif product_target == 'postgresql':
        target_script = f'bytea'
    elif product_target == 'mariadb':
        target_script = f'blob({char_max_size})'
    return target_script

def mediumblob(product_target: str, metadata_dict: dict[str: str]) -> str:
    """
    Transform mediumblob data type from mysql
    
    Args:
        - product_target(string): the product name of destination
        - metadata_dict(dictionary): dictionary of column metadata
    
    Returns:
        target_script(string): string for the column ddl
    """
    char_max_size = metadata_dict[char_max_size]
    if product_target == 'mysql':
        target_script = f'mediumblob'
    elif product_target == 'postgresql':
        target_script = f'bytea'
    elif product_target == 'mariadb':
        target_script = f'mediumblob'
    return target_script

def longblob(product_target: str, metadata_dict: dict[str: str]) -> str:
    """
    Transform longblob data type from mysql
    
    Args:
        - product_target(string): the product name of destination
        - metadata_dict(dictionary): dictionary of column metadata
    
    Returns:
        target_script(string): string for the column ddl
    """
    char_max_size = metadata_dict[char_max_size]
    if product_target == 'mysql':
        target_script = f'longblob'
    elif product_target == 'postgresql':
        target_script = f'bytea'
    elif product_target == 'mariadb':
        target_script = f'longblob'
    return target_script