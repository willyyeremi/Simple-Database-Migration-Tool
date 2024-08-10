def varchar(product_target: str, metadata_dict: dict[str: str]) -> str:
    """
    Transform varchar data type from mysql
    
    Args:
        - product_target(string): the product name of destination
        - metadata_dict(dictionary): dictionary of column metadata
    
    Returns:
        target_script(string): string for the column ddl
    """
    char_max_length = metadata_dict['char_max_length']
    if product_target == 'mysql':
        target_script = f'varchar({char_max_length})'
    elif product_target == 'mariadb':
        target_script = f'varchar({char_max_length})'
    elif product_target == 'postgresql':
        target_script = f'varchar({char_max_length})'
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
    char_max_length = metadata_dict['char_max_length']
    if product_target == 'mysql':
        target_script = f'tinytext({char_max_length})'
    elif product_target == 'mariadb':
        target_script = f'tinytext({char_max_length})'
    elif product_target == 'postgresql':
        target_script = f'text'
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
    char_max_length = metadata_dict['char_max_length']
    if product_target == 'mysql':
        target_script = f'text({char_max_length})'
    elif product_target == 'mariadb':
        target_script = f'text({char_max_length})'
    elif product_target == 'postgresql':
        target_script = f'text'
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
    char_max_length = metadata_dict['char_max_length']
    if product_target == 'mysql':
        target_script = f'mediumtext({char_max_length})'
    elif product_target == 'mariadb':
        target_script = f'mediumtext({char_max_length})'
    elif product_target == 'postgresql':
        target_script = f'text'
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
    char_max_length = metadata_dict['char_max_length']
    if product_target == 'mysql':
        target_script = f'longtext({char_max_length})'
    elif product_target == 'mariadb':
        target_script = f'longtext({char_max_length})'
    elif product_target == 'postgresql':
        target_script = f'text'
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
    char_max_size = metadata_dict['char_max_size']
    if product_target == 'mysql':
        target_script = f'binary({char_max_size})'
    elif product_target == 'mariadb':
        target_script = f'binary({char_max_size})'
    elif product_target == 'postgresql':
        target_script = f'bytea'
    return target_script

def varbinary(product_target: str, metadata_dict: dict[str: str]) -> str:
    """
    Transform varbinary data type from mysql
    
    Args:
        - product_target(string): the product name of destination
        - metadata_dict(dictionary): dictionary of column metadata
    
    Returns:
        target_script(string): string for the column ddl
    """
    char_max_size = metadata_dict['char_max_size']
    if product_target == 'mysql':
        target_script = f'varbinary({char_max_size})'
    elif product_target == 'mariadb':
        target_script = f'varbinary({char_max_size})'
    elif product_target == 'postgresql':
        target_script = f'bytea'
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
    if product_target == 'mysql':
        target_script = f'tinyblob'
    elif product_target == 'mariadb':
        target_script = f'tinyblob'
    elif product_target == 'postgresql':
        target_script = f'bytea'
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
    char_max_size = metadata_dict['char_max_size']
    if product_target == 'mysql':
        target_script = f'blob({char_max_size})'
    elif product_target == 'mariadb':
        target_script = f'blob({char_max_size})'
    elif product_target == 'postgresql':
        target_script = f'bytea'
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
    if product_target == 'mysql':
        target_script = f'mediumblob'
    elif product_target == 'mariadb':
        target_script = f'mediumblob'
    elif product_target == 'postgresql':
        target_script = f'bytea'
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
    if product_target == 'mysql':
        target_script = f'longblob'
    elif product_target == 'mariadb':
        target_script = f'longblob'
    elif product_target == 'postgresql':
        target_script = f'bytea'
    return target_script

def year(product_target: str, metadata_dict: dict[str: str]) -> str:
    """
    Transform date data type from mysql
    
    Args:
        - product_target(string): the product name of destination
        - metadata_dict(dictionary): dictionary of column metadata
    
    Returns:
        target_script(string): string for the column ddl
    """
    if product_target == 'mysql':
        target_script = f'year'
    elif product_target == 'mariadb':
        target_script = f'year'
    elif product_target == 'postgresql':
        target_script = f'int2'
    return target_script

def date(product_target: str, metadata_dict: dict[str: str]) -> str:
    """
    Transform date data type from mysql
    
    Args:
        - product_target(string): the product name of destination
        - metadata_dict(dictionary): dictionary of column metadata
    
    Returns:
        target_script(string): string for the column ddl
    """
    if product_target == 'mysql':
        target_script = f'date'
    elif product_target == 'mariadb':
        target_script = f'date'
    elif product_target == 'postgresql':
        target_script = f'date'
    return target_script

def time(product_target: str, metadata_dict: dict[str: str]) -> str:
    """
    Transform date data type from mysql
    
    Args:
        - product_target(string): the product name of destination
        - metadata_dict(dictionary): dictionary of column metadata
    
    Returns:
        target_script(string): string for the column ddl
    """
    time_precision = metadata_dict['time_precision']
    if time_precision is not None:
        time_precision = f'({time_precision})'
    else:
        time_precision = ''
    if product_target == 'mysql':
        target_script = f'time{time_precision}'
    elif product_target == 'mariadb':
        target_script = f'time{time_precision}'
    elif product_target == 'postgresql':
        target_script = f'time{time_precision}'
    return target_script

def datetime(product_target: str, metadata_dict: dict[str: str]) -> str:
    """
    Transform datetime data type from mysql
    
    Args:
        - product_target(string): the product name of destination
        - metadata_dict(dictionary): dictionary of column metadata
    
    Returns:
        target_script(string): string for the column ddl
    """
    time_precision = metadata_dict['time_precision']
    if time_precision is not None:
        time_precision = f'({time_precision})'
    else:
        time_precision = ''
    if product_target == 'mysql':
        target_script = f'datetime{time_precision}'
    elif product_target == 'mariadb':
        target_script = f'datetime{time_precision}'
    elif product_target == 'postgresql':
        target_script = f'timestamp{time_precision}'
    return target_script

def timestamp(product_target: str, metadata_dict: dict[str: str]) -> str:
    """
    Transform timestamp data type from mysql
    
    Args:
        - product_target(string): the product name of destination
        - metadata_dict(dictionary): dictionary of column metadata
    
    Returns:
        target_script(string): string for the column ddl
    """
    time_precision = metadata_dict['time_precision']
    if time_precision is not None:
        time_precision = f'({time_precision})'
    else:
        time_precision = ''
    if product_target == 'mysql':
        target_script = f'timestamp{time_precision}'
    elif product_target == 'mariadb':
        target_script = f'timestamp{time_precision}'
    elif product_target == 'postgresql':
        target_script = f'timestamp{time_precision}'
    return target_script

def json(product_target: str, metadata_dict: dict[str: str]) -> str:
    """
    Transform json data type from mysql
    
    Args:
        - product_target(string): the product name of destination
        - metadata_dict(dictionary): dictionary of column metadata
    
    Returns:
        target_script(string): string for the column ddl
    """
    if product_target == 'mysql':
        target_script = f'json'
    elif product_target == 'mariadb':
        target_script = f'json'
    elif product_target == 'postgresql':
        target_script = f'json'
    return target_script


# NOTE: not implemented yet
def enum(product_target: str, metadata_dict: dict[str: str]) -> str:
    raise NotImplementedError

def set(product_target: str, metadata_dict: dict[str: str]) -> str:
    raise NotImplementedError