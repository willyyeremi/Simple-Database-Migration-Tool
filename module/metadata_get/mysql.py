from pandas import DataFrame

def url(user: str, password: str, host: str, port: str, database: str) -> str:
    """
    Get connection url of sqlalchemy for Oracle database. 

    Args:
        - user(string): username to access database
        - password string): password to access database
        - host(string): host to access database
        - port(string): port to access database
        - database(string): database name

    Returns:
        url_string(string): connection url of sqlalchemy for Oracle database 
    """
    from urllib.parse import quote_plus
    return f"mysql+mysqlconnector://{user}:{quote_plus(password)}@{host}:{port}/{database}"

def version(connection: object):
    """
    Get the version of database. This will affect metadata table composition.
    
    Args:
        connection(object): sqlalchemy connection object
    
    Returns:
        data(string): the database engine version
    """
    from sqlalchemy.sql import text
    script = f"""
        SELECT 
            version()"""
    data: DataFrame = DataFrame(connection.execute(text(script)))
    data: str = data.iloc[0, 0]
    return data

def all_schema(connection: object) -> list[str]:
    """
    Get all avaible schema on the database. The dataframe columns description are:
    - schema_name(string): name of all schemas inside the database

    Args:
        - connection(object): sqlalchemy connection object

    Returns:
        data(list): list of schemas from database
    """
    from sqlalchemy.sql import text
    script = f"""
        SELECT 
            schema_name schema_name
        FROM 
            information_schema.schemata
        WHERE 
            SCHEMA_NAME NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
        ORDER BY 
            schema_name"""
    data: DataFrame = DataFrame(connection.execute(text(script)))
    data: list[str] = data['schema_name'].values.tolist()
    return data

def all_table(connection:object, schema:str) -> DataFrame:
    """
    Get all name of tables in a schema. The dataframe columns description are:
    - table_name(string): name of all tables inside the schema 
    - table_comment(string): the table comment or description

    Args:
        - connection(object): sqlalchemy connection object
        - schema(string): name of the schema that the metadata want to get extracted

    Returns:
        data(pandas DataFrame): dataframe containing desired metadata
    """
    from sqlalchemy.sql import text
    script = f"""
        select 
            table_name
            ,table_comment
        from
            INFORMATION_SCHEMA.tables
        where
            table_type = 'BASE TABLE'
            and
            table_schema = '{schema}'"""
    data: DataFrame = DataFrame(connection.execute(text(script)))
    return data

def column_rule(connection:object, schema:str) -> DataFrame:
    """
    Get all primary key in a schema. The dataframe columns description are:
    - table_name(string): name of the table
    - column_name(string): name of the column
    - ordinal_position(string): the position of column on table from left
    - is_nullable(string): 'NULL' if column can have null, 'NOT NULL' if can not
    - column_comment(string): column comment
    - default_value(string): the default value expression
    - data_type(string): the data type of the column
    - char_max_length(string): maximum length of string data type by total character
    - char_max_size(string): maximum length of string data type by total size
    - char_set(string): character set of string data type
    - char_collation(string): collation name of string data type
    - integer_type_attribute(string): 'signed', 'unsigned', 'zerofill' attribute for number data type
    - numeric_precision(integer): precision for numeric data type
    - numeric_scale(integer): scale for numeric data type
    - datetime_precision(integer): total digit of fraction second of datetime or timestamp data type
    - generated_column_type(string): type of generated value column, whether 'stored' or 'virtual'
    - extra(string): other expression for the column that will be useful

    Args:
        - connection(object): sqlalchemy connection object
        - schema(string): name of the schema that the metadata want to get extracted

    Returns:
        data(pandas DataFrame): dataframe containing desired metadata
    """
    from sqlalchemy.sql import text
    script = f"""
        SELECT 
            table_name
            ,column_name
            ,ordinal_position
            ,CASE 
                when
                    is_nullable = 'YES'
                    THEN 'NULL'
                ELSE 
                    'NOT NULL'
            END is_nullable
            ,column_comment
            ,column_default as default_value
            ,data_type
            ,character_maximum_length as char_max_length
            ,character_octet_length as char_max_size
            ,character_set_name as char_set
            ,collation_name as char_collation
            ,CASE 
                when
                    column_type like '%zerofill%'
                    then 'zerofill'
                when
                    column_type like '%unsigned%'
                    then 'unsigned'
                else
                    'signed'
            END as integer_type_attribute
            ,numeric_precision
            ,numeric_scale
            ,datetime_precision
            ,case
                when 
                    extra like '%STORED%'
                    then 'stored'
                when
                    extra like '%VIRTUAL%'
                    then 'virtual'
                else
                    ''
            end as generated_column_type
            ,case
                when
                    extra like '%auto_increment%'
                    then 'auto_increment'
                when
                    extra like '%on update CURRENT_TIMESTAMP%'
                    then 'on update CURRENT_TIMESTAMP'
                else
                    ''
            end as extra
        from
            information_schema.columns
        where
            TABLE_SCHEMA = '{schema}'
        order by
            table_name asc
            ,ordinal_position asc"""
    data: DataFrame = DataFrame(connection.execute(text(script)))
    return data

def primary_key(connection: object, schema: str) -> DataFrame:
    """
    Get all primary key in a schema. The dataframe columns description are:
    - table_name(string): name of all tables inside the schema 
    - column_name(string): name of all columns that selected as primary key
    - constraint_name(string): name of the constraint that define the primary key

    Args:
        - connection(object): sqlalchemy connection object
        - schema(string): name of the schema that the metadata want to get extracted

    Returns:
        data(pandas DataFrame): dataframe containing desired metadata
    """
    from sqlalchemy.sql import text
    script = f"""
        SELECT 
            c.table_name
            ,cn.column_name
            ,c.constraint_name 
        from
            information_schema.table_constraints c
            left join
            INFORMATION_SCHEMA.KEY_COLUMN_USAGE cn
            on
                c.CONSTRAINT_SCHEMA = cn.CONSTRAINT_SCHEMA 
                and
                c.TABLE_NAME  = cn.TABLE_NAME 
                and
                c.CONSTRAINT_NAME = cn.CONSTRAINT_NAME 
        WHERE 
            c.constraint_type = 'PRIMARY KEY'
            and
            c.table_schema = '{schema}'
            and
            cn.TABLE_SCHEMA = '{schema}'"""
    data: DataFrame = DataFrame(connection.execute(text(script)))
    return data

def relation(connection: object, schema: str) -> DataFrame:
    """
    Get all unique constraint in a schema. The dataframe columns description are:
    - table_name(string): name of all tables inside the schema 
    - parent_table_name(string): name of all parent tables used by the table at table_name
    - column_child(string): name of column that referencing to other table
    - column_parent(string): name of column that get referenced at parent table
    - constraint_name(string): name of the foreign key constraint
    - on_update(string): the action when value on parent table get updated (oracle does not have this data so this column will be null)
    - on_delete(string): the action when value on parent table get deleted

    Args:
        - connection(object): sqlalchemy connection object
        - schema(string): name of the schema that the metadata want to get extracted

    Returns:
        data(pandas DataFrame): dataframe containing desired metadata
    """
    from sqlalchemy.sql import text
    script = f"""
        SELECT 
            tc.TABLE_NAME as table_name
            ,kcu.REFERENCED_TABLE_NAME as parent_table_name
            ,kcu.COLUMN_NAME as column_child
            ,kcu.REFERENCED_COLUMN_NAME as column_parent
            ,tc.CONSTRAINT_NAME as constraint_name 
            ,rc.UPDATE_RULE as on_update
            ,rc.DELETE_RULE as on_delete
        FROM 
            INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc
            left JOIN 
            INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS kcu
            ON 
                tc.CONSTRAINT_SCHEMA = kcu.CONSTRAINT_SCHEMA 
                AND 
                tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
                and
                tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
            left join INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS as rc
            on
                tc.CONSTRAINT_SCHEMA = rc.CONSTRAINT_SCHEMA 
                and
                tc.TABLE_NAME = rc.TABLE_NAME 
                and
                tc.CONSTRAINT_NAME = rc.CONSTRAINT_NAME 
        WHERE 
            tc.CONSTRAINT_TYPE = 'FOREIGN KEY'
            and
            tc.TABLE_SCHEMA = '{schema}'
            and
            kcu.TABLE_SCHEMA = '{schema}'"""
    data: DataFrame = DataFrame(connection.execute(text(script)))
    return data

def unique_constraint(connection: object, schema: str) -> DataFrame:
    """
    Get all primary key in a schema. The dataframe columns description are:
    - table_name(string): name of all tables inside the schema 
    - column_name(string): name of all columns that selected for unique constraint
    - constraint_name(string): name of the unique constraint

    Args:
        - connection(object): sqlalchemy connection object
        - schema(string): name of the schema that the metadata want to get extracted

    Returns:
        data(pandas DataFrame): dataframe containing desired metadata
    """
    from sqlalchemy.sql import text
    script = f"""
        SELECT 
            c.table_name
            ,cn.column_name
            ,c.constraint_name 
        from
            information_schema.table_constraints c
            left join
            INFORMATION_SCHEMA.KEY_COLUMN_USAGE cn
            on
                c.CONSTRAINT_SCHEMA = cn.CONSTRAINT_SCHEMA 
                and
                c.TABLE_NAME  = cn.TABLE_NAME 
                and
                c.CONSTRAINT_NAME = cn.CONSTRAINT_NAME 
        WHERE 
            c.constraint_type = 'UNIQUE'
            and
            c.table_schema = '{schema}'
            and
            cn.TABLE_SCHEMA = '{schema}'"""
    data: DataFrame = DataFrame(connection.execute(text(script)))
    return data

def check_constraint(connection: object, schema: str) -> DataFrame:
    """
    Get all primary key in a schema. The dataframe columns description are:
    - table_name(string): name of all tables inside the schema 
    - constraint_name(string): name of the unique constraint
    - check_clause(string): the expression of check constraint

    Args:
        - connection(object): sqlalchemy connection object
        - schema(string): name of the schema that the metadata want to get extracted

    Returns:
        data(pandas DataFrame): dataframe containing desired metadata
    """
    from sqlalchemy.sql import text
    script = f"""
        select 
            a.TABLE_NAME as table_name
            ,a.CONSTRAINT_NAME as constraint_name
            ,B.CHECK_CLAUSE as constraint_expression
        from 
            information_schema.TABLE_CONSTRAINTS a
            left join
            information_schema.check_constraints b
            on
                a.CONSTRAINT_SCHEMA = b.CONSTRAINT_SCHEMA 
                and 
                a.CONSTRAINT_NAME = b.CONSTRAINT_NAME 
        where 
            a.CONSTRAINT_TYPE = 'CHECK'
            and
            a.CONSTRAINT_SCHEMA = '{schema}'
            and 
            b.CONSTRAINT_NAME = '{schema}'"""
    data: DataFrame = DataFrame(connection.execute(text(script)))
    return data

def all_index(connection: object, schema: str) -> DataFrame:
    """
    Get all primary key in a schema. The dataframe columns description are:
    - table_name(string): name of all tables inside the schema 
    - index_name(string): name of all index inside the schema
    - index_type(string): the index type (only BTree)
    - collation(string): How the column is sorted in the index. This can have values A (ascending), D (descending), or NULL (not sorted)
    - nullable(string): Contains 'YES' if the column may contain NULL values and 'NO' if not
    - is_unique(integer): 0 if the index cannot contain duplicates, 1 if it can
    - column_expression_cardinality(integer): The column sequence number in the index, starting with 1
    - column_expression(string): column expression (including the name of column) for the index

    Args:
        - connection(object): sqlalchemy connection object
        - schema(string): name of the schema that the metadata want to get extracted

    Returns:
        data(pandas DataFrame): dataframe containing desired metadata
    """
    from sqlalchemy.sql import text
    script = f"""
        SELECT 
            TABLE_NAME as table_name
            ,INDEX_NAME as index_name 
            ,INDEX_TYPE as index_type
            ,`COLLATION` as `collation`
            ,case 
                when
                    NULLABLE = ''
                    then 'NO'
                else
                    NULLABLE 
            end as nullable 
            ,NON_UNIQUE as is_unique
            ,INDEX_COMMENT as index_comment
            ,SEQ_IN_INDEX as column_expression_cardinality
            ,case 
                when
                    column_name is null
                    then expression
                when
                    sub_part is not null
                    then concat(column_name,'(', cast(sub_part as unsigned), ')')
                else
                    column_name
            end column_expression
        FROM 
            information_schema.statistics
        where
            INDEX_SCHEMA = '{schema}'"""
    data: DataFrame = DataFrame(connection.execute(text(script)))
    return data