"""
Module to connect and get metadata from Oracle database.
"""

from urllib.parse import quote_plus
from pandas import DataFrame
from sqlalchemy.sql import text


def url(*, user: str, password: str, host: str, port: str, database: str) -> str:
    """
    Get connection url of sqlalchemy for Oracle database. 

    Args:
        - user (string): username to access database
        - password (string): password to access database
        - host (string): host to access database
        - port (string): port to access database
        - database (string): database name

    Returns:
        url_string (string): connection url of sqlalchemy for Oracle database 
    """
    user = user
    password = password
    host = host
    port = port
    database = database
    url_string: str = f"oracle+cx_oracle://{user}:{quote_plus(password)}@{host}:{port}/?service_name={database}"
    return url_string

def all_table(connection:object, schema:str) -> DataFrame:
    """
    Get all name of tables in a schema. The dataframe columns description are:
    - table_name(string): name of all tables inside the schema 
    - table_comment(string): the table comment or description

    Args:
        - connection (object): sqlalchemy connection object
        - schema (string): name of the schema that the metadata want to get extracted

    Returns:
        data (pandas DataFrame): dataframe containing desired metadata
    """
    schema: str = schema
    script = f"""
        SELECT 
            a.table_name
            ,b.comments as table_comment
        FROM 
            all_tables a
            left join
            all_tab_comments b
            on
                a.owner = b.owner
                and
                a.table_name = b.table_name
        WHERE 
            a.owner = '{schema}'
            and
            b.owner = '{schema}'"""
    data: DataFrame = DataFrame(connection.execute(text(script)))
    return data

def column_rule(connection:object,schema:str) -> DataFrame:
    """
    Get all columns name, data type and nullability in a schema. The dataframe columns description are:
    - table_name(string): name of all tables inside the schema 
    - column_id(string): the location of the column
    - column_name(string): name of the column
    - nullable(boolean): nullability of the column. the value wether True or False
    - data_type(string): data type of the column
    - column_comments(string): the column comment or description
    - default_data(string): the default value of the column
    - data_length(string): length of the maximum data
    - data_precision(string): the precision of the decimal data

    Args:
        - connection (object): sqlalchemy connection object
        - schema (string): name of the schema that the metadata want to get extracted

    Returns:
        DataFrame: dataframe containing desired metadata
    """
    schema: str = schema
    script = f"""
        SELECT 	
            a.table_name
            ,a.column_id
            ,a.column_name
            ,a.nullable
            ,a.data_type
            ,b.comments as column_comment
            ,a.data_default as default_data
            ,a.data_length
            ,a.data_precision
            ,a.data_scale
        FROM
            all_tab_columns a
            left join
            all_col_comments b
            on
                a.owner = b.owner
                and
                a.table_name = b.table_name
                and
                a.column_name = b.column_name
        WHERE
            a.owner = '{schema}'
            and
            b.owner = '{schema}'"""
    data: DataFrame = DataFrame(connection.execute(text(script)))
    return data

def primary_key(connection:object,schema:str) -> DataFrame:
    """
    Get all primary key in a schema. The dataframe columns description are:
    - table_name(string): name of all tables inside the schema 
    - column_name(string): name of all columns that selected as primary key
    - constraint_name(string): name of the constraint that define the primary key

    Args:
        - connection (object): sqlalchemy connection object
        - schema (string): name of the schema that the metadata want to get extracted

    Returns:
        DataFrame: dataframe containing desired metadata
    """
    schema: str = schema
    script = f"""
        SELECT 
            a.table_name
            ,b.column_name
            ,a.constraint_name
        FROM 
            all_constraints a
            LEFT JOIN
            all_cons_columns b
            ON
                a.owner = b.owner
                and
                a.table_name = b.table_name
                and
                a.constraint_name = b.constraint_name
        WHERE 
            a.constraint_type = 'P'
            and
            a.owner = '{schema}'"""
    data: DataFrame = DataFrame(connection.execute(text(script)))
    return data

def unique_constraint(connection:object,schema:str) -> DataFrame:
    """
    Get all unique constraint in a schema. The dataframe columns description are:
    - table_name(string): name of all tables inside the schema 
    - column_name(string): name of all columns that selected for unique constraint
    - constraint_name(string): name of the unique constraint

    Args:
        - connection (object): sqlalchemy connection object
        - schema (string): name of the schema that the metadata want to get extracted

    Returns:
        DataFrame: dataframe containing desired metadata
    """
    schema: str = schema
    script = f"""
        SELECT 
            a.table_name
            ,b.column_name
            ,a.constraint_name
        FROM 
            all_constraints a
            LEFT JOIN
            all_cons_columns b
            ON
                a.owner = b.owner
                and
                a.table_name = b.table_name
                and
                a.constraint_name = b.constraint_name
        WHERE 
            a.constraint_type = 'U'
            and
            a.owner = '{schema}'
            and
            b.owner = '{schema}'"""
    data: DataFrame = DataFrame(connection.execute(text(script)))
    return data

def check_constraint(connection:object,schema:str) -> DataFrame:
    """
    Get all unique constraint in a schema. The dataframe columns description are:
    - table_name(string): name of all tables inside the schema 
    - column_name(string): name of all columns that selected for check constraint
    - constraint_name(string): name of the check constraint
    - search_condition(string): the check constraint expression

    Args:
        - connection (object): sqlalchemy connection object
        - schema (string): name of the schema that the metadata want to get extracted

    Returns:
        DataFrame: dataframe containing desired metadata
    """
    schema: str = schema
    script = f"""
        select
            a.table_name
            ,b.column_name
            ,a.constraint_name 
            ,a.search_condition
        from 
            all_constraints a
            LEFT JOIN
            all_cons_columns b
            ON
                a.owner = b.owner
                and
                a.table_name = b.table_name
                and
                a.constraint_name = b.constraint_name
            LEFT join
            all_tab_columns c
            ON
                b.owner = c.owner
                AND
                b.table_name  = c.table_name
                AND
                b.column_name = c.column_name
        where 
            a.constraint_type = 'C'
            and
            c.nullable = 'Y'
            and
            a.owner = '{schema}'
            and
            b.owner = '{schema}'
            and
            c.owner = '{schema}'
        """
    data: DataFrame = DataFrame(connection.execute(text(script)))
    data['search_condition'] = data['search_condition'].replace("\s+", " ", regex=True).str.strip()
    data['search_condition'] = data['search_condition'].replace("\(\s", "(", regex=True)
    data['search_condition'] = data['search_condition'].replace("\s\)", ")", regex=True)
    data['search_condition'] = data['search_condition'].replace(",\s", ",", regex=True)
    data['search_condition'] = data['search_condition'].replace("\s,", ",", regex=True)
    return data

def relation(connection:object,schema:str) -> DataFrame:
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
        - connection (object): sqlalchemy connection object
        - schema (string): name of the schema that the metadata want to get extracted

    Returns:
        DataFrame: dataframe containing desired metadata
    """
    schema: str = schema
    script = f"""
        SELECT
            a.table_name
            ,c.table_name AS parent_table_name
            ,b.column_name AS column_child
            ,d.column_name AS column_parent
            ,a.constraint_name
            ,null as on_update
            ,a.delete_rule as on_delete
        FROM
            all_constraints a
            LEFT JOIN
            ALL_CONS_COLUMNS b
            ON
                a.owner = b.owner
                and
                a.table_name = b.table_name
                and
                a.constraint_name = b.constraint_name
            LEFT JOIN 
            all_constraints c
            ON
                a.r_owner = c.owner
                AND 
                a.r_constraint_name = c.CONSTRAINT_NAME 
            LEFT JOIN 
            ALL_CONS_COLUMNS d
            ON
                c.owner = d.owner
                and
                c.table_name = d.table_name
                and
                c.constraint_name = d.constraint_name
                AND
                b."POSITION" = d."POSITION" 
        WHERE
            a.constraint_type = 'R'
            AND 
            c.constraint_type IN ('P','U')
            AND
            a.owner = '{schema}'
            and
            b.owner = '{schema}'
            AND 
            c.owner = '{schema}'
            and
            d.owner = '{schema}'"""
    data: DataFrame = DataFrame(connection.execute(text(script)))
    return data

def unique_index(connection:object,schema:str) -> DataFrame:
    """
    Get all unique index in a schema. This unique index exclude the unique index created by primary key and unique constraint. The dataframe columns description are:
    - 

    Args:
        - connection (object): sqlalchemy connection object
        - schema (string): name of the schema that the metadata want to get extracted

    Returns:
        DataFrame: dataframe containing desired metadata
    """
    schema: str = schema
    script = f'''
        SELECT 
            all_indexes.table_name AS "table_name"
            ,all_ind_columns.column_name AS "column_name"
            ,1 as "unique_identifier_exists"
        FROM 
            all_indexes
            LEFT JOIN 
            all_ind_columns 
            ON 
                all_indexes.index_name = all_ind_columns.index_name 
                AND 
                all_indexes.table_NAME = all_ind_columns.table_NAME
                AND
                all_indexes.owner = all_ind_columns.INDEX_owner
            LEFT JOIN 
            all_constraints
            on
                ALL_INDEXES.INDEX_NAME = ALL_CONSTRAINTS.INDEX_NAME
                AND 
                ALL_INDEXES.table_NAME = ALL_CONSTRAINTS.TABLE_NAME
                AND 
                all_indexes.owner = ALL_CONSTRAINTS.owner
        WHERE 
            all_indexes.table_owner = '{schema}'
            AND
            all_indexes.uniqueness = 'UNIQUE'
            AND 
            all_indexes.table_type = 'TABLE'
            AND
            (ALL_CONSTRAINTS.CONSTRAINT_TYPE NOT IN ('P','U')
            OR 
            ALL_CONSTRAINTS.CONSTRAINT_TYPE IS NULL)'''
    data: DataFrame = DataFrame(connection.execute(text(script)))
    raise NotImplementedError('Function still in development')