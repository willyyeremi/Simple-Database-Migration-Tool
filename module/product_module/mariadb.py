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
    return f"mariadb+mariadbconnector://{user}:{password}@{host}:{port}/{database}"

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