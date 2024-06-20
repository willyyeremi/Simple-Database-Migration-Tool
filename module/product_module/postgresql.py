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
    return f"postgresql+psycopg2://{user}:{quote_plus(password)}@{host}:{port}/{database}"

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
            schema_name
        FROM 
            information_schema.schemata
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
        SELECT
            c.relname AS table_name
            ,obj_description(c.oid, 'pg_class') AS table_comment
        FROM
            pg_class c
            JOIN
            pg_namespace n 
            ON 
                c.relnamespace = n.oid
        WHERE
            n.nspname = '{schema}'
            AND 
            c.relkind = 'r'"""
    data: DataFrame = DataFrame(connection.execute(text(script)))
    return data

def primary_key(connection: object, schema: str) -> DataFrame:
    """
    Get all primary key in a schema. The dataframe columns description are:
    - table_name(string): name of all tables inside the schema 
    - column_name(string): name of all columns that selected as primary key
    - constraint_name(string): name of the constraint that define the primary key

    Args:
        - connection (object): sqlalchemy connection object
        - schema (string): name of the schema that the metadata want to get extracted

    Returns:
        data(pandas DataFrame): dataframe containing desired metadata
    """
    from sqlalchemy.sql import text
    script = f"""
        with
            data_1 as (
                select
                    s.nspname as "schema"
                    ,cl.relname as table_name
                    ,unnest (c.conkey) as column_name_id
                    ,c.conname as constraint_name
                from 
                    pg_catalog.pg_constraint c
                    left join
                    pg_catalog.pg_class cl
                    on
                        c.conrelid = cl."oid"
                    left join
                    pg_catalog.pg_namespace s
                    on
                        c.connamespace = s."oid" 
                where 
                    c.contype = 'p'
                    and
                    s.nspname = '{schema}')
        select 
            data_1.table_name
            ,cp.column_name 
            ,data_1.constraint_name
        from
            data_1
            left join
            information_schema.columns cp
            on
                data_1."schema" = cp.table_schema 
                and
                data_1.table_name = cp.table_name 
                and
                data_1.column_name_id = cp.ordinal_position
        where
            cp.table_schema = '{schema}'"""
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
        with 
            data_1 as (
                select 
                    s.nspname as "schema"
                    ,cp.relname as table_name
                    ,cc.relname as parent_table_name
                    ,unnest(con.conkey) as column_child_id
                    ,unnest(con.confkey) as column_parent_id
                    ,con.conname as constraint_name
                    ,con.confupdtype as on_update
                    ,con.confdeltype as on_delete
                from 
                    pg_catalog.pg_constraint con
                    left join
                    pg_catalog.pg_class cc
                    on
                        con.confrelid = cc."oid" 
                    left join
                    pg_catalog.pg_class cp
                    on
                        con.conrelid = cp."oid" 
                    left join 
                    pg_catalog.pg_namespace s
                    on
                        con.connamespace = s."oid" 
                WHERE
                    con.contype = 'f'
                    and
                    s.nspname  = '{schema}')
        select 
            data_1.table_name
            ,data_1.parent_table_name
            ,ccn.column_name as column_child
            ,cpn.column_name as column_parent
            ,data_1.constraint_name
            ,data_1.on_update
            ,data_1.on_delete
        from 
            data_1
            left join
            information_schema.columns ccn
            on
                data_1."schema" = ccn.table_schema 
                and
                data_1.table_name = ccn.table_name 
                and
                data_1.column_child_id = ccn.ordinal_position 
            left join
            information_schema.columns cpn
            on
                data_1."schema" = cpn.table_schema 
                and
                data_1.table_name = cpn.table_name 
                and
                data_1.column_parent_id = cpn.ordinal_position 
        where 
            ccn.table_schema = '{schema}'"""
    data: DataFrame = DataFrame(connection.execute(text(script)))
    return data

def unique_constraint(connection: object, schema: str) -> DataFrame:
    """
    Get all primary key in a schema. The dataframe columns description are:
    - table_name(string): name of all tables inside the schema 
    - column_name(string): name of all columns that selected as primary key
    - constraint_name(string): name of the unique constraint

    Args:
        - connection (object): sqlalchemy connection object
        - schema (string): name of the schema that the metadata want to get extracted

    Returns:
        data(pandas DataFrame): dataframe containing desired metadata
    """
    from sqlalchemy.sql import text
    script = f"""
        with
            data_1 as (
                select
                    s.nspname as "schema"
                    ,cl.relname as table_name
                    ,unnest (c.conkey) as column_name_id
                    ,c.conname as constraint_name
                from 
                    pg_catalog.pg_constraint c
                    left join
                    pg_catalog.pg_class cl
                    on
                        c.conrelid = cl."oid"
                    left join
                    pg_catalog.pg_namespace s
                    on
                        c.connamespace = s."oid" 
                where 
                    c.contype = 'u'
                    and
                    s.nspname = '{schema}')
        select 
            data_1.table_name
            ,cp.column_name 
            ,data_1.constraint_name
        from
            data_1
            left join
            information_schema.columns cp
            on
                data_1."schema" = cp.table_schema 
                and
                data_1.table_name = cp.table_name 
                and
                data_1.column_name_id = cp.ordinal_position
        where
            cp.table_schema = '{schema}'"""
    data: DataFrame = DataFrame(connection.execute(text(script)))
    return data