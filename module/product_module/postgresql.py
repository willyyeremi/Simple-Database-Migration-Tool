def url(user,password,host,port,database):
    import urllib.parse
    return f"postgresql+psycopg2://{user}:{urllib.parse.quote_plus(password)}@{host}:{port}/{database}"

def script_primary_key(schema):
    schema = schema
    script = f"""
        SELECT
            a.table_name as "table name"
            ,c.column_name AS "column name"
        FROM
            information_schema.tables as a
            left JOIN
            information_schema.table_constraints as b
            ON 
                a.table_name = b.table_name 
                AND 
                a.table_schema = b.table_schema 
                AND 
                b.constraint_type = 'PRIMARY KEY'
            left JOIN
            information_schema.key_column_usage as c
            ON 
                a.table_name = c.table_name 
                AND 
                a.table_schema = c.table_schema 
                AND 
                b.constraint_name = c.constraint_name
        where
            a.table_schema = '{schema}' 
        ORDER BY
            a.table_schema,
            a.table_name,
            c.ordinal_position"""
    table_primary_key = table_primary_key.dropna(subset=['column name'])
    return script