def url(user,password,host,port,database):
    import urllib.parse
    return f"mysql+pymysql://{user}:{urllib.parse.quote_plus(password)}@{host}/{database}?charset=utf8mb4"

def script_all_table(schema):
    schema = schema
    script = f"""
        SELECT 
            TABLE_NAME as table_name
        FROM 
            INFORMATION_SCHEMA.TABLES
        WHERE
            TABLE_SCHEMA = '{schema}'
            AND
            TABLE_TYPE = 'BASE TABLE'"""
    return script

def script_relation(schema):
    schema = schema
    script = f"""
        SELECT
            TABLE_NAME as table_name
            ,REFERENCED_TABLE_NAME as "references"
        FROM
            INFORMATION_SCHEMA.KEY_COLUMN_USAGE
        WHERE
            TABLE_SCHEMA = '{schema}'
            AND
            REFERENCED_TABLE_NAME IS NOT NULL"""
    return script