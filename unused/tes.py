# default lib
import datetime
import pathlib
import os
import sys
import urllib.parse

# open lib
import pandas
import sqlalchemy

# custom lib
root = str(pathlib.Path(__file__).parent.parent)
sys.path.insert(0,str(root))
import access


# credential
product = 'mysql'
envi = access.trg_envi
user = access.trg_user
pwd = access.trg_pwd
host = access.trg_host
port = access.trg_port
db = access.trg_db
schema = 'intranet.balitower.co.id'

# current timestamp
current_timestamp = datetime.datetime.strftime(datetime.datetime.now(), '%Y%m%d_%H%M%S')

class unique_identifier:
    def __init__(self,product,host,port,user,pwd,db,schema,envi):
        self.product = product
        self.envi = envi
        self.host = host
        self.port = port
        self.user = user
        self.pwd = pwd
        self.db = db
        self.schema = schema

    def run_engine(self):
        method_name = f"{self.product}"
        if hasattr(self, method_name):
            method = getattr(self, method_name)
            return method()
        else:
            print(f"Engine '{method_name}' not found.")

    def postgres(self):
        url = f"postgresql+psycopg2://{self.user}:{urllib.parse.quote_plus(self.pwd)}@{self.host}:{self.port}/{self.db}"
        engine = sqlalchemy.create_engine(url)
        conn = engine.connect()
        table_primary_key = pandas.DataFrame(conn.execute(sqlalchemy.sql.text('''
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
                a.table_schema = '{schema_target}' 
            ORDER BY
                a.table_schema,
                a.table_name,
                c.ordinal_position'''.format(schema_target=schema))))
        table_primary_key = table_primary_key.dropna(subset=['column name'])
        table_unique_constraint = pandas.DataFrame(conn.execute(sqlalchemy.sql.text('''
            select 
                a.table_name as "table name"
                ,a.column_name as "column name"
                ,a.constraint_name as "constraint name"
            from 
                information_schema.constraint_column_usage as a
                left join
                information_schema.table_constraints as b
                on
                    a.table_schema = b.table_schema
                    and 
                    a.table_name = b.table_name
                    and 
                    a.constraint_name = b.constraint_name 
            where 
                b.constraint_type = 'UNIQUE'
                and 
                a.table_schema = '{schema_target}'
            '''.format(schema_target=schema))))
        table_unique_constraint = table_unique_constraint.dropna(subset=['column name'])
        foreign_key_table = pandas.DataFrame(conn.execute(sqlalchemy.sql.text("""
            with
                data_1 as(
                    select
                        row_number() over(order by b.nspname,c.relname,a.conname,array_position(a.conkey,e.attnum)) as id
                        ,b.nspname as schema_name
                        ,c.relname as table_name
                        ,d.relname  as parent_table_name
                        ,a.conname as constraint_name
                        ,a.conkey
                        ,e.attname as column_child
                        ,e.attnum
                    from
                        pg_catalog.pg_constraint as a
                        left join
                        pg_catalog.pg_namespace as b
                        on a.connamespace = b."oid" 
                        left join 
                        pg_catalog.pg_class as c
                        on a.conrelid = c."oid" 
                        left join 
                        pg_catalog.pg_class as d
                        on a.confrelid = d."oid" 
                        left join 
                        pg_catalog.pg_attribute as e
                        on 
                            e.attnum = any(a.conkey) 
                            and 
                            e.attrelid = a.conrelid 
                    where 
                        a.contype = 'f'
                        and 
                        b.nspname = '{schema_target}'
                    order by 
                        b.nspname
                        ,c.relname 
                        ,a.conname
                        ,array_position(a.conkey,e.attnum))
                ,data_2 as(
                    select
                        row_number() over(order by b.nspname,c.relname,a.conname,array_position(a.confkey,e.attnum)) as id
                        ,b.nspname as schema_name
                        ,c.relname as table_name
                        ,d.relname as parent_table_name
                        ,a.conname as constraint_name
                        ,a.confkey
                        ,e.attname as column_parent
                        ,e.attnum
                    from
                        pg_catalog.pg_constraint as a
                        left join
                        pg_catalog.pg_namespace as b
                        on a.connamespace = b."oid" 
                        left join 
                        pg_catalog.pg_class as c
                        on a.conrelid = c."oid" 
                        left join 
                        pg_catalog.pg_class as d
                        on a.confrelid = d."oid" 
                        left join 
                        pg_catalog.pg_attribute as e
                        on 
                            e.attnum = any(a.confkey) 
                            and 
                            e.attrelid = a.confrelid 
                    where 
                        a.contype = 'f'
                        and 
                        b.nspname = '{schema_target}'
                    order by 
                        b.nspname
                        ,c.relname 
                        ,a.conname
                        ,array_position(a.confkey,e.attnum))
            select 
                a.schema_name
                ,a.table_name
                ,a.parent_table_name
                ,a.column_child
                ,b.column_parent
            from 
                data_1 as a
                left join
                data_2 as b
                on
                    a.id=b.id
                return (table_primary_key,table_unique_constraint)
            """.format(schema_target=schema))))