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
product = 'postgres'
user = access.trg_user
pwd = access.trg_pwd
host = access.trg_host
port = access.trg_port
db = access.trg_db
schema = 'bss_billengine'

# current timestamp
current_timestamp = datetime.datetime.strftime(datetime.datetime.now(), '%Y%m%d_%H%M%S')

class table_and_relation:
    def __init__(self,product,host,port,user,pwd,db,schema,envi=''):
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

    def oracle(self):
        os.environ["PATH"] = f"{self.envi};" + os.environ["PATH"]
        url = f"oracle+cx_oracle://{self.user}:{urllib.parse.quote_plus(self.pwd)}@{self.host}:{self.port}/?service_name={self.db}"
        engine = sqlalchemy.create_engine(url)
        conn = engine.connect()
        relation = pandas.DataFrame(conn.execute(sqlalchemy.sql.text("""
        SELECT
            a.table_name AS "table_name"
            ,c.table_name AS "references"
        FROM
            all_cons_columns a
            LEFT JOIN
            all_constraints b
            ON a.owner = b.owner AND a.constraint_name = b.constraint_name
            LEFT JOIN
            all_constraints c 
            ON b.r_owner = c.owner AND b.r_constraint_name = c.constraint_name
        WHERE
            b.constraint_type = 'R'
        AND
            a.owner = {schema}""".format(schema=self.schema))))
        relation['key']=relation['table_name']+relation['references']
        relation = relation.drop_duplicates(subset=['key'])
        relation = relation.drop('key', axis=1)
        all_table = pandas.DataFrame(conn.execute(sqlalchemy.sql.text("""
        SELECT 
            table_name
        FROM 
            all_tables
        WHERE 
            owner = {schema}""".format(schema=self.schema))))
        return (relation,all_table)

    def postgres(self):
        url = f"postgresql+psycopg2://{self.user}:{urllib.parse.quote_plus(self.pwd)}@{self.host}:{self.port}/{self.db}"
        engine = sqlalchemy.create_engine(url)
        conn = engine.connect()
        relation =  pandas.DataFrame(conn.execute(sqlalchemy.sql.text("""
        select
            c.relname as table_name
            ,d.relname  as references
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
        where 
            a.contype = 'f'
            and b.nspname = '{schema}'
        order by 
            c.relname 
            ,d.relname
        """.format(schema=self.schema))))
        relation['key']=relation['table_name']+relation['references']
        relation = relation.drop_duplicates(subset=['key'])
        relation = relation.drop('key', axis=1)
        all_table = pandas.DataFrame(conn.execute(sqlalchemy.sql.text("""
        SELECT 
            table_name
        from 
            information_schema.tables
        WHERE 
            table_schema = '{schema}'
            and table_type = 'BASE TABLE'
        order by
            table_name
        """.format(schema=self.schema))))
        return (relation,all_table)

# engine runner
relation, all_table = table_and_relation(product,host,port,user,pwd,db,schema).run_engine()

# creating level 1 as base
level_0 = pandas.merge(all_table,relation, on='table_name', how='left')
level = level_0.loc[level_0['references'].isnull()].drop('references',axis=1)
level['LEVEL'] = '1'

# detecting relation to self
detector = level_0.dropna(subset='references').drop_duplicates(subset=['table_name','references'])

def self_relation_identifier(arg):
    if arg['table_name'] == arg['references']:
        return 'Y'
    elif arg['table_name'] != arg['references']:
        return 'X' 

detector['self_relation'] = detector.apply(self_relation_identifier,axis=1)
detector_pivot = detector.pivot_table(values='references',index='table_name',columns='self_relation',aggfunc='count').reset_index()
if 'Y' in detector_pivot.columns:
    level_1 = detector_pivot.loc[detector_pivot['X'].isnull()].drop(['X','Y'],axis=1)
    level = pandas.concat([level,level_1])
    non_leveled = detector_pivot.loc[detector_pivot['X'].notnull()].drop(['X','Y'],axis=1)
else:
    non_leveled = detector_pivot.loc[detector_pivot['X'].notnull()].drop(['X'],axis=1)

level_counter = 2
while non_leveled.shape[0] > 0 and level_counter < 11:
    level_x = pandas.merge(non_leveled,relation,on='table_name', how='left')
    level_x['self_relation'] = level_x.apply(self_relation_identifier,axis=1)
    level_x = level_x.loc[level_x['self_relation']=='X'].drop('self_relation',axis=1) 
    level_x = pandas.merge(level_x,level,left_on='references',right_on='table_name', how='left')

    def identifier(arg):
        if pandas.notna(arg['LEVEL']):
            return 'Y'
        elif pandas.isna(arg['LEVEL']):
            return 'X' 

    level_x['identifier_1'] = level_x.apply(identifier,axis=1)
    level_x['identifier_2'] = level_x['identifier_1']
    level_x = level_x.drop(['references','table_name_y','LEVEL'],axis = 1).rename(columns={"table_name_x": "table_name"})
    level_x = level_x.pivot_table(values='identifier_2',index='table_name',columns='identifier_1',aggfunc='count')
    level_x = level_x.reset_index()
    if 'X' in level_x.columns:
        new_data = level_x.loc[level_x['X'].isnull()].drop(['X','Y'],axis=1)
        new_data['LEVEL'] = '{}'.format(level_counter)
        level = pandas.concat([level,new_data])
        not_defined_data = level_x[level_x['X'].notna()].drop(['X','Y'],axis=1)
        non_leveled = not_defined_data
        level_counter = level_counter + 1
    else:
        new_data = level_x.drop('Y',axis=1)
        new_data['LEVEL'] = '{}'.format(level_counter)
        level = pandas.concat([level,new_data])
        non_leveled = pandas.DataFrame(columns=['table_name','LEVEL'])
level = level.rename(columns={"table_name": "table name"})
level.to_csv(path_or_buf=root+'\\table level measure\output\{schema} table level {current_timestamp}.csv'.format(schema=schema,current_timestamp=current_timestamp),sep='|',index=False)