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
import source_access
import target_access


# credential
product = 'oracle'
source_or_target = 'source'
schema = 'CRESTELBILLINGPRD623'

if source_or_target == 'source':
    envi = source_access.envi
    user = source_access.user
    pwd = source_access.pwd
    host = source_access.host
    port = source_access.port
    db = source_access.db
elif source_or_target == 'target':
    envi = target_access.envi
    user = target_access.user
    pwd = target_access.pwd
    host = target_access.host
    port = target_access.port
    db = target_access.db

# current timestamp
current_timestamp = datetime.datetime.strftime(datetime.datetime.now(), '%Y%m%d_%H%M%S')

class table_and_relation:
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

    def oracle(self):
        os.environ["PATH"] = f"{self.envi};" + os.environ["PATH"]
        url = f"oracle+cx_oracle://{self.user}:{urllib.parse.quote_plus(self.pwd)}@{self.host}:{self.port}/?service_name={self.db}"
        engine = sqlalchemy.create_engine(url)
        conn = engine.connect()
        all_table = pandas.DataFrame(conn.execute(sqlalchemy.sql.text(f"""
            SELECT 
                table_name
            FROM 
                all_tables
            WHERE 
                owner = '{self.schema}'""")))
        relation = pandas.DataFrame(conn.execute(sqlalchemy.sql.text(f"""
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
            a.owner = '{self.schema}'""")))
        if len(relation) != 0:
            relation['key']=relation['table_name']+relation['references']
            relation = relation.drop_duplicates(subset=['key'])
            relation = relation.drop('key', axis=1)
        table_datetime_column = pandas.DataFrame(conn.execute(sqlalchemy.sql.text(f'''
            SELECT 
                table_name as "table_name"
                ,1 as "datetime_exists"
            FROM 
                all_tab_columns
            WHERE 
                owner = '{self.schema}'
                AND 
                (data_type LIKE 'TIMESTAMP(%)' 
                OR
                data_type LIKE 'TIMESTAMP(%) WITH TIME ZONE'
                OR 
                data_type LIKE 'DATE')
            ORDER BY 
                table_name''')))
        if len(table_datetime_column) != 0:
            table_datetime_column = table_datetime_column.drop_duplicates(subset=['table_name'])
        table_unique_constraint = pandas.DataFrame(conn.execute(sqlalchemy.sql.text(f'''
            SELECT
                all_constraints.TABLE_NAME AS "table_name"
                ,all_cons_columns.COLUMN_NAME AS "column_name"
                ,1 as "unique_identifier_exists"
            FROM 
                all_constraints
                LEFT JOIN
                ALL_CONS_COLUMNS 
                ON
                    all_constraints.CONSTRAINT_NAME= ALL_CONS_COLUMNS.CONSTRAINT_NAME
            WHERE 
                all_constraints.owner = '{self.schema}'
                AND 
                ALL_CONSTRAINTS.CONSTRAINT_TYPE in ('U','P')
            ''')))
        if len(table_unique_constraint) != 0:
            table_unique_constraint = table_unique_constraint.dropna(subset=['column_name'])
            table_unique_constraint = table_unique_constraint.drop('column_name',axis=1).drop_duplicates(subset=['table_name'])
        table_unique_index = pandas.DataFrame(conn.execute(sqlalchemy.sql.text(f'''
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
                all_indexes.table_owner = '{self.schema}'
                AND
                all_indexes.uniqueness = 'UNIQUE'
                AND 
                all_indexes.table_type = 'TABLE'
                AND
                (ALL_CONSTRAINTS.CONSTRAINT_TYPE NOT IN ('P','U')
                OR 
                ALL_CONSTRAINTS.CONSTRAINT_TYPE IS NULL)
            ''')))
        if len(table_unique_index) != 0:
            table_unique_index = table_unique_index.dropna(subset=['column_name'])
            table_unique_index = table_unique_index.drop('column_name',axis=1).drop_duplicates(subset=['table_name'])
        unique_identifier = pandas.concat([table_unique_constraint,table_unique_index])
        unique_identifier = unique_identifier.drop_duplicates(subset=['table_name'])
        return (all_table,relation,table_datetime_column,unique_identifier)

    def postgres(self):
        url = f"postgresql+psycopg2://{self.user}:{urllib.parse.quote_plus(self.pwd)}@{self.host}:{self.port}/{self.db}"
        engine = sqlalchemy.create_engine(url)
        conn = engine.connect()
        all_table = pandas.DataFrame(conn.execute(sqlalchemy.sql.text(f"""
            SELECT 
                table_name
            from 
                information_schema.tables
            WHERE 
                table_schema = '{schema}'
                and table_type = 'BASE TABLE'
            order by
                table_name
            """)))
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
        if len(relation) != 0:
            relation['key']=relation['table_name']+relation['references']
            relation = relation.drop_duplicates(subset=['key'])
            relation = relation.drop('key', axis=1)
        return (all_table,relation)

    def mysql(self):
        url = f"mysql+pymysql://{self.user}:{urllib.parse.quote_plus(self.pwd)}@{self.host}/{self.db}?charset=utf8mb4"
        engine = sqlalchemy.create_engine(url)
        conn = engine.connect()
        all_table = pandas.DataFrame(conn.execute(sqlalchemy.sql.text("""
            SELECT 
                TABLE_NAME as table_name
            FROM 
                INFORMATION_SCHEMA.TABLES
            WHERE
                TABLE_SCHEMA = '{schema}'
                AND
                TABLE_TYPE = 'BASE TABLE';
            """.format(schema=self.schema))))
        relation =  pandas.DataFrame(conn.execute(sqlalchemy.sql.text("""
            SELECT
                TABLE_NAME as table_name
                ,REFERENCED_TABLE_NAME as "references"
            FROM
                INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE
                TABLE_SCHEMA = '{schema}'
                AND
                REFERENCED_TABLE_NAME IS NOT NULL;
            """.format(schema=self.schema))))
        if len(relation) != 0:
            relation['key']=relation['table_name']+relation['references']
            relation = relation.drop_duplicates(subset=['key'])
            relation = relation.drop('key', axis=1)
        return (all_table,relation)

# engine runner
all_table, relation, table_datetime_column, unique_identifier = table_and_relation(product,host,port,user,pwd,db,schema,envi).run_engine()

# determining table level
if len(relation) != 0:
    # level 1 (table without relation)
    table_with_relation = pandas.DataFrame(pandas.concat([relation['table_name'],relation['references']],ignore_index=True).drop_duplicates(),columns=['table name'])
    level_1 = pandas.merge(all_table,table_with_relation,left_on='table_name',right_on='table name',how='left')
    level = level_1[level_1['table name'].isna()].drop('table name',axis=1)
    level['LEVEL'] = 'LV 1'
    # creating level 3 as base (the most outer parent)
    level_1 = pandas.merge(level_1[level_1['table name'].notna()].drop('table name',axis=1),relation, on='table_name', how='left')
    level_3 = level_1.loc[level_1['references'].isnull()].drop('references',axis=1)
    level_3['LEVEL'] = 'LV 3'
    level = pandas.concat([level,level_3])
    # detecting relation to self
    detector = relation.dropna(subset='references').drop_duplicates(subset=['table_name','references'])
    # function to give designated value based on relation existence
    def self_relation_identifier(arg):
        if arg['table_name'] == arg['references']:
            return 'Y'
        elif arg['table_name'] != arg['references']:
            return 'X' 
    detector['self_relation'] = detector.apply(self_relation_identifier,axis=1)
    detector_pivot = detector.pivot_table(values='references',index='table_name',columns='self_relation',aggfunc='count').reset_index()
    if 'Y' in detector_pivot.columns:
        level_2 = detector_pivot.loc[detector_pivot['X'].isnull()].drop(['X','Y'],axis=1)
        level_2['LEVEL'] = 'LV 2'
        level = pandas.concat([level,level_2])
        non_leveled = detector_pivot.loc[detector_pivot['X'].notnull()].drop(['X','Y'],axis=1)
    else:
        non_leveled = detector_pivot.loc[detector_pivot['X'].notnull()].drop(['X'],axis=1)
    level_counter = 4
    while non_leveled.shape[0] > 0:
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
            new_data['LEVEL'] = 'LV {}'.format(level_counter)
            level = pandas.concat([level,new_data])
            not_defined_data = level_x[level_x['X'].notna()].drop(['X','Y'],axis=1)
            non_leveled = not_defined_data
            level_counter = level_counter + 1
        else:
            new_data = level_x.drop('Y',axis=1)
            new_data['LEVEL'] = 'LV {}'.format(level_counter)
            level = pandas.concat([level,new_data])
            non_leveled = pandas.DataFrame(columns=['table_name','LEVEL'])
else:
    level = all_table
    level = level.drop('references', axis=1)
    level['LEVEL'] = 'LV 1'
level['numerical_part'] = level['LEVEL'].str.extract(r'(\d+)').astype(int)
level = level.sort_values(by=['numerical_part', 'table_name'],ascending=[True, True])
level = level.drop(columns='numerical_part')

# joining the datetime and unique identifier to created table
data = pandas.merge(left=pandas.merge(left=level,right=table_datetime_column,how='left',on='table_name'),right=unique_identifier,how='left',on='table_name')
data.fillna({"datetime_exists": 0, "unique_identifier_exists": 0}, inplace=True)
data = data.astype({"datetime_exists": 'int', "unique_identifier_exists": 'int'})
def etl_type(row):
    if row['datetime_exists'] == 1 and row['unique_identifier_exists'] == 1:
        return 'update insert dengan tanggal'
    elif row['datetime_exists'] == 0 and row['unique_identifier_exists'] == 1:
        if row['LEVEL'] == 'LV 1' or row['LEVEL'] == 'LV 3':
            return 'update insert semua baris' 
        else:
            return 'update insert semua baris mengacu foreign key'
    else:
        return 'truncate insert'
data['jenis incremental load'] = data.apply(etl_type, axis=1)

data.to_csv(path_or_buf=root + f'\\etl type determinator\output\engine- {product}, host- {host}, db- {db}, schema-{schema} {current_timestamp}.csv',sep='|',index=False)