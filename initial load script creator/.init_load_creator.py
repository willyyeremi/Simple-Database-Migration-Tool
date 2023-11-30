# defaul lib
import sys
import pathlib
import glob
import os
import datetime

# open lib
import pandas

# custom lib
root = str(pathlib.Path(__file__).parent.parent)
sys.path.insert(0,str(root))
import access


# variable
schema_source = 'CRESTELBILLINGPRD623'
schema_target = 'bss_billing'

# current timestamp
current_timestamp = datetime.datetime.strftime(datetime.datetime.now(), '%Y%m%d_%H%M%S')

# list of table from source
table_level_list = pandas.read_csv(glob.glob(root+"\initial load script creator\data\*.csv".format(schema=schema_target))[0],delimiter="|")
level_list = table_level_list.drop('table name',axis=1).drop_duplicates()
level_list = level_list['LEVEL'].values.tolist()
table_list = table_level_list.drop('LEVEL',axis=1).drop_duplicates()
table_list = table_list['table name'].values.tolist()

# Variable for loop
var_a = 'init'
var_b = 'initial_load'
var_c = 'init_load'

# loop for creating file
for level in level_list:
    for table in table_list:
        table_source = str(table).upper()
        table_target = table
        # Specify the new file name and content
        new_file_name = '{table}_{var_a}.py'.format(table=table,var_a=var_a)
        new_file_content = '''# default lib
import sys
import pathlib
import os
import urllib.parse

# open lib
import pandas
import sqlalchemy
import sqlalchemy.orm

# custom lib
root = str(pathlib.Path(__file__).parent.parent.parent.parent.parent.parent.parent)
sys.path.insert(0,str(root))
import access


# source url dan engine
os.environ["PATH"] = f"{{access.src_envi}};" + os.environ["PATH"]
src_url = f"oracle+cx_oracle://{{access.src_user_crestelbeprd623}}:{{urllib.parse.quote_plus(access.src_pwd_crestelbeprd623)}}@{{access.src_host}}:{{access.src_port}}/?service_name={{access.src_service}}"
src_engine = sqlalchemy.create_engine(src_url)
src_conn = src_engine.connect()

# target url dan engine
trg_url = f"postgresql+psycopg2://{{access.trg_user}}:{{urllib.parse.quote_plus(access.trg_pwd)}}@{{access.trg_host}}:{{access.trg_port}}/{{access.trg_db}}"
trg_engine = sqlalchemy.create_engine(trg_url)
trg_conn =trg_engine.connect()

# truncate data di target
with sqlalchemy.orm.sessionmaker(bind=trg_engine)() as trg_truncate_session:
    trg_truncate_session.execute(sqlalchemy.sql.text("truncate {schema_target}.{table_target} cascade"))
    trg_truncate_session.commit()
    trg_truncate_session.close()

# transfer data dari source ke target
src_data = pandas.DataFrame(src_conn.execute(sqlalchemy.sql.text("""
    select 
        * 
    from 
        {schema_source}.{table_source}
    """)))
src_data.columns = src_data.columns.str.lower()
src_data.to_sql(name=str("{table_target}").lower(),con=trg_engine,schema="{schema_target}",if_exists='append', index=False)
src_conn.close()'''.format(table_source=table_source,table_target=table_target,schema_source=schema_source,schema_target=schema_target)
        # Construct the full path to the new file
        path = root+"\initial load script creator\output\{schema_target} {current_timestamp}\{level}".format(schema_target=schema_target,level=level,current_timestamp=current_timestamp)
        if not os.path.exists(path):
            os.makedirs(path)
        new_file_path = os.path.join(path, new_file_name)
        # Create the new file and write the content into it
        with open(new_file_path, 'w') as new_file:
            new_file.write(new_file_content)