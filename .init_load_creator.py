# defaul lib
import sys
import pathlib
import os

# open lib
import pandas

# custom lib
root = str(pathlib.Path(__file__))
sys.path.insert(0,str(root))
import access


# variable
level = '3'
schema_source = 
schema_target = 
type_load = 'initial'

# list of table from source
table_level_list = pandas.read_csv(root+"\schema {schema}\data\\table_level_list.csv".format(schema=schema_target),delimiter="|")
table_level_list = table_level_list.loc[table_level_list["LEVEL"] == 'LV '+level]
table_level_list = table_level_list["table name"].values.tolist()

# Variable for loop
if type_load == 'initial':
    var_a = 'init'
    var_b = 'initial_load'
    var_c = 'init_load'
elif type_load == 'incremental':
    var_a = 'init'
    var_b = 'incremental_load'
    var_c = 'incre_load'
sub_job_level = 'sub_job_1.'+level

# loop for creating file
for table in table_level_list:
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
    new_file_path = os.path.join(root+'\schema {schema_target}\etl\{var_b}\\{schema_target} {var_c}\sub_job_1\{sub_job_level}'.format(schema_target=schema_target,var_b=var_b,var_c=var_c,sub_job_level=sub_job_level), new_file_name)
    # Create the new file and write the content into it
    with open(new_file_path, 'w') as new_file:
        new_file.write(new_file_content)