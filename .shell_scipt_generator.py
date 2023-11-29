# defaul lib
import sys
import pathlib
import os

# open lib
import pandas


# root directory
root = str(pathlib.Path(__file__).parent.parent)
sys.path.insert(0,str(root))

schema = 'bss_billengine'
level = '3'

# list of table from source
table_list = pandas.read_csv(root+"\schema {schema}\data\\table_level_list.csv".format(schema=schema),delimiter="|")
table_list = table_list.loc[table_list["LEVEL"] == 'LV '+level]
table_list = table_list["table name"].values.tolist()

script_list=[]
for table in table_list:
    script ='"d:/BTS Academy/GIT Repo/bss_prd/.virtualenv/Scripts/python.exe"'+' '+'"d:/BTS Academy/GIT Repo/bss_prd/schema {schema}/etl/initial_load/{schema} init_load/sub_job_1/sub_job_1.{level}/{table}_init.py"'.format(schema=schema,table=table,level=level)
    script_list.append(script)

with open('./{schema} level {level} shell_script_list.txt'.format(schema=schema,level=level), 'w') as f:
    for txt in script_list:
        f.write(txt+'\n')
f.close()