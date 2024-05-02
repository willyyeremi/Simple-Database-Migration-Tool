# default lib
import datetime
import os
import pathlib

# open lib
import pandas
import sqlalchemy

# custom lib
import module
import connection_credential


# credential
product = connection_credential.source.product
host = connection_credential.source.host
port = connection_credential.source.port
user = connection_credential.source.user
password = connection_credential.source.password
database = connection_credential.source.database
local_environment = connection_credential.source.local_environment

# database schema
schema = 'SYS'

# current timestamp
current_timestamp = datetime.datetime.strftime(datetime.datetime.now(), '%Y%m%d_%H%M%S')

class runner:
    def __init__(self,*,product: str,host: str,port: str,user: str,password: str,database: str,local_environment: str,schema: str) -> None:
        self.product = product
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.local_environment = local_environment
        self.schema = schema

    def __str__(self) -> str:
        method_name = f"{self.product}"
        if hasattr(module, method_name):
            method = getattr(module, method_name)
            url = method.url(user=self.user,password=self.password,host=self.host,port=self.port,database=self.database)
            if self.local_environment != '':
                return f'product target:{self.product}, schema target: {self.schema},connection url: {url}'
            else:
                return f'product target:{self.product}, local environment path: {self.local_environment}, schema target: {self.schema}, connection url: {url}'
        else:
            print(f"Engine '{method_name}' not found.")

    def __repr__(self) -> str:
        method_name = f"{self.product}"
        if hasattr(module, method_name):
            return f'runner(product={self.product},local_environment={self.local_environment},host={self.host},port={self.port},user={self.user},password={self.password},database={self.database},schema={self.schema})'
        else:
            print(f"Engine '{method_name}' not found.")
    
    def __eq__(self, other: self) -> bool:
        return self.__dict__ == other.__dict__

    def level_measure(self):
        product = self.product
        local_environment = self.local_environment
        method_name = f"{product}"
        if hasattr(module, method_name):
            if self.local_environment != '':
                os.environ["PATH"] = f"{local_environment};" + os.environ["PATH"]
            method = getattr(module, method_name)
            url = method.url(user=self.user,password=self.password,host=self.host,port=self.port,database=self.database)
            engine = sqlalchemy.create_engine(url)
            conn = engine.connect()
            all_table = method.all_table(conn,self.schema)
            if len(all_table) != 0:
                all_table = all_table.drop(['table_comments'], axis=1)
            relation = method.relation(conn,self.schema)
            if len(relation) != 0:
                relation = relation.drop(['column_child', 'column_parent','constraint_name','on_update','on_delete'], axis=1)
                relation = relation.rename(columns={"parent_table_name": "references"})
                relation['key'] = relation['table_name'] + relation['references']
                relation = relation.drop_duplicates(subset=['key'])
                relation = relation.drop('key', axis=1)
            level = module.toolbox.level_measure(all_table,relation)
            return level
        else:
            print(f"Engine '{method_name}' not found.")

os.environ["PATH"] = f"{local_environment};" + os.environ["PATH"]
url = module.oracle.url(user=user,password=password,host=host,port=port,database=database)
engine = sqlalchemy.create_engine(url)
conn = engine.connect()
# check_constraint = pandas.DataFrame(conn.execute(sqlalchemy.sql.text(module.oracle.script_all_table(schema))))
# x = module.oracle.all_table(conn,schema)
# x.to_csv(path_or_buf=str(pathlib.Path(__file__).parent) + f'\\tes.csv',sep='|',index=False)

x = runner(product=product,user=user,password=password,host=host,port=port,database=database,local_environment=local_environment,schema=schema).level_measure()
print(x)