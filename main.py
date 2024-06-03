# default lib
import datetime
import os
import pathlib

# open lib
import pandas
import sqlalchemy

# custom lib
import module
import module.connection as connection

# current timestamp
current_timestamp:datetime = datetime.datetime.strftime(datetime.datetime.now(), '%Y%m%d_%H%M%S')

# credential
product: str = connection.source.product
host: str = connection.source.host
port: str = connection.source.port
user: str = connection.source.user
password: str = connection.source.password
database: str = connection.source.database
local_environment: str = connection.source.local_environment

# database schema
schema = 'SYS'

class runner:
    def __init__(self,*,product: str,host: str,port: str,user: str,password: str,database: str,local_environment: str,schema: str) -> None:
        self.product: str = product
        self.host: str = host
        self.port: str = port
        self.user: str = user
        self.password: str = password
        self.database: str = database
        self.local_environment: str = local_environment
        self.schema: str = schema

    def __str__(self) -> str:
        method_name:str = f"{self.product}"
        if hasattr(module, method_name):
            method = getattr(module, method_name)
            url = method.url(user=self.user,password=self.password,host=self.host,port=self.port,database=self.database)
            if self.local_environment != '':
                return f'product target:{self.product}, schema target: {self.schema}, connection url: {url}'
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
    
    def __eq__(self, other: object) -> bool:
        return self.__dict__ == other.__dict__

    def level_measure(self) -> pandas.DataFrame:
        product = self.product
        local_environment = self.local_environment
        method_name = f"{product}"
        if hasattr(module, method_name):
            if self.local_environment != '':
                os.environ["PATH"] = f"{local_environment};" + os.environ["PATH"]
            method = getattr(module, method_name)
            url = method.url(user = self.user, password = self.password, host = self.host, port = self.port, database = self.database)
            engine = sqlalchemy.create_engine(url)
            conn = engine.connect()
            all_table = method.all_table(conn, self.schema)
            if len(all_table) != 0:
                all_table = all_table.drop(['table_comments'], axis=1)
            relation = method.relation(conn, self.schema)
            if len(relation) != 0:
                relation = relation.drop(['column_child', 'column_parent', 'constraint_name',' on_update', 'on_delete'], axis=1)
                relation = relation.rename(columns = {"parent_table_name": "references"})
                relation['key'] = relation['table_name'] + relation['references']
                relation = relation.drop_duplicates(subset = ['key'])
                relation = relation.drop('key', axis = 1)
            level = module.toolbox.level_measure(all_table, relation)
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