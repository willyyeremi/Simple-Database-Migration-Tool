# this file for centralized container of credentials
# please write with order like below:
# 1. product
# 2. host
# 3. port
# 4. user
# 5. password
# 6. database
# 7. local_environment (if you are using oracle)
# you can set the variable name freely. just remember which is which when using it.

class connection:
    '''
    Class object to create a connection object that can be accessed from another module
    '''
    created_instances = {}
    rejected_instances = {}

    def __new__(cls, name: str, product: str, host: str, port: str, user: str, password: str, database: str, local_environment: str) -> object:
        connection_id = f'{product}{host}{port}{user}{password}{database}{local_environment}'
        identical_instances = [instances for instances, created_connection_id in cls.created_instances.items() if connection_id == created_connection_id]
        if len(identical_instances) != 0:
            cls.rejected_instances[identical_instances[0]].append(name)
        else:
            cls.created_instances[name]= connection_id
            cls.rejected_instances[name] = []
            return super().__new__(cls)

    def __init__(self, name: str, product: str, host: str, port: str, user: str, password: str, database: str, local_environment: str) -> None:
        # name of the connection
        self.name = name
        # name of the database product
        self.product = product
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        # name of the database
        self.database = database
        # path to oracle instantclient, if using oracle
        self.local_environment = local_environment

    def __str__(self) -> str:
        return f'product = {self.product}, local environment path = {self.local_environment} -> {self.user}:{self.password}@{self.host}:{self.port}'

    def __repr__(self) -> str:
        return f'connection(name={self.name},product={self.product},host={self.host},port={self.port},user={self.user},password={self.password},database={self.database},local_environment={self.local_environment})'

    def __eq__(self, other: object) -> bool:
        return self.__dict__ == other.__dict__

    @classmethod
    def duplicate_check(cls) -> None:
        non_duplicate_credential = [name for name in cls.rejected_instances.keys() if len(cls.rejected_instances[name]) == 0]
        if len(cls.rejected_instances.keys()) != len(non_duplicate_credential):
            for key in cls.rejected_instances.keys():
                if len(cls.rejected_instances[key]) != 0:
                    print(f'the credentials inside the list at below are identical with credential named "{key}". please delete.')
                    print(cls.rejected_instances[key])
        else:
            print('there is no duplicate credential')

# ----- This is the beginning of the section where you can write credential -----

source = connection(
    'source'
    ,'oracle'
    ,'10.13.0.42'
    ,'1521'
    ,'CRESTELBILLINGPRD623'
    ,'CRESTELBILLINGPRD623'
    ,'ocspri'
    ,'C:\oracle\instantclient_21_13')

target = connection(
    'target'
    ,'postgresql'
    ,'clstrdb.balitower.co.id'
    ,'3306'
    ,'afhive.pratama'
    ,'y3ki5piH3_p!'
    ,'intranet.balitower.co.id'
    ,'')

# ----- This is the end of the section where you can write credential -----

# check if there is duplicate credential by running this python file first
def main():
    connection.duplicate_check()

if __name__ == '__main__':
    main()