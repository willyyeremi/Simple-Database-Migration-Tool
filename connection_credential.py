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
    created_instances = {}
    rejected_instances = {}

    def __new__(cls,name: str,product: str,host: str,port: str,user: str,password: str,database: str,local_environment: str):
        connection_id = f'{product}{host}{port}{user}{password}{database}{local_environment}'
        identical_instances = [instances for instances, created_connection_id in cls.created_instances.items() if connection_id == created_connection_id]
        if len(identical_instances) != 0:
            # print(f'credential {name} is identical with credential {identical_instances[0]}. please delete credential {name}.')
            cls.rejected_instances[identical_instances[0]].append(name)
        else:
            cls.created_instances[name]= connection_id
            cls.rejected_instances[name] = []
            return super().__new__(cls)

    def __init__(self,name: str,product: str,host: str,port: str,user: str,password: str,database: str,local_environment: str) -> None:
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

    @classmethod
    def duplicate_check(cls):
        non_duplicate_credential = [name for name in cls.rejected_instances.keys() if len(cls.rejected_instances[name]) == 0]
        if len(cls.rejected_instances.keys()) != len(non_duplicate_credential):
            for key in cls.rejected_instances.keys():
                if len(cls.rejected_instances[key]) != 0:
                    print(f'the credentials inside the list at below are identical with credential named "{key}". please delete.')
                    print(cls.rejected_instances[key])
        else:
            print('there is no duplicate credential')

# ----- This is the beginning of the section where you can write credential -----



# ----- This is the end of the section where you can write credential -----

# check if there is duplicate credential by running this python file first

def main():
    connection.duplicate_check()

if __name__ == '__main__':
    main()