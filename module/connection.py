class connection:
    def __init__(self,product,host,port,user,password,database,local_environment):
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