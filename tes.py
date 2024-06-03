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

credential_data, credential_dict  = connection.credential_get()
connection.credential_check(credential_data)
for credential in credential_dict:
    globals()[credential['name']]: object = connection.connection(credential['product'], credential['host'], credential['port'], credential['user'], credential['password'], credential['database'], credential['local_environment'])


# credential
product: str = source.product


# database schema
schema = 'SYS'

print(product)