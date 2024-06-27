"""
Main module to store all function that can be used to do all main purpose of this project.
- level_measure = to get all table relation hierarchy on a schema
"""

import os
from sys import path
from pathlib import Path
import pandas
import sqlalchemy
from importlib import import_module

root = str(Path(__file__).parent.parent)
path.insert(0,str(root))

def database_version_value(version: str) -> dict[str: str]:
    """
    Get the major, minor, patch value from the version of the database
    
    Args:
        version(string): the version value of the database
    
    Returns
        semantic_version_dict(dict): dictionary that contain major, minor, and patch value
    """
    import re
    pattern: str = r'^.*?(?=[\-\+])'
    match = re.search(pattern, version)
    if match:
        match =  match.group(0)
    else:
        match = version
    match = match.split(".")
    semantic_version_part: list[str] = ["major", "minor", "patch"]
    semantic_version_dict: dict[str: str] = {}
    for i in range(0, len(match)):
        semantic_version_dict[semantic_version_part[i]] = match[i]
    if "minor" not in semantic_version_dict.keys():
        semantic_version_dict["minor"] = "0"
    if "patch" not in semantic_version_dict.keys():
        semantic_version_dict["patch"] = "0"
    return semantic_version_dict

def level_measure(all_table: pandas.DataFrame, relation: pandas.DataFrame) -> pandas.DataFrame:
    """
    Get the hierarchy position of table from certain schema. The dataframe columns description are:
    - table_name (string): name of the tale
    - level (integer): the hierarchy position of the table

    Args:
        - all_table (DataFrame): name of the table
        - relation (DataFrame): the hierarchy position of the table

    Returns:
        DataFrame: data of table and its hierarchy position
    """    
    if len(relation) != 0:
        # NOTE: managing relation column name
        all_table = all_table.drop(['table_comment'], axis = 1)
        relation = relation.drop(['column_child', 'column_parent', 'constraint_name', 'on_update', 'on_delete'], axis = 1).drop_duplicates()
        relation.columns = ['table_name', 'references']
        # NOTE: level 1 (table without relation)
        table_with_relation = pandas.DataFrame(pandas.concat([relation['table_name'], relation['references']], ignore_index = True).drop_duplicates(), columns = ['table name'])
        level_1 = pandas.merge(all_table, table_with_relation, left_on = 'table_name', right_on = 'table name', how = 'left')
        level = level_1[level_1['table name'].isna()].drop('table name', axis = 1)
        level['level'] = '1'
        # NOTE: creating level 3 as base (the most outer parent)
        level_1 = pandas.merge(level_1[level_1['table name'].notna()].drop('table name', axis = 1), relation, on = 'table_name', how = 'left')
        level_3 = level_1.loc[level_1['references'].isnull()].drop('references', axis = 1)
        level_3['level'] = '3'
        level = pandas.concat([level, level_3])
        # NOTE: detecting relation to self
        detector = relation.dropna(subset = 'references').drop_duplicates(subset = ['table_name', 'references'])
        detector['self_relation'] = ''
        detector = detector.to_dict('records')
        for data in detector:
            if data['table_name'] == data['references']:
                data['self_relation'] = 'Y'
            elif data['table_name'] != data['references']:
                data['self_relation'] = 'X'
        detector = pandas.DataFrame(detector)
        detector_pivot = detector.pivot_table(values = 'references', index = 'table_name', columns = 'self_relation', aggfunc = 'count').reset_index()
        # NOTE: level 2 (table with relation only to itself)
        if 'Y' in detector_pivot.columns:
            level_2 = detector_pivot.loc[detector_pivot['X'].isnull()].drop(['X','Y'],axis=1)
            level_2['level'] = '2'
            level = pandas.concat([level,level_2])
            non_leveled = detector_pivot.loc[detector_pivot['X'].notnull()].drop(['X','Y'],axis=1)
        else:
            non_leveled = detector_pivot.loc[detector_pivot['X'].notnull()].drop(['X'],axis=1)
        # NOTE: determining the rest tables
        level_counter = 4
        while non_leveled.shape[0] > 0:
            level_x = pandas.merge(non_leveled,relation,on='table_name', how='left')
            level_x['self_relation'] = ''
            level_x = level_x.to_dict('records')
            for data in level_x:
                if data['table_name'] == data['references']:
                    data['self_relation'] = 'Y'
                elif data['table_name'] != data['references']:
                    data['self_relation'] = 'X'
            level_x = pandas.DataFrame(level_x)
            level_x = level_x.loc[level_x['self_relation']=='X'].drop('self_relation',axis=1) 
            level_x = pandas.merge(level_x,level,left_on='references',right_on='table_name', how='left')
            level_x['identifier_1'] = ''
            level_x = level_x.to_dict('records')
            for data in level_x:
                if pandas.notna(data['level']):
                    data['identifier_1'] = 'Y'
                elif pandas.isna(data['level']):
                    data['identifier_1'] = 'X'
            level_x = pandas.DataFrame(level_x)
            level_x['identifier_2'] = level_x['identifier_1']
            level_x = level_x.drop(['references','table_name_y','level'],axis = 1).rename(columns={"table_name_x": "table_name"})
            level_x = level_x.pivot_table(values='identifier_2',index='table_name',columns='identifier_1',aggfunc='count')
            level_x = level_x.reset_index()
            if 'X' in level_x.columns:
                new_data = level_x.loc[level_x['X'].isnull()].drop(['X','Y'],axis=1)
                new_data['level'] = f'{level_counter}'
                level = pandas.concat([level,new_data])
                not_defined_data = level_x[level_x['X'].notna()].drop(['X','Y'],axis=1)
                non_leveled = not_defined_data
                level_counter = level_counter + 1
            else:
                new_data = level_x.drop('Y',axis=1)
                new_data['level'] = f'{level_counter}'
                level = pandas.concat([level,new_data])
                non_leveled = pandas.DataFrame(columns=['table_name','level'])
    else:
        level = all_table
        level = level.drop('table_comment', axis=1)
        level['level'] = '1'
    level['level'] = pandas.to_numeric(level['level'])
    level = level.sort_values(by=['level', 'table_name'], ascending=[True, True])
    level = level.reset_index(drop = True)
    return level

def ddl_transfer(source_product: str, source_connection: object, source_schema: str, target_product: str, target_connection: object, target_schema: str):
    source_metadata_get_path: str = f"module.metadata_get"
    source_metadata_get = import_module(source_metadata_get_path)
    source_metadata_get_method = getattr(source_metadata_get, source_product)
    all_table: pandas.DataFrame = source_metadata_get_method.all_table(source_connection, source_schema)
    column_rule: pandas.DataFrame = source_metadata_get_method.column_rule(source_connection, source_schema)
    primary_key: pandas.DataFrame = source_metadata_get_method.primary_key(source_connection, source_schema)
    relation: pandas.DataFrame = source_metadata_get_method.relation(source_connection, source_schema)
    unique_constraint: pandas.DataFrame = source_metadata_get_method.unique_constraint(source_connection, source_schema)
    check_constraint: pandas.DataFrame = source_metadata_get_method.check_constraint(source_connection, source_schema)
    all_index: pandas.DataFrame = source_metadata_get_method.all_index(source_connection, source_schema)
    table_list: list[str] = all_table['table_name'].values.tolist()
    for table in table_list:
        table_column_rule: pandas.DataFrame = column_rule.loc[column_rule['table_name'] == table]
        table_primary_key: pandas.DataFrame = primary_key.loc[primary_key['table_name'] == table]
        table_relation: pandas.DataFrame = relation.loc[relation['table_name'] == table]
        table_unique_constraint: pandas.DataFrame = unique_constraint.loc[unique_constraint['table_name'] == table]
        table_check_constraint: pandas.DataFrame = check_constraint.loc[check_constraint['table_name'] == table]
        table_all_index: pandas.DataFrame = all_index.loc[all_index['table_name'] == table]
        

def main_runner(connection_dict: list[dict[str, str]]):
    from datetime import datetime
    current_timestamp: datetime = datetime.strftime(datetime.now(), '%Y%m%d_%H%M%S')
    action_choose_dialogue = """\nAvailable tools:
1. Level measure
2. DDL transfer

What tool you want to use: """
    action_choose: int = int(input(f"{action_choose_dialogue}"))
    if action_choose == 1:
        connection_choose_dialogue = "\nAvailable connection:"
        for credential in connection_dict:
            connection_choose_dialogue = connection_choose_dialogue + f"\n{connection_dict.index(credential) + 1}. credential {credential['name']}: product = {credential['product']}, local environment path = {credential['local_environment']} -> {credential['user']}:{credential['password']}@{credential['host']}:{credential['port']}"
        connection_choose_dialogue = connection_choose_dialogue + "\n\nChoose which connection you want to measure: "
        connection_choose: int = int(input(connection_choose_dialogue))
        connection_choose = connection_dict[connection_choose - 1]
        module_name = connection_choose["product"]
        module_path = f"module.metadata_get"
        metadata_get = import_module(module_path)
        method = getattr(metadata_get, module_name)
        url = method.url(user = connection_choose['user'], password = connection_choose['password'], host = connection_choose['host'], port = connection_choose['port'], database = connection_choose['database'])
        engine = sqlalchemy.create_engine(url)
        conn = engine.connect()
        schema_list = method.all_schema(conn)
        schema_choose_dialogue = "\nAvailable schema:"
        for i, j in enumerate(schema_list, 1):
            schema_choose_dialogue = schema_choose_dialogue + f"\n{i}. {j}"
        schema_choose_dialogue = schema_choose_dialogue + "\n\nWhat schema you want to measure? "
        schema_choose: int = int(input(schema_choose_dialogue))
        schema = schema_list[schema_choose - 1]
        all_table = method.all_table(conn, schema)
        relation = method.relation(conn, schema)
        level_measure_result = level_measure(all_table, relation)
        save_result = input("\nDo you want to save the result?(y/n) ")
        while save_result not in ['y', 'n']:
            save_result = input("\nPlease enter valid answer. Do you want to save the result?(y/n) ")
        if save_result == 'y':
            new_path = root + '\\result\\level_measure'
            if not os.path.exists(new_path):
                os.makedirs(new_path)
            new_file_path = os.path.join(new_path, f'{connection_choose["host"]} {connection_choose["database"]} {schema} {current_timestamp}.csv')
            if os.path.isfile(new_file_path):
                os.remove(new_file_path)
            level_measure_result.to_csv(path_or_buf = new_file_path, sep = '|', index = False)
        return level_measure_result
    elif action_choose == 2:
        module_path = f"module.metadata_get"
        metadata_get = import_module(module_path)
        connection_choose_dialogue = "\nAvailable connection:"
        for credential in connection_dict:
            connection_choose_dialogue = connection_choose_dialogue + f"\n{connection_dict.index(credential) + 1}. credential {credential['name']}: product = {credential['product']}, local environment path = {credential['local_environment']} -> {credential['user']}:{credential['password']}@{credential['host']}:{credential['port']}"
        print(connection_choose_dialogue)
        source_connection_choose: int = int(input("\n\nChoose which one is the source connection: "))
        source_connection_choose = connection_dict[source_connection_choose - 1]
        source_module_name = source_connection_choose["product"]
        source_method = getattr(metadata_get, source_module_name)
        source_url = source_method.url(user = source_connection_choose['user'], password = source_connection_choose['password'], host = source_connection_choose['host'], port = source_connection_choose['port'], database = source_connection_choose['database'])
        source_engine = sqlalchemy.create_engine(source_url)
        source_conn = source_engine.connect()
        source_schema_list = source_method.all_schema(source_conn)
        source_schema_choose_dialogue = "\nAvailable schema on source:"
        for i, j in enumerate(source_schema_list, 1):
            source_schema_choose_dialogue = source_schema_choose_dialogue + f"\n{i}. {j}"
        source_schema_choose_dialogue = source_schema_choose_dialogue + "\n\nWhat schema you want to transfer? "
        source_schema_choose: int = int(input(source_schema_choose_dialogue))
        source_schema = source_schema_list[source_schema_choose - 1]
        x = source_method.column_rule(source_conn, source_schema)
        x = x.to_dict('records')
        print(x)
        # connection_choose_dialogue = "\nAvailable connection:"
        # for credential in connection_dict:
        #     connection_choose_dialogue = connection_choose_dialogue + f"\n{connection_dict.index(credential) + 1}. credential {credential['name']}: product = {credential['product']}, local environment path = {credential['local_environment']} -> {credential['user']}:{credential['password']}@{credential['host']}:{credential['port']}"
        # print(connection_choose_dialogue)
        # target_connection_choose: int = int(input("\n\nChoose which one is the target connection: "))
        # target_connection_choose = connection_dict[target_connection_choose - 1]
        # target_module_name = target_connection_choose["product"]
        # target_method = getattr(metadata_get, target_module_name)
        # target_url = target_method.url(user = target_connection_choose['user'], password = target_connection_choose['password'], host = target_connection_choose['host'], port = target_connection_choose['port'], database = target_connection_choose['database'])
        # target_engine = sqlalchemy.create_engine(target_url)
        # target_conn = target_engine.connect()
        # target_schema_list = target_method.all_schema(target_conn)
        # target_schema_choose_dialogue = "\nAvailable schema on target:"
        # for i, j in enumerate(target_schema_list, 1):
        #     target_schema_choose_dialogue = target_schema_choose_dialogue + f"\n{i}. {j}"
        # target_schema_choose_dialogue = target_schema_choose_dialogue + "\n\nWhat schema is the transfer destination? "
        # target_schema_choose: int = int(input(target_schema_choose_dialogue))
        # target_schema = target_schema_list[target_schema_choose - 1]
        # raise NotImplementedError
