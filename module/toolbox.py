"""
Main module to store all function that can be used to do all main purpose of this project.
- level_measure = to get all table relation hierarchy on a schema
"""

import os
from sys import path
from pathlib import Path
import pandas
import sqlalchemy
from .product_module import *

root = str(Path(__file__).parent.parent)
path.insert(0,str(root))

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
        # NOTE: level 1 (table without relation)
        table_with_relation = pandas.DataFrame(pandas.concat([relation['table_name'],relation['references']],ignore_index=True).drop_duplicates(),columns=['table name'])
        level_1 = pandas.merge(all_table,table_with_relation,left_on='table_name',right_on='table name',how='left')
        level = level_1[level_1['table name'].isna()].drop('table name',axis=1)
        level['level'] = '1'
        # NOTE: creating level 3 as base (the most outer parent)
        level_1 = pandas.merge(level_1[level_1['table name'].notna()].drop('table name',axis=1),relation, on='table_name', how='left')
        level_3 = level_1.loc[level_1['references'].isnull()].drop('references',axis=1)
        level_3['level'] = '3'
        level = pandas.concat([level,level_3])
        # NOTE: detecting relation to self
        detector = relation.dropna(subset='references').drop_duplicates(subset=['table_name','references'])
        detector['self_relation'] = ''
        detector = detector.to_dict('records')
        for data in detector:
            if data['table_name'] == data['references']:
                data['self_relation'] = 'Y'
            elif data['table_name'] != data['references']:
                data['self_relation'] = 'X'
        detector = pandas.DataFrame(detector)
        detector_pivot = detector.pivot_table(values='references',index='table_name',columns='self_relation',aggfunc='count').reset_index()
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
        level = level.drop('references', axis=1)
        level['level'] = '1'
    level['level'] = pandas.to_numeric(level['level'])
    level = level.sort_values(by=['level', 'table_name'],ascending=[True, True])
    return level

def runner(connection_dict: list[dict[str, str]], schema: str):
    action_choose_dialogue = """1. Level measure
2. DDL transfer

What tool you want to use: """
    action_choose: int = int(input(f"{action_choose_dialogue}"))
    print("Avaible connection:")
    for credential in connection_dict:
        print(f"{connection_dict.index(credential) + 1}. credential {credential['name']}: product = {credential['product']}, local environment path = {credential['local_environment']} -> {credential['user']}:{credential['password']}@{credential['host']}:{credential['port']}")
    if action_choose == 1:
        target_choose_script: int = int(input("""Choose which connection you want to measure: """))
        target_choose = connection_dict[target_choose_script - 1]
        module_name = target_choose["product"]
        module_list = [filename.rstrip('.py') for filename in os.listdir(f"{root}\module\product_module") if filename.endswith('.py') and filename != '__init__.py']
        if module_name in module_list:
            method = getattr(product_module, module_name)
            url = method.url(host = {target_choose['host']}, port = {target_choose['port']}, user = {target_choose['user']}, password = {target_choose['password']})
            engine = sqlalchemy.create_engine(url)
            conn = engine.connect()
            all_table = method.all_table(conn, schema)
            print(all_table)
    elif action_choose == 2:
        raise NotImplementedError
    
