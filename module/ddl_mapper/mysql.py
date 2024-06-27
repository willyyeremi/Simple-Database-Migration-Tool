from importlib import import_module
from pandas import DataFrame


def mysql(table_name: str, column_rule: DataFrame, primary_key: DataFrame, relation: DataFrame, unique_constraint: DataFrame, check_constraint: DataFrame, all_index: DataFrame, product_target: str):
#     ddl_script = f"""-- script {table_name}
# CREATE TABLE {table_name} (
#     )
# ;"""
    column_rule_dict = column_rule.to_dict('records')
    source_data_type_mapper_path = f"module.data_type_mapper"
    source_data_type_mapper = import_module(source_data_type_mapper_path)
    source_data_type_mapper_method = getattr(source_data_type_mapper, 'mysql')
    column_rule_script = ""
    for i, j in enumerate(column_rule_dict):
        data_type_func = getattr(source_data_type_mapper_method, j['data_type'])
        data_type_string = data_type_func(product_target, j)
        if column_rule_dict['generated_column_type'] != '' and column_rule_dict['default_value'] != '':
            string = f"`{column_rule_dict['column_name']}` {data_type_string} {column_rule_dict['is_nullable']} generated always as {column_rule_dict['default_value']} {column_rule_dict['generated_column_type']} {column_rule_dict['column_comment']}"
        else:
            string = f"`{column_rule_dict['column_name']}` {data_type_string} {column_rule_dict['is_nullable']} default {column_rule_dict['default_value']} {column_rule_dict['extra']} {column_rule_dict['column_comment']}"
        if i != 1:
            string = f"\n    ,{string}"
        column_rule_script = column_rule_script + string
    primary_key_name_list: list[str] = primary_key['constraint_name'].drop_duplicates().values.tolist()
    for primary_key_name in primary_key_name_list:
        key_column_list = primary_key.loc[primary_key['constraint_name'] == primary_key_name]['column_name'].values.tolist()
        column_string = ""
        for postion, key_column in enumerate(key_column_list):
            if postion != 0:
                column_string = column_string + ", "
            column_string = column_string + key_column
        primary_key_script = f"\n    ,constraint `{primary_key_name}` primary key ({column_string})"
    relation_list: list[str] = relation['constraint_name'].drop_duplicates().values.tolist()
    relation_script = ""
    for relation_name in relation_list:
        child_column_list = relation.loc[relation['constraint_name'] == relation_name]['column_child'].values.tolist()
        child_column_string = ""
        for postion, child_column in enumerate(child_column_list):
            if postion != 0:
                child_column_string = child_column_string + ", "
            child_column_string = child_column_string + f"`{child_column}`"
        parent_column_list = relation.loc[relation['constraint_name'] == relation_name]['column_parent'].values.tolist()
        parent_column_string = ""
        for postion, parent_column in enumerate(parent_column_list):
            if postion != 0:
                parent_column_string = parent_column_string + ", "
            parent_column_string = parent_column_string + f"`{parent_column}`"
        parent_table_name =  relation.loc[relation['constraint_name'] == relation_name]['parent_table_name'].drop_duplicates().values.tolist()[0]
        on_update_action = relation.loc[relation['constraint_name'] == relation_name]['on_update'].drop_duplicates().values.tolist()[0]
        on_delete_action = relation.loc[relation['constraint_name'] == relation_name]['on_delete'].drop_duplicates().values.tolist()[0]
        string = f"\n    ,constraint `{relation_name}` foreign key ({child_column_string}) references `{parent_table_name}` ({parent_column_string}) on update {on_update_action} on delete {on_delete_action}"
        relation_script = relation_script + string
    
    
    