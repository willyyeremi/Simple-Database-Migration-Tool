def level_measure(all_table,relation):
    import pandas
    if len(relation) != 0:
        # level 1 (table without relation)
        table_with_relation = pandas.DataFrame(pandas.concat([relation['table_name'],relation['references']],ignore_index=True).drop_duplicates(),columns=['table name'])
        level_1 = pandas.merge(all_table,table_with_relation,left_on='table_name',right_on='table name',how='left')
        level = level_1[level_1['table name'].isna()].drop('table name',axis=1)
        level['level'] = '1'
        # creating level 3 as base (the most outer parent)
        level_1 = pandas.merge(level_1[level_1['table name'].notna()].drop('table name',axis=1),relation, on='table_name', how='left')
        level_3 = level_1.loc[level_1['references'].isnull()].drop('references',axis=1)
        level_3['level'] = '3'
        level = pandas.concat([level,level_3])
        # detecting relation to self
        detector = relation.dropna(subset='references').drop_duplicates(subset=['table_name','references'])
        # function to give designated value based on relation existence
        def self_relation_identifier(arg):
            if arg['table_name'] == arg['references']:
                return 'Y'
            elif arg['table_name'] != arg['references']:
                return 'X' 
        detector['self_relation'] = detector.apply(self_relation_identifier,axis=1)
        detector_pivot = detector.pivot_table(values='references',index='table_name',columns='self_relation',aggfunc='count').reset_index()
        if 'Y' in detector_pivot.columns:
            level_2 = detector_pivot.loc[detector_pivot['X'].isnull()].drop(['X','Y'],axis=1)
            level_2['level'] = '2'
            level = pandas.concat([level,level_2])
            non_leveled = detector_pivot.loc[detector_pivot['X'].notnull()].drop(['X','Y'],axis=1)
        else:
            non_leveled = detector_pivot.loc[detector_pivot['X'].notnull()].drop(['X'],axis=1)
        level_counter = 4
        while non_leveled.shape[0] > 0:
            level_x = pandas.merge(non_leveled,relation,on='table_name', how='left')
            level_x['self_relation'] = level_x.apply(self_relation_identifier,axis=1)
            level_x = level_x.loc[level_x['self_relation']=='X'].drop('self_relation',axis=1) 
            level_x = pandas.merge(level_x,level,left_on='references',right_on='table_name', how='left')
            def identifier(arg):
                if pandas.notna(arg['level']):
                    return 'Y'
                elif pandas.isna(arg['level']):
                    return 'X' 
            level_x['identifier_1'] = level_x.apply(identifier,axis=1)
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
    level['numerical_part'] = level['level'].str.extract(r'(\d+)').astype(int)
    level = level.sort_values(by=['numerical_part', 'table_name'],ascending=[True, True])
    level = level.drop(columns='numerical_part').reset_index(drop=True)
    return level