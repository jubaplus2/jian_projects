#!/usr/bin/env python
# coding: utf-8


def remove_tables_4_weeks_ago():

    import sqlalchemy
    import pandas as pd
    import datetime
    import json
    import glob
    import os
    dict_config=json.load(open("/mnt/clients/juba/hqjubaapp02/jliang/Projects/Big_Lots/Predictive_Model/Model_Scripts/config.json"))
    high_date=dict_config['pos_end_date']
    username=dict_config['username']
    password=dict_config['password']
    database=dict_config['database']
    script_folder_for_table_json=dict_config['script_folder']

    sql_engine=sqlalchemy.create_engine("mysql+pymysql://%s:%s@localhost/%s" % (username, password, database))


    # In[12]:


    str_8_digit_date_to_remove=str(datetime.datetime.strptime(high_date,"%Y-%m-%d").date()-datetime.timedelta(days=28)).replace("-","")
    print("date_to_remove: %s"%str_8_digit_date_to_remove)
    list_str_week=[str_8_digit_date_to_remove] # feasible to add in more


    # In[14]:


    for w in list_str_week:

        list_tables_to_delete=pd.read_sql("show tables;",con=sql_engine).iloc[:,0].values.tolist()
        list_tables_to_delete=[x for x in list_tables_to_delete if x[-8:]==w]
        print("list_tables_to_delete :\n%s"%str(list_tables_to_delete))
        
        with sql_engine.connect() as connection:
            for t in list_tables_to_delete:
                query="drop table if exists %s"%t
                result = connection.execute(query)
                
        list_table_name_json_to_delete=glob.glob(script_folder_for_table_json+"table_names*.json")
        list_table_name_json_to_delete=[x for x in list_table_name_json_to_delete if x.split(".")[-2][-8:]==w]
        print("list_table_name_json_to_delete :\n%s"%str(list_table_name_json_to_delete))
        for i_json in list_table_name_json_to_delete:
            os.remove(i_json)






