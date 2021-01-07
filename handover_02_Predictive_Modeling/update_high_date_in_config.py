#!/usr/bin/env python
# coding: utf-8

# In[16]:



def update_high_date_in_config():

    import datetime
    import json

    date_last_sturday=[datetime.datetime.now().date()-datetime.timedelta(days=x) for x in range(6)] 
    date_last_sturday=[x for x in date_last_sturday if x.weekday()==5][0]

    path_json="/mnt/clients/juba/hqjubaapp02/jliang/Projects/Big_Lots/Predictive_Model/Model_Scripts/config.json"

    config_prev=json.load(open(path_json,"r"))

    config_new=config_prev.copy()
    config_new['pos_end_date']=str(date_last_sturday)
    config_new['crm_end_date']=str(date_last_sturday)

    print("pos_end_date & crm_end_date in config.json have been updated to: %s"%str(date_last_sturday))


    with open(path_json, 'w') as outfile:
        json.dump(config_new, outfile)
    


# In[17]:




