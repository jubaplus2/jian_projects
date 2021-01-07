#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import logging
import sqlalchemy
import datetime

logging.basicConfig(filename='/home/jian/celery/BL_MySQL/Weekly_Update_MySQL/create_index_on_DCM_BL_table.log', 
                    level=logging.INFO)

BL_SQL_CONNECTION= 'mysql+pymysql://jian:JubaPlus-2017@localhost/BigLots' 
BL_engine = sqlalchemy.create_engine(
        BL_SQL_CONNECTION, 
        pool_recycle=1800
    )

print(datetime.datetime.now())


# In[5]:


list_index=['customer_id_hashed','date_est','impressions','clicks','week_end_dt']
# others can be added once in need


# In[8]:


for col in list_index:
    query="create index %s on Pred_ExposureV2_BL_id(%s);" %(col,col)
    print(query)
    print("start_run",col,datetime.datetime.now())
    pd.read_sql(query,con=BL_engine)
    print("done_of_index",col,datetime.datetime.now())
    
print("all indexes created",datetime.datetime.now())

