#!/usr/bin/env python
# coding: utf-8

# In[1]:


# 2020 Q1
import pandas as pd
import numpy as np
import datetime
import os
import glob
import gc
import sqlalchemy

BL_SQL_CONNECTION= 'mysql+pymysql://jian:JubaPlus-2017@localhost/BigLots' 
BL_engine = sqlalchemy.create_engine(
        BL_SQL_CONNECTION, 
        pool_recycle=1800
    )

os.getcwd()


# In[11]:


start_2020_Q1="'2020-02-02'"
end_2020_Q1="'2020-05-02'"

start_2019_Q1="'2019-02-03'"
end_2019_Q1="'2019-05-04'"


# In[12]:


df_2020_shoppers=pd.read_sql("select location_id, count(distinct customer_id_hashed) from Pred_POS_Department where transaction_dt between %s and %s and customer_id_hashed is not null group by location_id"%(start_2020_Q1,end_2020_Q1),con=BL_engine)


# In[ ]:


df_2019_shoppers=pd.read_sql("select location_id, count(distinct customer_id_hashed) from Pred_POS_Department where transaction_dt between %s and %s and customer_id_hashed is not null group by location_id"%(start_2019_Q1,end_2019_Q1),con=BL_engine)


# In[ ]:


df_2019_shoppers.to_csv("./df_2019_shoppers.csv",index=False)
df_2020_shoppers.to_csv("./df_2020_shoppers.csv",index=False)

