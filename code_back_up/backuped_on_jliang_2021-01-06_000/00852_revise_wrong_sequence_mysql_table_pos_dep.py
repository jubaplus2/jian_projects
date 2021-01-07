#!/usr/bin/env python
# coding: utf-8

# In[1]:


import sqlalchemy
import pandas as pd
import datetime
BL_engine=sqlalchemy.create_engine(
            "mysql+pymysql://%s:%s@localhost/%s" % ('jian', 'JubaPlus-2017', 'BigLots'))


# In[2]:

print('check point1',datetime.datetime.now())
df_wrong_ids=pd.read_sql("select * from Pred_POS_Department as t1 inner join (select customer_id_hashed, min(trans_order_since_18Q1) as min_order from Pred_POS_Department group by customer_id_hashed having min_order>1) as t2 on t1.customer_id_hashed=t1.customer_id_hashed;",con=BL_engine)
print(df_wrong_ids.shape,df_wrong_ids['customer_id_hashed'].nunique())
df_wrong_ids.head(3)


# In[ ]:

print('check point2',datetime.datetime.now())
df_first_order=df_wrong_ids[['customer_id_hashed','transaction_dt']].sort_values(['customer_id_hashed','transaction_dt']).drop_duplicates("customer_id_hashed")
df_first_order=pd.merge(df_first_order,df_wrong_ids,on=['customer_id_hashed','transaction_dt'],how="left")
df_first_order.head(5)


# In[ ]:


df_first_order['transaction_dt'].unique().tolist()


# In[ ]:

print('check point 3', datetime.datetime.now())
df_wrong_ids.to_csv("./df_wrong_ids.csv",index=False)
df_first_order.to_csv("./df_first_order.csv",index=False)


# In[ ]:




