# -*- coding: utf-8 -*-
# File Name: etl_pos.py
# Author: Jian Liang
# Editor: Ben Ho
# Created: 2020/04/10
# Last Edited: 2020/07/02
# Description: Create Common Id for DCM in MySQL

import pandas as pd
import datetime
import sqlalchemy
import json

BL_SQL_CONNECTION = 'mysql+pymysql://jian:JubaPlus-2017@localhost/BigLots'
BL_engine = sqlalchemy.create_engine(
    BL_SQL_CONNECTION,
    pool_recycle=1800
)

# In[8]:


high_date = datetime.date(2020, 1, 18)
sign_up_start_date = datetime.date(2018, 2, 4)
POS_start_date = datetime.date(2018, 2, 4)

# exclude the rencet 3 months new signs
sign_up_end_date = high_date - datetime.timedelta(days=7 * 12)

# In[12]:


sql_str_high_date = "'%s'" % str(high_date)
sql_sign_up_start_date = "'%s'" % str(sign_up_start_date)
sql_POS_start_date = "'%s'" % str(POS_start_date)
sql_sign_up_end_date = "'%s'" % str(sign_up_end_date)

# In[9]:


df_1 = pd.read_sql(
    "select distinct customer_id_hashed as customer_id_hashed from BL_Rewards_Master where sign_up_date between %s and %s;" % (
    sql_sign_up_start_date, sql_sign_up_end_date), con=BL_engine)
df_2 = pd.read_sql(
    "select distinct customer_id_hashed as customer_id_hashed from Pred_POS_Department where transaction_dt<=%s" % sql_str_high_date,
    con=BL_engine)
df_3 = pd.read_sql(
    "select distinct customer_id_hashed as customer_id_hashed from Pred_ExposureV3_BL_id where date_est<=%s" % sql_str_high_date,
    con=BL_engine)
df_4 = pd.read_sql(
    "select distinct customer_id_hashed as customer_id_hashed from Pred_ExpV2_Activity_BL_id where date_est<=%s" % sql_str_high_date,
    con=BL_engine)

# Pred_ExposureV3_BL_id: impression & click
# ExpV2_Activity_BL_id: activity


# In[ ]:


df_commonid = pd.merge(df_1, df_2, on="customer_id_hashed", how="inner")
print("1&2", df_commonid.shape)
del df_1
del df_2

df_commonid = pd.merge(df_commonid, df_3, on="customer_id_hashed", how="inner")
print("1-3", df_commonid.shape)
del df_3

df_commonid = pd.merge(df_commonid, df_4, on="customer_id_hashed", how="inner")
print("1-4", df_commonid.shape)
del df_4

print(df_commonid.shape, df_commonid['customer_id_hashed'].nunique())

# In[ ]:


# please modified as your convience to embed into your code

import pymysql.cursors

engine_pymysql_db_BL = pymysql.connect(host='localhost', user='jian',
                                       password='JubaPlus-2017', db='BigLots',
                                       charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)


def create_Common_ID_table():
    with engine_pymysql_db_BL.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS Pred_Temp_CommonID_ending_20190831_since18Q1;")
        cur.execute(
            "CREATE TABLE Pred_Temp_CommonID_ending_20190831_since18Q1         (        customer_id_hashed varchar(64)         );")
    print("1:An empty TABLE Pred_Temp_CommonID_ending_20190831_since18Q1 has been created.", datetime.datetime.now())


create_Common_ID_table()
# feel free to change the table name
str_table_temp_name = "Pred_Temp_CommonID_ending_%s_since18Q1" % str(high_date).replace("-", "")
df_commonid.to_sql(str_table_temp_name, if_exists='replace', con=BL_engine, index=False)

# overwrite

# In[ ]:
