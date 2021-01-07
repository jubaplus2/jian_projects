#!/usr/bin/env python
# coding: utf-8

# In[11]:


# Since the week end of 2018-08-11

import pandas as pd
import numpy as np
import os
import glob
import datetime
import gc
import hashlib
import gc
from sqlalchemy import create_engine
import sqlalchemy
import logging


os.getcwd()


# In[12]:


logging.basicConfig(filename='/home/jian/celery/BL_MySQL/Initial_Tables/POS_item_save_to_MySQL.log',level="INFO")

# DB name: BigLots
BL_SQL_CONNECTION= 'mysql+pymysql://jian:JubaPlus-2017@localhost/BigLots' 
BL_engine = create_engine(
        BL_SQL_CONNECTION, 
        pool_recycle=1800
    )


# In[13]:


samplerows=None

def recursive_file_gen(root_path):
    for root, dirs, files in os.walk(root_path):
        for file in files:
            yield os.path.join(root,file)


# In[14]:


existing_tables=pd.read_sql("show tables;",con=BL_engine)
existing_tables


# # Create POS Item Table

# In[15]:


import pymysql.cursors
engine_pymysql_db_BL = pymysql.connect(host='localhost',user='jian',
                         password='JubaPlus-2017',db='BigLots',
                         charset='utf8mb4',cursorclass=pymysql.cursors.DictCursor)


# In[16]:


def create_BL_POS_Item_table():
    with engine_pymysql_db_BL.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS BL_POS_Item")
        cur.execute("CREATE TABLE BL_POS_Item         (        location_id int,         transaction_dt Date,         transaction_id varchar(16),         customer_id_hashed char(64),         class_code_id varchar(16),         subclass_id varchar(16),         item_id varchar(16),         item_transaction_units int,         item_transaction_amt decimal(10,2)         );")
    print("1:An empty TABLE BL_Rewards_Master has been created.",datetime.datetime.now())
    logging.info("1:An empty TABLE BL_Rewards_Master has been created."+str(datetime.datetime.now()))
 
 
create_BL_POS_Item_table()


# # Write into MySQL

# In[17]:


list_df_POS_files_daily=list(recursive_file_gen("/home/jian/BigLots/"))
list_df_POS_files_daily=[x for x in list_df_POS_files_daily if "daily" in x.lower() and x[-4:]==".txt" and "2019-" in x]

list_df_POS_files_daily=[x for x in list_df_POS_files_daily if x.split("/MediaStorm_")[1][:10]>="2019-02-16"]

list_df_POS_files_daily.sort()

list_df_POS_files_hist=glob.glob("/home/jian/BigLots/hist_daily_data_itemlevel_decompressed/*.txt")
list_df_POS_files_hist.sort()

list_all_item_POS_files=list_df_POS_files_hist+list_df_POS_files_daily
# list_all_item_POS_files # since 20180811


# In[18]:


def item_POS_df_clean_type(df):
    # All fields to keep as str not changed
    df['location_id']=df['location_id'].astype(int)
    df['transaction_dt']=pd.to_datetime(df['transaction_dt'],format="%Y-%m-%d").dt.date
    # df['transaction_id']=df['transaction_id'].astype(str) varchar(16)
    # df['customer_id_hashed']=   char(64)
    # df['class_code_id']=df['class_code_id'].astype(str) varchar(16)
    # df['subclass_id']=df['subclass_id'].astype(str) varchar(16)
    # df['item_id']=df['item_id'].astype(str) varchar(16)
    df['item_transaction_units']=df['item_transaction_units'].astype(int)
    df['item_transaction_amt']=df['item_transaction_amt'].astype(float)
    
    
    # printthe len of str cols
    # print('transaction_id',df['transaction_id'].apply(len).max())
    # print('class_code_id',df['class_code_id'].apply(len).max())
    # print('subclass_id',df['subclass_id'].apply(len).max())
    # print('item_id',df['item_id'].apply(len).max())
    
    return df
    


# In[19]:


col_list=pd.read_table(list_all_item_POS_files[0],dtype=str,sep="|",nrows=10).columns.tolist()

for file in list_all_item_POS_files:
    df=pd.read_table(file,dtype=str,sep="|",nrows=samplerows)
    print(df.columns.tolist()==col_list, datetime.datetime.now(), os.path.basename(file))
    print(df.shape,df['transaction_dt'].min(),df['transaction_dt'].max())
    
    logging.info(str(df.columns.tolist()==col_list)+", "+str(datetime.datetime.now())+", "+str(os.path.basename(file)))
    logging.info(str(df.shape)+", "+str(df['transaction_dt'].min())+", "+str(df['transaction_dt'].max()))
    
    df=item_POS_df_clean_type(df)
    df.to_sql('BL_POS_Item',if_exists='append',con=BL_engine,index=False)
print("All_Done: ",datetime.datetime.now())
logging.info("All_Done: "+str(datetime.datetime.now()))


# In[ ]:




