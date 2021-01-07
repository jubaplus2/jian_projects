#!/usr/bin/env python
# coding: utf-8

# In[1]:


# Table 2 -- POS by department
# All id, trans since 2018-02-04


# In[2]:


# ID Level spec from Yoram, modified on 20191115
# Email 20191115 6:30 p.m.
import pandas as pd
import numpy as np
import datetime
import os
import paramiko
import sqlalchemy
import logging
import glob
import gc
logging.basicConfig(filename='/home/jian/Projects/Big_Lots/Analysis/2019_Q4/Predictive_Model_Building/predictive_dataset_building_T2_POS_by_id_department.log',level="INFO")

print(datetime.datetime.now())
print(os.getcwd())

logging.info("start now: "+str(datetime.datetime.now()))
# IDs to include since 2018Q1

BL_SQL_CONNECTION= 'mysql+pymysql://jian:JubaPlus-2017@localhost/BigLots' 
BL_engine = sqlalchemy.create_engine(
        BL_SQL_CONNECTION, 
        pool_recycle=1800
    )


# In[3]:


pd.read_sql("show tables;",con=BL_engine)


# In[4]:


pd.read_sql("desc BL_Rewards_Master;",con=BL_engine)


# # Create Product Taxonomy

# In[5]:


# Use last one
host = "64.237.51.251" #hard-coded
port = 22
transport = paramiko.Transport((host, port))

password = "bwRi3V6fgZsfJrMl" #hard-coded
username = "client" #hard-coded
transport.connect(username = username, password = password)
sftp = paramiko.SFTPClient.from_transport(transport)
# list_taxonomy=[]
part_1=sftp.listdir("/mnt/drv5/biglots_data/")
part_1=["/mnt/drv5/biglots_data/"+x for x in part_1 if "ProductTaxonomy" in x]

part_2=sftp.listdir("/mnt/drv5/biglots_data/Monthly_Taxonomy/")
part_2=["/mnt/drv5/biglots_data/Monthly_Taxonomy/"+x for x in part_2 if "ProductTaxonomy" in x]
list_taxonomy=part_1+part_2


last_prod_taxonomy=sorted(list_taxonomy,key=lambda x: x.split("/MediaStormProductTaxonomy")[1][:8])[-1]
print(last_prod_taxonomy)
logging.info("last_prod_taxonomy: "+str(last_prod_taxonomy))


# In[6]:


local_prod_taxo="/home/jian/BigLots/static_files/ProductTaxonomy/"+os.path.basename(last_prod_taxonomy)

if not os.path.exists(local_prod_taxo):
    
    sftp.get(last_prod_taxonomy,local_prod_taxo)
sftp.close()
transport.close()


# In[7]:


df_prod_taxo=pd.read_csv(local_prod_taxo,dtype=str,sep="|")
df_prod_taxo_dep=df_prod_taxo[['department_id','class_code_id','subclass_id']].drop_duplicates()


# In[8]:


df_prod_taxo_dep


# # POS_data_from_files

# In[9]:


def recursive_file_gen(root_path):
    for root, dirs, files in os.walk(root_path):
        for file in files:
            yield os.path.join(root,file)


# In[10]:


# Read from each file

list_file_subclass=glob.glob("/home/jian/BigLots/hist_daily_data_subclasslevel/*.txt")
list_file_subclass=[x for x in list_file_subclass if x.split("_ending_")[1][:10]>="2018-02-04"]
list_file_subclass.sort()
print(list_file_subclass[0])
print(list_file_subclass[-1])


# In[11]:


list_file_subclass_weekly=list(recursive_file_gen("/home/jian/BigLots/2018_by_weeks/"))
list_file_subclass_weekly=[x for x in list_file_subclass_weekly if "dailysales" in x.lower()]
list_file_subclass_weekly=[x for x in list_file_subclass_weekly if x[-4:]==".txt"]
list_file_subclass_weekly=[x for x in list_file_subclass_weekly if x.split("s/MediaStorm_")[1][:10]<="2018-08-04"]
list_file_subclass_weekly.sort()
list_file_subclass_weekly


# In[12]:


list_file_item=glob.glob("/home/jian/BigLots/hist_daily_data_itemlevel_decompressed/*.txt")
list_file_item.sort()
print(list_file_item[0])
print(list_file_item[-1])


# In[13]:


list_file_item_weekly=list(recursive_file_gen("/home/jian/BigLots/2019_by_weeks/"))
list_file_item_weekly=[x for x in list_file_item_weekly if "dailysales" in x.lower()]
list_file_item_weekly=[x for x in list_file_item_weekly if x[-4:]==".txt"]
list_file_item_weekly=[x for x in list_file_item_weekly if x.split("/MediaStorm_")[1][:10]>="2019-02-16"]
list_file_item_weekly.sort()
print(list_file_item_weekly[0])
print(list_file_item_weekly[-1])


# In[14]:


list_all_weekly_data=list_file_subclass+list_file_subclass_weekly+list_file_item+list_file_item_weekly
print(len(list_all_weekly_data))

logging.info("len(list_all_weekly_data): "+str(len(list_all_weekly_data)))
logging.info("list_all_weekly_data[0]: "+str(list_all_weekly_data[0]))
logging.info("list_all_weekly_data[-1]: "+str(list_all_weekly_data[-1]))


# In[15]:


import pymysql.cursors
engine_pymysql_db_BL = pymysql.connect(host='localhost',user='jian',
                         password='JubaPlus-2017',db='BigLots',
                         charset='utf8mb4',cursorclass=pymysql.cursors.DictCursor)

def create_BL_Pred_POS_department_table():
    with engine_pymysql_db_BL.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS Pred_POS_Department")
        cur.execute("CREATE TABLE Pred_POS_Department         (        location_id int,         transaction_dt Date,         transaction_id varchar(16),         customer_id_hashed char(64),         department_id varchar(16),         sales decimal(10,2),         units int,         trans_order_since_18Q1 int         );"
                   )
    print("1:An empty TABLE Pred_POS_Department has been created.",datetime.datetime.now())
    logging.info("1:An empty TABLE Pred_POS_Department has been created."+str(datetime.datetime.now()))
 
 
create_BL_Pred_POS_department_table()


# In[16]:


list_df_by_week_day_summary=[]
total_sales=0
i_counter=0

df_id_order_count=pd.DataFrame(columns=['customer_id_hashed','trans_order_since_18Q1'])

for file in list_all_weekly_data:
    df=pd.read_csv(file,dtype=str,nrows=None,sep="|")
    df=df.rename(columns={"subclass_transaction_amt":"sales"})
    df=df.rename(columns={"item_transaction_amt":"sales"})
    
    df=df.rename(columns={"subclass_transaction_units":"units"})
    df=df.rename(columns={"item_transaction_units":"units"})
    
    df['sales']=df['sales'].astype(float)
    df['units']=df['units'].astype(int)
    
    
    df=pd.merge(df,df_prod_taxo_dep,on=['class_code_id','subclass_id'],how="left")
    df['department_id']=df['department_id'].fillna("-1")
    df['customer_id_hashed']=df['customer_id_hashed'].fillna("non_rewards")
    
    df=df.groupby(['location_id','transaction_dt','transaction_id','customer_id_hashed','department_id'])['sales','units'].sum().reset_index()
    df=df.sort_values(['customer_id_hashed','transaction_dt','location_id','transaction_id','department_id'])
    
    # add the transaction_order
    df_order_this_week_rewards=df[df['customer_id_hashed']!="non_rewards"]
    df_order_this_week_rewards=df_order_this_week_rewards[['customer_id_hashed','transaction_dt','location_id','transaction_id']].drop_duplicates()
    df_order_this_week_rewards=df_order_this_week_rewards.sort_values(['customer_id_hashed','transaction_dt','location_id','transaction_id'])
    df_order_this_week_rewards['trans_order_in_week']=pd.Categorical(df_order_this_week_rewards['customer_id_hashed']+ '_'+                                                                     df_order_this_week_rewards['transaction_dt']+ '_'+                                                                     df_order_this_week_rewards['location_id']+ '_'+                                                                     df_order_this_week_rewards['transaction_id']
                                                                    ).codes

    df_min_index_per_id=df_order_this_week_rewards[['customer_id_hashed','trans_order_in_week']].sort_values(['customer_id_hashed','trans_order_in_week'],ascending=[True,True]).drop_duplicates("customer_id_hashed")
    df_min_index_per_id=df_min_index_per_id.rename(columns={"trans_order_in_week":"min_order"})
    df_order_this_week_rewards=pd.merge(df_order_this_week_rewards,df_min_index_per_id,on="customer_id_hashed",how="left")
    df_order_this_week_rewards['trans_order_in_week']=df_order_this_week_rewards['trans_order_in_week']-df_order_this_week_rewards['min_order']+1
    
    df_order_this_week_rewards=pd.merge(df_order_this_week_rewards,df_id_order_count,on='customer_id_hashed',how="left")
    df_order_this_week_rewards['trans_order_since_18Q1']=df_order_this_week_rewards['trans_order_since_18Q1'].fillna(0)
    df_order_this_week_rewards['trans_order_since_18Q1']=df_order_this_week_rewards['trans_order_since_18Q1']+df_order_this_week_rewards['trans_order_in_week']
    df_order_this_week_rewards=df_order_this_week_rewards[['customer_id_hashed','transaction_dt','location_id','transaction_id','trans_order_since_18Q1']]
    df=pd.merge(df,df_order_this_week_rewards,on=['customer_id_hashed','transaction_dt','location_id','transaction_id'],how="left")

    #
    df_order_this_week_rewards=df_order_this_week_rewards[['customer_id_hashed','trans_order_since_18Q1']].sort_values(["customer_id_hashed","trans_order_since_18Q1"],ascending=[True,False]).drop_duplicates("customer_id_hashed")
    df_id_order_count=df_order_this_week_rewards.append(df_id_order_count).drop_duplicates("customer_id_hashed")
    
    # format
    df['location_id']=df['location_id'].astype(int)
    df['transaction_dt']=pd.to_datetime(df['transaction_dt'],format="%Y-%m-%d").dt.date
    df['customer_id_hashed']=df['customer_id_hashed'].replace("non_rewards",np.nan)
    df=df.round({'sales': 2})
    
    print(df['transaction_dt'].min(),df['transaction_dt'].max(),datetime.datetime.now())
    logging.info(str(df['transaction_dt'].min())+" | "+str(df['transaction_dt'].max())+" | "+str(datetime.datetime.now()))
    

    df.to_sql("Pred_POS_Department",if_exists='append', con=BL_engine, index=False,chunksize=300000,
                    dtype={
                        'location_id':sqlalchemy.types.INTEGER(),
                        'transaction_dt':sqlalchemy.Date(), 
                        'transaction_id':sqlalchemy.types.VARCHAR(length=16),
                        'customer_id_hashed':sqlalchemy.types.VARCHAR(length=64),
                        'department_id':sqlalchemy.types.VARCHAR(length=16),
                        'sales':sqlalchemy.types.DECIMAL(precision=10,scale=2,asdecimal=True),
                        'units':sqlalchemy.types.INTEGER()
                    })

    
    i_counter+=1
    print("done of file: ",i_counter,file)
    logging.info("done of file: "+str(i_counter)+" | "+file)

    total_sales+=df['sales'].sum()


# In[17]:


print("done of write to sql: "+str(datetime.datetime.now()))
logging.info("done of write to sql: "+str(datetime.datetime.now()))
print('total_sales',total_sales)
logging.info('total_sales'+str(total_sales))


# In[18]:


with engine_pymysql_db_BL.cursor() as cur:
    cur.execute("create index location_id on Pred_POS_Department(location_id);")
    cur.execute("create index transaction_dt on Pred_POS_Department(transaction_dt);")
    cur.execute("create index customer_id_hashed on Pred_POS_Department(customer_id_hashed);")
    cur.execute("create index department_id on Pred_POS_Department(department_id);")
    cur.execute("create index trans_order_since_18Q1 on Pred_POS_Department(trans_order_since_18Q1);")
print('Done of indexing',datetime.datetime.now())
logging.info('total_sales'+str(datetime.datetime.now()))


# In[ ]:




