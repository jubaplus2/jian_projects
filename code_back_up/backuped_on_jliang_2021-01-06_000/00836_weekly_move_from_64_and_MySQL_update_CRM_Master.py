#!/usr/bin/env python
# coding: utf-8

# In[1]:


# Update MySQL: CRM master
# and transfer the weekly data
import pandas as pd
import datetime
import os
import numpy as np
import hashlib
import gc
import logging
import sqlalchemy
import glob
import paramiko

BL_SQL_CONNECTION= 'mysql+pymysql://jian:JubaPlus-2017@localhost/BigLots' 
BL_engine = sqlalchemy.create_engine(
        BL_SQL_CONNECTION, 
        pool_recycle=1800
    )


# In[2]:


logging.basicConfig(filename='/home/jian/BL_weekly_crontab/cron_0_MySQL_CRM/biglots_weekly_crontab0_weekly_MySQL_update.log', level=logging.INFO)
thisweeksdate=str(datetime.datetime.now().date()-datetime.timedelta(days=datetime.datetime.now().date().weekday()-1+3))
print("thisweeksdate: "+thisweeksdate)


# In[6]:


MySQL_max_date_CRM=pd.read_sql("select max(sign_up_date) from BL_Rewards_Master;",con=BL_engine)
MySQL_max_date_CRM=str(MySQL_max_date_CRM.iloc[0,0])

print("MySQL_max_date_CRM: "+MySQL_max_date_CRM)


# In[8]:


if thisweeksdate!=MySQL_max_date_CRM:
    recent_weekly_data_folder="/home/jian/BigLots/MediaStorm_"+thisweeksdate+"/"
    try:
        os.stat(recent_weekly_data_folder)
    except:
        os.mkdir(recent_weekly_data_folder)
    host = "64.237.51.251" #hard-coded
    port = 22
    transport = paramiko.Transport((host, port))
     
    password = "bwRi3V6fgZsfJrMl" #hard-coded
    username = "client" #hard-coded
    transport.connect(username = username, password = password)
    sftp = paramiko.SFTPClient.from_transport(transport)
    
    
    Client_Today_STR=str(datetime.datetime.now().date())
    Client_Today_NUM_STR =Client_Today_STR[0:4]+Client_Today_STR[5:7]+Client_Today_STR[8:10]
    
    new_weekly_file_list=sftp.listdir("/mnt/drv5/biglots_data/")
    new_weekly_file_list=["/mnt/drv5/biglots_data/"+x for x in new_weekly_file_list if Client_Today_NUM_STR in x]
    
    for new_weekly_file in new_weekly_file_list:
        localpath=recent_weekly_data_folder+new_weekly_file.split("/")[len(new_weekly_file.split("/"))-1]
        try:
            os.stat(localpath)
        except:
            sftp.get(new_weekly_file,localpath)
    
    sftp.close()
    transport.close()
    logging.info(str(datetime.datetime.now())+": Finished copying")


# In[9]:


def recursive_file_gen(my_root_dir):
    for root, dirs, files in os.walk(my_root_dir):
        for file in files:
            yield os.path.join(root, file)

files_master_plain=list(recursive_file_gen("/home/jian/BigLots/"))
files_master_plain=[x for x in files_master_plain if x[-4:]==".txt" and "master" in x.lower() and "/MediaStorm_" in x]
files_master_plain=[x for x in files_master_plain if x.split("/MediaStorm_")[1][:10]>MySQL_max_date_CRM]
files_master_plain


# In[20]:


col_list=pd.read_sql("select * from BL_Rewards_Master limit 2;",con=BL_engine).columns.tolist()[:-1]

df_new_sign_ups=pd.DataFrame()
for file in files_master_plain:
    df=pd.read_csv(file,dtype=str,sep="|")
    print(datetime.datetime.now())
    print(os.path.basename(file),df.columns.tolist()==col_list)

    logging.info(str(datetime.datetime.now()))
    logging.info(os.path.basename(file)+": columns names matching -- "+str(df.columns.tolist()==col_list))
    
    df['file_path']=file
    df_new_sign_ups=df_new_sign_ups.append(df)
    
print(df_new_sign_ups.shape)
logging.info("total new sign up df.shape"+str(df_new_sign_ups.shape))

print(df_new_sign_ups['sign_up_date'].min(),df_new_sign_ups['sign_up_date'].max())
logging.info("total new sign up df.['sign_up_date'].min(): "+str(df_new_sign_ups['sign_up_date'].min()))
logging.info("total new sign up df.['sign_up_date'].max(): "+str(df_new_sign_ups['sign_up_date'].max()))

# Clean dataframe column value types

df_new_sign_ups['sign_up_date']=pd.to_datetime(df_new_sign_ups['sign_up_date'],format="%Y-%m-%d").dt.date

#
df_new_sign_ups['sign_up_location']=df_new_sign_ups['sign_up_location'].fillna(-999).astype(int)
df_new_sign_ups['sign_up_location']=df_new_sign_ups['sign_up_location'].replace(-999,np.nan)

df_new_sign_ups['transaction_count']=df_new_sign_ups['transaction_count'].astype(float)
df_new_sign_ups['transaction_amount']=df_new_sign_ups['transaction_amount'].astype(float)
df_new_sign_ups['experian_multi_cluster']=df_new_sign_ups['experian_multi_cluster'].astype(float)
df_new_sign_ups['experian_demo_cluster']=df_new_sign_ups['experian_demo_cluster'].astype(float)

print("df_new_sign_ups.shape",df_new_sign_ups.shape)
print("df_new_sign_ups['customer_id_hashed'].nunique()",df_new_sign_ups['customer_id_hashed'].nunique())
print("df_new_sign_ups['email_address_hash'].nunique()",df_new_sign_ups['email_address_hash'].nunique())

logging.info("df_new_sign_ups.shape: "+str(df_new_sign_ups.shape))
logging.info("df_new_sign_ups['customer_id_hashed'].nunique(): "+str(df_new_sign_ups['customer_id_hashed'].nunique()))
logging.info("df_new_sign_ups['email_address_hash'].nunique(): "+str(df_new_sign_ups['email_address_hash'].nunique()))

df_new_sign_ups.to_sql("BL_Rewards_Master",if_exists='append', con=BL_engine, index=False,chunksize=300000)

print(datetime.datetime.now(),"done of updating mysql master")

print(str(datetime.datetime.now())+": done of updating mysql master")

