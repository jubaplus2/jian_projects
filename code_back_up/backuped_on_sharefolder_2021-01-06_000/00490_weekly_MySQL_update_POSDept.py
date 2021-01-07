#!/usr/bin/env python
# coding: utf-8

# In[6]:


# Update MySQL: Pred_CRM_table

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

logging.basicConfig(filename='/home/jian/celery/BL_MySQL/Weekly_Update_MySQL/POS_weekly_update.log', level=logging.INFO)

BL_SQL_CONNECTION= 'mysql+pymysql://jian:JubaPlus-2017@localhost/BigLots' 
BL_engine = sqlalchemy.create_engine(
        BL_SQL_CONNECTION, 
        pool_recycle=1800
    )


# # Pred_POS_Department

# In[7]:


file_product_taxonomy=glob.glob("/home/jian/BigLots/static_files/ProductTaxonomy/*.txt")
file_product_taxonomy=[x for x in file_product_taxonomy if "taxonomy" in x.lower()]
file_product_taxonomy.sort()
file_product_taxonomy=file_product_taxonomy[-1]

print(file_product_taxonomy)

df_prod_taxo_dep=pd.read_csv(file_product_taxonomy,dtype=str,sep="|")
df_prod_taxo_dep=df_prod_taxo_dep[['department_id','class_code_id','subclass_id']].drop_duplicates()


# In[9]:


def recursive_file_gen(my_root_dir):
    for root, dirs, files in os.walk(my_root_dir):
        for file in files:
            yield os.path.join(root, file)

MySQL_max_date_POSDepart=pd.read_sql("select max(transaction_dt) from Pred_POS_Department;",con=BL_engine)
MySQL_max_date_POSDepart=str(MySQL_max_date_POSDepart.iloc[0,0])

print("MySQL_max_date_POSDepart: "+MySQL_max_date_POSDepart)
logging.info("MySQL_max_date_POSDepart: "+MySQL_max_date_POSDepart)

files_item_POS_plain=list(recursive_file_gen("/home/jian/BigLots/"))
files_item_POS_plain=[x for x in files_item_POS_plain if x[-4:]==".txt" and "dailysales" in x.lower() and "/MediaStorm_" in x]
files_item_POS_plain=[x for x in files_item_POS_plain if x.split("/MediaStorm_")[1][:10]>MySQL_max_date_POSDepart]
print(files_item_POS_plain)

logging.info(files_item_POS_plain)


# In[4]:


col_list=pd.read_sql("select * from Pred_POS_Department limit 2;",con=BL_engine).columns.tolist()
print("start_extract_max_order: "+str(datetime.datetime.now()))
logging.info("start_extract_max_order: "+str(datetime.datetime.now()))

df_trans_order_by_id=pd.read_sql("select customer_id_hashed, max(trans_order_since_18Q1) as trans_order_since_18Q1 from Pred_POS_Department group by customer_id_hashed;",con=BL_engine)

print("done_extract_max_order: "+str(datetime.datetime.now()))
logging.info("done_extract_max_order: "+str(datetime.datetime.now()))


# In[11]:




print("df_trans_order_by_id.shape",df_trans_order_by_id.shape)
logging.info("df_trans_order_by_id.shape: "+str(df_trans_order_by_id.shape))


# In[ ]:


total_sales=0
i_counter=0
for file in files_item_POS_plain:
    print(datetime.datetime.now(),"start",file)
    logging.info(str(datetime.datetime.now())+" | start | "+file)
    
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
    
    df_order_this_week_rewards=pd.merge(df_order_this_week_rewards,df_trans_order_by_id,on='customer_id_hashed',how="left")
    df_order_this_week_rewards['trans_order_since_18Q1']=df_order_this_week_rewards['trans_order_since_18Q1'].fillna(0)
    df_order_this_week_rewards['trans_order_since_18Q1']=df_order_this_week_rewards['trans_order_since_18Q1']+df_order_this_week_rewards['trans_order_in_week']
    df_order_this_week_rewards=df_order_this_week_rewards[['customer_id_hashed','transaction_dt','location_id','transaction_id','trans_order_since_18Q1']]
    df=pd.merge(df,df_order_this_week_rewards,on=['customer_id_hashed','transaction_dt','location_id','transaction_id'],how="left")

    #
    df_order_this_week_rewards=df_order_this_week_rewards[['customer_id_hashed','trans_order_since_18Q1']].sort_values(["customer_id_hashed","trans_order_since_18Q1"],ascending=[True,False]).drop_duplicates("customer_id_hashed")
    df_trans_order_by_id=df_order_this_week_rewards.append(df_trans_order_by_id).drop_duplicates("customer_id_hashed")
    
    # format
    df['location_id']=df['location_id'].astype(int)
    df['transaction_dt']=pd.to_datetime(df['transaction_dt'],format="%Y-%m-%d").dt.date
    df['customer_id_hashed']=df['customer_id_hashed'].replace("non_rewards",np.nan)
    df=df.round({'sales': 2})
    
    print(df['transaction_dt'].min(),df['transaction_dt'].max(),datetime.datetime.now())
    logging.info("df['transaction_dt'].min(),df['transaction_dt'].max(),datetime.datetime.now()")
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
    print(datetime.datetime.now(),"done",file)
    logging.info(str(datetime.datetime.now())+" | done | "+file)
    


# In[ ]:


print("done of write to sql: "+str(datetime.datetime.now()))
logging.info("done of write to sql: "+str(datetime.datetime.now()))
print('total_sales',total_sales)
logging.info('total_sales'+str(total_sales))


# # Pred_CRM_table

# In[10]:


Q1_2018_Start_Date="'2018-02-03'"
df_all_since_18Q1=pd.read_sql("select customer_id_hashed, email_address_hash, sign_up_date, sign_up_channel, sign_up_location, customer_zip_code from BL_Rewards_Master where sign_up_date >= %sorder by customer_id_hashed, sign_up_date desc;" %Q1_2018_Start_Date,con=BL_engine)
print(df_all_since_18Q1.shape)
df_all_since_18Q1=df_all_since_18Q1.drop_duplicates("customer_id_hashed")
print(df_all_since_18Q1.shape)


# In[11]:


print(df_all_since_18Q1['sign_up_date'].min(),df_all_since_18Q1['sign_up_date'].max())
print(df_all_since_18Q1['customer_id_hashed'].nunique())


# In[12]:


df_all_since_18Q1['Registration_Online']=np.where(df_all_since_18Q1['sign_up_channel']=="Online",1,0)


# In[13]:


df_all_since_18Q1.to_sql("Pred_CRM_table",if_exists='replace', con=BL_engine, index=False,chunksize=300000,
                    dtype={
                        'customer_id_hashed':sqlalchemy.types.VARCHAR(length=64),
                        'email_address_hash':sqlalchemy.types.VARCHAR(length=64),
                        'sign_up_date':sqlalchemy.Date(),
                        'sign_up_channel':sqlalchemy.types.VARCHAR(length=64),
                        'sign_up_location':sqlalchemy.types.INTEGER(),
                        'customer_zip_code':sqlalchemy.types.VARCHAR(length=16),
                        'Registration_Online':sqlalchemy.types.INTEGER()
                    })


# In[14]:


print(datetime.datetime.now())
logging.info("Pred_CRM_also_updated: "+str(datetime.datetime.now()))


# In[15]:


pd.read_sql("select min(sign_up_date), max(sign_up_date), count(customer_id_hashed), count(distinct customer_id_hashed) from Pred_CRM_table;",con=BL_engine)

