#!/usr/bin/env python
# coding: utf-8

# In[1]:


# copy files from 107 and update 2 SQL tables: CRM & POS

import pandas as pd
import numpy as np
import datetime
import paramiko
import os
import glob
import sqlalchemy
import logging

logging.basicConfig(filename='/mnt/clients/juba/hqjubaapp02/sharefolder/Automation/BigLots_crontab_on_Merkle/BL_weekly_cron01_Tue_per_week.log',level="INFO")

def recursive_file_gen(root_path):
    for root, dirs, files in os.walk(root_path):
        for file in files:
            yield os.path.join(root,file)
            
BL_SQL_CONNECTION= 'mysql+pymysql://jliang:H1Dswk&Fxz@localhost/BigLots'
BL_engine = sqlalchemy.create_engine(
        BL_SQL_CONNECTION,
        pool_recycle=1800
    )

print("start: ",datetime.datetime.now())
logging.info("start: %s"%str(datetime.datetime.now()))


# In[2]:


today_date=datetime.datetime.now().date()
last_Saturday=None
for i in range(7):
    x=today_date-datetime.timedelta(days=i)
    if x.weekday()==5:
        last_Saturday=x
        break
print("last_Saturday", last_Saturday)
logging.info("last_Saturday: %s"%last_Saturday)
str_last_Saturday=str(last_Saturday)


# In[3]:


str_year=str(last_Saturday.year)
print("str_year: %s"%str_year)
logging.info("str_year: %s"%str_year)


# In[4]:


# biglots_data: 1.store list; 2.prod_taxo

remote_data_path_client="/mnt/drv5/biglots_data/"

host = "107.191.32.220" #hard-coded
port = 22
transport = paramiko.Transport((host, port))

password = "vgn5UucsUNHL4n9R" #hard-coded
username = "biglots_data" #hard-coded
transport.connect(username = username, password = password)
sftp = paramiko.SFTPClient.from_transport(transport)


# In[5]:


list_remote_files=sftp.listdir()
remote_year_folder="Weekly_%s/"%str_year
if remote_year_folder[:-1] in list_remote_files:
    remote_year_folder="/mnt/drv5/biglots_data/"+remote_year_folder
else:
    remote_year_folder="/mnt/drv5/biglots_data/"+remote_year_folder
    sftp.mkdir(remote_year_folder)
##    
folder_remote_move_in=remote_year_folder+"data_%s/"%str_last_Saturday
try:
    sftp.stat(folder_remote_move_in)
except:
    sftp.mkdir(folder_remote_move_in)


# In[6]:


local_data_folder="/mnt/clients/juba/hqjubaapp02/sharefolder/biglots_data/"
list_local_files=os.listdir(local_data_folder)
local_year_folder="%s_by_weeks/"%str_year
if local_year_folder[:-1] in list_local_files:
    local_year_folder=local_data_folder+local_year_folder
else:
    local_year_folder=local_data_folder+local_year_folder
    os.mkdir(local_year_folder)
##
folder_local_move_in=local_year_folder+"MediaStorm_%s/"%str_last_Saturday
try:
    os.stat(folder_local_move_in)
except:
    os.mkdir(folder_local_move_in)
    


# # Copy Files

# In[7]:


list_remote_weekly=[x for x in list_remote_files if "MediaStorm" in x]
list_remote_weekly=[x for x in list_remote_weekly if x[-4:]==".txt"]

if len(list_remote_weekly)==6:
    print(list_remote_weekly)
    logging.info("list of new weekly files: \n%s"%str(list_remote_weekly))
    
    for basename_file in list_remote_weekly:
        sftp.get(remote_data_path_client+basename_file,folder_local_move_in+basename_file)
        sftp.rename(remote_data_path_client+basename_file, folder_remote_move_in+basename_file)
        
        print("file copied %s:"%basename_file)
        logging.info("file copied %s:"%basename_file)
        
else:
    print("New received files are not 6")
    logging.info("New received files are not 6")


# In[8]:


list_remote_weekly


# In[9]:


folder_local_move_in


# # Update MySQL

# In[10]:


import glob
local_list=glob.glob(folder_local_move_in+"*.txt")
local_list.sort()
print(local_list)


# ## update CRM

# In[11]:


file_new_crm=[x for x in local_list if "MediaStormMasterWeekly" in x]
if len(file_new_crm)==1:
    file_new_crm=file_new_crm[0]
    print("crm file: %s"%file_new_crm)
    logging.info("crm file: %s"%file_new_crm)
    
else:
    print("len new CRM file in local folder is not 1")
    logging.info("len new CRM file in local folder is not 1")


# In[13]:


col_list=pd.read_sql("select * from BL_Rewards_Master limit 1;",con=BL_engine)
col_list=col_list.columns.tolist()
# col_list


# In[15]:


max_date_already_in_SQL=pd.read_sql("select max(sign_up_date) from BL_Rewards_Master;", con=BL_engine).iloc[0,0]
print("max_date_already_in_SQL: %s"%max_date_already_in_SQL)
logging.info("max_date_already_in_SQL: %s"%max_date_already_in_SQL)


# In[24]:


if (last_Saturday-max_date_already_in_SQL).days!=7:
    print("the new high date and latest in MySQL for CRM is not 7")
    logging.info("the new high date and latest in MySQL for CRM is not 7")
else:
    df_new_sign_ups=pd.read_csv(file_new_crm,dtype=str,sep="|")
    df_new_sign_ups['file_path']=file_new_crm
    print(os.path.basename(file_new_crm),datetime.datetime.now(),df_new_sign_ups.columns.tolist()==col_list)
    print(df_new_sign_ups.shape)
    df_new_sign_ups['sign_up_date']=pd.to_datetime(df_new_sign_ups['sign_up_date'],format="%Y-%m-%d").dt.date
    #
    df_new_sign_ups['sign_up_location']=df_new_sign_ups['sign_up_location'].fillna(-999).astype(int)
    df_new_sign_ups['sign_up_location']=df_new_sign_ups['sign_up_location'].replace(-999,np.nan)

    df_new_sign_ups['transaction_count']=df_new_sign_ups['transaction_count'].astype(float)
    df_new_sign_ups['transaction_amount']=df_new_sign_ups['transaction_amount'].astype(float)
    df_new_sign_ups['experian_multi_cluster']=df_new_sign_ups['experian_multi_cluster'].astype(float)
    df_new_sign_ups['experian_demo_cluster']=df_new_sign_ups['experian_demo_cluster'].astype(float)
    
    print("check headers: ",df_new_sign_ups.columns.tolist()==col_list)
    logging.info("check headers: "+str(df_new_sign_ups.columns.tolist()==col_list))
    
    print("df_new_sign_ups.shape",df_new_sign_ups.shape)
    print("df_new_sign_ups['customer_id_hashed'].nunique()",df_new_sign_ups['customer_id_hashed'].nunique())
    print("df_new_sign_ups['email_address_hash'].nunique()",df_new_sign_ups['email_address_hash'].nunique())
    
    logging.info("df_new_sign_ups.shape: %s"%str(df_new_sign_ups.shape))
    logging.info("df_new_sign_ups['customer_id_hashed'].nunique(): %s"%str(df_new_sign_ups['customer_id_hashed'].nunique()))
    logging.info("df_new_sign_ups['email_address_hash'].nunique(): %s"%str(df_new_sign_ups['email_address_hash'].nunique()))
    
    # Write into sql
    df_new_sign_ups.to_sql("BL_Rewards_Master",if_exists='append', con=BL_engine, index=False,chunksize=300000)
    print("Done of CRM updating %s"%str(datetime.datetime.now()))
    logging.info("Done of CRM updating %s"%str(datetime.datetime.now()))
    


# ## update POS department

# In[25]:


# procedure in mysql
'''
delimiter //
create procedure get_max_trans_order()
begin
select customer_id_hashed, max(trans_order_since_18Q1) as trans_order_since_18Q1 from Pred_POS_Department group by customer_id_hashed; 
end //
delimiter ;
'''


# In[26]:


file_product_taxonomy=glob.glob("/mnt/clients/juba/hqjubaapp02/sharefolder/biglots_data/static_files/ProductTaxonomy/*.txt")
file_product_taxonomy=[x for x in file_product_taxonomy if "taxonomy" in x.lower()]
file_product_taxonomy.sort()
file_product_taxonomy=file_product_taxonomy[-1]

print(file_product_taxonomy)

df_prod_taxo_dep=pd.read_csv(file_product_taxonomy,dtype=str,sep="|")
df_prod_taxo_dep=df_prod_taxo_dep[['department_id','class_code_id','subclass_id']].drop_duplicates()


# In[34]:


def recursive_file_gen(my_root_dir):
    for root, dirs, files in os.walk(my_root_dir):
        for file in files:
            yield os.path.join(root, file)

MySQL_max_date_POSDepart_before=pd.read_sql("select max(transaction_dt) from Pred_POS_Department;",con=BL_engine)
MySQL_max_date_POSDepart_before=MySQL_max_date_POSDepart_before.iloc[0,0]

print("MySQL_max_date_POSDepart_before: %s"%str(MySQL_max_date_POSDepart_before))
logging.info("MySQL_max_date_POSDepart_before: %s"%str(MySQL_max_date_POSDepart_before))


# In[31]:


file_new_pos=[x for x in local_list if "/MediaStormDailySales" in x]
if len(file_new_pos)==1:
    file_new_pos=file_new_pos[0]
    print("pos file: %s"%file_new_pos)
    logging.info("pos file: %s"%file_new_pos)
    
else:
    print("len new POS file in local folder is not 1")
    logging.info("len new POS file in local folder is not 1")


# In[35]:


MySQL_max_date_POSDepart_before


# In[36]:


if (last_Saturday-MySQL_max_date_POSDepart_before).days!=7:
    print("the new high date and latest in MySQL for POS department is not 7")
    logging.info("the new high date and latest in MySQL for POS department is not 7")
else:
    pass


# In[32]:


col_list=pd.read_sql("select * from Pred_POS_Department limit 2;",con=BL_engine).columns.tolist()
print("start_extract_max_order: "+str(datetime.datetime.now()))
logging.info("start_extract_max_order: "+str(datetime.datetime.now()))

df_trans_order_by_id=pd.read_sql("call get_max_trans_order();",con=BL_engine)

print("done_extract_max_order: "+str(datetime.datetime.now()))
logging.info("done_extract_max_order: "+str(datetime.datetime.now()))


# In[39]:


print("df_trans_order_by_id.shape",df_trans_order_by_id.shape)
logging.info("df_trans_order_by_id.shape: "+str(df_trans_order_by_id.shape))


# In[41]:


file_new_pos


# In[42]:


total_sales=0
i_counter=0

print(datetime.datetime.now(),"start",file_new_pos)
logging.info(str(datetime.datetime.now())+" | start | "+file_new_pos)

df=pd.read_csv(file_new_pos,dtype=str,nrows=None,sep="|")
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
df_order_this_week_rewards['trans_order_in_week']=pd.Categorical(df_order_this_week_rewards['customer_id_hashed']+ '_'+                                                                 df_order_this_week_rewards['transaction_dt']+ '_'+                                                                 df_order_this_week_rewards['location_id']+ '_'+                                                                 df_order_this_week_rewards['transaction_id']
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
print("done of file: ",i_counter,file_new_pos)
logging.info("done of file: "+str(i_counter)+" | "+file_new_pos)

total_sales+=df['sales'].sum()
print(datetime.datetime.now(),"done",file_new_pos)
logging.info(str(datetime.datetime.now())+" | done | "+file_new_pos)
    


# In[43]:


print("done of write to sql: "+str(datetime.datetime.now()))
logging.info("done of write to sql: "+str(datetime.datetime.now()))
print('total_sales',total_sales)
logging.info('total_sales'+str(total_sales))


# In[45]:


MySQL_max_date_POSDepart_after=MySQL_max_date_POSDepart=pd.read_sql("select max(transaction_dt) from Pred_POS_Department;",con=BL_engine)
MySQL_max_date_POSDepart_after=MySQL_max_date_POSDepart_after.iloc[0,0]

print("MySQL_max_date_POSDepart_after: %s"%str(MySQL_max_date_POSDepart_after))
logging.info("MySQL_max_date_POSDepart_after: %s"%str(MySQL_max_date_POSDepart_after))


# In[46]:


df_output_confirmation=pd.DataFrame({"date_before_run":[MySQL_max_date_POSDepart_before],
                                     "date_after_run":[MySQL_max_date_POSDepart_after],
                                    "files_used":file_new_pos},index=[0])
df_output_confirmation=df_output_confirmation.reset_index()
del df_output_confirmation['index']
df_output_confirmation


# In[49]:


write_folder="./folder_check_with_successful_run/"
try:
    os.stat(write_folder)
except:
    os.mkdir(write_folder)
    
df_output_confirmation.to_csv(write_folder+"done_in_crontab_"+str(MySQL_max_date_POSDepart_before)+"_to_"+str(MySQL_max_date_POSDepart_after)+"_run_on_"+str(datetime.datetime.now().date())+".csv",index=False)


# In[50]:


print("finished: %s"%str(datetime.datetime.now()))
logging.info("finished: %s"%str(datetime.datetime.now()))


# In[51]:


df_qc=pd.read_sql("select * from Pred_POS_Department where transaction_dt='2020-12-16' limit 200;",con=BL_engine)
list_qc_ids=df_qc['customer_id_hashed'].unique().tolist()


# In[54]:


i=11

str_id="'"+list_qc_ids[i]+"'"
str_id


# In[55]:


df_res_id=pd.read_sql("select * from Pred_POS_Department where customer_id_hashed=%s;"%str_id,con=BL_engine)
df_res_id.tail(20)


# In[56]:


'''
col_list=pd.read_sql("select * from Pred_POS_Department limit 2;",con=BL_engine).columns.tolist()
print("start_extract_max_order: "+str(datetime.datetime.now()))
logging.info("start_extract_max_order: "+str(datetime.datetime.now()))

df_trans_order_by_id=pd.read_sql("call get_max_trans_order();",con=BL_engine)

print("done_extract_max_order: "+str(datetime.datetime.now()))
logging.info("done_extract_max_order: "+str(datetime.datetime.now()))
'''


# In[57]:


pd.read_sql("desc Pred_POS_Department;",con=BL_engine)


# In[ ]:




