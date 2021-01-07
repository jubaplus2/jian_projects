#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import datetime
import numpy as np
import os
import glob
import gc
import logging


logging.basicConfig(filename='/home/jian/BL_weekly_crontab/cron_7_no_shopper_in_audience/ID_list_no_shopping_6_weeks.log', level=logging.INFO)

def recursive_file_gen(root_folder):
    for root, dirs, files in os.walk(root_folder):
        for file in files:
            yield os.path.join(root, file)
            
os.getcwd()


# In[2]:


last_sturday = (datetime.datetime.now()-datetime.timedelta(days=(datetime.datetime.now().weekday()+2))).date()
print("last_sturday: "+str(last_sturday))
logging.info("last_sturday: "+str(last_sturday))
last_day_of_2018Q4=datetime.date(2019,2,2)

year_of_quarter=(last_sturday-last_day_of_2018Q4).days/(52*7)
year_of_quarter=str(int(2019+np.floor(year_of_quarter)))
print("Year",year_of_quarter)
logging.info("Year "+str(year_of_quarter))


quarter_of_quarter=(last_sturday-last_day_of_2018Q4).days/7
quarter_of_quarter=np.floor(quarter_of_quarter/13)%4
quarter_of_quarter=str(int(1+quarter_of_quarter))
print("Quarter",quarter_of_quarter)
logging.info("Quarter "+str(quarter_of_quarter))

str_current_quarter=year_of_quarter+"_Q"+quarter_of_quarter

print("str_current_quarter", str_current_quarter)
logging.info("str_current_quarter "+str(str_current_quarter))

current_week=int((last_sturday-last_day_of_2018Q4).days/7%13)
print("current_week",current_week)
logging.info("current_week: "+str(current_week))

if current_week==0:
    quarter_of_quarter=int(quarter_of_quarter)-1+4
    year_of_quarter=str(int(year_of_quarter)-1)
    str_current_quarter=year_of_quarter+"_Q"+str(quarter_of_quarter)
    current_week=13
    print("week0-Quarter",quarter_of_quarter)
    print("week0-current_week",current_week)
    
    logging.info("week0-Quarter: "+str(quarter_of_quarter))
    logging.info("week0-current_week: "+str(current_week))


# In[3]:


current_quarter_beginning=last_day_of_2018Q4+datetime.timedelta(days=(int(year_of_quarter)-2019)*52*7+                                                                (int(quarter_of_quarter)-1)*13*7+1)
print("current_quarter_beginning: "+str(current_quarter_beginning))
logging.info("current_quarter_beginning: "+str(current_quarter_beginning))

previous_quarter_beginning=current_quarter_beginning-datetime.timedelta(days=7*13)
print("previous_quarter_beginning: "+str(previous_quarter_beginning))
logging.info("current_quarter_beginning: "+str(previous_quarter_beginning))

Today_date=datetime.datetime.now().date()

if Today_date>=current_quarter_beginning:
    audience_Quarter_num=int(quarter_of_quarter)
    audience_year=int(year_of_quarter)
else:
    audience_Quarter_num=int(quarter_of_quarter)-1
    audience_year=int(year_of_quarter)
    if audience_Quarter_num==0:
        audience_Quarter_num=4
        audience_year=int(year_of_quarter)-1
        
        
        
print("audience_Quarter_num: "+str(audience_Quarter_num))
logging.info("audience_Quarter_num: "+str(audience_Quarter_num))

print("audience_year: "+str(audience_year))
logging.info("audience_year: "+str(audience_year))


# In[4]:


folder_current_audience="/home/jian/Projects/Big_Lots/Live_Ramp/Quarterly_Update_"+str(audience_year)+"Q"+str(audience_Quarter_num)+"/final_segments_uploaded_LR_0_18/"
list_aud_files_uploaded=glob.glob(folder_current_audience+"*.csv")
list_aud_files_uploaded_T=[x for x in list_aud_files_uploaded if os.path.basename(x)[0]=="T"]
print("len(list_aud_files_uploaded_T): "+str(len(list_aud_files_uploaded_T)))
logging.info("len(list_aud_files_uploaded_T): "+str(len(list_aud_files_uploaded_T)))


# In[5]:


df_current_test_aud=pd.DataFrame()

for file in list_aud_files_uploaded_T:
    df=pd.read_csv(file,dtype=str)
    df_current_test_aud=df_current_test_aud.append(df)
print(df_current_test_aud.shape,df_current_test_aud['customer_id_hashed'].nunique()) 
logging.info(str(df_current_test_aud.shape)+"|"+str(df_current_test_aud['customer_id_hashed'].nunique()))


# In[6]:


date_start_4_weeks_start_Sunday=last_sturday-datetime.timedelta(days=27)
date_start_6_weeks_start_Sunday=last_sturday-datetime.timedelta(days=27+14)


list_files_POS_in_6_weeks=list(recursive_file_gen("/home/jian/BigLots/"))
list_files_POS_in_6_weeks=[x for x in list_files_POS_in_6_weeks if "daily" in x.lower() and x[-4:]==".txt"]
list_files_POS_in_6_weeks=[x for x in list_files_POS_in_6_weeks if "/MediaStorm_"in x]

list_files_POS_in_6_weeks=[x for x in list_files_POS_in_6_weeks if x.split("/MediaStorm_")[1][:10]>=str(date_start_6_weeks_start_Sunday)]
print("len(list_files_POS_in_6_weeks): "+str(len(list_files_POS_in_6_weeks)))
print("len(list_files_POS_in_6_weeks): "+str(len(list_files_POS_in_6_weeks)))

logging.info("len(list_files_POS_in_6_weeks): "+str(len(list_files_POS_in_6_weeks)))
logging.info("len(list_files_POS_in_6_weeks): "+str(len(list_files_POS_in_6_weeks)))

list_files_POS_in_6_weeks=sorted(list_files_POS_in_6_weeks,key=lambda x: x.split("/MediaStorm_")[1][:10],reverse=True)
print(list_files_POS_in_6_weeks)
logging.info(str(list_files_POS_in_6_weeks))


# In[7]:


df_ids_purchased_positive_in_6_weeks=pd.DataFrame()

for file in list_files_POS_in_6_weeks:
    df=pd.read_table(file,dtype=str,sep="|",
                     usecols=['customer_id_hashed','transaction_dt','item_transaction_amt'])
    df['item_transaction_amt']=df['item_transaction_amt'].astype(float)
    df=df.groupby(['customer_id_hashed','transaction_dt'])['item_transaction_amt'].sum().to_frame().reset_index().rename(columns={"item_transaction_amt":"sales"})
    df=df[df['sales']>0]
    df_ids_purchased_positive_in_6_weeks=df_ids_purchased_positive_in_6_weeks.append(df)
    print(str(os.path.basename(file))+" | "+str(datetime.datetime.now()))
    logging.info(str(os.path.basename(file))+" | "+str(datetime.datetime.now()))
    
    


# In[8]:


del df_ids_purchased_positive_in_6_weeks['sales']
df_ids_purchased_positive_in_6_weeks=df_ids_purchased_positive_in_6_weeks.sort_values("transaction_dt",ascending=False).drop_duplicates("customer_id_hashed")
df_ids_purchased_positive_in_6_weeks=df_ids_purchased_positive_in_6_weeks.rename(columns={"transaction_dt":"last_transaction_dt"})


# In[9]:


df_ids_purchased_positive_in_6_weeks.head(2)


# In[10]:


print("date_start_4_weeks_start_Sunday", date_start_4_weeks_start_Sunday)
print("date_start_6_weeks_start_Sunday", date_start_6_weeks_start_Sunday)

logging.info("date_start_4_weeks_start_Sunday"+str(date_start_4_weeks_start_Sunday))
logging.info("date_start_6_weeks_start_Sunday"+str(date_start_6_weeks_start_Sunday))


# In[11]:


df_ids_purchased_positive_in_6_weeks['recency_group']=np.where(df_ids_purchased_positive_in_6_weeks['last_transaction_dt']>=str(date_start_4_weeks_start_Sunday),"Shopped_in_4_weeks",
                                                              np.where(df_ids_purchased_positive_in_6_weeks['last_transaction_dt']>=str(date_start_6_weeks_start_Sunday),"NoShopped_4-6_weeks","NoShopped_7+_weeks")
                                                              )
print("df_ids_purchased_positive_in_6_weeks['recency_group'].unique(): "+str(df_ids_purchased_positive_in_6_weeks['recency_group'].unique()))
logging.info("df_ids_purchased_positive_in_6_weeks['recency_group'].unique(): "+str(df_ids_purchased_positive_in_6_weeks['recency_group'].unique()))
del df_ids_purchased_positive_in_6_weeks['last_transaction_dt']


# In[12]:


df_current_test_aud=pd.merge(df_current_test_aud,df_ids_purchased_positive_in_6_weeks,on="customer_id_hashed",how="left")
df_current_test_aud['recency_group']=df_current_test_aud['recency_group'].fillna("NoShopped_7+_weeks")
print("df_current_test_aud['recency_group'].unique(): "+str(df_current_test_aud['recency_group'].unique()))
logging.info("df_current_test_aud['recency_group'].unique(): "+str(df_current_test_aud['recency_group'].unique()))


# In[13]:


df_current_test_aud.shape


# In[14]:


# Add the decile

df_defile_file=pd.read_csv("/home/jian/celery/Audience_No_Shop/Quarterly_Decile_Detail/df_detail_"+str(audience_year)+"Q"+str(audience_Quarter_num)+".csv",
                           dtype=str,usecols=['customer_id_hashed','frmindex','zip_type']).drop_duplicates()
df_defile_file.shape


# In[15]:


print("decile_details read done: "+str(datetime.datetime.now()))
logging.info("decile_details read done: "+str(datetime.datetime.now()))


# In[16]:


df_current_test_aud=pd.merge(df_current_test_aud,df_defile_file,on="customer_id_hashed",how="left")

print("decile_details merged into audience: "+str(datetime.datetime.now()))
logging.info("decile_details merged into audience: "+str(datetime.datetime.now()))

df_current_test_aud.shape


# In[17]:


writer_folder_quarter="/home/jian/celery/Audience_No_Shop/output_no_shopping/Q"+str(audience_Quarter_num)+"_"+str(audience_year)+"/"

try:
    os.stat(writer_folder_quarter)
except:
    os.mkdir(writer_folder_quarter)
    
writer_folder_week=writer_folder_quarter+"output_"+str(last_sturday)+"/"

try:
    os.stat(writer_folder_week)
except:
    os.mkdir(writer_folder_week)


# In[18]:


print(df_current_test_aud.shape,df_current_test_aud['customer_id_hashed'].nunique())
logging.info(str(df_current_test_aud.shape)+str(df_current_test_aud['customer_id_hashed'].nunique()))


# In[19]:


df_summary=df_current_test_aud.groupby(['zip_type',"frmindex",'recency_group'])['customer_id_hashed'].nunique().to_frame().reset_index()
df_summary=df_summary.pivot_table(index=['zip_type',"frmindex"],columns="recency_group",values='customer_id_hashed').reset_index()


# In[20]:


df_output=df_current_test_aud[df_current_test_aud['recency_group']!='Shopped_in_4_weeks']

print(df_output.shape)
print(df_output['recency_group'].unique())
print(df_output['frmindex'].unique())
print(df_output['zip_type'].unique())

logging.info(str(df_output.shape))
logging.info(str(df_output['recency_group'].unique()))
logging.info(str(df_output['frmindex'].unique()))
logging.info(str(df_output['zip_type'].unique()))


# In[21]:


df_output[pd.isnull(df_output['zip_type'])]['frmindex'].unique()


# In[22]:


count_audience_total=df_defile_file.groupby(["frmindex","zip_type"])['customer_id_hashed'].count().to_frame().reset_index()
df_summary=pd.merge(count_audience_total,df_summary,on=["frmindex","zip_type"],how="outer")

df_summary=df_summary[df_summary['zip_type']!="T"]
df_summary=df_summary.fillna(0)
df_summary=df_summary[['frmindex','zip_type','customer_id_hashed','Shopped_in_4_weeks','NoShopped_4-6_weeks','NoShopped_7+_weeks']]
df_summary.to_csv(writer_folder_week+"BL_summary_by_decile_zip_uploaded_audience_"+str(last_sturday)+"_JL_"+str(datetime.datetime.now().date())+".csv",index=False)


# In[23]:


df_current_test_aud.shape


# In[24]:


df_output.shape


# In[25]:


df_current_test_aud.to_csv(writer_folder_week+"/BL_df_all_uploaded_audience_"+str(last_sturday)+"_JL_"+str(datetime.datetime.now().date())+".csv",index=False)
df_output.to_csv(writer_folder_week+"/BL_df_no_shop_uploaded_audience_"+str(last_sturday)+"_JL_"+str(datetime.datetime.now().date())+".csv",index=False)

print("local files saved: "+str(datetime.datetime.now()))
logging.info("local files saved: "+str(datetime.datetime.now()))


# # tranfer to 64 for SP

# In[26]:


import paramiko

host = "64.237.51.251" #hard-coded
port = 22
transport = paramiko.Transport((host, port))

password = "jian@juba2017" #hard-coded
username = "jian" #hard-coded
transport.connect(username = username, password = password)
sftp = paramiko.SFTPClient.from_transport(transport)


# In[27]:


remote_folder="/mnt/drv5/lr_biglots_data/Audience_NoShopping/output_"+str(last_sturday)+"/"

try:
    sftp.listdir(remote_folder)
except:
    sftp.mkdir(remote_folder)


# In[28]:


local_file_list=glob.glob(writer_folder_week+"*.csv")
local_file_list=[x for x in local_file_list if "BL_df_all_uploaded_audience" not in x]
for local_file in local_file_list:
    sftp.put(local_file,remote_folder+os.path.basename(local_file))
sftp.close()
transport.close()


print("transfered to 64: "+str(datetime.datetime.now()))
logging.info("transfered to 64: "+str(datetime.datetime.now()))


# In[29]:


local_file_list


# In[ ]:




