#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pytz
import datetime
import pandas as pd
import glob
import os
import numpy as np
import sqlalchemy
import gc
import logging

logging.basicConfig(filename='/home/jian/Projects/Big_Lots/Analysis/2019_Q4/Predictive_Model_Building/save_dcm_lr_to_mysql/Activity_Table_Initiation_BL_id_only_log.log', level=logging.INFO)
print(datetime.datetime.now())
logging.info("Start: "+str(datetime.datetime.now()))
BL_SQL_CONNECTION= 'mysql+pymysql://jian:JubaPlus-2017@localhost/BigLots' 
BL_engine = sqlalchemy.create_engine(
        BL_SQL_CONNECTION, 
        pool_recycle=1800
    )


# In[2]:


# test the timezone change
'''
est = pytz.timezone('US/Eastern')
utc = pytz.utc
fmt = '%Y-%m-%d %H:%M:%S'
fmt_date = '%Y-%m-%d'
winter = datetime.datetime(2016, 1, 24, 4, 30, 0, tzinfo=utc)
summer = datetime.datetime(2016, 7, 24, 4, 30, 0, tzinfo=utc)

print(winter.date())
print(str(winter.astimezone(est).strftime(fmt_date)))

print(summer.date())
print(str(summer.astimezone(est).strftime(fmt_date)))
'''


# # Copy new mapping

# In[3]:


import paramiko

host = "64.237.51.251" #hard-coded
port = 22
transport = paramiko.Transport((host, port))

password = "jian@juba2017" #hard-coded
username = "jian" #hard-coded
transport.connect(username = username, password = password)
sftp = paramiko.SFTPClient.from_transport(transport)

remote_mapping_list=sftp.listdir("/mnt/drv5/lr_biglots_data/download_logs/others/")
remote_mapping_list.sort()


# In[4]:


remote_mapping_list_1=[x for x in remote_mapping_list if "allrewards" in x]
remote_mapping_list_1_0908=[x for x in remote_mapping_list_1 if "0908" in x]
remote_mapping_list_1_0908=remote_mapping_list_1_0908[-1]

remote_mapping_list_1_1011=[x for x in remote_mapping_list_1 if "1011" in x]
remote_mapping_list_1_1011=remote_mapping_list_1_1011[-1]

remote_mapping_list_1=[remote_mapping_list_1_0908,remote_mapping_list_1_1011]
remote_mapping_list_1


# In[5]:


remote_mapping_list_2=[x for x in remote_mapping_list if "mapping" in x]
remote_mapping_list_2.sort()
remote_mapping_list_2


# In[6]:


remote_mapping_list_2.pop(0)
remote_mapping_list_2


# In[7]:


remote_mapping_list=remote_mapping_list_1+remote_mapping_list_2
local_mapping_existing_list=[os.path.basename(y) for y in glob.glob("/home/jian/Projects/Big_Lots/Analysis/2019_Q4/Predictive_Model_Building/save_dcm_lr_to_mysql/*.gz")]
remote_mapping_list=[x for x in remote_mapping_list if x not in local_mapping_existing_list]
remote_mapping_list


# In[8]:


for file in remote_mapping_list:
    remote_path="/mnt/drv5/lr_biglots_data/download_logs/others/"+file
    local_path="/home/jian/Projects/Big_Lots/Analysis/2019_Q4/Predictive_Model_Building/save_dcm_lr_to_mysql/"+file
    sftp.get(remote_path,local_path)
    print("New mapping copied to the local as: "+str(local_path))
    logging.info("New mapping copied to the local as: "+str(local_path))
sftp.close()
transport.close()


# In[9]:


list_local_mapping=glob.glob("/home/jian/Projects/Big_Lots/Analysis/2019_Q4/Predictive_Model_Building/save_dcm_lr_to_mysql/*.psv.gz")
df_all_mapping_file=pd.DataFrame({"file_path":list_local_mapping})
df_all_mapping_file['mapping_up_to_date']=df_all_mapping_file['file_path'].apply(lambda x: x.split("_")[-3])
df_all_mapping_fil=df_all_mapping_file.sort_values("mapping_up_to_date")
df_all_mapping_fil=df_all_mapping_fil.reset_index()
del df_all_mapping_fil['index']
# Ascending


# In[10]:


df_all_mapping_file['file_path'].apply(lambda x: os.path.basename(x)).unique().tolist()


# ## Update the all time LR IDL_BL_id mapping

# In[11]:


# In other jupyter code


# In[12]:


mapping_IDL_BLid=pd.read_csv("/home/jian/Projects/Big_Lots/Analysis/2019_Q4/Predictive_Model_Building/save_dcm_lr_to_mysql/df_total_mapping_IDL_BLid_JL_2020-03-02.csv",
                            dtype=str)

print(mapping_IDL_BLid.shape)
print(mapping_IDL_BLid['Customer_Link'].nunique())
print(mapping_IDL_BLid['customer_id_hashed'].nunique())

logging.info("mapping_IDL_BLid.shape: "+str(mapping_IDL_BLid.shape))
logging.info("mapping_IDL_BLid['Customer_Link'].nunique(): "+str(mapping_IDL_BLid['Customer_Link'].nunique()))
logging.info("mapping_IDL_BLid['customer_id_hashed'].nunique(): "+str(mapping_IDL_BLid['customer_id_hashed'].nunique()))

mapping_IDL_BLid.head(10)


# # Load Mapping File

# In[13]:


mapping_file_uid_idl=pd.read_csv("/home/jian/Projects/Big_Lots/Analysis/2019_Q4/Predictive_Model_Building/UID_IDL_mapping/BL_GoogleID_IDL_mapping_20180524_20191231_JL_2020-02-23.csv",
                                nrows=None,dtype=str)
mapping_file_uid_idl=mapping_file_uid_idl.rename(columns={"file_date":"valid_since_dt"})

logging.info("mapping_file_uid_idl['valid_since_dt'].min(): "+str(mapping_file_uid_idl['valid_since_dt'].min()))
logging.info("mapping_file_uid_idl['valid_since_dt'].max(): "+str(mapping_file_uid_idl['valid_since_dt'].max()))
logging.info("mapping_IDL_BLid['customer_id_hashed'].nunique(): "+str(mapping_IDL_BLid['customer_id_hashed'].nunique()))


print(mapping_file_uid_idl['valid_since_dt'].min(),mapping_file_uid_idl['valid_since_dt'].max())
mapping_file_uid_idl.head(4)


# In[14]:


mapping_file_uid_idl.shape,mapping_file_uid_idl['User ID'].nunique(),mapping_file_uid_idl['Customer_Link'].nunique()


# # Activity table creating

# In[15]:


list_mapping_files=glob.glob("/home/jian/Projects/Big_Lots/Analysis/2019_Q4/Predictive_Model_Building/save_dcm_lr_to_mysql/*.psv.gz")
list_mapping_files=sorted(list_mapping_files,key = lambda x: x.split("_")[-3])
list_mapping_files


# In[16]:


# list_files_lr_returned_act=glob.glob("/home/jian/Projects/Big_Lots/Analysis/2019_Q4/Predictive_Model_Building/LR_returned_logs_BL/activities/*.gz")
list_dcm_logs_act=glob.glob("/home/jian/Projects/Big_Lots/Analysis/2019_Q4/Predictive_Model_Building/DCM_raw_logs_BL/activities/*.tsv")
df_log_files_by_date=pd.DataFrame({"file_path":list_dcm_logs_act})
df_log_files_by_date['date']=df_log_files_by_date['file_path'].apply(lambda x: os.path.basename(x).split("utc_")[1][:8])
df_log_files_by_date['month']=df_log_files_by_date['date'].apply(lambda x: x[:6])


# In[17]:


# Create new table

import pymysql.cursors
engine_pymysql_db_BL = pymysql.connect(host='localhost',user='jian',
                         password='JubaPlus-2017',db='BigLots',
                         charset='utf8mb4',cursorclass=pymysql.cursors.DictCursor)

def create_Pred_act_table_BL_id():
    with engine_pymysql_db_BL.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS Pred_Activity_BL_id")
        cur.execute("CREATE TABLE Pred_Activity_BL_id(Event_Time bigint, date_utc date, time_utc time, User_ID varchar(64), Customer_Link varchar(128), customer_id_hashed varchar(64), Activity_ID varchar(16), url varchar(2048), search_term varchar(256), session_sequence int, activity_sequence int);")
    print("An empty TABLE Pred_Activity_BL_id has been created.",datetime.datetime.now())
    logging.info("An empty TABLE Pred_Activity_BL_id has been created: "+str(datetime.datetime.now()))

 
 
create_Pred_act_table_BL_id()

##########
'''
def create_Pred_act_table_GU_id():
    with engine_pymysql_db_BL.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS Pred_Activity_GU_id")
        cur.execute("CREATE TABLE Pred_Activity_GU_id\
(Event_Time bigint, \
date_utc date, \
time_utc time, \
User_ID varchar(64), \
Customer_Link varchar(128), \
customer_id_hashed varchar(64), \
Activity_ID varchar(16), \
url varchar(2048), \
search_term varchar(256), \
session_sequence int, \
activity_sequence int\
);")
    print("An empty TABLE Pred_Activity_GU_id has been created.",datetime.datetime.now())
    logging.info("An empty TABLE Pred_Activity_GU_id has been created: "+str(datetime.datetime.now()))

create_Pred_act_table_GU_id()
'''


# In[18]:


from urllib.parse import urlparse
# max_term_length=256
def get_url_and_kwd(url):
    res = urlparse(url)
    if url.startswith('u2=') or url.startswith('~oref='):
        term_detail='not defined'
        return term_detail

    elif 'ntt=' in res.query:
        term_detail=res.query.split('ntt=')[-1].split('&')[0]
    elif '_/n-' in url:
        term_detail=url.split('/c/')[-1].split('?')[0]
    else:
        term_detail='not defined'
        return term_detail
        
    if '?ntt' in url:
        term_detail=term_detail.replace('+',' ').replace('=',' ').replace('-',' ').strip()
        if len(term_detail)>=256:
            return "term_parsed_but_longer_than_256"
        return term_detail
    
    else:
        if '_/n-' in url:
            term_detail=term_detail[:-14]
        term_detail=term_detail.replace("_","")
        if not term_detail:
            term_detail="/"
        elif term_detail[-1]!="/":
            term_detail=term_detail+"/"
            
        term_detail=term_detail.split('/')[-2]
        term_detail=term_detail.replace('+',' ').replace('=',' ').replace('-',' ').strip()
        if len(term_detail)>=256:
            return "term_parsed_but_longer_than_256"
        return term_detail


# In[19]:


def count_sessions(df_input,id_col,df_previous_session_count,session_len=1800):
    df=df_input[['Event Time',id_col]].drop_duplicates()
    df=df.sort_values([id_col,'Event Time'])
    df=df.reset_index()
    del df['index']
    df=df.reset_index()

    df_shift=df.copy()
    df_shift['index']=df_shift['index']+1
    df_shift['index']=df_shift['index'].astype(int)
    df_shift=df_shift.rename(columns={"Event Time":"shift_time"})

    df_merge=pd.merge(df,df_shift,on=[id_col,"index"],how="left")
    df_merge['diff']=df_merge['Event Time']-df_merge['shift_time']
    df_1=df_merge[pd.isnull(df_merge['diff'])]
    df_2=df_merge[pd.notnull(df_merge['diff'])]
    df_2=df_2[df_2['diff']>=session_len*10**6]
    df_ind=df_1.append(df_2).sort_values([id_col,"Event Time"]).reset_index()
    del df_ind['index']
    df_ind['seq_in_month']=df_ind.groupby(id_col).cumcount()
    df_ind=df_ind[['Event Time',id_col,'seq_in_month']]
    df_output=pd.merge(df_input,df_ind,on=['Event Time',id_col],how="left")
    df_output=df_output.sort_values([id_col,'Event Time'])
    df_output['seq_in_month']=df_output['seq_in_month'].fillna(method="ffill")
    df_output['seq_in_month']=df_output['seq_in_month'].astype(int)+1
    df_output=pd.merge(df_output,df_previous_session_count,on=id_col,how="left")
    df_output['session_sequence']=df_output['session_sequence'].fillna(0)
    df_output['session_sequence']=df_output['session_sequence']+df_output['seq_in_month']
    
    df_output['date_time']=pd.to_datetime(df_output['Event Time'],unit="us")
    df_output['date_utc']=df_output['date_time'].dt.date
    df_output['time_utc']=df_output['date_time'].dt.time    
    del df_output['date_time']
    df_output=df_output.sort_values(['Event Time',id_col])
    
    return df_output


# In[ ]:


# df_previous_session_count_GUID=pd.DataFrame(columns=["User ID","session_sequence"])
df_previous_session_count_BLID=pd.DataFrame(columns=["customer_id_hashed","session_sequence"])

for month, df_file_month in df_log_files_by_date.groupby("month"):
    list_files=df_file_month['file_path'].tolist()
    df_month_act=pd.DataFrame()
    for file in list_files:
        date_file=file.split("_utc_")[1][:8] #str
        df=pd.read_csv(file,dtype=str,sep="\t",usecols=['Event Time','User ID','Other Data','Activity ID'])
        df['Other Data']=df['Other Data'].astype(str)
        df=df[df['Event Time'].str.isdigit()]
        df['Event Time']=df['Event Time'].astype(int)
        
        list_uid_day=df['User ID'].unique().tolist()
        
        df['url']=df['Other Data'].apply(lambda x: x.split('u1=')[-1].split(';')[0])
        df['url']=df['url'].astype(str)
        df['search_term'] =df['url'].apply(get_url_and_kwd)
        df['url']=df['url'].str.slice(stop=2048)
        
        # match in idl
        # ago(later>earlier)>after(earlier>later)
        df_day_mapping_uid_idl=mapping_file_uid_idl[mapping_file_uid_idl['User ID'].isin(list_uid_day)]
        
        df_day_mapping_uid_idl_ago=df_day_mapping_uid_idl[df_day_mapping_uid_idl['valid_since_dt']<=date_file] #8-digit-str
        df_day_mapping_uid_idl_after=df_day_mapping_uid_idl[df_day_mapping_uid_idl['valid_since_dt']>date_file] #8-digit-str
        
        df_day_mapping_uid_idl_ago=df_day_mapping_uid_idl_ago.sort_values(["User ID","valid_since_dt"],ascending=[True,False]).drop_duplicates("User ID")
        df_day_mapping_uid_idl_after=df_day_mapping_uid_idl_after.sort_values(["User ID","valid_since_dt"],ascending=[True,True]).drop_duplicates("User ID")
        df_day_mapping_uid_idl=df_day_mapping_uid_idl_ago.append(df_day_mapping_uid_idl_after).drop_duplicates("User ID")
        del df_day_mapping_uid_idl['valid_since_dt']
        df=pd.merge(df,df_day_mapping_uid_idl,how="left",on="User ID")
        # match in BL id
        list_idl_day=df['Customer_Link'].unique().tolist()
        df_day_mapping_IDL_BLid=mapping_IDL_BLid[mapping_IDL_BLid['Customer_Link'].isin(list_idl_day)]
        
        df_day_mapping_idl_blid_ago=df_day_mapping_IDL_BLid[df_day_mapping_IDL_BLid['date_up_to']<date_file] #8-digit-str
        df_day_mapping_idl_blid_after=df_day_mapping_IDL_BLid[df_day_mapping_IDL_BLid['date_up_to']>=date_file] #8-digit-str
        
        df_day_mapping_idl_blid_ago=df_day_mapping_idl_blid_ago.sort_values(["Customer_Link",'date_up_to'],ascending=[True,False]).drop_duplicates("Customer_Link")
        df_day_mapping_idl_blid_after=df_day_mapping_idl_blid_after.sort_values(["Customer_Link",'date_up_to'],ascending=[True,True]).drop_duplicates("Customer_Link")
        df_day_mapping_IDL_BLid=df_day_mapping_idl_blid_after.append(df_day_mapping_idl_blid_ago).drop_duplicates("Customer_Link")
        del df_day_mapping_IDL_BLid['date_up_to']
        df=pd.merge(df,df_day_mapping_IDL_BLid,how="left",on="Customer_Link")
        
        #loop in above
        df_month_act=df_month_act.append(df)
        del df_month_act['Other Data'] 
        
        # print(os.path.basename(file).split("utc_")[1][:8],datetime.datetime.now())
    print(df_month_act.shape)
    logging.info("df_month_act.shape: "+str(df_month_act.shape))

    del df
    
    del df_day_mapping_uid_idl_ago
    del df_day_mapping_uid_idl_after
    
    del df_day_mapping_idl_blid_ago
    del df_day_mapping_idl_blid_after
    
    df_month_act_BLID=df_month_act[pd.notnull(df_month_act['customer_id_hashed'])]
    df_month_act_GUID=df_month_act[pd.isnull(df_month_act['customer_id_hashed'])]
    print(datetime.datetime.now(),month,df_month_act_BLID.shape,df_month_act_BLID.shape,np.round(df_month_act_BLID.shape[0]/df_month_act.shape[0],4))
    logging.info(str(datetime.datetime.now())+" month: "+str(month))
    logging.info("df_month_act_BLID.shape"+str(df_month_act_BLID.shape))
    logging.info(str(np.round(df_month_act_BLID.shape[0]/df_month_act.shape[0],4)))
    '''
    df_month_act_GUID=count_sessions(df_month_act_GUID,"User ID",df_previous_session_count_GUID)
    df_month_act_GUID['activity_sequence']=df_month_act_GUID.groupby(["User ID","session_sequence"]).cumcount()+1
    df_month_act_GUID=df_month_act_GUID[['Event Time','date_utc','time_utc',
                                 'User ID','Customer_Link','customer_id_hashed',
                                 'Activity ID','url','search_term',
                                'session_sequence','activity_sequence']]
    df_previous_session_count_GUID=df_month_act_GUID[['User ID',"session_sequence"]].drop_duplicates().sort_values("session_sequence",ascending=False).drop_duplicates("User ID").append(df_previous_session_count_GUID).drop_duplicates("User ID")
    for col in df_month_act_GUID.columns.tolist():
        df_month_act_GUID=df_month_act_GUID.rename(columns={col:col.replace(" ","_")})
    df_month_act_GUID.to_csv("/home/jian/Projects/Big_Lots/Analysis/2019_Q4/Predictive_Model_Building/save_dcm_lr_to_mysql/actative_table_output/BL_act_table_Google_id_"+str(month)+".csv",index=False)
    print(datetime.datetime.now(),"File wrote of df_month_act_GUID")
    logging.info(str(datetime.datetime.now())+": File wrote of df_month_act_GUID")
    
    df_month_act_GUID.to_sql("Pred_Activity_GU_id",if_exists='append', con=BL_engine, index=False,chunksize=300000,
                    dtype={
                        'Event_Time':sqlalchemy.types.BigInteger(),
                        'date_utc':sqlalchemy.types.Date(),
                        'time_utc':sqlalchemy.types.Time(),
                        'User_ID':sqlalchemy.types.VARCHAR(length=64),
                        'Customer_Link':sqlalchemy.types.VARCHAR(length=64),
                        'customer_id_hashed':sqlalchemy.types.VARCHAR(length=64),
                        'Activity_ID':sqlalchemy.types.VARCHAR(length=16),
                        'url':sqlalchemy.types.VARCHAR(length=2048),
                        'search_term':sqlalchemy.types.VARCHAR(length=256),
                        'session_sequence':sqlalchemy.types.INTEGER(),
                        'activity_sequence':sqlalchemy.types.INTEGER()
                    })
    print(datetime.datetime.now(),"File wrote to MySQL df_month_act_GUID")
    logging.info(str(datetime.datetime.now())+": File wrote to MySQL df_month_act_GUID")
    '''

    df_month_act_BLID=count_sessions(df_month_act_BLID,"customer_id_hashed",df_previous_session_count_BLID)
    df_month_act_BLID['activity_sequence']=df_month_act_BLID.groupby(["customer_id_hashed","session_sequence"]).cumcount()+1
    df_month_act_BLID=df_month_act_BLID[['Event Time','date_utc','time_utc',
                                     'User ID','Customer_Link','customer_id_hashed',
                                     'Activity ID','url','search_term',
                                    'session_sequence','activity_sequence']]
    df_previous_session_count_BLID=df_month_act_BLID[['customer_id_hashed',"session_sequence"]].drop_duplicates().sort_values("session_sequence",ascending=False).drop_duplicates("customer_id_hashed").append(df_previous_session_count_BLID).drop_duplicates("customer_id_hashed")
    for col in df_month_act_BLID.columns.tolist():
        df_month_act_BLID=df_month_act_BLID.rename(columns={col:col.replace(" ","_")})
    df_month_act_BLID.to_csv("/home/jian/Projects/Big_Lots/Analysis/2019_Q4/Predictive_Model_Building/save_dcm_lr_to_mysql/actative_table_output/BL_act_table_BL_id_"+str(month)+".csv",index=False)
    print(datetime.datetime.now(),"File wrote of df_month_act_BLID")
    logging.info(str(datetime.datetime.now())+": File wrote of df_month_act_BLID")
    df_month_act_BLID.to_sql("Pred_Activity_BL_id",if_exists='append', con=BL_engine, index=False,chunksize=300000,
                    dtype={
                        'Event_Time':sqlalchemy.types.BigInteger(),
                        'date_utc':sqlalchemy.types.Date(),
                        'time_utc':sqlalchemy.types.Time(),
                        'User_ID':sqlalchemy.types.VARCHAR(length=64),
                        'Customer_Link':sqlalchemy.types.VARCHAR(length=64),
                        'customer_id_hashed':sqlalchemy.types.VARCHAR(length=64),
                        'Activity_ID':sqlalchemy.types.VARCHAR(length=16),
                        'url':sqlalchemy.types.VARCHAR(length=2048),
                        'search_term':sqlalchemy.types.VARCHAR(length=256),
                        'session_sequence':sqlalchemy.types.INTEGER(),
                        'activity_sequence':sqlalchemy.types.INTEGER()
                    })
    print(datetime.datetime.now(),"File wrote to MySQL df_month_act_BLID")
    logging.info(str(datetime.datetime.now())+": File wrote to MySQL df_month_act_BLID")
    
    
    print(datetime.datetime.now(),"Done of the month: ",month)
    logging.info(str(datetime.datetime.now())+"Done of the month: "+str(month))

    del df_month_act


gc.collect()


# In[ ]:


# df_previous_session_count_GUID.to_csv("./df_previous_session_count_GUID_up_to_"+str(date_file)+".csv",index=False)
df_previous_session_count_BLID.to_csv("./df_previous_session_count_BLID_up_to_"+str(date_file)+".csv",index=False)

print("previous_session_count saved", datetime.datetime.now())
logging.info("previous_session_count saved: "+str(datetime.datetime.now()))


# In[ ]:




