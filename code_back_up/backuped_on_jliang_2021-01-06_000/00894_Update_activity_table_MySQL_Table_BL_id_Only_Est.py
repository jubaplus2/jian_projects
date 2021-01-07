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


logging.basicConfig(filename='/home/jian/Projects/Big_Lots/Predictive_Model/MySQL_table_code/Update_Activity_table_per_month_BL_id_only_log.log', level=logging.INFO)

BL_SQL_CONNECTION= 'mysql+pymysql://jian:JubaPlus-2017@localhost/BigLots' 
BL_engine = sqlalchemy.create_engine(
        BL_SQL_CONNECTION, 
        pool_recycle=1800
    )


# In[2]:


list_local_mapping=glob.glob("/home/jian/Projects/Big_Lots/Live_Ramp/Mapping_Files/all_LR_returned/*.psv.gz")
df_all_mapping_file=pd.DataFrame({"file_path":list_local_mapping})
df_all_mapping_file['mapping_up_to_date']=df_all_mapping_file['file_path'].apply(lambda x: x.split("_")[-3])
df_all_mapping_fil=df_all_mapping_file.sort_values("mapping_up_to_date")
df_all_mapping_fil=df_all_mapping_fil.reset_index()
del df_all_mapping_fil['index']
# Ascending


# In[3]:


df_all_mapping_fil


# In[6]:


mapping_IDL_BLid=pd.read_csv("/home/jian/Projects/Big_Lots/Predictive_Model/LR_Mapping_File/IDL_BLid/df_total_mapping_IDL_BLid_JL_2020-06-29.csv",
                            dtype=str)
shape=mapping_IDL_BLid.shape
Customer_Link_nunique=mapping_IDL_BLid['Customer_Link'].nunique()
customer_id_hashed_nunique=mapping_IDL_BLid['customer_id_hashed'].nunique()
print("mapping_IDL_BLid.shape",shape)
print("mapping_IDL_BLid['Customer_Link'].nunique()",Customer_Link_nunique)
print("mapping_IDL_BLid['customer_id_hashed'].nunique()",customer_id_hashed_nunique)

logging.info("mapping_IDL_BLid.shape: %s"%str(shape))
logging.info("mapping_IDL_BLid['Customer_Link'].nunique(): %s"%str(Customer_Link_nunique))
logging.info("mapping_IDL_BLid['customer_id_hashed'].nunique(): %s"%str(customer_id_hashed_nunique))

mapping_IDL_BLid.head(10)


# In[7]:


# Use the latest on per customer_id since it's the update for the last quarter
mapping_IDL_BLid=mapping_IDL_BLid.sort_values(['customer_id_hashed','date_up_to'],ascending=[True,False]).drop_duplicates("customer_id_hashed")
shape=mapping_IDL_BLid.shape
print("mapping_IDL_BLid.shape",shape)
logging.info("mapping_IDL_BLid.shape: %s"%str(shape))


# In[15]:


# Using the impression only
mapping_file_uid_idl_1=pd.read_csv("/home/jian/Projects/Big_Lots/Predictive_Model/LR_Mapping_File/GID_IDL/BL_GoogleID_IDL_mapping_20180524_20191231_JL_2020-02-23.csv",
                                nrows=None,dtype=str)
mapping_file_uid_idl_2=pd.read_csv("/home/jian/Projects/Big_Lots/Predictive_Model/LR_Mapping_File/GID_IDL/BL_GoogleID_IDL_mapping_based_on_imrp_20200101_20200502_JL_2020-05-27.csv",
                                nrows=None,dtype=str)
mapping_file_uid_idl=mapping_file_uid_idl_1.append(mapping_file_uid_idl_2)
del mapping_file_uid_idl_1
del mapping_file_uid_idl_2
gc.collect()

mapping_file_uid_idl=mapping_file_uid_idl.rename(columns={"file_date":"valid_since_dt"})

valid_since_dt_min=mapping_file_uid_idl['valid_since_dt'].min()
valid_since_dt_max=mapping_file_uid_idl['valid_since_dt'].max()
customer_id_hashed_nunique=mapping_IDL_BLid['customer_id_hashed'].nunique()
logging.info("mapping_file_uid_idl['valid_since_dt'].min(): %s"%str(valid_since_dt_min))
logging.info("mapping_file_uid_idl['valid_since_dt'].max(): %s"%str(valid_since_dt_max))
logging.info("mapping_IDL_BLid['customer_id_hashed'].nunique(): %s"%str(customer_id_hashed_nunique))
print("mapping_file_uid_idl['valid_since_dt'].min(),mapping_file_uid_idl['valid_since_dt'].max(): %s, %s"%(str(valid_since_dt_min),str(valid_since_dt_max)))
logging.info("mapping_file_uid_idl['valid_since_dt'].min(),mapping_file_uid_idl['valid_since_dt'].max(): %s, %s"%(str(valid_since_dt_min),str(valid_since_dt_max)))

mapping_file_uid_idl.head(4)


# In[16]:


gc.collect()


# In[26]:


del shape
del Customer_Link_nunique
del customer_id_hashed_nunique
del valid_since_dt_min
del valid_since_dt_max
gc.collect()


# # Activities

# In[17]:


list_dcm_logs_act=glob.glob("/home/jian/Projects/Big_Lots/Predictive_Model/DCM_files_by_day/activities/*.tsv")
df_log_files_by_date=pd.DataFrame({"file_path":list_dcm_logs_act})
df_log_files_by_date['date']=df_log_files_by_date['file_path'].apply(lambda x: os.path.basename(x).split("utc_")[1][:8])
df_log_files_by_date['month']=df_log_files_by_date['date'].apply(lambda x: x[:6])


# In[18]:


df_log_files_by_date


# In[19]:


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


# In[20]:


import pytz

est = pytz.timezone('US/Eastern')

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
    
    df_output['date_time']=pd.to_datetime(df_output['Event Time'],unit="us",utc=True)
    df_output['date_time']=df_output['date_time'].apply(lambda x: x.astimezone(est)) # converted to Estern Time
    df_output['date_est']=df_output['date_time'].dt.date
    df_output['time_est']=df_output['date_time'].dt.time    
    del df_output['date_time']
    del df_output['seq_in_month']
    df_output=df_output.sort_values(['Event Time',id_col])
    
    return df_output


# In[21]:


max_date_utc_in_sql_BL=pd.read_sql("select max(date_est) from Pred_ExpV2_Activity_BL_id",con=BL_engine)
max_date_utc_in_sql_BL=max_date_utc_in_sql_BL['max(date_est)'].tolist()[0]
print(max_date_utc_in_sql_BL)


# In[22]:


df_log_files_by_date_remaining=df_log_files_by_date[df_log_files_by_date['month']>str(max_date_utc_in_sql_BL).replace("-","")[:6]]
print(df_log_files_by_date_remaining.shape)


# In[23]:


df_log_files_by_date_remaining=df_log_files_by_date_remaining[df_log_files_by_date_remaining['month']<"202005"]
print(df_log_files_by_date_remaining.shape)


# In[24]:


gc.collect()


# In[ ]:


print(datetime.datetime.now())
df_previous_session_count_BLID=pd.read_sql("select customer_id_hashed, max(session_sequence) as session_sequence from Pred_ExpV2_Activity_BL_id group by customer_id_hashed",con=BL_engine)
print(datetime.datetime.now(),df_previous_session_count_BLID.shape)


# In[ ]:


gc.collect()


# In[ ]:


for month, df_file_month in df_log_files_by_date_remaining.groupby("month"):
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
        print(1,datetime.datetime.now())
        df_day_mapping_uid_idl_ago=df_day_mapping_uid_idl_ago.sort_values(["User ID","valid_since_dt"],ascending=[True,False]).drop_duplicates("User ID")
        print(2,datetime.datetime.now())
        df_day_mapping_uid_idl_after=df_day_mapping_uid_idl_after.sort_values(["User ID","valid_since_dt"],ascending=[True,True]).drop_duplicates("User ID")
        print(3,datetime.datetime.now())
        df_day_mapping_uid_idl=df_day_mapping_uid_idl_ago.append(df_day_mapping_uid_idl_after).drop_duplicates("User ID")
        del df_day_mapping_uid_idl['valid_since_dt']
        df=pd.merge(df,df_day_mapping_uid_idl,how="left",on="User ID")
        print(4,datetime.datetime.now())
        # match in BL id
        list_idl_day=df['Customer_Link'].unique().tolist()
        df_day_mapping_IDL_BLid=mapping_IDL_BLid[mapping_IDL_BLid['Customer_Link'].isin(list_idl_day)]
        print(5,datetime.datetime.now())
        df_day_mapping_idl_blid_ago=df_day_mapping_IDL_BLid[df_day_mapping_IDL_BLid['date_up_to']<date_file] #8-digit-str
        print(6,datetime.datetime.now())
        df_day_mapping_idl_blid_after=df_day_mapping_IDL_BLid[df_day_mapping_IDL_BLid['date_up_to']>=date_file] #8-digit-str
        print(7,datetime.datetime.now())
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
    gc.collect()
    
    df_month_act_BLID=df_month_act[pd.notnull(df_month_act['customer_id_hashed'])]
    # df_month_act_GUID=df_month_act[pd.isnull(df_month_act['customer_id_hashed'])]
    del df_month_act
    gc.collect()
    
    print(datetime.datetime.now(),month,df_month_act_BLID.shape,df_month_act_BLID.shape,np.round(df_month_act_BLID.shape[0]/df_month_act.shape[0],4))
    logging.info(str(datetime.datetime.now())+" month: "+str(month))
    logging.info("df_month_act_BLID.shape"+str(df_month_act_BLID.shape))
    logging.info(str(np.round(df_month_act_BLID.shape[0]/df_month_act.shape[0],4)))
    
    
    
    '''
    df_month_act_GUID=count_sessions(df_month_act_GUID,"User ID",df_previous_session_count_GUID)
    df_month_act_GUID['activity_sequence']=df_month_act_GUID.groupby(["User ID","session_sequence"]).cumcount()+1
    df_month_act_GUID=df_month_act_GUID[['Event Time','date_est','time_est',
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
                        'date_est':sqlalchemy.types.Date(),
                        'time_est':sqlalchemy.types.Time(),
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
    df_month_act_BLID=df_month_act_BLID[['Event Time','date_est','time_est',
                                     'User ID','Customer_Link','customer_id_hashed',
                                     'Activity ID','url','search_term',
                                    'session_sequence','activity_sequence']]
    df_previous_session_count_BLID=df_month_act_BLID[['customer_id_hashed',"session_sequence"]].drop_duplicates().sort_values("session_sequence",ascending=False).drop_duplicates("customer_id_hashed").append(df_previous_session_count_BLID).drop_duplicates("customer_id_hashed")
    for col in df_month_act_BLID.columns.tolist():
        df_month_act_BLID=df_month_act_BLID.rename(columns={col:col.replace(" ","_")})
    df_month_act_BLID.to_csv("/home/jian/Projects/Big_Lots/Analysis/2019_Q4/Predictive_Model_Building/save_dcm_lr_to_mysql/actative_table_output/BL_act_table_BL_id_"+str(month)+".csv",index=False)
    print(datetime.datetime.now(),"File wrote of df_month_act_BLID")
    logging.info(str(datetime.datetime.now())+": File wrote of df_month_act_BLID")
    df_month_act_BLID.to_sql("Pred_ExpV2_Activity_BL_id",if_exists='append', con=BL_engine, index=False,chunksize=300000,
                    dtype={
                        'Event_Time':sqlalchemy.types.BigInteger(),
                        'date_est':sqlalchemy.types.Date(),
                        'time_est':sqlalchemy.types.Time(),
                        'User_ID':sqlalchemy.types.VARCHAR(length=64),
                        'Customer_Link':sqlalchemy.types.VARCHAR(length=128),
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

    


gc.collect()


# In[ ]:




