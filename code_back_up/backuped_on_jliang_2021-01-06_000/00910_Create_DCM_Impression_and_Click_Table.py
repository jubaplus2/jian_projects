#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import datetime
import os
import numpy as np
import glob
import gc
import pytz
import sqlalchemy
import logging

logging.basicConfig(filename='/home/jian/Projects/Big_Lots/Analysis/2019_Q4/Predictive_Model_Building/save_dcm_lr_to_mysql/Exposure_Table_Initiation_log.log', level=logging.INFO)

BL_SQL_CONNECTION= 'mysql+pymysql://jian:JubaPlus-2017@localhost/BigLots' 
BL_engine = sqlalchemy.create_engine(
        BL_SQL_CONNECTION, 
        pool_recycle=1800
    )

print(datetime.datetime.now())
logging.info("Start: "+str(datetime.datetime.now()))
print(os.getcwd())
logging.info(str(os.getcwd()))


# In[2]:


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

# In[3]:


mapping_file_uid_idl=pd.read_csv("/home/jian/Projects/Big_Lots/Analysis/2019_Q4/Predictive_Model_Building/UID_IDL_mapping/BL_GoogleID_IDL_mapping_20180524_20191231_JL_2020-02-23.csv",
                                nrows=None,dtype=str)
mapping_file_uid_idl=mapping_file_uid_idl.rename(columns={"file_date":"valid_since_dt"})

logging.info("mapping_file_uid_idl['valid_since_dt'].min(): "+str(mapping_file_uid_idl['valid_since_dt'].min()))
logging.info("mapping_file_uid_idl['valid_since_dt'].max(): "+str(mapping_file_uid_idl['valid_since_dt'].max()))
logging.info("mapping_IDL_BLid['customer_id_hashed'].nunique(): "+str(mapping_IDL_BLid['customer_id_hashed'].nunique()))

print(mapping_file_uid_idl['valid_since_dt'].min(),mapping_file_uid_idl['valid_since_dt'].max())
logging.info(str(mapping_file_uid_idl['valid_since_dt'].min()),str(mapping_file_uid_idl['valid_since_dt'].max()))


mapping_file_uid_idl.head(4)


# In[4]:


mapping_file_uid_idl.shape,mapping_file_uid_idl['User ID'].nunique(),mapping_file_uid_idl['Customer_Link'].nunique()


# In[5]:


mapping_file_uid_idl['valid_since_dt'].max()


# In[6]:


mapping_IDL_BLid['date_up_to'].max()


# In[7]:


mapping_file_uid_idl['Customer_Link'].apply(len).unique()


# In[8]:


def BL_week_end_from_str(date_str,q1_start=datetime.date(2018,2,4)):
    # date_str: %Y%m%d
    date_=pd.to_datetime(date_str,format="%Y%m%d").date()
    weekday=date_.weekday()
    if weekday==6:
        week_end_dt=date_+datetime.timedelta(days=6)
    else:
        week_end_dt=date_+datetime.timedelta(days=5-weekday)
    days_diff=week_end_dt-q1_start
    return week_end_dt,int(days_diff.days/7)+1

def BL_week_end_from_date(date_input,q1_start=datetime.date(2018,2,4)):
    # date_input: %Y-%m-%d
    weekday=date_input.weekday()
    if weekday==6:
        week_end_dt=date_input+datetime.timedelta(days=6)
    else:
        week_end_dt=date_input+datetime.timedelta(days=5-weekday)
    # days_diff=date_input-q1_start
    return week_end_dt#,int(days_diff.days/7)+1


# In[9]:


list_dcm_logs_impr=glob.glob("/home/jian/Projects/Big_Lots/Analysis/2019_Q4/Predictive_Model_Building/DCM_raw_logs_BL/impressions/*.tsv")
df_impr_log_files_by_date=pd.DataFrame({"file_path":list_dcm_logs_impr})
df_impr_log_files_by_date['date']=df_impr_log_files_by_date['file_path'].apply(lambda x: os.path.basename(x).split("utc_")[1][:8])
df_impr_log_files_by_date['month']=df_impr_log_files_by_date['date'].apply(lambda x: x[:6])
df_impr_log_files_by_date['week_end_dt'],df_impr_log_files_by_date['week_count']=zip(*df_impr_log_files_by_date['date'].apply(BL_week_end_from_str))


list_dcm_logs_click=glob.glob("/home/jian/Projects/Big_Lots/Analysis/2019_Q4/Predictive_Model_Building/DCM_raw_logs_BL/clicks/*.tsv")
df_click_log_files_by_date=pd.DataFrame({"file_path":list_dcm_logs_click})
df_click_log_files_by_date['date']=df_click_log_files_by_date['file_path'].apply(lambda x: os.path.basename(x).split("utc_")[1][:8])
df_click_log_files_by_date['month']=df_click_log_files_by_date['date'].apply(lambda x: x[:6])
df_click_log_files_by_date['week_end_dt'],df_click_log_files_by_date['week_count']=zip(*df_click_log_files_by_date['date'].apply(BL_week_end_from_str))


# In[10]:


df_click_log_files_by_date['type']="click"
df_impr_log_files_by_date['type']="impression"
df_both_log_files_by_date=df_click_log_files_by_date.append(df_impr_log_files_by_date)


# In[13]:


import pymysql.cursors
engine_pymysql_db_BL = pymysql.connect(host='localhost',user='jian',
                         password='JubaPlus-2017',db='BigLots',
                         charset='utf8mb4',cursorclass=pymysql.cursors.DictCursor)

def create_Pred_Exposure_table_BL_id():
    with engine_pymysql_db_BL.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS Pred_Exposure_BL_id")
        cur.execute("CREATE TABLE Pred_Exposure_BL_id(User_ID varchar(64), Customer_Link varchar(128), customer_id_hashed varchar(64), date_est date, Site_ID varchar(16), impressions int, clicks int,  week_end_dt date );")
    print("An empty TABLE Pred_Exposure_BL_id has been created.",datetime.datetime.now())
    logging.info("An empty TABLE Pred_Exposure_BL_id has been created: "+str(datetime.datetime.now()))
create_Pred_Exposure_table_BL_id()


def create_Pred_Exposure_table_GU_id():
    with engine_pymysql_db_BL.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS Pred_Exposure_GU_id")
        cur.execute("CREATE TABLE Pred_Exposure_GU_id(User_ID varchar(64), Customer_Link varchar(128), customer_id_hashed varchar(64), date_est date, Site_ID varchar(16), impressions int, clicks int,  week_end_dt date );")
    print("An empty TABLE Pred_Exposure_GU_id has been created.",datetime.datetime.now())
    logging.info("An empty TABLE Pred_Exposure_GU_id has been created: "+str(datetime.datetime.now()))
create_Pred_Exposure_table_GU_id()


# In[ ]:


# rolling 1 week ago in tempary dataframe

df_click_log_files_by_date['type']="click"
df_impr_log_files_by_date['type']="impression"
df_both_log_files_by_date=df_click_log_files_by_date.append(df_impr_log_files_by_date)


est = pytz.timezone('US/Eastern')
df_BL_output_temp=pd.DataFrame()
df_GU_output_temp=pd.DataFrame()


week_end_1_week_ago=df_both_log_files_by_date['week_end_dt'].min()
i=0
# total_rows_impr=0
# total_rows_click=0
print("Start to iterate: "+str(datetime.datetime.now()))
logging.info("Start to iterate: "+str(datetime.datetime.now()))

for date_week,df_group_log_both_week in df_both_log_files_by_date.groupby(["date","week_end_dt"]):
    i+=1
    date_file=date_week[0]
    week_dateformat=date_week[1]

    filedate_dateformat=datetime.datetime.strptime(date_week[0],"%Y%m%d").date()
    
    if df_group_log_both_week['type'].tolist()!=['click','impression']:
        print(datetime.datetime.now(),"Error, the file path data frame of the day %s is not corret"%date_file)
        logging.info(str(datetime.datetime.now())+": Error, the file path data frame of the day %s is not corret"%date_file)
        break
    else:
        file_impr=df_group_log_both_week[df_group_log_both_week['type']=='impression']['file_path'].values[0]
        df_impr=pd.read_csv(file_impr,dtype=str,sep="\t").drop_duplicates() #20+ columns
        # total_rows_impr+=df_impr.shape[0]
        # print(date_file,datetime.datetime.now(),"total_rows_impr",total_rows_impr)
        
        df_impr=df_impr[["User ID","Event Time",'Site ID (DCM)','Impression ID']]
        df_impr['Event Time']=df_impr['Event Time'].astype(int)
        df_impr['datetime_utc']=pd.to_datetime(df_impr['Event Time'],unit="us",utc=True)
        df_impr['date_est']=df_impr['datetime_utc'].apply(lambda x: x.astimezone(est)).dt.date
        df_impr['Impression ID']=df_impr['Impression ID'].astype(str)
        df_impr=df_impr.groupby(["User ID","date_est",'Site ID (DCM)'])['Impression ID'].count().to_frame().reset_index().rename(columns={"Impression ID":"impressions"})
        
        file_click=df_group_log_both_week[df_group_log_both_week['type']=='click']['file_path'].values[0]
        df_click=pd.read_csv(file_click,dtype=str,sep="\t").drop_duplicates() #20+ columns
        # total_rows_click+=df_click.shape[0]
        # print(date_file,datetime.datetime.now(),"total_rows_click",total_rows_click)
        
        df_click=df_click[["User ID","Event Time",'Site ID (DCM)','Impression ID']]
        df_click['Event Time']=df_click['Event Time'].astype(int)
        df_click['datetime_utc']=pd.to_datetime(df_click['Event Time'],unit="us",utc=True)
        df_click['date_est']=df_click['datetime_utc'].apply(lambda x: x.astimezone(est)).dt.date
        df_click['Impression ID']=df_click['Impression ID'].astype(str)
        df_click=df_click.groupby(["User ID","date_est",'Site ID (DCM)'])['Impression ID'].count().to_frame().reset_index().rename(columns={"Impression ID":"clicks"})
        
        df=pd.merge(df_impr,df_click,on=["User ID","date_est",'Site ID (DCM)'],how="outer")
        df['impressions']=df['impressions'].fillna(0)
        df['clicks']=df['clicks'].fillna(0)
        
        ######
        list_uid_day=df['User ID'].unique().tolist()
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
        df['Customer_Link']=df['Customer_Link'].fillna("nan")
        df['customer_id_hashed']=df['customer_id_hashed'].fillna("nan")
        
        df=df.groupby(["User ID",'Customer_Link','customer_id_hashed',"date_est",'Site ID (DCM)'])['impressions','clicks'].sum().reset_index()
        df['week_end_dt_est']=df['date_est'].apply(BL_week_end_from_date)
        
        df_BL_id=df[df['customer_id_hashed']!='nan']
        df_GU_id=df[df['customer_id_hashed']=='nan']
        
        
        df_BL_output_temp=df_BL_output_temp.append(df_BL_id)
        df_GU_output_temp=df_GU_output_temp.append(df_GU_id)
    
    
        if filedate_dateformat==week_end_1_week_ago+datetime.timedelta(days=7):
            df_BL_to_sql=df_BL_output_temp[df_BL_output_temp['date_est']<=week_end_1_week_ago]
            df_BL_to_sql=df_BL_to_sql.groupby(["User ID",'Customer_Link','customer_id_hashed',"date_est",'Site ID (DCM)'])['impressions','clicks'].sum().reset_index()
            df_GU_to_sql=df_GU_output_temp[df_GU_output_temp['date_est']<=week_end_1_week_ago]
            df_GU_to_sql=df_GU_to_sql.groupby(["User ID",'Customer_Link','customer_id_hashed',"date_est",'Site ID (DCM)'])['impressions','clicks'].sum().reset_index()
            
            df_BL_output_temp=df_BL_output_temp[df_BL_output_temp['date_est']>week_end_1_week_ago]
            df_GU_output_temp=df_GU_output_temp[df_GU_output_temp['date_est']>week_end_1_week_ago]
            
            
            df_GU_to_sql['customer_id_hashed']=df_GU_to_sql['customer_id_hashed'].replace("nan",np.nan)
            df_GU_to_sql['Customer_Link']=df_GU_to_sql['Customer_Link'].replace("nan",np.nan)
            
            df_BL_to_sql['week_end_dt']=week_end_1_week_ago
            df_GU_to_sql['week_end_dt']=week_end_1_week_ago
            for col in df_BL_to_sql.columns.tolist():
                df_BL_to_sql=df_BL_to_sql.rename(columns={col:col.replace(" ","_").replace("_(DCM)","")})
            for col in df_GU_to_sql.columns.tolist():
                df_GU_to_sql=df_GU_to_sql.rename(columns={col:col.replace(" ","_").replace("_(DCM)","")})
            
            
            
            df_BL_to_sql.to_sql("Pred_Exposure_BL_id",if_exists='append', con=BL_engine, index=False,chunksize=300000,
                    dtype={
                        'User_ID':sqlalchemy.types.VARCHAR(length=64),
                        'Customer_Link':sqlalchemy.types.VARCHAR(length=128),
                        'customer_id_hashed':sqlalchemy.types.VARCHAR(length=64),
                        'date_est':sqlalchemy.types.Date(),
                        'Site_ID':sqlalchemy.types.VARCHAR(length=16),
                        'impressions':sqlalchemy.types.INTEGER(),
                        'clicks':sqlalchemy.types.INTEGER(),
                        'week_end_dt':sqlalchemy.types.Date()
                    })
            
            df_GU_to_sql.to_sql("Pred_Exposure_GU_id",if_exists='append', con=BL_engine, index=False,chunksize=300000,
                    dtype={
                        'User_ID':sqlalchemy.types.VARCHAR(length=64),
                        'Customer_Link':sqlalchemy.types.VARCHAR(length=128),
                        'customer_id_hashed':sqlalchemy.types.VARCHAR(length=64),
                        'date_est':sqlalchemy.types.Date(),
                        'Site_ID':sqlalchemy.types.VARCHAR(length=16),
                        'impressions':sqlalchemy.types.INTEGER(),
                        'clicks':sqlalchemy.types.INTEGER(),
                        'week_end_dt':sqlalchemy.types.Date()
                    }) 
            print(str(datetime.datetime.now())+" Table of this week is saved to SQL: " +str(week_end_1_week_ago))
            logging.info(str(datetime.datetime.now())+" Table of this week is saved to SQL: " +str(week_end_1_week_ago))
            
            week_end_1_week_ago=week_end_1_week_ago+datetime.timedelta(days=7)

print("Done",datetime.datetime.now())
    


# In[ ]:




