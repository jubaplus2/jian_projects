#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import datetime
import os
import gc
import logging

import sqlalchemy
BL_SQL_CONNECTION= 'mysql+pymysql://jian:JubaPlus-2017@localhost/BigLots' 
BL_engine = sqlalchemy.create_engine(
        BL_SQL_CONNECTION, 
        pool_recycle=1800
    )

logging.basicConfig(filename='/home/jian/BL_weekly_crontab/cron_4_JT_rolling_rewards_tracker/fiscal_year_run_JT_rewards_rolling_id_count_logs.log', level=logging.INFO)

datetime.datetime.now().date().weekday()

def recursive_file_gen(root_path):
    for root, dirs, files in os.walk(root_path):
        for file in files:
            yield os.path.join(root,file)
            
last_sturday = (datetime.datetime.now()-datetime.timedelta(days=(datetime.datetime.now().weekday()+2))).date()
last_sturday


# In[2]:


logging.info("last_sturday: "+str(last_sturday))

last_day_of_2018Q4=datetime.date(2019,2,2)

year_of_quarter=(last_sturday-last_day_of_2018Q4).days/(52*7)
year_of_quarter=str(int(2019+np.floor(year_of_quarter)))
print("Year",year_of_quarter)
logging.info("Year: "+str(year_of_quarter))


quarter_of_quarter=(last_sturday-last_day_of_2018Q4).days/7
quarter_of_quarter=np.floor(quarter_of_quarter/13)%4
quarter_of_quarter=str(int(1+quarter_of_quarter))
print("Quarter",quarter_of_quarter)
logging.info("Quarter: "+str(quarter_of_quarter))


str_current_quarter=year_of_quarter+"_Q"+quarter_of_quarter

print(str_current_quarter)
logging.info("str_current_quarter: "+str(str_current_quarter))

current_week=int((last_sturday-last_day_of_2018Q4).days/7%13)
print("current_week",current_week)
logging.info("current_week: "+str(current_week))

if current_week==0:
    quarter_of_quarter=int(quarter_of_quarter)-1
    str_current_quarter=year_of_quarter+"_Q"+str(quarter_of_quarter)
    current_week=13
    print("Quarter",quarter_of_quarter)
    print(str_current_quarter)
    print("current_week",current_week)
    logging.info("Quarter: "+str(quarter_of_quarter))
    logging.info("str_current_quarter: "+str(str_current_quarter))
    logging.info("current_week: "+str(current_week))

    
logging.info("quarter_of_quarter: "+str(quarter_of_quarter))


# In[3]:


current_quarter_beginning=last_day_of_2018Q4+datetime.timedelta(days=(int(year_of_quarter)-2019)*52*7+(int(quarter_of_quarter)-1)*13*7+1)

fiscal_year_diff=int(np.ceil((datetime.date(2020,2,2)-last_day_of_2018Q4).days/(52*7)))
print(fiscal_year_diff," years since 2018")
date_2018Q1_start=datetime.date(2018,2,4)
fiscal_year_start=date_2018Q1_start+datetime.timedelta(days=52*7*fiscal_year_diff)

print("current_quarter_beginning",current_quarter_beginning)
print("fiscal_year_start",fiscal_year_start)
logging.info("current_quarter_beginning: "+str(current_quarter_beginning))
logging.info("fiscal_year_start: "+str(fiscal_year_start))


# In[4]:


str_last_week_start="'"+str(last_sturday-datetime.timedelta(days=6))+"'"
print("str_last_week_start: "+str_last_week_start)
logging.info("str_last_week_start: "+str_last_week_start)
list_ids_in_week=pd.read_sql("select customer_id_hashed from BL_Rewards_Master where sign_up_date>=%s"%str_last_week_start ,con=BL_engine)
list_ids_in_week=list_ids_in_week.drop_duplicates()
print("len(list_ids_in_week): "+str(len(list_ids_in_week)))

###
str_quarter_start="'"+str(current_quarter_beginning)+"'"
print("str_quarter_start: "+str_quarter_start)
logging.info("str_quarter_start: "+str_quarter_start)
list_ids_in_quater=pd.read_sql("select customer_id_hashed from BL_Rewards_Master where sign_up_date>=%s"%str_quarter_start ,con=BL_engine)
print("len(list_ids_in_quater): "+str(len(list_ids_in_quater)))

###
str_FiscalYear_start="'"+str(fiscal_year_start)+"'"
print("str_FiscalYear_start: "+str_FiscalYear_start)
logging.info("str_FiscalYear_start: "+str_FiscalYear_start)
list_ids_in_year=pd.read_sql("select customer_id_hashed from BL_Rewards_Master where sign_up_date>=%s"%str_FiscalYear_start ,con=BL_engine)
print("len(list_ids_in_year): "+str(len(list_ids_in_year)))


# In[5]:


all_daily_sales_since_20Q1=list(recursive_file_gen("/home/jian/BigLots/"))
all_daily_sales_since_20Q1=[x for x in all_daily_sales_since_20Q1 if "aily" in x]
all_daily_sales_since_20Q1=[x for x in all_daily_sales_since_20Q1 if "MediaStorm_" in x]
all_daily_sales_since_20Q1=[x for x in all_daily_sales_since_20Q1 if x.split("MediaStorm_")[1][:10]>="2020-02-02"]


df_all_daily_sales_since_20Q1=pd.DataFrame({"file_path":all_daily_sales_since_20Q1})
df_all_daily_sales_since_20Q1['week_end_dt']=df_all_daily_sales_since_20Q1['file_path'].apply(lambda x: x.split("/MediaStorm_")[1][:10])
df_all_daily_sales_since_20Q1=df_all_daily_sales_since_20Q1.sort_values("week_end_dt",ascending=True)

df_all_daily_sales_since_20Q1.shape


# In[6]:


list_POS_daily_this_fiscal_year=df_all_daily_sales_since_20Q1[df_all_daily_sales_since_20Q1['week_end_dt']>=str(fiscal_year_start)]

df_shoppers_in_year=pd.DataFrame()

i=0
for ind, row in list_POS_daily_this_fiscal_year.iterrows():
    file_path=row['file_path']
    week_end_dt=row['week_end_dt']
    df=pd.read_csv(file_path,dtype=str,sep="|",
                   usecols=['customer_id_hashed']).drop_duplicates()
    df=df[pd.notnull(df['customer_id_hashed'])]
    
    df['week_end_dt']=week_end_dt
    df_shoppers_in_year=df_shoppers_in_year.append(df)
    i+=1
    if i%5==1:
        print(datetime.datetime.now(),i)
        
    


# In[7]:


count_new_sign_ups_fiscal_year=list_ids_in_year['customer_id_hashed'].nunique()

count_shoppers_fiscal_year=df_shoppers_in_year['customer_id_hashed'].nunique()

count_new_shoppers_fiscal_year=df_shoppers_in_year[['customer_id_hashed']].drop_duplicates()
count_new_shoppers_fiscal_year=pd.merge(list_ids_in_year,count_new_shoppers_fiscal_year,on="customer_id_hashed",how="inner")
count_new_shoppers_fiscal_year=count_new_shoppers_fiscal_year['customer_id_hashed'].nunique()

print("count_new_sign_ups_fiscal_year: "+str(count_new_sign_ups_fiscal_year))
logging.info("count_new_sign_ups_fiscal_year: "+str(count_new_sign_ups_fiscal_year))

print("count_shoppers_fiscal_year: "+str(count_shoppers_fiscal_year))
logging.info("count_shoppers_fiscal_year: "+str(count_shoppers_fiscal_year))

print("count_new_shoppers_fiscal_year: "+str(count_new_shoppers_fiscal_year))
logging.info("count_new_shoppers_fiscal_year: "+str(count_new_shoppers_fiscal_year))


# In[8]:


# over-wroten in as quarter only

count_new_sign_ups_current_quarter=list_ids_in_quater['customer_id_hashed'].nunique()

df_shoppers_in_year=df_shoppers_in_year[df_shoppers_in_year['week_end_dt']>=str(current_quarter_beginning)]
count_shoppers_current_quarter=df_shoppers_in_year['customer_id_hashed'].nunique()

count_new_shoppers_current_quater=df_shoppers_in_year[['customer_id_hashed']].drop_duplicates()
count_new_shoppers_current_quater=pd.merge(list_ids_in_quater,count_new_shoppers_current_quater,on="customer_id_hashed",how="inner")
count_new_shoppers_current_quater=count_new_shoppers_current_quater['customer_id_hashed'].nunique()

print("count_new_sign_ups_current_quarter: "+str(count_new_sign_ups_current_quarter))
logging.info("count_new_sign_ups_current_quarter: "+str(count_new_sign_ups_current_quarter))

print("count_shoppers_current_quarter: "+str(count_shoppers_current_quarter))
logging.info("count_shoppers_current_quarter: "+str(count_shoppers_current_quarter))

print("count_new_shoppers_current_quater: "+str(count_new_shoppers_current_quater))
logging.info("count_new_shoppers_current_quater: "+str(count_new_shoppers_current_quater))


# In[9]:


# over-wroten in as last week only

count_new_sign_ups_last_week=list_ids_in_week['customer_id_hashed'].nunique()

df_shoppers_in_year=df_shoppers_in_year[df_shoppers_in_year['week_end_dt']>=str_last_week_start.replace("'","")]
count_shoppers_last_week=df_shoppers_in_year['customer_id_hashed'].nunique()

count_new_shoppers_last_week=df_shoppers_in_year[['customer_id_hashed']].drop_duplicates()
count_new_shoppers_last_week=pd.merge(list_ids_in_week,count_new_shoppers_last_week,on="customer_id_hashed",how="inner")
count_new_shoppers_last_week=count_new_shoppers_last_week['customer_id_hashed'].nunique()

print("count_new_sign_ups_last_week: "+str(count_new_sign_ups_last_week))
logging.info("count_new_sign_ups_last_week: "+str(count_new_sign_ups_last_week))

print("count_shoppers_last_week: "+str(count_shoppers_last_week))
logging.info("count_shoppers_last_week: "+str(count_shoppers_last_week))

print("count_new_shoppers_last_week: "+str(count_new_shoppers_last_week))
logging.info("count_new_shoppers_last_week: "+str(count_new_shoppers_last_week))


# In[10]:


df_output_this_week=pd.DataFrame({"week_end_dt":week_end_dt,
                                 "count_new_sign_ups_last_week":count_new_sign_ups_last_week,
                                 "count_shoppers_last_week":count_shoppers_last_week,
                                 "count_new_shoppers_last_week":count_new_shoppers_last_week,
                                  
                                  "count_new_sign_ups_current_quarter":count_new_sign_ups_current_quarter,
                                 "count_shoppers_current_quarter":count_shoppers_current_quarter,
                                 "count_new_shoppers_current_quater":count_new_shoppers_current_quater,
                                  
                                  "count_new_sign_ups_fiscal_year":count_new_sign_ups_fiscal_year,
                                 "count_shoppers_fiscal_year":count_shoppers_fiscal_year,
                                 "count_new_shoppers_fiscal_year":count_new_shoppers_fiscal_year,
                                  
                                 "date_begin_year":fiscal_year_start,
                                 "date_begin_quarter":current_quarter_beginning,
                                 "Current_Quarter":str_current_quarter},index=[0])


# In[11]:


str_previous_week_end=str(last_sturday-datetime.timedelta(days=7))
print("str_previous_week_end: "+str_previous_week_end)
logging.info("str_previous_week_end: "+str_previous_week_end)


# In[12]:

'''
df_output_previous_week=df_output_this_week.head(0)
df_output_previous_week.to_csv()

df_output_previous_week.to_csv("/home/simeng/outputs_"+str_previous_week_end+"/"+"updated_New_rewards_as_fisical_year_"+str_previous_week_end+".csv",index=False)
df_output_previous_week.to_csv("/home/jian/celery/JT_new_sing_ups_rewards_rolling_table/output/"+"updated_New_rewards_as_fisical_year_"+str_previous_week_end+".csv",index=False)
'''

# In[17]:


df_output_previous_week=pd.read_csv("/home/jian/celery/JT_new_sing_ups_rewards_rolling_table/output/"+"updated_New_rewards_as_fisical_year_"+str_previous_week_end+".csv")

df_output=df_output_previous_week.append(df_output_this_week)
df_output.to_csv("/home/simeng/outputs_"+str(last_sturday)+"/"+"updated_New_rewards_as_fisical_year_"+str(last_sturday)+".csv",index=False)
df_output.to_csv("/home/jian/celery/JT_new_sing_ups_rewards_rolling_table/output/"+"updated_New_rewards_as_fisical_year_"+str(last_sturday)+".csv",index=False)


logging.info("done_of_file_creating, to send email: "+str(datetime.datetime.now()))


# In[13]:


# # Email

# In[22]:


import smtplib
from os.path import basename
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate
import datetime
import os


# In[23]:


sender="jubapluscc@gmail.com"
receivers='jthomas@mediastorm.biz, jian@jubaplus.com, breznik@jubaplus.com'
subject="Big Lots New Sign Ups Report Qumulative of Year/Quarter/Week"


# In[24]:


file = "/home/jian/celery/JT_new_sing_ups_rewards_rolling_table/output/"+"updated_New_rewards_as_fisical_year_"+str(last_sturday)+".csv"
text_message_str="/home/jian/celery/JT_new_sing_ups_rewards_rolling_table/email_message_new_rewards_weekly.txt"

msg = MIMEMultipart()
msg['From'] = sender
msg['To'] = receivers
msg['Date'] = formatdate(localtime=True)
msg['Subject'] = subject
with open(text_message_str,'r') as f:
    text_mesaage = f.read()
msg.attach(MIMEText(text_mesaage))

with open(file,'rb') as attachment:
    att = MIMEApplication(
        attachment.read(),name=os.path.basename(file)
    )
    att['Content-Disposition'] = 'attachment; filename="%s"' %os.path.basename(file)
    msg.attach(att)



smtp = smtplib.SMTP('smtp.gmail.com',587)
smtp.ehlo()
smtp.starttls()
smtp.login(sender,"mfppxsfikqmazbqj")
smtp.send_message(msg)

smtp.close()

logging.info("Email sent: "+str(datetime.datetime.now()))


# In[ ]:

import pandas as pd
import numpy as np 
import datetime
import os

def recursive_file_gen(root_path):
    for root, dirs, files in os.walk(root_path):
        for file in files:
            yield os.path.join(root,file)
            
# start_date_2019Q4=datetime.date(2019,11,3)
# str current_quarter_beginning
last_saturday=datetime.datetime.now().date()-datetime.timedelta(days=datetime.datetime.now().date().weekday()+2)

pos_list=list(recursive_file_gen("/home/jian/BigLots/"))
pos_list=[x for x in pos_list if "daily" in x.lower() and "s/MediaStorm_" in x and x[-4:]==".txt"]
pos_list=[x for x in pos_list if x.split("s/MediaStorm_")[1][:10]>=str(current_quarter_beginning)]
pos_list.sort()
print(len(pos_list))

df_all_POS_by_store=pd.DataFrame()
for file in pos_list:
    df=pd.read_csv(file,dtype=str,sep="|")
    df['item_transaction_amt']=df['item_transaction_amt'].astype(float)
    df_rewards=df[pd.notnull(df['customer_id_hashed'])]
    df_non_rewards=df[pd.isnull(df['customer_id_hashed'])]
    
    df_rewards_sales=df_rewards.groupby(['location_id','transaction_dt'])['item_transaction_amt'].sum().to_frame().reset_index().rename(columns={"item_transaction_amt":"rewards_sales"})
    df_rewards_trans=df_rewards[['location_id','transaction_dt','transaction_id','customer_id_hashed']].drop_duplicates()
    df_rewards_trans['rewards_trans']=1
    df_rewards_trans=df_rewards_trans.groupby(['location_id','transaction_dt'])['rewards_trans'].sum().to_frame().reset_index()
    df_rewards=pd.merge(df_rewards_sales,df_rewards_trans,on=['location_id','transaction_dt'],how="outer")
    
    df_non_rewards_sales=df_non_rewards.groupby(['location_id','transaction_dt'])['item_transaction_amt'].sum().to_frame().reset_index().rename(columns={"item_transaction_amt":"non_rewards_sales"})
    df_non_rewards_trans=df_non_rewards[['location_id','transaction_dt','transaction_id']].drop_duplicates()
    df_non_rewards_trans['non_rewards_trans']=1
    df_non_rewards_trans=df_non_rewards_trans.groupby(['location_id','transaction_dt'])['non_rewards_trans'].sum().to_frame().reset_index()
    df_non_rewards=pd.merge(df_non_rewards_sales,df_non_rewards_trans,on=['location_id','transaction_dt'],how="outer")
    
    df=pd.merge(df_rewards,df_non_rewards,on=['location_id','transaction_dt'],how="outer")
    df['week_end_dt']=df['transaction_dt'].max()
    df_all_POS_by_store=df_all_POS_by_store.append(df)
    print(datetime.datetime.now(),df['transaction_dt'].max())
df_all_POS_by_store['store_type']=np.where(df_all_POS_by_store['location_id']=="6990","online","instore")
df_summary=df_all_POS_by_store.groupby(["week_end_dt",'store_type'])['rewards_sales','non_rewards_sales','rewards_trans','non_rewards_trans'].sum().reset_index()

df_summary.to_csv("/home/jian/celery/JT_new_sing_ups_rewards_rolling_table/output/sales_in_quarter_so_far_"+week_end_dt+".csv",index=False)


file = "/home/jian/celery/JT_new_sing_ups_rewards_rolling_table/output/sales_in_quarter_so_far_"+week_end_dt+".csv"
text_message_str="/home/jian/celery/JT_new_sing_ups_rewards_rolling_table/email_message_sales_in_quarter_so_far.txt"

msg = MIMEMultipart()
msg['From'] = sender
msg['To'] = receivers
msg['Date'] = formatdate(localtime=True)
msg['Subject'] = "Big Lots sales in this qurrent up to last week"
with open(text_message_str,'r') as f:
    text_mesaage = f.read()
msg.attach(MIMEText(text_mesaage))

with open(file,'rb') as attachment:
    att = MIMEApplication(
        attachment.read(),name=os.path.basename(file)
    )
    att['Content-Disposition'] = 'attachment; filename="%s"' %os.path.basename(file)
    msg.attach(att)
smtp = smtplib.SMTP('smtp.gmail.com',587)
smtp.ehlo()
smtp.starttls()
smtp.login(sender,"mfppxsfikqmazbqj")
smtp.send_message(msg)

smtp.close()