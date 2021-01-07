#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import datetime
import os
import gc
import logging
logging.basicConfig(filename='/home/jian/BL_weekly_crontab/cron_4_JT_rolling_rewards_tracker/manually_run_JT_rewards_rolling_id_count_logs.log', level=logging.INFO)

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

    
    
quarter_of_quarter   
logging.info("quarter_of_quarter: "+str(quarter_of_quarter))


# In[3]:


print(datetime.datetime.now())
logging.info("datetime.datetime.now(): "+str(datetime.datetime.now()))

current_quarter_beginning=last_day_of_2018Q4+datetime.timedelta(days=(int(year_of_quarter)-2019)*52*7+(int(quarter_of_quarter)-1)*13*7+1)
year_start=datetime.date(last_sturday.year,1,1)  

print("current_quarter_beginning",current_quarter_beginning)
print("year_start",year_start)
logging.info("current_quarter_beginning: "+str(current_quarter_beginning))
logging.info("year_start: "+str(year_start))


# In[4]:


# All new sign ups since 2019-01-01
all_2019_new_sign_ups=list(recursive_file_gen("/home/jian/BigLots/"))
all_2019_new_sign_ups=[x for x in all_2019_new_sign_ups if "aster" in x]
all_2019_new_sign_ups=[x for x in all_2019_new_sign_ups if "MediaStorm_" in x]
all_2019_new_sign_ups=[x for x in all_2019_new_sign_ups if int(x.split("MediaStorm_")[1][:4])>=2019]


df_all_2019_new_sign_ups=pd.DataFrame({"file_path":all_2019_new_sign_ups})
df_all_2019_new_sign_ups['week_end_dt']=df_all_2019_new_sign_ups['file_path'].apply(lambda x: x.split("/MediaStorm_")[1][:10])
df_all_2019_new_sign_ups=df_all_2019_new_sign_ups.sort_values("week_end_dt",ascending=True)
df_all_2019_new_sign_ups=df_all_2019_new_sign_ups[df_all_2019_new_sign_ups['week_end_dt']>="2019-06-08"]
df_all_2019_new_sign_ups.shape


# In[5]:


all_2019_daily_sales=list(recursive_file_gen("/home/jian/BigLots/"))
all_2019_daily_sales=[x for x in all_2019_daily_sales if "aily" in x]
all_2019_daily_sales=[x for x in all_2019_daily_sales if "MediaStorm_" in x]
all_2019_daily_sales=[x for x in all_2019_daily_sales if int(x.split("MediaStorm_")[1][:4])>=2019]


df_all_2019_daily_sales=pd.DataFrame({"file_path":all_2019_daily_sales})
df_all_2019_daily_sales['week_end_dt']=df_all_2019_daily_sales['file_path'].apply(lambda x: x.split("/MediaStorm_")[1][:10])
df_all_2019_daily_sales=df_all_2019_daily_sales.sort_values("week_end_dt",ascending=True)

df_all_2019_daily_sales.shape


# In[6]:


'''
df_cum_new_rewards_year_0=pd.read_table("/home/jian/BigLots/New_Sing_Ups_2018_Fiscal_Year/All Rewards Members 2018-02-04 - 2019-05-04.zip",
                                       compression="zip",dtype=str,sep="|",usecols=['customer_id_hashed','sign_up_date'])
df_cum_new_rewards_year_0=df_cum_new_rewards_year_0[df_cum_new_rewards_year_0['sign_up_date']>="2019-01-01"]
df_cum_new_rewards_year_1=pd.read_table("/home/jian/BigLots/New_Sing_Ups_2018_Fiscal_Year/MediaStorm Rewards Master P4 2019 - no transaction info.zip",
                                       compression="zip",dtype=str,sep="|",usecols=['customer_id_hashed','sign_up_date'])
df_cum_new_rewards_year=df_cum_new_rewards_year_1.append(df_cum_new_rewards_year_0)
df_cum_new_rewards_year=df_cum_new_rewards_year.sort_values("sign_up_date",ascending=False).drop_duplicates("customer_id_hashed")

############
df_cum_new_rewards_quarter=df_cum_new_rewards_year[df_cum_new_rewards_year['sign_up_date']>=str(current_quarter_beginning)]


del df_cum_new_rewards_year_0
del df_cum_new_rewards_year_1
gc.collect()


print(datetime.datetime.now())
logging.info("datetime.datetime.now(): "+str(datetime.datetime.now()))
'''


# In[7]:


year_of_quarter


# In[9]:


df_cum_new_rewards_quarter=pd.DataFrame()
df_cum_new_rewards_year=pd.DataFrame()
for file_new_sign_ups in df_all_2019_new_sign_ups['file_path'].tolist():
    week_end_date_str=file_new_sign_ups.split("MediaStorm_")[1][:10]
    if ((week_end_date_str>=str(year_start)) or (week_end_date_str>=str(current_quarter_beginning))):
        print(file_new_sign_ups)
        df=pd.read_csv(file_new_sign_ups,dtype=str,sep="|",usecols=['customer_id_hashed','sign_up_date'])
        df_year=df[df['sign_up_date'].apply(lambda x: x[:4]==str(last_sturday.year))]
        df_cum_new_rewards_year=df_cum_new_rewards_year.append(df_year)

        df_quarter=df[df['sign_up_date']>=str(current_quarter_beginning)]
        df_cum_new_rewards_quarter=df_cum_new_rewards_quarter.append(df_quarter)
    
df_new_rewards_week=df.copy()


# In[10]:


df_cum_daily_sales_year=pd.DataFrame()
df_cum_daily_sales_quarter=pd.DataFrame()

i_counter=0
for file_daily_sales in df_all_2019_daily_sales['file_path'].tolist():
    week_end_date_str=file_daily_sales.split("MediaStorm_")[1][:10]
    if ((week_end_date_str>= str(year_start)) or (week_end_date_str>=str(current_quarter_beginning))):
        print(file_daily_sales)
        df=pd.read_csv(file_daily_sales,dtype=str,sep="|",usecols=['customer_id_hashed','transaction_dt']).drop_duplicates()
        df=df[pd.notnull(df['customer_id_hashed'])]
        
        df_year=df[df['transaction_dt'].apply(lambda x: x[:4]==str(last_sturday.year))]
        df_cum_daily_sales_year=df_cum_daily_sales_year.append(df_year)
        del df_cum_daily_sales_year['transaction_dt']
    
        df_quarter=df[df['transaction_dt']>=str(current_quarter_beginning)]
        df_cum_daily_sales_quarter=df_cum_daily_sales_quarter.append(df_quarter)
        del df_cum_daily_sales_quarter['transaction_dt']
        i_counter+=1
    
        if i_counter%5==1:
            print(i_counter,datetime.datetime.now())
            logging.info("i_counter: "+str(datetime.datetime.now()))

df_cum_daily_sales_year=df_cum_daily_sales_year.drop_duplicates()
df_cum_daily_sales_quarter=df_cum_daily_sales_quarter.drop_duplicates()

df_daily_sales_week=df[['customer_id_hashed']].drop_duplicates()

print(i_counter)
logging.info("done_of_reading_in_the_loop: "+str(i_counter))

df_cum_daily_sales_quarter.head(2)


# In[11]:


df_cum_daily_sales_year['Shoppers']="Purchased"
df_cum_daily_sales_quarter['Shoppers']="Purchased"
df_daily_sales_week['Shoppers']="Purchased"

df_cum_new_rewards_year=pd.merge(df_cum_new_rewards_year,df_cum_daily_sales_year,on="customer_id_hashed",how="left")
df_cum_new_rewards_quarter=pd.merge(df_cum_new_rewards_quarter,df_cum_daily_sales_quarter,on="customer_id_hashed",how="left")
df_new_rewards_week=pd.merge(df_new_rewards_week,df_daily_sales_week,on="customer_id_hashed",how="left")

week_end_dt=str(last_sturday)

rewards_shoppers_in_week=df_daily_sales_week.shape[0]
rewards_shoppers_cum_quarter=df_cum_daily_sales_quarter.shape[0]
rewards_shoppers_cum_year=df_cum_daily_sales_year.shape[0]

new_sign_ups_cum_year=df_cum_new_rewards_year.shape[0]
new_signed_shoppers_cum_year=df_cum_new_rewards_year[pd.notnull(df_cum_new_rewards_year['Shoppers'])].shape[0]

new_sign_ups_cum_quarter=df_cum_new_rewards_quarter.shape[0]
new_signed_shoppers_cum_quarter=df_cum_new_rewards_quarter[pd.notnull(df_cum_new_rewards_quarter['Shoppers'])].shape[0]

new_signed_week=df_new_rewards_week.shape[0]
new_signed_shoppers_week=df_new_rewards_week[pd.notnull(df_new_rewards_week['Shoppers'])].shape[0]

date_begin_year=str(year_start)
date_begin_quarter=str(current_quarter_beginning)
Current_Quarter=str_current_quarter


df_output_this_week=pd.DataFrame({"week_end_dt":week_end_dt,
                                 "rewards_shoppers_in_week":rewards_shoppers_in_week,
                                 "rewards_shoppers_cum_quarter":rewards_shoppers_cum_quarter,
                                 "rewards_shoppers_cum_year":rewards_shoppers_cum_year,
                                 "new_sign_ups_cum_year":new_sign_ups_cum_year,
                                 "new_signed_shoppers_cum_year":new_signed_shoppers_cum_year,
                                 "new_sign_ups_cum_quarter":new_sign_ups_cum_quarter,
                                 "new_signed_shoppers_cum_quarter":new_signed_shoppers_cum_quarter,
                                 "new_signed_week":new_signed_week,
                                 "new_signed_shoppers_week":new_signed_shoppers_week,
                                 "date_begin_year":date_begin_year,
                                 "date_begin_quarter":date_begin_quarter,
                                 "Current_Quarter":Current_Quarter},index=[0])
df_output_this_week=df_output_this_week[['week_end_dt','rewards_shoppers_in_week','rewards_shoppers_cum_quarter','rewards_shoppers_cum_year',
                                        'new_sign_ups_cum_year','new_signed_shoppers_cum_year','new_sign_ups_cum_quarter','new_signed_shoppers_cum_quarter',
                                        'new_signed_week','new_signed_shoppers_week','date_begin_year','date_begin_quarter','Current_Quarter']]


previous_week_end_dt=(datetime.datetime.strptime(week_end_dt,"%Y-%m-%d")-datetime.timedelta(days=7)).date()
previous_week_end_dt=str(previous_week_end_dt)
previous_week_end_dt


df_output_previous_week=pd.read_csv("/home/simeng/outputs_"+previous_week_end_dt+"/"+"New_rewards_df_this_week_"+previous_week_end_dt+".csv")

cum_this_week_output=df_output_previous_week.append(df_output_this_week)               
cum_this_week_output.to_csv("/home/simeng/outputs_"+week_end_dt+"/"+"New_rewards_df_this_week_"+week_end_dt+".csv",index=False)
cum_this_week_output.to_csv("/home/jian/celery/JT_new_sing_ups_rewards_rolling_table/output/"+"New_rewards_df_this_week_"+week_end_dt+".csv",index=False)

cum_this_week_output


# In[12]:


print("Stop here", datetime.datetime.now())
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


file = "/home/jian/celery/JT_new_sing_ups_rewards_rolling_table/output/"+"New_rewards_df_this_week_"+week_end_dt+".csv"
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