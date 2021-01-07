#!/usr/bin/env python
# coding: utf-8

# In[16]:


import pandas as pd
import numpy as np
import os
import datetime
import glob
import logging

logging.basicConfig(filename='/home/jian/BL_weekly_crontab/cron_8_count_of_12_months_active_shoppers/biglots_12M_counter.log', level=logging.INFO)

print(datetime.datetime.now())
os.getcwd()


# In[17]:


def recursive_file_gen(root_path):
    for root, dirs, files in os.walk(root_path):
        for file in files:
            yield os.path.join(root, file)
            
last_saturday=datetime.datetime.now().date()
last_saturday= last_saturday-datetime.timedelta(days= last_saturday.weekday()+2)

tracking_start_week=last_saturday-datetime.timedelta(days=52*7)
print("last_saturday",last_saturday)
print("tracking_start_week",tracking_start_week)
logging.info("last_saturday: "+str(last_saturday))
logging.info("tracking_start_week: "+str(tracking_start_week))

# In[30]:


writer_folder='/home/jian/BL_weekly_crontab/cron_8_count_of_12_months_active_shoppers/output/'
try:
    os.stat(writer_folder)
except:
    os.mkdir(writer_folder)

df_pre_week=pd.read_csv(writer_folder+"BL_active_shoppers_last_52_weeks_"+str(last_saturday-datetime.timedelta(days=7))+".csv")



# In[19]:


list_all_POS=list(recursive_file_gen("/home/jian/BigLots/"))
list_all_POS=[x for x in list_all_POS if "daily" in x.lower() and "/MediaStorm_" in x]
list_all_POS=[x for x in list_all_POS if x.split("/MediaStorm_")[1][:10]>=str(tracking_start_week)]
list_all_POS=[x for x in list_all_POS if x.split("/MediaStorm_")[1][:10]>="2019-02-16"]
list_all_POS.sort()

list_historical_POS=glob.glob("/home/jian/BigLots/hist_daily_data_itemlevel_decompressed/*.txt")
list_historical_POS=[x for x in list_historical_POS if x.split("/MediaStormDailySalesHistory")[1][:8]>str(tracking_start_week).replace("-","")]
list_historical_POS=[x for x in list_historical_POS if x.split("/MediaStormDailySalesHistory")[1][:8]<="2019-02-16".replace("-","")]
list_historical_POS.sort()

list_all_POS=list_historical_POS+list_all_POS
print("len(list_all_POS)",len(list_all_POS))
logging.info("len(list_all_POS): "+str(len(list_all_POS)))


# In[28]:


df_unique_shoppers=pd.DataFrame()
i_counter=0
for file in list_all_POS:
    df=pd.read_table(file,dtype=str,sep="|",usecols=['customer_id_hashed']).drop_duplicates()
    df_unique_shoppers=df_unique_shoppers.append(df)
    i_counter+=1
    print(i_counter,datetime.datetime.now(),os.path.basename(file))
    logging.info(str(i_counter)+" "+str(datetime.datetime.now())+" "+str(os.path.basename(file)))
    
    if i_counter%10==3:
        df_unique_shoppers=df_unique_shoppers.drop_duplicates()
df_unique_shoppers=df_unique_shoppers.drop_duplicates()


# In[29]:


print(df_unique_shoppers.shape,df_unique_shoppers['customer_id_hashed'].nunique())
logging.info(str(df_unique_shoppers.shape)+" "+str(df_unique_shoppers['customer_id_hashed'].nunique()))
    


df=pd.DataFrame({"last_week_end_dt":str(last_saturday),"1st_week_end_dt":str(tracking_start_week),"active_shopper_count":df_unique_shoppers.shape[0]},index=[0])
df


# In[ ]:


df_output=df_pre_week.append(df)
df_output.to_csv(writer_folder+"BL_active_shoppers_last_52_weeks_"+str(last_saturday)+".csv",index=False)


# In[33]:


# To test

import smtplib
from os.path import basename
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate
import datetime
import os



sender="jubapluscc@gmail.com"
receivers='jthomas@mediastorm.biz, jian@jubaplus.com, breznik@jubaplus.com'
subject="Big Lots 12 months (52 weeks) active shopper count"

file = writer_folder+"BL_active_shoppers_last_52_weeks_"+str(last_saturday)+".csv"
text_message_str="/home/jian/BL_weekly_crontab/cron_8_count_of_12_months_active_shoppers/email_message_new_rewards_weekly.txt"

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
logging.info(str(datetime.datetime.now())+" Done ")

# In[ ]:




