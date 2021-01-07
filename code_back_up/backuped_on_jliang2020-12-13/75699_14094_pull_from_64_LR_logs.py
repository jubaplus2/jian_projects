#!/usr/bin/env python
# coding: utf-8

# In[1]:


# nohup run in the output folder

import pandas as pd
import paramiko
import numpy as np
import os
import datetime
import glob
print(os.getcwd())
import logging

logging.basicConfig(filename="./copy_LR_returned_file_from_64.log",level=logging.INFO)

impr_local_folder="/home/jian/Projects/Big_Lots/Analysis/2019_Q4/Predictive_Model_Building/LR_returned_logs_BL/impressions/"
click_local_folder="/home/jian/Projects/Big_Lots/Analysis/2019_Q4/Predictive_Model_Building/LR_returned_logs_BL/clicks/"
act_local_folder="/home/jian/Projects/Big_Lots/Analysis/2019_Q4/Predictive_Model_Building/LR_returned_logs_BL/activities/"


# In[2]:


try:
    os.stat(impr_local_folder)
except:
    os.mkdir(impr_local_folder)

try:
    os.stat(click_local_folder)
except:
    os.mkdir(click_local_folder)
    
try:
    os.stat(act_local_folder)
except:
    os.mkdir(act_local_folder)


# In[3]:


host = "64.237.51.251" #hard-coded
port = 22
transport = paramiko.Transport((host, port))

password = "jian@juba2017" #hard-coded
username = "jian" #hard-coded
transport.connect(username = username, password = password)
sftp = paramiko.SFTPClient.from_transport(transport)


# In[4]:


remote_impression_folder="/mnt/drv5/lr_biglots_data/download_logs/impressions/"
list_remote_impr_files=[remote_impression_folder+x for x in sftp.listdir(remote_impression_folder)]
list_remote_impr_files.sort()

remote_click_folder="/mnt/drv5/lr_biglots_data/download_logs/clicks/"
list_remote_click_files=[remote_click_folder+x for x in sftp.listdir(remote_click_folder)]
list_remote_click_files.sort()

remote_activity_folder="/mnt/drv5/lr_biglots_data/download_logs/activities/"
list_remote_act_files=[remote_activity_folder+x for x in sftp.listdir(remote_activity_folder)]
list_remote_act_files.sort()


# In[5]:


earliest_date_raw_log=glob.glob("/home/jian/Projects/Big_Lots/Analysis/2019_Q4/Predictive_Model_Building/DCM_raw_logs_BL/impressions/*")

print(min(x.split("_utc_")[1][:8] for x in earliest_date_raw_log))

# >=20180524 since the 1st day is not completed

min_date_needed="20180524"

logging.info(str(min(x.split("_utc_")[1][:8] for x in earliest_date_raw_log)))

print("min_date_needed: ",min_date_needed)
logging.info("min_date_needed: "+str(min_date_needed))

max_date_needed="20191231"
print("max_date_needed: ",max_date_needed)
logging.info("max_date_needed: "+str(max_date_needed))


# In[6]:


# impression
i_counter=0
for file in list_remote_impr_files:
    basename=os.path.basename(file)
    
    if ("_impression_" in basename) & (basename.split("_impression_")[1][:8]>=min_date_needed) & (basename.split("_impression_")[1][:8]<=max_date_needed):

        sftp.get(file,impr_local_folder+basename)
        i_counter+=1
        if i_counter%100==11:
            print('impr: ',i_counter,datetime.datetime.now())
            logging.info("impr: "+str(i_counter)+" | "+str(datetime.datetime.now()))
            # break


# click
i_counter=0
for file in list_remote_click_files:
    basename=os.path.basename(file)
    
    if ("_click_" in basename) & (basename.split("_click_")[1][:8]>=min_date_needed) & (basename.split("_click_")[1][:8]<=max_date_needed):
        
        sftp.get(file,click_local_folder+basename)
        i_counter+=1
        if i_counter%100==11:
            print('click: ',i_counter,datetime.datetime.now())
            logging.info("click: "+str(i_counter)+" | "+str(datetime.datetime.now()))
            # break

        
        
# activity
i_counter=0
for file in list_remote_act_files:
    basename=os.path.basename(file)
    if ("_activity_" in basename) & (basename.split("_activity_")[1][:8]>=min_date_needed) & (basename.split("_activity_")[1][:8]<=max_date_needed):
        sftp.get(file,act_local_folder+basename)
        i_counter+=1
        if i_counter%10==1:
            print('act: ',i_counter,datetime.datetime.now())
            logging.info("act: "+str(i_counter)+" | "+str(datetime.datetime.now()))
            # break

        
sftp.close()
transport.close()

print("all done", datetime.datetime.now())
logging.info("all done: "+str(datetime.datetime.now()))


# In[7]:


file


# In[ ]:





# In[ ]:




