#!/usr/bin/env python
# coding: utf-8

# In[1]:


# copy files from 107 agileone for the monthly email unsubscribers

import pandas as pd
import numpy as np
import datetime
import paramiko
import os
import glob
import logging

logging.basicConfig(filename='/mnt/clients/juba/hqjubaapp02/sharefolder/Automation/BigLots_crontab_on_Merkle/BL_monthly_cron02_25th_per_month.log',level="INFO")

def recursive_file_gen(root_path):
    for root, dirs, files in os.walk(root_path):
        for file in files:
            yield os.path.join(root,file)


print(datetime.datetime.now())


# # Email subscribers and unsubscribers

# In[3]:


# agilone: 1.email sub delta; 2.email unsub refresh

remote_agile_one="/home/agilone/"

host = "107.191.32.220" #hard-coded
port = 22
transport = paramiko.Transport((host, port))

password = "Tur87ZqzF9xV5mYF" #hard-coded
username = "agilone" #hard-coded
transport.connect(username = username, password = password)
sftp = paramiko.SFTPClient.from_transport(transport)


# In[4]:


folder_move_in_remote_subscribers="/home/agilone/"
folder_movliste_in_local_subscribers="/mnt/clients/juba/hqjubaapp02/sharefolder/biglots_data/Email_Subscription_Files/subscribers/"

folder_move_in_remote_Unsub="/home/agilone/"
folder_move_in_local_Unsub="/mnt/clients/juba/hqjubaapp02/sharefolder/biglots_data/Email_Subscription_Files/Unsubs/"


# In[5]:


folder_move_in_remote_subscribers="/home/agilone/archived_email_subscriber_files/"
folder_move_in_local_subscribers="/mnt/clients/juba/hqjubaapp02/sharefolder/biglots_data/Email_Subscription_Files/subscribers/"

folder_move_in_remote_ubsub="/home/agilone/archived_email_Unsub_files/"
folder_move_in_local_ubsub="/mnt/clients/juba/hqjubaapp02/sharefolder/biglots_data/Email_Subscription_Files/Unsubs/"


# In[6]:


remote_list=sftp.listdir()


# In[8]:


# Subscribters delta
remote_file_email_subscribers=[x for x in remote_list if "BL_Email_Subscriber_File_Delta__20" in x]
if len(remote_file_email_subscribers)==1:
    remote_email_subscribers_file=remote_file_email_subscribers[0]
    basename=os.path.basename(remote_email_subscribers_file)
    sftp.get(remote_email_subscribers_file,folder_move_in_local_subscribers+basename)
    sftp.rename(remote_email_subscribers_file, folder_move_in_remote_subscribers+basename)
    print("File Email Subscriber copied to hqjubaapp02 and moved into the subfoler: %s"%basename)
    
else:
    print("new Email Subscriber file length is not 1")
    logging.info("new Email Subscriber file length is not 1")
    


# In[9]:


# UnSubscribters delta
remote_file_email_unsub=[x for x in remote_list if "BL_Email_UnSubscriber_File_Refresh__20" in x]
if len(remote_file_email_unsub)==1:
    remote_email_unsub_file=remote_file_email_unsub[0]
    basename=os.path.basename(remote_email_unsub_file)
    sftp.get(remote_email_unsub_file,folder_move_in_local_ubsub+basename)
    sftp.rename(remote_email_unsub_file, folder_move_in_remote_ubsub+basename)
    print("File Email UnSub copied to hqjubaapp02 and moved into the subfoler: %s"%basename)
    
else:
    print("new Email UnSub file length is not 1")
    logging.info("new Email UnSub file length is not 1")
    


# In[ ]:




