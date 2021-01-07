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
import logging

logging.basicConfig(filename='/mnt/clients/juba/hqjubaapp02/sharefolder/Automation/BigLots_crontab_on_Merkle/weekly_files_copy_and_mysql_update.log',level="INFO")

def recursive_file_gen(root_path):
    for root, dirs, files in os.walk(root_path):
        for file in files:
            yield os.path.join(root,file)


print(datetime.datetime.now())


# In[3]:


def copy_file_to_local(pattern_unique,remote_mother_folder,remote_movein_folder,local_folder,sftp):
    list_new_files=[x for x in sftp.listdir() if x[-4:]==".txt" or x[-4:]==".csv"]
    list_files_fit_pattern=[x for x in list_new_files if pattern_unique in x]
    if not list_files_fit_pattern:
        raise ValueError("no table found with the pattern: %s" %pattern_unique)
    elif len(list_files_fit_pattern)>1:
        raise ValueError("multiple tables found with the pattern: %s" %pattern_unique)
    else:
        file_path_remote_received=remote_mother_folder+list_files_fit_pattern[0]
        basename=os.path.basename(file_path_remote_received)
        sftp.get(file_path_remote_received,local_folder+basename)
        sftp.rename(file_path_remote_received, remote_movein_folder+basename)
        print("File copied to hqjubaapp02 and moved into the subfoler: %s"%basename)


# # Part 1: 107 biglots_data

# In[2]:


# 1. product taxonomy
# 2. store list
# 3. unsub 


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


folder_move_in_remote_storelist="/mnt/drv5/biglots_data/Monthly_Store_List/"
folder_move_in_local_storelist="/mnt/clients/juba/hqjubaapp02/sharefolder/biglots_data/static_files/Store_list/"

folder_move_in_remote_product="/mnt/drv5/biglots_data/Monthly_Taxonomy/"
folder_move_in_local_product="/mnt/clients/juba/hqjubaapp02/sharefolder/biglots_data/static_files/ProductTaxonomy/"


# In[6]:


# store list
copy_file_to_local(
    pattern_unique="MediaStormStores",
    remote_movein_folder=folder_move_in_remote_storelist,
    remote_mother_folder=remote_data_path_client,
    local_folder=folder_move_in_local_storelist,
    sftp=sftp
)
    
    
# Product taxonomy
copy_file_to_local(
    pattern_unique="MediaStormProductTaxonomy",
    remote_movein_folder=folder_move_in_remote_product,
    remote_mother_folder=remote_data_path_client,
    local_folder=folder_move_in_local_product,
    sftp=sftp
)


# # Part 2: 107 agilone

# In[7]:


# agilone: 1.email sub delta; 2.email unsub refresh

remote_agile_one="/home/agilone/"

host = "107.191.32.220" #hard-coded
port = 22
transport = paramiko.Transport((host, port))

password = "Tur87ZqzF9xV5mYF" #hard-coded
username = "agilone" #hard-coded
transport.connect(username = username, password = password)
sftp = paramiko.SFTPClient.from_transport(transport)


# In[8]:


folder_move_in_remote_subscribers="/home/agilone/"
folder_movliste_in_local_subscribers="/mnt/clients/juba/hqjubaapp02/sharefolder/biglots_data/Email_Subscription_Files/subscribers/"

folder_move_in_remote_Unsub="/home/agilone/"
folder_move_in_local_Unsub="/mnt/clients/juba/hqjubaapp02/sharefolder/biglots_data/Email_Subscription_Files/Unsubs/"


# In[10]:


# Subscribters delta
for i in range(13):
    pattern="BL_Email_Subscriber_File_Delta__20"
    try:
        copy_file_to_local(
            pattern_unique=pattern,
            remote_movein_folder=folder_move_in_remote_subscribers,
            remote_mother_folder=remote_agile_one,
            local_folder=folder_movliste_in_local_subscribers,
            sftp=sftp
        )
    except:
        pass


# In[ ]:




