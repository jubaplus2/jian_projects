#!/usr/bin/env python
# coding: utf-8

# In[11]:


import paramiko
import pandas as pd
import numpy as np
import datetime
import os
import glob
# biglots_data: 1.store list; 2.prod_taxo
# agileone: email unsubsription 

print(datetime.datetime.now())


# In[28]:


def copy_file_to_local(pattern_unique,remote_mother_folder,remote_movein_folder,local_folder,sftp):
    list_new_files=[x for x in sftp.listdir() if x[-4:]==".txt" or x[-4:]==".csv"]
    list_files_fit_pattern=[x for x in list_new_files if pattern_unique in x]
    if not list_files_fit_pattern:
        raise ValueError("not table found with the pattern: %s" %pattern_unique)
    elif len(list_files_fit_pattern)>1:
        raise ValueError("multiple tables found with the pattern: %s" %pattern_unique)
    else:
        file_path_remote_received=remote_mother_folder+list_files_fit_pattern[0]
        basename=os.path.basename(file_path_remote_received)
        sftp.get(file_path_remote_received,local_folder+basename)
        sftp.rename(file_path_remote_received, remote_movein_folder+basename)
        print("File copied to 192 and moved into the subfoler: %s"%basename)


# # Part 1: 107 biglots_data

# In[3]:


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
folder_move_in_local_storelist="/home/sharefolder/biglots_data/static_files/Store_list/"

folder_move_in_remote_product="/mnt/drv5/biglots_data/Monthly_Taxonomy/"
folder_move_in_local_product="/home/sharefolder/biglots_data/static_files/ProductTaxonomy/"


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

# In[23]:


# agilone: 1.email sub delta; 2.email unsub refresh

remote_agile_one="/home/agilone/"

host = "107.191.32.220" #hard-coded
port = 22
transport = paramiko.Transport((host, port))

password = "Tur87ZqzF9xV5mYF" #hard-coded
username = "agilone" #hard-coded
transport.connect(username = username, password = password)
sftp = paramiko.SFTPClient.from_transport(transport)


# In[24]:


folder_move_in_remote_subscribers="/home/agilone/archived_email_subscriber_files/"
folder_movliste_in_local_subscribers="/home/sharefolder/biglots_data/Email_Subscription_Files/subscribers/"

folder_move_in_remote_Unsub="/home/agilone/archived_email_Unsub_files/"
folder_move_in_local_Unsub="/home/sharefolder/biglots_data/Email_Subscription_Files/Unsubs/"


# In[25]:


list_local_subscribers=glob.glob(folder_move_in_remote_subscribers)
list_local_Unsubs=glob.glob(folder_move_in_remote_Unsub)


# In[33]:


# Subscribters delta
for i in range(8):
    pattern="BL_Email_Subscriber_File_Delta__20200%d"%i
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


# In[34]:


# UbSubs refresh
for i in range(8):
    pattern="BL_Email_UnSubscriber_File_Refresh__20200%d"%i
    try:
        copy_file_to_local(
            pattern_unique=pattern,
            remote_movein_folder=folder_move_in_remote_Unsub,
            remote_mother_folder=remote_agile_one,
            local_folder=folder_move_in_local_Unsub,
            sftp=sftp
        )
    except:
        pass


# In[ ]:




