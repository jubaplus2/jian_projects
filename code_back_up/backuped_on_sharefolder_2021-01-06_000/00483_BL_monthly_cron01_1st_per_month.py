#!/usr/bin/env python
# coding: utf-8

# In[1]:


# copy files from 107 for the monthly store list and product taxonomy

import pandas as pd
import numpy as np
import datetime
import paramiko
import os
import glob
import logging

logging.basicConfig(filename='/mnt/clients/juba/hqjubaapp02/sharefolder/Automation/BigLots_crontab_on_Merkle/BL_monthly_cron01_1st_per_month.log',level="INFO")

def recursive_file_gen(root_path):
    for root, dirs, files in os.walk(root_path):
        for file in files:
            yield os.path.join(root,file)


print(datetime.datetime.now())


# # Store list and product taxonomy

# In[2]:


# biglots_data: 1.store list; 2.prod_taxo

remote_data_path_client="/mnt/drv5/biglots_data/"

host = "107.191.32.220" #hard-coded
port = 22
transport = paramiko.Transport((host, port))

password = "vgn5UucsUNHL4n9R" #hard-coded
username = "biglots_data" #hard-coded
transport.connect(username = username, password = password)
sftp = paramiko.SFTPClient.from_transport(transport)


# In[3]:


folder_move_in_remote_storelist="/mnt/drv5/biglots_data/Monthly_Store_List/"
folder_move_in_local_storelist="/mnt/clients/juba/hqjubaapp02/sharefolder/biglots_data/static_files/Store_list/"

folder_move_in_remote_product="/mnt/drv5/biglots_data/Monthly_Taxonomy/"
folder_move_in_local_product="/mnt/clients/juba/hqjubaapp02/sharefolder/biglots_data/static_files/ProductTaxonomy/"


# In[4]:


remote_list=sftp.listdir()


# In[5]:


remote_file_store_list=[x for x in remote_list if "MediaStormStores" in x]
if len(remote_file_store_list)==1:
    remote_store_list_file=remote_file_store_list[0]
    basename=os.path.basename(remote_store_list_file)
    sftp.get(remote_store_list_file,folder_move_in_local_storelist+basename)
    sftp.rename(remote_store_list_file, folder_move_in_remote_storelist+basename)
    print("File new store list copied to hqjubaapp02 and moved into the subfoler: %s"%basename)
    
else:
    print("new store list file length is not 1")
    logging.info("new store list file length is not 1")
    


# In[6]:


remote_file_prod_taxonomy=[x for x in remote_list if "MediaStormProductTaxonomy" in x]
if len(remote_file_prod_taxonomy)==1:
    remote_prod_taxonomy_file=remote_file_prod_taxonomy[0]
    basename=os.path.basename(remote_prod_taxonomy_file)
    sftp.get(remote_prod_taxonomy_file,folder_move_in_local_product+basename)
    sftp.rename(remote_prod_taxonomy_file, folder_move_in_remote_product+basename)
    print("File new product taxonomy copied to hqjubaapp02 and moved into the subfoler: %s"%basename)
    
else:
    print("new new product taxonomy file length is not 1")
    logging.info("new new product taxonomy file length is not 1")
    


# In[3]:


'''
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
'''


# In[ ]:




