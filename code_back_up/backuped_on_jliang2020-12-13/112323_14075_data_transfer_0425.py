
# coding: utf-8

# In[1]:

import datetime
import os
import paramiko
import pandas as pd


# In[2]:

def recrusive_file_gen(root_folder):
    for root, dirs, files in os.walk(root_folder):
        for file in files:
            yield os.path.join(root, file)


# In[3]:

remote_server_root="/mnt/drv5/saatva/BL_data_for_Circ_analysis/"


# In[4]:

host = "64.237.51.251" #hard-coded
port = 22
transport = paramiko.Transport((host, port))

password = "jian@juba2017" #hard-coded
username = "jian" #hard-coded
transport.connect(username = username, password = password)
sftp = paramiko.SFTPClient.from_transport(transport)




# In[5]:

start_weekend_date_2019="2019-01-01"

data_2019=list(recrusive_file_gen("/home/jian/BigLots/2019_by_weeks/"))
data_2019=[x for x in data_2019 if "DailySales" in x]
data_2019_df=pd.DataFrame({"file_path":data_2019})
data_2019_df['week_end_dt']=data_2019_df['file_path'].apply(lambda x: x.split("y_weeks/MediaStorm_")[1][:10])
data_2019_df=data_2019_df[data_2019_df['week_end_dt']>=start_weekend_date_2019]

for local_path in data_2019_df['file_path'].tolist():
    remote_path="/mnt/drv5/saatva/BL_data_for_Circ_analysis/ItemLevel_2019/"+os.path.basename(local_path)
    sftp.put(local_path,remote_path)

    


# In[5]:

start_weekend_date_2018="2018-01-01"
end_weekend_date_2018="2018-12-31"

data_2018=list(recrusive_file_gen("/home/jian/BigLots/hist_daily_data_subclasslevel/"))
data_2018=[x for x in data_2018 if "DailySales" in x]
data_2018_df=pd.DataFrame({"file_path":data_2018})


data_2018_df['week_end_dt']=data_2018_df['file_path'].apply(lambda x: x.split("MediaStormDailySales_week_ending_")[1][:10])
data_2018_df=data_2018_df[data_2018_df['week_end_dt']>=start_weekend_date_2018]
data_2018_df=data_2018_df[data_2018_df['week_end_dt']<=end_weekend_date_2018]
max_hist_subclass_2018=data_2018_df['week_end_dt'].max()


# In[6]:

data_2018_weekly=list(recrusive_file_gen("/home/jian/BigLots/2018_by_weeks/"))
data_2018_weekly=[x for x in data_2018_weekly if "DailySales" in x]
data_2018_weekly_df=pd.DataFrame({"file_path":data_2018_weekly})
data_2018_weekly_df['week_end_dt']=data_2018_weekly_df['file_path'].apply(lambda x: x.split("y_weeks/MediaStorm_")[1][:10])
data_2018_weekly_df=data_2018_weekly_df[data_2018_weekly_df['week_end_dt']>=max_hist_subclass_2018]

data_2018_weekly_df['week_end_dt'].min()


# In[9]:

all_2018_weekly_sub_class=data_2018_df.append(data_2018_weekly_df)
print(all_2018_weekly_sub_class.shape)
print(sorted(all_2018_weekly_sub_class['week_end_dt'].unique()))

print(len(set(all_2018_weekly_sub_class['week_end_dt'].unique())))


# In[9]:

for local_path in data_2018_df['file_path'].tolist():
    remote_path="/mnt/drv5/saatva/BL_data_for_Circ_analysis/SubclassLevel_2018/"+os.path.basename(local_path)
    sftp.put(local_path,remote_path)


# In[10]:

sftp.put("/home/jian/BigLots/static_files/ProductTaxonomy/MediaStormProductTaxonomy20190401-134939-109.txt",
         "/mnt/drv5/saatva/BL_data_for_Circ_analysis/Mapping_File/MediaStormProductTaxonomy20190401-134939-109.txt")

sftp.put("/home/jian/BigLots/static_files/MediaStorm Data Extract - Division Names.txt",
         "/mnt/drv5/saatva/BL_data_for_Circ_analysis/Mapping_File/MediaStorm Data Extract - Division Names.txt")

sftp.put("/home/jian/BigLots/static_files/MediaStorm Data Extract - Product Taxonomy.txt",
         "/mnt/drv5/saatva/BL_data_for_Circ_analysis/Mapping_File/MediaStorm Data Extract - Product Taxonomy.txt")

sftp.close()


# In[ ]:



