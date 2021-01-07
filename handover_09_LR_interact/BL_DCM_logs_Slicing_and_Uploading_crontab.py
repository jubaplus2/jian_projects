#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import os
import datetime
import paramiko
import sys
import logging
import gc
import glob
import datetime

logging.basicConfig(filename='BL_uploadtolr_bl_JL.log', level=logging.INFO)

host = "files.liveramp.com"
port = 22
password = "Biglots2018!"
username = "lr-big-lots"


# In[2]:


print("Run on: "+str(datetime.datetime.now().date()))
logging.info("Run on: "+str(datetime.datetime.now().date()))

log_file_date=datetime.datetime.now().date()-datetime.timedelta(days=7)

print("log_file_date: "+str(log_file_date))
logging.info("log_file_date: "+str(log_file_date))

# placement_pattern_keywords=["lr","liveramp",'experian','crm'] # before 2019Q4
placement_pattern_keywords=["rewards"] # 2019Q4, confirmed with Faith & JT, email Nov. 14 12:54
print("placement_pattern_keywords: "+str(placement_pattern_keywords))
logging.info("placement_pattern_keywords: "+str(placement_pattern_keywords))

def build_transport():
    transport = paramiko.Transport((host, port))
    transport.default_window_size=paramiko.common.MAX_WINDOW_SIZE
    transport.connect(username = username, password=password)
    return transport


# In[3]:


def fetch_placement_ids(log_file_date,list_keywords):
    start_date=str(log_file_date-datetime.timedelta(days=60)).replace('-','')
    end_date=str(log_file_date+datetime.timedelta(days=7)).replace('-','')

    ###
    all_campaign_matching=glob.glob('/mnt/drv5/dcm_logs/match_tables/*campaigns_*')
    all_campaign_matching=[x for x in all_campaign_matching if x.split("campaigns_")[1][:8]>=start_date]
    all_campaign_matching=[x for x in all_campaign_matching if x.split("campaigns_")[1][:8]<=end_date]

    df_campaign=pd.DataFrame()
    for file in all_campaign_matching:
        df=pd.read_csv(file,dtype = 'str',usecols=['Campaign ID','Advertiser ID'])
        df=df[df['Advertiser ID']=='8095847']
        df_campaign=df_campaign.append(df,ignore_index=True)
    df_campaign=df_campaign.drop_duplicates()
    list_campaign_id=df_campaign['Campaign ID'].tolist()
    del df_campaign
    
    ###    
    all_placement_matching=glob.glob('/mnt/drv5/dcm_logs/match_tables/*placements*')
    all_placement_matching=[x for x in all_placement_matching if x.split("placements_")[1][:8]>=start_date]
    all_placement_matching=[x for x in all_placement_matching if x.split("placements_")[1][:8]<=end_date]
    
    df_placement=pd.DataFrame()
    for file in all_placement_matching:
        df=pd.read_csv(file,dtype = 'str',usecols=['Campaign ID','Placement ID','Placement']) # no 'Advertiser ID' in the file
        df=df[df['Campaign ID'].isin(list_campaign_id)]
        df_placement=df_placement.append(df,ignore_index=True)
    df_placement=df_placement.drop_duplicates()
    
    # Fileter the placement based on the keywords, "or" statement and case insensitive
    df_placement=df_placement[df_placement['Placement'].str.contains("|".join(list_keywords),case=False)]
    
    return df_placement['Placement ID'].unique().tolist()


# In[4]:


list_placement_ids=fetch_placement_ids(datetime.datetime.now().date(),list_keywords=placement_pattern_keywords)
print("Done of placement ids fetching:"+str(datetime.datetime.now()))
logging.info("Done of placement ids fetching:"+str(datetime.datetime.now()))


# In[5]:


# Impression of the day
# Based on the date in the file name, not modifity time

path_impr='/mnt/drv5/dcm_logs/impressions/'
list_impr_files=glob.glob(path_impr+"*.csv.gz")
list_impr_files=[x for x in list_impr_files if x.split("count7252_impression_")[1][:8]==str(log_file_date).replace("-","")]

df=pd.DataFrame({"file_path":list_impr_files})
df['basepattern']=df['file_path'].apply(lambda x: "_".join(os.path.basename(x).split("_")[:5]))
df=df.sort_values("file_path",ascending=False)
df=df.drop_duplicates("basepattern")
list_impr_files=df['file_path'].tolist()

print("len(list_impr_files)",len(list_impr_files))
logging.info("len(list_impr_files):"+str(len(list_impr_files)))

df_agg_impr_day_BL_LR=pd.DataFrame()
for file in list_impr_files:
    df=pd.read_csv(file,dtype=str,compression="gzip")
    df = df[df['Advertiser ID']=="8095847"]
    df = df[(df['Placement ID'].isin(list_placement_ids))]
    df = df[df['User ID']!='0']
    
    df_cols=df.columns.tolist()
    if 'Partner2 ID' not in df_cols:
        df_cols.insert(df_cols.index("Partner1 ID")+1,"Partner2 ID")
        df['Partner2 ID']=df['Partner1 ID']
        df=df[df_cols]
    df_agg_impr_day_BL_LR=df_agg_impr_day_BL_LR.append(df)
    
df_agg_impr_day_BL_LR=df_agg_impr_day_BL_LR.drop_duplicates()
print("Done of impression files aggregation:"+str(datetime.datetime.now()))
logging.info("Done of impression files aggregation:"+str(datetime.datetime.now()))


# In[8]:


df_agg_impr_day_BL_LR.shape


# In[6]:


# Click of the day
# Based on the date in the file name, not modifity time

path_click='/mnt/drv5/dcm_logs/clicks/'
list_click_files=glob.glob(path_click+"*.csv.gz")
list_click_files=[x for x in list_click_files if x.split("count7252_click_")[1][:8]==str(log_file_date).replace("-","")]

df=pd.DataFrame({"file_path":list_click_files})
df['basepattern']=df['file_path'].apply(lambda x: "_".join(os.path.basename(x).split("_")[:5]))
df=df.sort_values("file_path",ascending=False)
df=df.drop_duplicates("basepattern")
list_click_files=df['file_path'].tolist()

print("len(list_click_files)",len(list_click_files))
logging.info("len(list_click_files):"+str(len(list_click_files)))

df_agg_click_day_BL_LR=pd.DataFrame()
for file in list_click_files:
    df=pd.read_csv(file,dtype=str,compression="gzip")
    df = df[df['Advertiser ID']=="8095847"]
    df = df[(df['Placement ID'].isin(list_placement_ids))]
    df = df[df['User ID']!='0']
    
    df_cols=df.columns.tolist()
    if 'Partner2 ID' not in df_cols:
        df_cols.insert(df_cols.index("Partner1 ID")+1,"Partner2 ID")
        df['Partner2 ID']=df['Partner1 ID']
        df=df[df_cols]
    df_agg_click_day_BL_LR=df_agg_click_day_BL_LR.append(df)
    
df_agg_click_day_BL_LR=df_agg_click_day_BL_LR.drop_duplicates()
print("Done of click files aggregation:"+str(datetime.datetime.now()))
logging.info("Done of click files aggregation:"+str(datetime.datetime.now()))


# In[26]:


str(log_file_date).replace("_","")


# In[ ]:


# Activity of the day
# Based on the date in the file name, not modifity time

path_activity='/mnt/drv5/dcm_logs/activities/'
list_activity_files=glob.glob(path_activity+"*.csv.gz")
list_activity_files=[x for x in list_activity_files if x.split("count7252_activity_")[1][:8]==str(log_file_date).replace("-","")]

df=pd.DataFrame({"file_path":list_activity_files})
df['basepattern']=df['file_path'].apply(lambda x: "_".join(os.path.basename(x).split("_")[:5]))
df=df.sort_values("file_path",ascending=False)
df=df.drop_duplicates("basepattern")
list_activity_files=df['file_path'].tolist()

print("len(list_activity_files)",len(list_activity_files))
logging.info("len(list_activity_files):"+str(len(list_activity_files)))


df_agg_activity_day_BL_LR=pd.DataFrame()
for file in list_activity_files:
    df=pd.DataFrame()
    for df_chunck in pd.read_csv(file,dtype=str,compression="gzip",encoding="iso-8859-1",chunksize=10**6):

        df_chunck=df_chunck[df_chunck['Advertiser ID']=="8095847"]
        df_chunck=df_chunck[df_chunck['Placement ID'].isin(list_placement_ids)]
        df_chunck=df_chunck[df_chunck['User ID']!="0"]
        df=df.append(df_chunck)
        
    df_cols=df.columns.tolist()
    if 'Partner2 ID' not in df_cols:
        df_cols.insert(df_cols.index("Partner1 ID")+1,"Partner2 ID")
        df['Partner2 ID']=df['Partner1 ID']
        df=df[df_cols]
    df_agg_activity_day_BL_LR=df_agg_activity_day_BL_LR.append(df)
df_agg_activity_day_BL_LR=df_agg_activity_day_BL_LR.drop_duplicates()
print("Done of activity files aggregation:"+str(datetime.datetime.now()))
logging.info("Done of activity files aggregation:"+str(datetime.datetime.now()))


# In[ ]:


df_agg_activity_day_BL_LR.shape


# In[ ]:


print("df_agg_impr_day_BL_LR.shape "+str(df_agg_impr_day_BL_LR.shape))
print("df_agg_click_day_BL_LR.shape "+str(df_agg_click_day_BL_LR.shape))
print("df_agg_activity_day_BL_LR.shape "+str(df_agg_activity_day_BL_LR.shape))

logging.info("df_agg_impr_day_BL_LR.shape "+str(df_agg_impr_day_BL_LR.shape))
logging.info("df_agg_click_day_BL_LR.shape "+str(df_agg_click_day_BL_LR.shape))
logging.info("df_agg_activity_day_BL_LR.shape "+str(df_agg_activity_day_BL_LR.shape))


# In[ ]:


impr_local_path="/home/jian/lr_dcm_biglots/BL_DCM_upload/impressions_uploaded/"+"dcm_account7252_impression_"+str(log_file_date).replace("-","")+"_"+str(log_file_date).replace("-","")+"_agg_BL"+'.tsv.gz'
df_agg_impr_day_BL_LR.to_csv(impr_local_path,sep='\t',compression='gzip',index = False)

click_local_path="/home/jian/lr_dcm_biglots/BL_DCM_upload/clicks_uploaded/"+"dcm_account7252_click_"+str(log_file_date).replace("-","")+"_"+str(log_file_date).replace("-","")+"_agg_BL"+'.tsv.gz'
df_agg_click_day_BL_LR.to_csv(click_local_path,sep='\t',compression='gzip',index = False)

activity_local_path="/home/jian/lr_dcm_biglots/BL_DCM_upload/activities_uploaded/"+"dcm_account7252_activity_"+str(log_file_date).replace("-","")+"_"+str(log_file_date).replace("-","")+"_agg_BL"+'.tsv.gz'
df_agg_activity_day_BL_LR.to_csv(activity_local_path,sep='\t',compression='gzip',index = False)
print("3 fiels saved to the local folder"+str(datetime.datetime.now()))
logging.info("3 fiels saved to the local folder"+str(datetime.datetime.now()))


# In[ ]:


impr_local_path


# In[ ]:


transport = build_transport()
sftp = paramiko.SFTPClient.from_transport(transport)

impr_remote_path="/uploads/big_lots_measurement_dcm_impression/"+os.path.basename(impr_local_path)
sftp.put(impr_local_path,impr_remote_path)

click_remote_path="/uploads/big_lots_measurement_dcm_click/"+os.path.basename(click_local_path)
sftp.put(click_local_path,click_remote_path)

activity_remote_path="/uploads/big_lots_measurement_dcm_activity/"+os.path.basename(activity_local_path)
sftp.put(activity_local_path,activity_remote_path)

print("Done of uploading 3 files: "+str(datetime.datetime.now())+" | "+str(log_file_date))
logging.info("Done of uploading 3 files: "+str(datetime.datetime.now())+" | "+str(log_file_date))
sftp.close()
transport.close()


# # Downloading LR processed files

# In[ ]:


#### Download the processed DCM files from LiveRamp sftp download folders
dest_match = {
    'activity':'activities/',
    'impression':'impressions/',
    'click':'clicks/'
}

REMOTE_FOLDER = '/downloads/'

def get_modify_date(f_name, _sftp):
    try:
        st = _sftp.stat(f_name)
    except IOError:
        logging.INFO("failed to get information about"+file)
    else:
        return datetime.datetime.fromtimestamp(st.st_mtime).strftime('%Y-%m-%d')

def biglots_liveramp():
    host = "files.liveramp.com"
    port = 22
    transport = paramiko.Transport((host, port))

    password = "Biglots2018!"
    username = "lr-big-lots"
    transport.connect(username = username, password = password)
    sftp = paramiko.SFTPClient.from_transport(transport)

    files = []
    for f in sftp.listdir(REMOTE_FOLDER):
        mdate = get_modify_date(REMOTE_FOLDER+f, sftp)
        if mdate>=str((datetime.datetime.now()-datetime.timedelta(days=2)).date()):
            files.append(f)

    for f in files:
        try:
            f_type = f.split('_')[4]
            if f_type in dest_match.keys():
                dest_folder = '/home/jian/lr_dcm_biglots/BL_LR_download/'+dest_match[f_type]
                logging.info('Downloading file: '+str(f))
                sftp.get(REMOTE_FOLDER+f, dest_folder+f)
            else:
                dest_folder = '/home/jian/lr_dcm_biglots/BL_LR_download/others/'
                logging.info('Downloading file: '+str(f))
                sftp.get(REMOTE_FOLDER+f, dest_folder+f)
        except:
            dest_folder = '/home/jian/lr_dcm_biglots/BL_LR_download/others/'
            logging.info('Downloading file: '+str(f))
            sftp.get(REMOTE_FOLDER+f, dest_folder+f)
    return "Files downloaded "+str(datetime.datetime.now())
biglots_liveramp()

