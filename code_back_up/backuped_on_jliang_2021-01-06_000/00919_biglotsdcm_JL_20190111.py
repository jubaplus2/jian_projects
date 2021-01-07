
# coding: utf-8

# In[16]:


import pandas as pd
import numpy as np
import os
import time
time.sleep(60*60*12)
outputpath = '/mnt/drv5/lr_biglots_data/DCM/'
startdate = '20181201'
enddate = '20181231'


# In[5]:

Id_list_for_24_Dec_Urls=['7941165','7941168', '7941174', '7945962', '7945965', '7945968', '7945971', '7945974', '7945980', '7952846', '7999849', '7999855', '7999867', '8000752', '8000755', '8000758', '8000761', '8001436', '8001439', '8001442', '8001445', '8001451', '8001721', '8001727']


impfolder = '/home/spencer/dcm_log/activities/'
filelist = os.listdir(impfolder)
filelist = pd.DataFrame(filelist)
filelist = filelist[filelist[0].str.contains('activity')==True]
filelist['date'] = filelist[0].apply(lambda x:x.split('_')[3]).str[0:8]
filelist = filelist[(filelist['date']>=startdate)&(filelist['date']<=enddate)]


# In[ ]:


df_impressions = pd.DataFrame()

for i in list(filelist[0]):
    try:
        dfimp = pd.read_csv(impfolder+i,sep = ',',dtype = 'str')#,usecols = [0,1,2,3,4,5,6,7,8,53])
        dfimp = dfimp[(dfimp['Other Data'].str.contains('biglots', case=False)==True)|              (dfimp['Advertiser ID']=='8095847')]
        # dfimp = dfimp[dfimp['User ID']!='0']
        dfimp = dfimp[dfimp['Activity ID'].isin(Id_list_for_24_Dec_Urls)]
        # dfimp = dfimp[dfimp['Site ID (DCM)']!='4832446']
        dfimp.to_csv(outputpath + i[:-7]+'_biglots.tsv.gz',
                  sep = '\t',compression = 'gzip',index = False)
        print i, len(dfimp.index)
        #df_impressions = df_impressions.append(dfimp,ignore_index = True)
    except:
        print(i)
#df_impressions.to_csv(outputpath + 'activity_combined.csv',index = False)
