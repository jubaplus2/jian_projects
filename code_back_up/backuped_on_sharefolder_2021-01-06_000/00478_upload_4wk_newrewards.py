
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np

# In[2]:


filelist = ['/home/nielsen/Spencer/BigLotsCRM/inputdata/MediaStorm_2018_12_1/MediaStormMasterBiWeekly20181204-130330-962.txt',
'/home/nielsen/Spencer/BigLotsCRM/inputdata/MediaStorm_2018_11_17/MediaStormMasterBiWeekly20181120-132340-144.txt']


# In[3]:


dfall = pd.DataFrame()
for i in filelist:
    df = pd.read_csv(i,nrows = None,sep= '|',usecols = ['customer_id_hashed','email_address_hash','customer_zip_code'],
                     dtype = 'str')
    dfall = dfall.append(df,ignore_index = True)
print len(dfall.index)
dfall = dfall.drop_duplicates('email_address_hash')
print len(dfall.index)


# In[ ]:


filepath = '/home/nielsen/Spencer/BigLotsCRM/segments_4/upload_newscore_0922/'
import os
filstlist = os.listdir(filepath)

for i in filstlist:
    df = pd.read_csv(filepath + i,usecols = ['email_address_hash'])
    dfall = dfall[~dfall['email_address_hash'].isin(list(df['email_address_hash']))]

print len(dfall.index)


# In[6]:


filepath = '/home/nielsen/Spencer/BigLotsCRM/segments_4/upload0712_sotf/'
import os
filstlist = os.listdir(filepath)
filstlist = [i for i in filstlist if 'Copy of 48 Stores_727.csv'!=i]
for i in filstlist:
    df = pd.read_csv(filepath + i,usecols = ['email_address_hash'])
    dfall = dfall[~dfall['email_address_hash'].isin(list(df['email_address_hash']))]

print len(dfall.index)


# In[7]:


filepath = '/home/nielsen/Spencer/BigLotsCRM/segments_4/upload1018/'
import os
filstlist = os.listdir(filepath)

for i in filstlist:
    df = pd.read_csv(filepath + i,usecols = ['email_address_hash'])
    dfall = dfall[~dfall['email_address_hash'].isin(list(df['email_address_hash']))]

print len(dfall.index)


# In[8]:


filepath = '/home/nielsen/Spencer/BigLotsCRM/segments_4/upload1116/'
import os
filstlist = os.listdir(filepath)

for i in filstlist:
    df = pd.read_csv(filepath + i,usecols = ['email_address_hash'])
    dfall = dfall[~dfall['email_address_hash'].isin(list(df['email_address_hash']))]

print len(dfall.index)


# In[9]:


dfall = dfall.drop_duplicates('email_address_hash')
dfall = dfall.drop_duplicates('customer_id_hashed')
print len(dfall.index)


# In[10]:


zipmap = pd.read_csv('/home/nielsen/Spencer/BigLotsCRM/inputdata/zip_with_ta_dma.csv',dtype = 'str')
zipmap['zipcodegroup'] = zipmap['revenue_flag']
zipmap = zipmap[['zip','zipcodegroup']].drop_duplicates('zip')
zipmap.columns = ['customer_zip_code','zipcodegroup']
dfall = pd.merge(dfall,zipmap,on ='customer_zip_code',how = 'left' )
print dfall['zipcodegroup'].unique()
dfall['zipcodegroup'].fillna('T',inplace = True)
print dfall['zipcodegroup'].unique()


# In[11]:


for i in ['P','S','T']:
    df = dfall[dfall['zipcodegroup'].isin([i])]
    df = df[['customer_id_hashed','email_address_hash','customer_zip_code']]
    df['segment'] = 'NewReward_120118_' + i
    print i,len(df.index)
    df.to_csv('/home/nielsen/Spencer/BigLotsCRM/segments_4/upload1201/NewReward_120118_'+i+'.csv',index = False)
