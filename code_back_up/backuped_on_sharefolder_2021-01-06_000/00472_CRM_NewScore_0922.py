
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import os
import datetime
import logging
import hashlib
import gc
logging.basicConfig(filename='crmnewscore.log', level=logging.INFO)
logging.info('Started')


# In[2]:


samplerows = None
activemos = '2017-09-22'
lapsed = '2017-03-22'
lastdate = '2018-09-22'


# In[3]:


folderpath = '/home/nielsen/Spencer/BigLotsCRM/crm_newscore_0922/'


# In[4]:


dftrans = pd.read_csv('/home/nielsen/Spencer/BigLotsCRM/combinedfiles/combinedtransactions_0922.csv',
                     nrows = samplerows)


# In[5]:


#dftrans = dftrans[dftrans['transaction_date']>=lapsed]


# In[6]:


dftrans = dftrans[['customer_id_hashed','transaction_date','transaction_time',
                   'transaction_id','location_id','total_transaction_units',
                   'total_transaction_amt']].drop_duplicates()


# In[7]:


###get recency
dfrecency = dftrans[['customer_id_hashed','transaction_date']].drop_duplicates()
print (min(dfrecency['transaction_date']))
print (max(dfrecency['transaction_date']))
dfrecency = dfrecency.sort_values(['transaction_date'],ascending = False)
dfrecency = dfrecency.drop_duplicates('customer_id_hashed')
dfrecency.to_csv(folderpath + 'dfrecency.csv',index = False)

dfrecency['transaction_date'] = pd.to_datetime(dfrecency['transaction_date'])
dfrecency['recency'] =  datetime.datetime.strptime(str(lastdate), '%Y-%m-%d').date() - dfrecency['transaction_date']
dfrecency['recency'] = dfrecency['recency'].apply(lambda x:x.days)
dfrecency['recency'] = np.ceil((dfrecency['recency']+1)/30)

dfrecency = dfrecency[['customer_id_hashed','recency']]
dfrecency = dfrecency.drop_duplicates('customer_id_hashed')
dfrecency.to_csv(folderpath + 'dfrecency2.csv',index = False)


# In[8]:


dfiddetail = pd.read_csv('/home/nielsen/Spencer/BigLotsCRM/combinedfiles/masterids_0922.csv',nrows = samplerows)
dfiddetail = dfiddetail.drop_duplicates('customer_id_hashed')



# In[9]:


dfiddetail2 = pd.read_csv('/home/nielsen/Spencer/BigLotsCRM/inputdata/lapsed20140826_20170226/MediaStorm_Lapsed_Reward_Member_Master_from2014-08-26to2017-02-26.zip',
                     nrows = samplerows,dtype = 'str',sep = '|',
                       usecols = ['customer_id_hashed','email_address_hash','customer_zip_code'])


# In[10]:


dfiddetail = dfiddetail.append(dfiddetail2,ignore_index = True)
dfiddetail = dfiddetail.drop_duplicates('customer_id_hashed')


# In[11]:


dftrans['transactions'] = 1

dftotal = dftrans[['customer_id_hashed','total_transaction_amt',
                   'total_transaction_units','transactions']].groupby(['customer_id_hashed']).sum()
dftotal.reset_index(inplace = True)
dftotal = pd.merge(dftotal,dfrecency,on = 'customer_id_hashed',how='outer')

del dftrans, dfrecency
gc.collect()


# In[12]:


###rank the file, get decile
dftotal = dftotal.sort_values(['transactions','recency','total_transaction_amt'],ascending = [0,1,0])
dftotal.reset_index(drop = True, inplace = True)
dftotal.reset_index(inplace = True)
dftotal = dftotal.rename(columns = {'index':'Transindex'})

dftotal = dftotal.sort_values(['total_transaction_amt','recency','transactions'],ascending = [0,1,0])
dftotal.reset_index(drop = True, inplace = True)
dftotal.reset_index(inplace = True)
dftotal = dftotal.rename(columns = {'index':'Amtindex'})

dftotal = dftotal.sort_values(['recency','transactions','total_transaction_amt'],ascending = [1,0,0])
dftotal.reset_index(drop = True, inplace = True)
dftotal.reset_index(inplace = True)
dftotal = dftotal.rename(columns = {'index':'recencyindex'})

c_ids = len(dftotal.index)
logging.info('total customers from transaction and amt: ')
logging.info(c_ids)
c_ids = np.ceil(c_ids/5.0)

dftotal['Transindex'] = np.ceil((dftotal['Transindex']+1)/c_ids)
dftotal['Amtindex'] = np.ceil((dftotal['Amtindex']+1)/c_ids)
dftotal['recencyindex'] = np.ceil((dftotal['recencyindex']+1)/c_ids)

dftotal['RFM'] = dftotal['recencyindex']*100 + dftotal['Transindex']*10 + dftotal['Amtindex']
dftotal = dftotal.sort_values(['RFM','recency','transactions',
                               'total_transaction_amt'],ascending = [1,1,0,0])
dftotal.reset_index(drop = True, inplace = True)
dftotal.reset_index(inplace = True)
dftotal = dftotal.rename(columns = {'index':'frmindex'})
c_ids = len(dftotal.index)
c_ids = np.ceil(c_ids/10.0)
dftotal['frmindex'] = np.ceil((dftotal['frmindex']+1)/c_ids)

dftotal.to_csv(folderpath + 'dfrfm.csv',index = False)


# In[13]:


dftotal = pd.read_csv(folderpath + 'dfrfm.csv')


# In[14]:


dftotal = dftotal[['customer_id_hashed','frmindex']]


# In[15]:


dfrecency = pd.read_csv(folderpath + 'dfrecency.csv')
dfrecency['active'] = np.where(dfrecency['transaction_date']>=activemos,'active',
                    np.where(dfrecency['transaction_date']>=lapsed,'lapsed','other'))
dfrecency['active'].unique()


# In[16]:


dftotal = pd.merge(dftotal,dfrecency[['customer_id_hashed','active']],on = 'customer_id_hashed')


# In[17]:


dfiddetail['customer_zip_code'] = dfiddetail['customer_zip_code'].astype('str')
dfiddetail['customer_zip_code'] = dfiddetail['customer_zip_code'].str[0:5]
dfiddetail['customer_zip_code'].fillna('00000',inplace = True)
dfiddetail['customer_zip_code'] = dfiddetail['customer_zip_code'].apply(lambda x:x.zfill(5))
print len(dfiddetail.index)


# In[18]:


print "totalids_trans:",len(dftotal.index)
dftotal = pd.merge(dftotal,dfiddetail,on = 'customer_id_hashed')
print "totalids_trans_mergewithmaster:",len(dftotal.index)


# In[20]:


zipmap = pd.read_csv('/home/nielsen/Spencer/BigLotsCRM/inputdata/zip_with_ta_dma.csv',dtype = 'str')
zipmap['zipcodegroup'] = zipmap['revenue_flag']
zipmap = zipmap[['zip','zipcodegroup']].drop_duplicates('zip')
zipmap.columns = ['customer_zip_code','zipcodegroup']
dftotal = pd.merge(dftotal,zipmap,on ='customer_zip_code',how = 'left' )
print dftotal['zipcodegroup'].unique()
dftotal['zipcodegroup'].fillna('T',inplace = True)
print dftotal['zipcodegroup'].unique()


# In[21]:


dftotal.to_csv(folderpath + 'dfrfm_wemail.csv',index = False)
