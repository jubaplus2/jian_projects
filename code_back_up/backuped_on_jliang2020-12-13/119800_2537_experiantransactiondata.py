
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import os
import datetime
import logging
import hashlib
logging.basicConfig(filename='combinedfiles.log', level=logging.INFO)
logging.info('Started')

samplerows = None
folderpath = '/home/nielsen/Spencer/BigLotsCRM/experian_file/'


# In[7]:


startdate = '2018-06-24'
enddate = '2018-07-22'


# In[32]:


dftrans = pd.read_csv('/home/nielsen/Spencer/BigLotsCRM/inputdata/MediaStorm_2018_06_16/MediaStormSalesBiWeekly.txt',
                      sep = '|',nrows = samplerows)
dftrans = dftrans.rename(columns = {'transaction_dt':'transaction_date'})


dftrans2 = pd.read_csv('/home/nielsen/Spencer/BigLotsCRM/inputdata/MediaStorm_2018_06_23/MediaStormSalesBiWeekly2018-06-01 -2018-06-02.zip',
                      sep = '|',nrows = samplerows,compression = 'zip')
dftrans2 = dftrans2.rename(columns = {'transaction_dt':'transaction_date'})
dftrans = dftrans.append(dftrans2,ignore_index = True)

dftrans2 = pd.read_csv('/home/nielsen/Spencer/BigLotsCRM/inputdata/MediaStorm_2018_06_30/MediaStormSalesBiWeekly.txt',
                      sep = '|',nrows = samplerows)
dftrans2 = dftrans2.rename(columns = {'transaction_dt':'transaction_date'})
dftrans = dftrans.append(dftrans2,ignore_index = True)

dftrans2 = pd.read_csv('/home/nielsen/Spencer/BigLotsCRM/inputdata/MediaStorm_2018_07_14/MediaStormSalesBiWeekly20180717-132936-272.txt',
                      sep = '|',nrows = samplerows)
dftrans2 = dftrans2.rename(columns = {'transaction_dt':'transaction_date'})
dftrans = dftrans.append(dftrans2,ignore_index = True)

dftrans2 = pd.read_csv('/home/nielsen/Spencer/BigLotsCRM/inputdata/MediaStorm_2018_07_28/MediaStormSalesBiWeekly20180731-131255-894.txt',
                      sep = '|',nrows = samplerows)
dftrans2 = dftrans2.rename(columns = {'transaction_dt':'transaction_date'})
dftrans = dftrans.append(dftrans2,ignore_index = True)

dftrans = dftrans[(dftrans['transaction_date']>=startdate)&                  (dftrans['transaction_date']<=enddate)]

print len(dftrans.index)
dftrans = dftrans[['customer_id_hashed','transaction_date','transaction_time',
                   'transaction_id','location_id','total_transaction_units',
                   'total_transaction_amt']].drop_duplicates()
print len(dftrans.index)


# In[17]:


dfmaster = pd.read_csv('/home/nielsen/Spencer/BigLotsCRM/combinedfiles/masterids_0714.csv',nrows = samplerows,
                      usecols = [0,1])
dfmaster2 = pd.read_csv('/home/nielsen/Spencer/BigLotsCRM/inputdata/MediaStorm_2018_07_28/MediaStormMasterBiWeekly20180731-130714-098.txt',
                       sep = '|',nrows = samplerows,usecols = [0,1])

dfmaster = dfmaster.append(dfmaster2,ignore_index = True)
dfmaster = dfmaster.drop_duplicates('customer_id_hashed')


# In[35]:

dftrans.to_csv(folderpath + 'BLTransactions_0624_0722_step0.csv',index = False)
print len(dftrans.index)
dftrans = pd.merge(dfmaster,dftrans,on = 'customer_id_hashed')
print len(dftrans.index)


# In[20]:


dftrans.to_csv(folderpath + 'BLTransactions_0624_0722.csv',index = False)
