
# coding: utf-8

# In[ ]:


import pandas as pd
import numpy as np
import os
import datetime
import logging
import hashlib
logging.basicConfig(filename='combinedfiles.log', level=logging.INFO)
logging.info('Started')


samplerows = None


folderpath = '/home/nielsen/Spencer/BigLotsCRM/combinedfiles/'


dfiddetail = pd.read_table('/home/nielsen/Spencer/BigLotsCRM/inputdata/MediaStormCustTot-hashed-email.txt',
                       header=None,nrows = samplerows,sep = ',',usecols=[0,1,5],dtype = 'str')
dfiddetail.columns = ['customer_id', 'email_address_hash','customer_zip_code']
dfiddetail['customer_id_hashed'] = dfiddetail['customer_id'].apply(lambda x:hashlib.sha256(x).hexdigest())
dfiddetail = dfiddetail[['customer_id_hashed','email_address_hash','customer_zip_code']]

dfiddetail2 = pd.read_csv('/home/nielsen/Spencer/BigLotsCRM/bl_data_0402/MediaStormCustomerTransactionTotals_2018-01-09_2018-03-31.txt',
                          nrows = samplerows,sep = ',',usecols=[0,1,5],dtype = 'str')
dfiddetail = dfiddetail.append(dfiddetail2,ignore_index = True)

dfiddetail2 = pd.read_csv('/home/nielsen/Spencer/BigLotsCRM/bl_data_0402/Existing Reward Member Master as of 2018-06-05.txt',
                          nrows = samplerows,sep = '|',usecols=[0,1,5],dtype = 'str')
dfiddetail = dfiddetail.append(dfiddetail2,ignore_index = True)

dfiddetail2 = pd.read_csv('/home/nielsen/Spencer/BigLotsCRM/bl_data_0402/New Reward Member Master as of 2018-06-05.txt',
                          nrows = samplerows,sep = '|',usecols=[0,1,5],dtype = 'str')
dfiddetail = dfiddetail.append(dfiddetail2,ignore_index = True)

dfiddetail2 = pd.read_csv('/home/nielsen/Spencer/BigLotsCRM/inputdata/MediaStorm_2018_06_30/New Reward Member Master as of 2018-07-03.txt',
                          nrows = samplerows,sep = '|',usecols=[0,1,5],dtype = 'str')
dfiddetail = dfiddetail.append(dfiddetail2,ignore_index = True)

dfiddetail2 = pd.read_csv('/home/nielsen/Spencer/BigLotsCRM/inputdata/MediaStorm_2018_07_14/MediaStormMasterBiWeekly20180717-132337-377.txt',
                          nrows = samplerows,sep = '|',usecols=[0,1,5],dtype = 'str')
dfiddetail = dfiddetail.append(dfiddetail2,ignore_index = True)



dfiddetail.to_csv(folderpath + 'masterids_0714.csv',index = False)


# In[ ]:


dftrans.to_csv(folderpath + 'combinedtransactions_0714.csv',index = False)
