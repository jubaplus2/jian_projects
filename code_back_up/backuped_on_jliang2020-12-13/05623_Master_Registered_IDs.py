
# coding: utf-8

# In[1]:

import pandas as pd
import numpy as np
import os
import datetime
import logging
import hashlib


# In[34]:

samplerows = None


# In[35]:

folderpath = '/home/jian/Projects/Big_Lots/Loyal_members/loyalty_register_data/' # usecols=[0,1,5],
selected_columns = ['customer_id_hashed', 'email_address_hash','sign_up_location','customer_zip_code']


# In[44]:

dfiddetail = pd.read_table(folderpath+"/MediaStormCustTot-hashed-email.txt",
                       header=None,nrows = samplerows,sep = ',',dtype = 'str',usecols=[0,1,4,5])
dfiddetail.columns=['customer_id', 'email_address_hash','sign_up_location','customer_zip_code']
dfiddetail['customer_id_hashed'] = dfiddetail['customer_id'].apply(lambda x: hashlib.sha256(x.encode('utf-8')).hexdigest())
dfiddetail=dfiddetail[selected_columns]

#
dfiddetail2 = pd.read_csv(folderpath+'MediaStormCustomerTransactionTotals_2018-01-09_2018-03-31.txt',
                          nrows = samplerows,sep = ',',usecols=[0,1,4,5],dtype = 'str')
dfiddetail2=dfiddetail2[selected_columns]
dfiddetail = dfiddetail.append(dfiddetail2)

#
dfiddetail2 = pd.read_csv(folderpath+'Existing Reward Member Master as of 2018-06-05.txt',
                          nrows = samplerows,sep = '|',usecols=[0,1,4,5],dtype = 'str')
dfiddetail2=dfiddetail2[selected_columns]
dfiddetail = dfiddetail.append(dfiddetail2)

#
dfiddetail2 = pd.read_csv(folderpath+'New Reward Member Master as of 2018-06-05.txt',
                          nrows = samplerows,sep = '|',usecols=[0,1,4,5],dtype = 'str')
dfiddetail2=dfiddetail2[selected_columns]
dfiddetail = dfiddetail.append(dfiddetail2)

#
dfiddetail2 = pd.read_csv(folderpath+'New Reward Member Master as of 2018-07-03.txt',
                          nrows = samplerows,sep = '|',usecols=[0,1,4,5],dtype = 'str')
dfiddetail2=dfiddetail2[selected_columns]
dfiddetail = dfiddetail.append(dfiddetail2)

#
dfiddetail2 = pd.read_csv(folderpath+'MediaStormMasterBiWeekly20180717-132337-377.txt',
                          nrows = samplerows,sep = '|',usecols=[0,1,4,5],dtype = 'str')
dfiddetail2=dfiddetail2[selected_columns]
dfiddetail = dfiddetail.append(dfiddetail2)

#
dfiddetail2 = pd.read_csv(folderpath+'MediaStormMasterBiWeekly20180731-130714-098.txt',
                          nrows = samplerows,sep = '|',usecols=[0,1,4,5],dtype = 'str')
dfiddetail2=dfiddetail2[selected_columns]
dfiddetail = dfiddetail.append(dfiddetail2)

#
dfiddetail2 = pd.read_csv(folderpath+'MediaStormMasterBiWeekly20180814-130703-491.txt',
                          nrows = samplerows,sep = '|',usecols=[0,1,4,5],dtype = 'str')
dfiddetail2=dfiddetail2[selected_columns]
dfiddetail = dfiddetail.append(dfiddetail2)

#
dfiddetail=dfiddetail[['customer_id_hashed','sign_up_location','customer_zip_code']].drop_duplicates()



# In[49]:

def unique_list_len(x):
    y=len(set(x))
    return y
output=dfiddetail.groupby(['sign_up_location'])['customer_id_hashed'].agg(unique_list_len).to_frame().reset_index()
output.to_csv(folderpath+"output/unique_id_signed_up_per_store_"+str(datetime.datetime.now().date())+".csv",index=False)

