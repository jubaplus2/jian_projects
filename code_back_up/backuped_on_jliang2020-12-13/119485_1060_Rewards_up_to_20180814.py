
# coding: utf-8

# In[59]:

import pandas as pd
import numpy as np
import os
import datetime
import logging
import hashlib


# # Master ID (Sing up info)

# In[60]:

samplerows = None


# In[61]:

folderpath = '/home/jian/Projects/Big_Lots/Loyal_members/loyalty_register_data/' # usecols=[0,1,5],
selected_columns = ['customer_id_hashed', 'email_address_hash','sign_up_location','customer_zip_code']
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


# In[62]:

def unique_list_len(x):
    y=len(set(x))
    return y
output_master=dfiddetail.groupby(['sign_up_location'])['customer_id_hashed'].agg(unique_list_len).to_frame().reset_index()
# output_master.to_csv(folderpath+"output/unique_id_signed_up_per_store_"+str(datetime.datetime.now().date())+".csv",index=False)
output_master=output_master.sort_values(['sign_up_location'])


# In[63]:

unique_id_master_list=dfiddetail['customer_id_hashed'].unique().tolist()


# # Sales unique id visits

# In[64]:

sales_from_SP=pd.read_csv("/home/jian/Projects/Big_Lots/Loyal_members/loyalty_sales_data/From_Sp/combinedtransactions_0811.csv",
                          dtype=str,nrows=samplerows,usecols=['customer_id_hashed','transaction_date','location_id'])
id_list_sales=sales_from_SP['customer_id_hashed'].unique().tolist()


# In[65]:

# sales_from_SP_in_Master=sales_from_SP[sales_from_SP['customer_id_hashed'].isin(unique_id_master_list)]
# MemError above
transaction_date=sorted(sales_from_SP['transaction_date'].unique().tolist())
purchase_dates_df=pd.DataFrame({"Date":transaction_date},index=range(len(transaction_date)))


# In[66]:

del sales_from_SP['transaction_date']


# In[67]:

consumption_output=sales_from_SP.groupby(['location_id'])['customer_id_hashed'].agg(unique_list_len).to_frame().reset_index()
consumption_output=consumption_output.sort_values(['location_id'])


# In[68]:

output_master=output_master.rename(columns={"sign_up_location":"store","customer_id_hashed":"signup_id count"})
consumption_output=consumption_output.rename(columns={"location_id":"store","customer_id_hashed":"consumption_id count"})
Merge_by_store=pd.merge(output_master,consumption_output,on="store",how="left")
Merge_by_store=Merge_by_store[Merge_by_store['store'].apply(lambda x: len(x))<=4]
Merge_by_store['store']=Merge_by_store['store'].astype(int)
Merge_by_store=Merge_by_store[Merge_by_store['store']<=6990]
Merge_by_store=Merge_by_store.sort_values('store')


# In[ ]:




# In[69]:

store_list_cols=['location_id','address_line_1','address_line_2','city_nm','state_nm','zip_cd']
store_list=pd.read_table("/home/jian/BigLots/static_files/MediaStormStores20180801-133641-576.txt",dtype=str,sep="|")
store_list=store_list[store_list_cols]

store_list_2=pd.read_table("/home/jian/BigLots/static_files/MediaStormStores_20180703.txt",dtype=str,sep="|")
store_list_2=store_list_2[~store_list_2['location_id'].isin(store_list['location_id'].unique().tolist())]
store_list_2=store_list_2[store_list_cols]
store_list=store_list.append(store_list_2)

store_list_2=pd.read_table("/home/jian/BigLots/static_files/MediaStormStoreList_Nov15.txt",dtype=str,sep="|")
store_list_2=store_list_2[~store_list_2['location_id'].isin(store_list['location_id'].unique().tolist())]
store_list_2=store_list_2[store_list_cols]
store_list=store_list.append(store_list_2)

store_list_2=pd.read_table("/home/jian/BigLots/static_files/MediaStormStoresList_0913.txt",dtype=str,sep="|")
store_list_2=store_list_2[~store_list_2['location_id'].isin(store_list['location_id'].unique().tolist())]
store_list_2=store_list_2[store_list_cols]
store_list=store_list.append(store_list_2)

store_list['zip_cd']=store_list['zip_cd'].apply(lambda x: x.split("-")[0].zfill(5))
store_list=store_list.rename(columns={"location_id":"store"})
store_list['store']=store_list['store'].astype(int)
Merge_by_store=pd.merge(Merge_by_store,store_list,on="store",how="outer")


# In[70]:

DMA=pd.read_excel("/home/jian/Docs/Geo_mapping/Zips by DMA by County16-17 nielsen.xlsx",dtype=str,skiprows=1,usecols=[0,2])
DMA.columns=['zip_cd','DMA']
DMA=DMA.drop_duplicates("zip_cd")
Merge_by_store=pd.merge(Merge_by_store,DMA,on="zip_cd",how="left")
Merge_by_store=Merge_by_store[['store','address_line_1','address_line_2','city_nm','state_nm','zip_cd','DMA','signup_id count','consumption_id count']]


# # Write to Excel

# In[ ]:

writer=pd.ExcelWriter(folderpath+"output/unique_id_by_store_"+str(datetime.datetime.now().date())+".xlsx",engine='xlsxwriter')
Merge_by_store.to_excel(writer,"distribution_table",index=False)
output_master.to_excel(writer,"register_id_by_location",index=False)
consumption_output.to_excel(writer,"purchase_id_by_location",index=False)
purchase_dates_df.to_excel(writer,"purchase_date_range",index=False)
writer.save()

