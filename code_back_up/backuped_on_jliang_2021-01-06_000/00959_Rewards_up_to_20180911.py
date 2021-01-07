
# coding: utf-8

# In[1]:

import pandas as pd
import numpy as np
import os
import datetime
import logging
import hashlib
import gc
logging.basicConfig(filename='rewards_py_20180912.log', level=logging.INFO)
datetime.datetime.now()


# In[ ]:




# In[ ]:




# In[2]:

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
store_list['store']=store_list['store'].astype(str)
store_list=store_list[~store_list['store'].isin(['145','6990'])]
inclusion_store_list_set=set(store_list['store'].unique().tolist())
del store_list_2


# # Master ID (Sing up info)

# In[3]:

samplerows = None


# In[4]:

folderpath = '/home/jian/Projects/Big_Lots/Loyal_members/loyalty_register_data/' # usecols=[0,1,5],
selected_columns = ['customer_id_hashed', 'email_address_hash','sign_up_location','customer_zip_code']
dfiddetail = pd.read_table(folderpath+"/MediaStormCustTot-hashed-email.txt",
                       header=None,nrows = samplerows,sep = ',',dtype = 'str',usecols=[0,1,4,5])
dfiddetail.columns=['customer_id', 'email_address_hash','sign_up_location','customer_zip_code']
dfiddetail['customer_id_hashed'] = dfiddetail['customer_id'].apply(lambda x: hashlib.sha256(x.encode('utf-8')).hexdigest())
dfiddetail=dfiddetail[selected_columns]
dfiddetail=dfiddetail[dfiddetail['sign_up_location'].isin(inclusion_store_list_set)]
#
dfiddetail2 = pd.read_csv(folderpath+'MediaStormCustomerTransactionTotals_2018-01-09_2018-03-31.txt',
                          nrows = samplerows,sep = ',',usecols=[0,1,4,5],dtype = 'str')
dfiddetail2=dfiddetail2[selected_columns]
dfiddetail2['sign_up_location']=dfiddetail2['sign_up_location'].fillna("nan")
dfiddetail2=dfiddetail2[dfiddetail2['sign_up_location'].isin(inclusion_store_list_set)]
dfiddetail = dfiddetail.append(dfiddetail2)

#
dfiddetail2 = pd.read_csv(folderpath+'Existing Reward Member Master as of 2018-06-05.txt',
                          nrows = samplerows,sep = '|',usecols=[0,1,4,5],dtype = 'str')
dfiddetail2=dfiddetail2[selected_columns]
dfiddetail2['sign_up_location']=dfiddetail2['sign_up_location'].fillna("nan")
dfiddetail2=dfiddetail2[dfiddetail2['sign_up_location'].isin(inclusion_store_list_set)]
dfiddetail = dfiddetail.append(dfiddetail2)

#
dfiddetail2 = pd.read_csv(folderpath+'New Reward Member Master as of 2018-06-05.txt',
                          nrows = samplerows,sep = '|',usecols=[0,1,4,5],dtype = 'str')
dfiddetail2=dfiddetail2[selected_columns]
dfiddetail2['sign_up_location']=dfiddetail2['sign_up_location'].fillna("nan")
dfiddetail2=dfiddetail2[dfiddetail2['sign_up_location'].isin(inclusion_store_list_set)]
dfiddetail = dfiddetail.append(dfiddetail2)

#
dfiddetail2 = pd.read_csv(folderpath+'New Reward Member Master as of 2018-07-03.txt',
                          nrows = samplerows,sep = '|',usecols=[0,1,4,5],dtype = 'str')
dfiddetail2=dfiddetail2[selected_columns]
dfiddetail2['sign_up_location']=dfiddetail2['sign_up_location'].fillna("nan")
dfiddetail2=dfiddetail2[dfiddetail2['sign_up_location'].isin(inclusion_store_list_set)]
dfiddetail = dfiddetail.append(dfiddetail2)

#
dfiddetail2 = pd.read_csv(folderpath+'MediaStormMasterBiWeekly20180717-132337-377.txt',
                          nrows = samplerows,sep = '|',usecols=[0,1,4,5],dtype = 'str')
dfiddetail2=dfiddetail2[selected_columns]
dfiddetail2['sign_up_location']=dfiddetail2['sign_up_location'].fillna("nan")
dfiddetail2=dfiddetail2[dfiddetail2['sign_up_location'].isin(inclusion_store_list_set)]
dfiddetail = dfiddetail.append(dfiddetail2)

#
dfiddetail2 = pd.read_csv(folderpath+'MediaStormMasterBiWeekly20180731-130714-098.txt',
                          nrows = samplerows,sep = '|',usecols=[0,1,4,5],dtype = 'str')
dfiddetail2=dfiddetail2[selected_columns]
dfiddetail2['sign_up_location']=dfiddetail2['sign_up_location'].fillna("nan")
dfiddetail2=dfiddetail2[dfiddetail2['sign_up_location'].isin(inclusion_store_list_set)]
dfiddetail = dfiddetail.append(dfiddetail2)

#
dfiddetail2 = pd.read_csv(folderpath+'MediaStormMasterBiWeekly20180814-130703-491.txt',
                          nrows = samplerows,sep = '|',usecols=[0,1,4,5],dtype = 'str')
dfiddetail2=dfiddetail2[selected_columns]
dfiddetail2['sign_up_location']=dfiddetail2['sign_up_location'].fillna("nan")
dfiddetail2=dfiddetail2[dfiddetail2['sign_up_location'].isin(inclusion_store_list_set)]
dfiddetail = dfiddetail.append(dfiddetail2)

#
dfiddetail=dfiddetail[['customer_id_hashed','sign_up_location','customer_zip_code']].drop_duplicates()
logging.info("Finished read of singing up data, with rows of "+str(dfiddetail.shape[0]))
del dfiddetail2


# In[5]:

def unique_list_len(x):
    y=len(set(x))
    return y

output_master=dfiddetail.groupby(['sign_up_location'])['customer_id_hashed'].agg(unique_list_len).to_frame().reset_index()
# output_master.to_csv(folderpath+"output/unique_id_signed_up_per_store_"+str(datetime.datetime.now().date())+".csv",index=False)
output_master=output_master.sort_values(['sign_up_location'])


# In[6]:

unique_id_master_list_set=set(dfiddetail['customer_id_hashed'].unique().tolist())


# # Sales unique id visits

# In[7]:
sales_from_SP=pd.read_csv("/home/jian/Projects/Big_Lots/Loyal_members/loyalty_sales_data/From_Sp/combinedtransactions_0811.csv",dtype=str,nrows=samplerows,usecols=['customer_id_hashed','transaction_date','transaction_id','location_id','total_transaction_amt'])
sales_from_SP=sales_from_SP.drop_duplicates()

sales_from_SP=sales_from_SP[['customer_id_hashed','transaction_date','location_id','total_transaction_amt']]

sales_from_SP['total_transaction_amt']=sales_from_SP['total_transaction_amt'].astype(float)

sales_from_SP=sales_from_SP[sales_from_SP['total_transaction_amt']>0]

del sales_from_SP['total_transaction_amt']

sales_from_SP=sales_from_SP[sales_from_SP['location_id'].isin(inclusion_store_list_set)]

id_list_sales_set=set(sales_from_SP['customer_id_hashed'].unique().tolist())

logging.info("Finished read of transaction data from spencer, with rows of "+str(sales_from_SP.shape[0]))


# In[8]:

transaction_date=sorted(sales_from_SP['transaction_date'].unique().tolist())
purchase_dates_df=pd.DataFrame({"Date":transaction_date},index=range(len(transaction_date)))


# In[9]:

del sales_from_SP['transaction_date']
gc.collect()


# In[10]:

logging.info("Start of 1st task"+str(datetime.datetime.now()))
no_signingup_df=sales_from_SP[~sales_from_SP['customer_id_hashed'].isin(unique_id_master_list_set)]

output1_trans_group=sales_from_SP.groupby(['customer_id_hashed'])['location_id'].count().to_frame().reset_index()

id_transa_group=output1_trans_group.copy()
id_transa_group=id_transa_group.rename(columns={"location_id":"transaction_group"})

id_transa_group['transaction_group']=np.where(id_transa_group['transaction_group']>10,"10+",id_transa_group['transaction_group'])
id_transa_group['transaction_group']=id_transa_group['transaction_group'].astype(str)


logging.info("Done of groupby sales_from_SP "+str(datetime.datetime.now()))
rewards_set_1=set(output1_trans_group[output1_trans_group['location_id']==1]['customer_id_hashed'].tolist())
logging.info("Done of set 1 "+str(datetime.datetime.now()))
rewards_set_2=set(output1_trans_group[output1_trans_group['location_id']==2]['customer_id_hashed'].tolist())
rewards_set_3=set(output1_trans_group[output1_trans_group['location_id']==3]['customer_id_hashed'].tolist())
rewards_set_4=set(output1_trans_group[output1_trans_group['location_id']==4]['customer_id_hashed'].tolist())
rewards_set_5=set(output1_trans_group[output1_trans_group['location_id']==5]['customer_id_hashed'].tolist())
logging.info("3 "+str(datetime.datetime.now()))
rewards_set_6=set(output1_trans_group[output1_trans_group['location_id']==6]['customer_id_hashed'].tolist())
rewards_set_7=set(output1_trans_group[output1_trans_group['location_id']==7]['customer_id_hashed'].tolist())
rewards_set_8=set(output1_trans_group[output1_trans_group['location_id']==8]['customer_id_hashed'].tolist())
rewards_set_9=set(output1_trans_group[output1_trans_group['location_id']==9]['customer_id_hashed'].tolist())
rewards_set_10=set(output1_trans_group[output1_trans_group['location_id']==10]['customer_id_hashed'].tolist())
rewards_set_10_Plus=set(output1_trans_group[output1_trans_group['location_id']>10]['customer_id_hashed'].tolist())
rewards_set_0=set([x for x in unique_id_master_list_set if x not in id_list_sales_set])
logging.info("Done of set all "+str(datetime.datetime.now()))



output1_trans_group=output1_trans_group.rename(columns={"location_id":"transaction_count"})
output1_trans_group['transaction_count']=np.where(output1_trans_group['transaction_count']>10,"10+",output1_trans_group['transaction_count'])
output1_trans_group=output1_trans_group.groupby(['transaction_count'])['customer_id_hashed'].agg(unique_list_len).to_frame().reset_index()
logging.info("Done of groupby unique id")

output1_trans_group=output1_trans_group.rename(columns={"transaction_count":"transaction_group","customer_id_hashed":"count_unique_ids"})
output1_trans_group['transaction_group']=output1_trans_group['transaction_group'].astype(str)
output1_trans_group=pd.DataFrame({"transaction_group":"0","count_unique_ids":len(rewards_set_0)},index=[0]).append(output1_trans_group)

logging.info("Done of 1st task"+str(datetime.datetime.now()))
output1_trans_group=output1_trans_group[['transaction_group','count_unique_ids']]

#
output1_trans_group_new_trans=pd.merge(sales_from_SP,id_transa_group,on="customer_id_hashed",how="outer")
output1_trans_group_new_trans=output1_trans_group_new_trans.groupby(['transaction_group'])['customer_id_hashed'].count().to_frame().reset_index()
output1_trans_group_new_trans=output1_trans_group_new_trans.rename(columns={"customer_id_hashed":"total_transactions"})

output1_trans_group=pd.merge(output1_trans_group,output1_trans_group_new_trans,on="transaction_group",how="outer")
output1_trans_group=output1_trans_group.fillna(0)
#

output1_trans_group.to_csv("/home/jian/Projects/Big_Lots/Loyal_members/Rewards_Members_Distritbution_Analysis/output1.csv",index=False)


# In[11]:

gc.collect()
output2_unique_id_by_store=pd.DataFrame({"store":list(inclusion_store_list_set)},index=list(inclusion_store_list_set))
for i in range(10):
    k=i+1
    df=sales_from_SP[sales_from_SP['customer_id_hashed'].isin(locals()['rewards_set_'+str(k)])]
    df=df.groupby(['location_id'])['customer_id_hashed'].agg(unique_list_len).to_frame().reset_index()
    df=df.rename(columns={"location_id":"store","customer_id_hashed":"id_counts_transacted_"+str(k)})
    output2_unique_id_by_store=pd.merge(output2_unique_id_by_store,df,on="store",how="left")
    logging.info("Done of 2nd task, transaction freq "+str(k)+" "+str(datetime.datetime.now()))
    del locals()['rewards_set_'+str(k)]
    
#10+
df=sales_from_SP[sales_from_SP['customer_id_hashed'].isin(rewards_set_10_Plus)]
df=df.groupby(['location_id'])['customer_id_hashed'].agg(unique_list_len).to_frame().reset_index()
df=df.rename(columns={"location_id":"store","customer_id_hashed":"id_counts_transacted_10+"})
output2_unique_id_by_store=pd.merge(output2_unique_id_by_store,df,on="store",how="left")

logging.info("Done of 2nd task "+str(datetime.datetime.now()))
output2_unique_id_by_store['store']=output2_unique_id_by_store['store'].astype(int)
output2_unique_id_by_store=output2_unique_id_by_store.sort_values("store",ascending=True)
output2_unique_id_by_store=output2_unique_id_by_store.fillna(0)
output2_unique_id_by_store.to_csv("/home/jian/Projects/Big_Lots/Loyal_members/Rewards_Members_Distritbution_Analysis/output2.csv",index=False)


# In[12]:

logging.info("Start of taks3 "+str(datetime.datetime.now()))
def id_set(x):
    y=set(x)
    return y
output3_unique_id_by_store=sales_from_SP.groupby(['customer_id_hashed'])['location_id'].agg(unique_list_len).to_frame().reset_index()
output3_unique_id_by_store['location_id']=np.where(output3_unique_id_by_store['location_id']>10,"10+",output3_unique_id_by_store['location_id'])
output3_unique_id_by_store['location_id']=output3_unique_id_by_store['location_id'].astype(str)

output3_unique_id_by_store=output3_unique_id_by_store.groupby(['location_id'])['customer_id_hashed'].agg(id_set).to_frame().reset_index()
output3_unique_id_by_store=output3_unique_id_by_store.rename(columns={"location_id":"went_uniq_store_group","customer_id_hashed":"id_list"})
output3_unique_id_by_store['ID_counts']=output3_unique_id_by_store['id_list'].apply(lambda x: len(x))

transaction_id=pd.DataFrame()
for i in range(len(output3_unique_id_by_store)):
    store_group=output3_unique_id_by_store['went_uniq_store_group'][i]
    id_set=output3_unique_id_by_store['id_list'][i]
    len_trans=len(sales_from_SP[sales_from_SP['customer_id_hashed'].isin(id_set)])
    df=pd.DataFrame({"went_uniq_store_group":store_group,"trnasactions_count":len_trans},index=[0])
    transaction_id=transaction_id.append(df)
    
del output3_unique_id_by_store['id_list']

output3_unique_id_by_store=pd.merge(output3_unique_id_by_store,transaction_id,on="went_uniq_store_group",how="left")
logging.info("Done of taks3 "+str(datetime.datetime.now()))



# In[13]:

consumption_output=sales_from_SP.groupby(['location_id'])['customer_id_hashed'].agg(unique_list_len).to_frame().reset_index()
consumption_output=consumption_output.sort_values(['location_id'])


# In[14]:

output_master=output_master.rename(columns={"sign_up_location":"store","customer_id_hashed":"signup_id count"})
consumption_output=consumption_output.rename(columns={"location_id":"store","customer_id_hashed":"consumption_id count"})
Merge_by_store=pd.merge(output_master,consumption_output,on="store",how="left")
Merge_by_store['store']=Merge_by_store['store'].astype(int)
Merge_by_store=Merge_by_store.sort_values('store')


# In[15]:

DMA=pd.read_excel("/home/jian/Docs/Geo_mapping/Zips by DMA by County16-17 nielsen.xlsx",dtype=str,skiprows=1,usecols=[0,2])
DMA.columns=['zip_cd','DMA']
DMA=DMA.drop_duplicates("zip_cd")
store_list['store']=store_list['store'].astype(int)
Merge_by_store=pd.merge(Merge_by_store,store_list,on="store",how="outer")
Merge_by_store=pd.merge(Merge_by_store,DMA,on="zip_cd",how="left")
Merge_by_store=Merge_by_store[['store','address_line_1','address_line_2','city_nm','state_nm','zip_cd','DMA','signup_id count','consumption_id count']]


# # Write to Excel

# In[16]:

outputfolder="/home/jian/Projects/Big_Lots/Loyal_members/Rewards_Members_Distritbution_Analysis/output/"
writer=pd.ExcelWriter(outputfolder+"BL_unique_id_by_store_"+str(datetime.datetime.now().date())+".xlsx",engine='xlsxwriter')
output1_trans_group.to_excel(writer,"output1_trans_group",index=False)
output2_unique_id_by_store.to_excel(writer,"output2_unique_id_by_store",index=False)
output3_unique_id_by_store.to_excel(writer,"output3_id_transactions",index=False)
Merge_by_store.to_excel(writer,"distribution_table",index=False)
output_master.to_excel(writer,"register_id_by_location",index=False)
consumption_output.to_excel(writer,"purchase_id_by_location",index=False)
purchase_dates_df.to_excel(writer,"purchase_date_range",index=False)
writer.save()


# In[ ]:



