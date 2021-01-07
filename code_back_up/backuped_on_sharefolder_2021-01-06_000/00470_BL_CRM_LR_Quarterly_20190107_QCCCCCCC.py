
# coding: utf-8

# In[1]:

#To check the 1st date of SP's transaction 

import pandas as pd
import numpy as np
import os
import datetime
import logging
import hashlib
import gc
import glob
logging.basicConfig(filename='crmnewscore_QC_20190123.log', level=logging.INFO)
logging.info('Started')

samplerows = None
activemos = '2017-12-29'
lapsed = '2017-06-29'
lastdate = '2018-12-29'


folder_write = '/home/jian/Projects/Big_Lots/Live_Ramp/Quarterly_Update/checking_crm_newscore_20190107/'
try:
    os.stat(folder_write)
except:
    os.mkdir(folder_write)
    
# Adding control members


# In[2]:

#To check the 1st date

chunksize_num = 10**7
filename='/home/jian/Projects/Big_Lots/Live_Ramp/Quarterly_Update/crm_newscore_0922/combinedtransactions_0922.csv'
dftrans_before_20180922=pd.DataFrame()
count_i=0

for chunk in pd.read_csv(filename, chunksize=chunksize_num,dtype=str):
    chunk['total_transaction_amt']=chunk['total_transaction_amt'].astype(float)
    chunk['total_transaction_units']=chunk['total_transaction_units'].astype(float)
    chunk = chunk[['customer_id_hashed','transaction_date','transaction_time',
                   'transaction_id','location_id','total_transaction_units',
                   'total_transaction_amt']].drop_duplicates()
    dftrans_before_20180922=dftrans_before_20180922.append(chunk)
    count_i+=1
    print(count_i,datetime.datetime.now())
    

'''
dftrans_before_20180922 = pd.read_csv('/home/jian/Projects/Big_Lots/Live_Ramp/Quarterly_Update/crm_newscore_0922/combinedtransactions_0922.csv',dtype=str)
#dftrans = dftrans[dftrans['transaction_date']>=lapsed]
dftrans_before_20180922['total_transaction_amt']=dftrans_before_20180922['total_transaction_amt'].astype(float)
dftrans_before_20180922['total_transaction_units']=dftrans_before_20180922['total_transaction_units'].astype(float)

dftrans_before_20180922 = dftrans_before_20180922[['customer_id_hashed','transaction_date','transaction_time',
                   'transaction_id','location_id','total_transaction_units',
                   'total_transaction_amt']].drop_duplicates()
'''

del chunk

dftrans_before_20180922=dftrans_before_20180922.drop_duplicates()

print("Deduped",datetime.datetime.now())


# In[ ]:

def recursive_file_gen(my_root_dir):
    for root, dirs, files in os.walk(my_root_dir):
        for file in files:
            yield os.path.join(root, file)

weeks_after_20180922=[datetime.date(2018,9,29)+datetime.timedelta(days=x*7) for x in range(999)]
possible_recent_folders=["/home/jian/BigLots/MediaStorm_"+str(x)+"/" for x in weeks_after_20180922]
recent_file_list=[]
for dirc in possible_recent_folders:
    list_recent=[x for x in list(recursive_file_gen(dirc)) if ("DailySales" in x) & (".txt" in x) ]
    recent_file_list=recent_file_list+list_recent
recent_file_df=pd.DataFrame({"path":recent_file_list,"date":[datetime.datetime.strptime(x.split("DailySales")[1][:8],"%Y%m%d").date()-datetime.timedelta(days=3) for x in recent_file_list]},index=[x for x in range(len(recent_file_list))])


list_1_after_201806=[x for x in list(recursive_file_gen("/home/jian/BigLots/2018_by_weeks/")) if ("DailySales" in x) & (".txt" in x) ]
folder_date=[datetime.datetime.strptime(x.split("/")[len(x.split("/"))-2].split("_")[1],"%Y-%m-%d").date() for x in list_1_after_201806]
df_1_after_201806=pd.DataFrame({"date":folder_date,"path":list_1_after_201806},index=[x for x in range(len(list_1_after_201806))])
df_1_after_201806['date'].apply(lambda x: x.weekday()).unique()
df_1_after_201806=df_1_after_201806.sort_values("date").reset_index()
del df_1_after_201806['index']
new_dailysales_files=df_1_after_201806.append(recent_file_df)

new_dailysales_files=new_dailysales_files[new_dailysales_files['date']>datetime.date(2018,9,22)]
new_dailysales_files=new_dailysales_files[new_dailysales_files['date']<=datetime.date(2018,12,29)]

print(len(new_dailysales_files))
new_dailysales_files=new_dailysales_files['path'].unique().tolist()


# In[5]:

combined_rewards_transaction_after_20180922_agg=pd.DataFrame() 
count_i=1
print("Total Files: "+str(len(new_dailysales_files)))
for file_daily in new_dailysales_files:
    df=pd.read_table(file_daily,nrows = None,sep= '|',dtype =str)
    df['subclass_transaction_amt']=df['subclass_transaction_amt'].astype(float)
    df['subclass_transaction_units']=df['subclass_transaction_units'].astype(float)
    df=df[~pd.isnull(df['customer_id_hashed'])]
    df_sales=df.groupby(['location_id','transaction_dt','customer_id_hashed'])['subclass_transaction_amt','subclass_transaction_units'].sum().reset_index().rename(columns={"subclass_transaction_amt":"total_transaction_amt","subclass_transaction_units":"total_transaction_units"})
    df_trans=df[['location_id','transaction_dt','transaction_id','customer_id_hashed']].drop_duplicates().groupby(['location_id','transaction_dt','customer_id_hashed'])['transaction_id'].count().to_frame().reset_index().rename(columns={"transaction_id":"transactions"})
    df=pd.merge(df_sales,df_trans,on=['location_id','transaction_dt','customer_id_hashed'],how="left")
    combined_rewards_transaction_after_20180922_agg=combined_rewards_transaction_after_20180922_agg.append(df)
    print(count_i,"done",datetime.datetime.now())
    count_i+=1
    
combined_rewards_transaction_after_20180922_agg=combined_rewards_transaction_after_20180922_agg.rename(columns={"transaction_dt":"transaction_date"})

# combined_rewards_transaction_after_20180922_agg.to_csv("/home/jian/Projects/Big_Lots/Live_Ramp/Quarterly_Update/combined_agged_rewards_transactions_20180929_20181229.csv",index=False)


# In[10]:

#Getting the store for an id

df_2018_transaction_by_id_1=dftrans_before_20180922[dftrans_before_20180922['transaction_date'].apply(lambda x: x[:4]=="2018")]
df_2018_transaction_by_id_1=df_2018_transaction_by_id_1[df_2018_transaction_by_id_1['total_transaction_amt']>0]
df_2018_transaction_by_id_1=df_2018_transaction_by_id_1.groupby(['customer_id_hashed','location_id'])['total_transaction_amt'].count().to_frame().reset_index()
df_2018_transaction_by_id_1=df_2018_transaction_by_id_1.rename(columns={"total_transaction_amt":"trans"})


df_2018_transaction_by_id_2=combined_rewards_transaction_after_20180922_agg[combined_rewards_transaction_after_20180922_agg['total_transaction_amt']>0]
df_2018_transaction_by_id_2=df_2018_transaction_by_id_2.groupby(['customer_id_hashed','location_id'])['transactions'].sum().to_frame().reset_index().rename(columns={"transactions":"trans"})



# In[15]:

df_2018_transaction_by_id=df_2018_transaction_by_id_1.append(df_2018_transaction_by_id_2)
df_2018_transaction_by_id=df_2018_transaction_by_id.groupby(['customer_id_hashed','location_id'])['trans'].sum().to_frame().reset_index()
df_2018_transaction_by_id=df_2018_transaction_by_id.sort_values(['customer_id_hashed','trans'],ascending=[True,False]).drop_duplicates(['customer_id_hashed'])
df_2018_transaction_by_id.to_csv(folder_write+"id_by_store_based_on_2018_trans.csv",index=False)


# In[17]:

del df_2018_transaction_by_id_1
del df_2018_transaction_by_id_2
del df_2018_transaction_by_id
gc.collect()


# In[18]:


###get recency
dfrecency=combined_rewards_transaction_after_20180922_agg[['customer_id_hashed','transaction_date']].append(dftrans_before_20180922[['customer_id_hashed','transaction_date']]) #Allready combined
dfrecency = dfrecency[['customer_id_hashed','transaction_date']].drop_duplicates()
print (min(dfrecency['transaction_date']))
print (max(dfrecency['transaction_date']))
dfrecency = dfrecency.sort_values(['transaction_date'],ascending = False)
dfrecency = dfrecency.drop_duplicates('customer_id_hashed')
dfrecency.to_csv(folder_write + 'dfrecency.csv',index = False)


# In[19]:

dfrecency['transaction_date'] = pd.to_datetime(dfrecency['transaction_date'])
dfrecency['recency'] =  datetime.datetime.strptime(str(lastdate), '%Y-%m-%d').date() - dfrecency['transaction_date']
dfrecency['recency'] = dfrecency['recency'].apply(lambda x:x.days)
dfrecency['recency'] = np.ceil((dfrecency['recency']+1)/30)

dfrecency = dfrecency[['customer_id_hashed','recency']]
dfrecency = dfrecency.drop_duplicates('customer_id_hashed')
dfrecency.to_csv(folder_write + 'dfrecency2.csv',index = False)

print("saved dfrecency2: ",dfrecency.shape)


# In[20]:

dfiddetail = pd.read_csv('/home/jian/Projects/Big_Lots/Loyal_members/loyalty_register_data/combined_masterids_up_to_20181229_JL.csv',nrows = samplerows)
dfiddetail = dfiddetail.drop_duplicates('customer_id_hashed')
dfiddetail2 = pd.read_csv('/home/jian/Projects/Big_Lots/Loyal_members/loyalty_register_data/MediaStorm_Lapsed_Reward_Member_Master_from2014-08-26to2017-02-26.zip',
                     nrows = samplerows,dtype = 'str',sep = '|',
                       usecols = ['customer_id_hashed','email_address_hash','customer_zip_code'])
dfiddetail = dfiddetail.append(dfiddetail2,ignore_index = True)
dfiddetail = dfiddetail.drop_duplicates('customer_id_hashed')
dfiddetail = dfiddetail.drop_duplicates('email_address_hash')


# In[ ]:

dftrans_before_20180922['transactions'] = 1
dftrans_before_20180922 = dftrans_before_20180922[['customer_id_hashed','total_transaction_amt',
                   'total_transaction_units','transactions']].groupby(['customer_id_hashed']).sum().reset_index()
combined_rewards_transaction_after_20180922_agg=combined_rewards_transaction_after_20180922_agg[['customer_id_hashed','total_transaction_amt',
                   'total_transaction_units','transactions']].groupby(['customer_id_hashed']).sum().reset_index()


# In[ ]:

dftrans_before_20180922=dftrans_before_20180922.append(combined_rewards_transaction_after_20180922_agg) #Allready combined
dftrans_before_20180922 = dftrans_before_20180922[['customer_id_hashed','total_transaction_amt',
                   'total_transaction_units','transactions']].groupby(['customer_id_hashed']).sum().reset_index()

dftotal=dftrans_before_20180922 #Allready combined


dftotal = pd.merge(dftotal,dfrecency,on = 'customer_id_hashed',how='outer')
del dfrecency

gc.collect()


# In[ ]:

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

dftotal.to_csv(folder_write + 'dfrfm.csv',index = False)


# In[13]:


dftotal = pd.read_csv(folder_write + 'dfrfm.csv')


# In[14]:


dftotal = dftotal[['customer_id_hashed','frmindex']]


# In[15]:


dfrecency = pd.read_csv(folder_write + 'dfrecency.csv')
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
print(len(dfiddetail.index))


# In[18]:


print("totalids_trans:",len(dftotal.index))
dftotal = pd.merge(dftotal,dfiddetail,on = 'customer_id_hashed')
print("totalids_trans_mergewithmaster:",len(dftotal.index))


# In[20]:


zipmap = pd.read_csv('/home/jian/Projects/Big_Lots/New_TA/zips_in_new_ta/zip_with_ta_dma.csv',dtype = 'str')
zipmap['zipcodegroup'] = zipmap['revenue_flag']
zipmap = zipmap[['zip','zipcodegroup']].drop_duplicates('zip')
zipmap.columns = ['customer_zip_code','zipcodegroup']
dftotal = pd.merge(dftotal,zipmap,on ='customer_zip_code',how = 'left' )
print(dftotal['zipcodegroup'].unique())
dftotal['zipcodegroup'].fillna('T',inplace = True)
print(dftotal['zipcodegroup'].unique())


# In[21]:

dftotal.to_csv(folder_write + 'dfrfm_wemail.csv',index = False)
print("Final wemailcsv:",dftotal.shape)


# # Summary

# In[ ]:

# detailed_scores_df=pd.read_csv(folder_write + 'dfrfm_wemail.csv',dtype=str)
detailed_scores_df=dftotal
detailed_scores_df['frmindex']=detailed_scores_df['frmindex'].apply(lambda x: str(int(float(x))).zfill(2))
detailed_scores_df['customer_zip_code']=detailed_scores_df['customer_zip_code'].apply(lambda x: x.zfill(5))
detailed_scores_df['frmindex']=detailed_scores_df['frmindex'].apply(lambda x:"D"+x)


# In[ ]:

# Random 500000 ids as control


# In[4]:

import random
random.seed(1)
total_rows=len(detailed_scores_df)

test_all_df=pd.DataFrame()
control_all_df=pd.DataFrame()

i_counter=0

for comb,group in detailed_scores_df.groupby(['active','zipcodegroup','frmindex']):
    random_list=random.sample(range(len(group)), int(np.round(len(group)/total_rows*500000)))

    group=group.reset_index()
    del group['index']
    group=group.reset_index()
    df_control=group[group['index'].isin(random_list)]
    df_test=group[~group['index'].isin(random_list)]
    
    test_all_df=test_all_df.append(df_test)
    control_all_df=control_all_df.append(df_control)
    i_counter+=1
    print(i_counter,datetime.datetime.now())


# In[7]:

test_all_df['segment']="T_"+test_all_df['active']+"_chain_"+test_all_df['zipcodegroup']+"_"+test_all_df['frmindex']+"_2019Q1"
control_all_df['segment']="C_"+control_all_df['active']+"_chain_"+control_all_df['zipcodegroup']+"_"+control_all_df['frmindex']+"_2019Q1"


test_all_df['HML_group']=np.where(test_all_df['frmindex'].isin(['D01','D02','D03']),"H",
                                 np.where(test_all_df['frmindex'].isin(['D04','D05','D06']),"M","L"))
control_all_df['HML_group']=np.where(control_all_df['frmindex'].isin(['D01','D02','D03']),"H",
                                 np.where(control_all_df['frmindex'].isin(['D04','D05','D06']),"M","L"))

test_all_df['segment_new']="T_"+test_all_df['active']+test_all_df['zipcodegroup']+"_"+test_all_df['HML_group']+"_2019Q1"
control_all_df['segment_new']="C_"+control_all_df['active']+control_all_df['zipcodegroup']+"_"+control_all_df['HML_group']+"_2019Q1"


# In[8]:

test_all_df.to_csv(folder_write+"all_test.csv",index=False)
control_all_df.to_csv(folder_write+"all_control.csv",index=False)

folder_write_inner = folder_write+'/by_group/'
try:
    os.stat(folder_write_inner)
except:
    os.mkdir(folder_write_inner)


# In[ ]:




# In[9]:

i_counter=0
for seg,group in test_all_df.groupby(['segment']):
    group=group[['customer_id_hashed','email_address_hash','segment']]
    group.to_csv(folder_write_inner+seg+".csv",index=False)
    i_counter+=1
    print(i_counter,datetime.datetime.now())


# In[10]:

i_counter=0
for seg,group in control_all_df.groupby(['segment']):
    group=group[['customer_id_hashed','email_address_hash','segment']]
    group.to_csv(folder_write_inner+seg+".csv",index=False)
    i_counter+=1
    print(i_counter,datetime.datetime.now())

