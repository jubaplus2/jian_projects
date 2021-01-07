
# coding: utf-8

# In[1]:

# V5
# upto 20190330
# merge email first
# then calculate decile
# Remove 18+
# Added the P,L,18+, no quadrants

# In other words: all the same, except for the time range
import pandas as pd
import numpy as np
import os
import datetime
import logging
import hashlib
import gc
import glob
logging.basicConfig(filename='BL_CRM_LR_Quarterly_upto20190330_Final_20190422.log', level=logging.INFO)
logging.info('Started')

samplerows = None

lastdate = datetime.date(2019,3,30) # Recent Saturday
active_Sunday = str(lastdate-datetime.timedelta(days=52*7-1))
# active_Sunday = "2017-12-29"
lapsed_Sunday = str(lastdate-datetime.timedelta(days=52*7*1.5-1))
# lapsed_Sunday = "2017-06-29"
Beginning_18_months_ago=str(lastdate-datetime.timedelta(days=52*7*1.5-1))
# Beginning_18_months_ago = "2017-06-29"

lastdate=str(lastdate)
print("Lapsed Start on: "+lapsed_Sunday) #>=
print("Active Start on: "+active_Sunday) #>=
print("Store Allocation Starting on: "+Beginning_18_months_ago) #>=
print("Last Saturday: "+lastdate) #<=

def recrusive_file_gen(root_folder):
    for root, dirs, files in os.walk(root_folder):
        for file in files:
            yield os.path.join(root, file)
            
folder_write = '/home/jian/Projects/Big_Lots/Live_Ramp/Quarterly_Update_2019Q2/output_final_20190422/'
try:
    os.stat(folder_write)
except:
    os.mkdir(folder_write)
    
os.getcwd()


# In[2]:

chunksize_num = 10**7
filename='/home/jian/Projects/Big_Lots/Live_Ramp/Quarterly_Update_2019Q1/crm_newscore_0922/combinedtransactions_0922.csv'
dftrans_before_20180922=pd.DataFrame()
count_i=0

for chunk in pd.read_csv(filename, chunksize=chunksize_num,dtype=str,usecols=['customer_id_hashed','transaction_date','transaction_time',
                   'transaction_id','location_id','total_transaction_amt'],nrows=samplerows): #Add back the transaction info,,
    chunk['total_transaction_amt']=chunk['total_transaction_amt'].astype(float)
    chunk = chunk.drop_duplicates()
    
    dftrans_before_20180922=dftrans_before_20180922.append(chunk)
    count_i+=1
    print(count_i,datetime.datetime.now())


del chunk
print("Earliest Date:" + str(dftrans_before_20180922['transaction_date'].min()))
print("Latest Date:" + str(dftrans_before_20180922['transaction_date'].max()))
dftrans_before_20180922=dftrans_before_20180922.drop_duplicates()

logging.info("Deduped: "+str(datetime.datetime.now()))
del dftrans_before_20180922['transaction_time']

gc.collect()


# In[3]:

# Up to 2019-03-30
# All item level data, weekly and the 1-time transfered historical data
historical_daily_data_folder="/home/jian/BigLots/hist_daily_data_itemlevel_decompressed/"
historical_daily_data_list=list(recrusive_file_gen(historical_daily_data_folder))
historical_daily_data_list=[x for x in historical_daily_data_list if (".txt" in x) & ("DailySales" in x)]
historical_daily_df=pd.DataFrame({"file_path":historical_daily_data_list})
historical_daily_df['week_end_dt']=historical_daily_df['file_path'].apply(lambda x: x.split(".")[0].split("MediaStormDailySalesHistory")[1])
historical_daily_df['week_end_dt']=historical_daily_df['week_end_dt'].apply(lambda x: datetime.datetime.strptime(x,"%Y%m%d").date())
historical_daily_df=historical_daily_df[historical_daily_df['week_end_dt']<=datetime.date(2019,2,12)]
historical_daily_df=historical_daily_df[historical_daily_df['week_end_dt']>datetime.date(2018,9,22)]
print(historical_daily_df.shape)
print("Min_Date: "+str(historical_daily_df['week_end_dt'].min()))
print("Max_Date: "+str(historical_daily_df['week_end_dt'].max()))


# In[4]:


new_daily_data_folder="/home/jian/BigLots/2019_by_weeks/"
new_daily_data_list=list(recrusive_file_gen(new_daily_data_folder))
new_daily_data_list=[x for x in new_daily_data_list if (".txt" in x) & ("DailySales" in x)]
new_daily_data_list=[x for x in new_daily_data_list if "hist" not in x]

new_daily_df=pd.DataFrame({"file_path":new_daily_data_list})

new_daily_df['week_end_dt']=new_daily_df['file_path'].apply(lambda x: x.split(".")[0].split("2019_by_weeks/MediaStorm_")[1][:10])
new_daily_df['week_end_dt']=new_daily_df['week_end_dt'].apply(lambda x: datetime.datetime.strptime(x,"%Y-%m-%d").date())
new_daily_df=new_daily_df[new_daily_df['week_end_dt']>historical_daily_df['week_end_dt'].max()]
new_daily_df=new_daily_df[new_daily_df['week_end_dt']<=datetime.date(2019,3,30)]
print(new_daily_df.shape)
print("Min_Date: "+str(new_daily_df['week_end_dt'].min()))
print("Max_Date: "+str(new_daily_df['week_end_dt'].max()))

daily_df_file_after_20180922=historical_daily_df.append(new_daily_df)
new_dailysales_files=daily_df_file_after_20180922['file_path'].tolist()


# In[5]:

combined_rewards_transaction_after_20180922_agg=pd.DataFrame() 
count_i=1
print("Total Files: "+str(len(new_dailysales_files)))
for file_daily in new_dailysales_files:
    df=pd.read_table(file_daily,sep= '|',dtype =str,nrows=samplerows,
                     usecols=['customer_id_hashed','transaction_dt','transaction_id','location_id','item_transaction_amt'])
    df=df[~pd.isnull(df['customer_id_hashed'])]
    df['item_transaction_amt']=df['item_transaction_amt'].astype(float)
    df=df.groupby(['customer_id_hashed','transaction_dt','transaction_id','location_id'])['item_transaction_amt'].sum().to_frame().reset_index()
    df=df.drop_duplicates()

    
    combined_rewards_transaction_after_20180922_agg=combined_rewards_transaction_after_20180922_agg.append(df)
    print(count_i,"done",datetime.datetime.now())
    count_i+=1
del df
gc.collect()
combined_rewards_transaction_after_20180922_agg=combined_rewards_transaction_after_20180922_agg.rename(columns={"transaction_dt":"transaction_date"})


# In[6]:

gc.collect()

combined_rewards_transaction_after_20180922_agg=combined_rewards_transaction_after_20180922_agg.groupby(['customer_id_hashed','transaction_date','transaction_id','location_id'])['item_transaction_amt'].sum().to_frame().reset_index()
combined_rewards_transaction_after_20180922_agg=combined_rewards_transaction_after_20180922_agg.rename(columns={"item_transaction_amt":"total_transaction_amt"})

all_rewards_since_201606=dftrans_before_20180922.append(combined_rewards_transaction_after_20180922_agg)

del dftrans_before_20180922
del combined_rewards_transaction_after_20180922_agg
gc.collect()


# In[7]:

all_rewards_since_201606.to_csv("/home/jian/Projects/Big_Lots/Live_Ramp/Quarterly_Update_2019Q2/BL_Rewards_Transactions_20160626_to_20190330.csv")
gc.collect()


# In[8]:

#Getting the store for an id

frequently_visit_stores_18_months=all_rewards_since_201606[all_rewards_since_201606['transaction_date']>=Beginning_18_months_ago]

frequently_visit_stores_2=frequently_visit_stores_18_months.groupby(['customer_id_hashed','location_id'])['total_transaction_amt'].sum().to_frame().reset_index().rename(columns={"total_transaction_amt":"sales"})
frequently_visit_stores_18_months=frequently_visit_stores_18_months.groupby(['customer_id_hashed','location_id'])['transaction_id'].count().to_frame().reset_index().rename(columns={"transaction_id":"trans"})

frequently_visit_stores_18_months=pd.merge(frequently_visit_stores_18_months,frequently_visit_stores_2,on=['customer_id_hashed','location_id'],how="outer")
del frequently_visit_stores_2
print(frequently_visit_stores_18_months.shape)
frequently_visit_stores_18_months=frequently_visit_stores_18_months.sort_values(['customer_id_hashed','trans','sales'],ascending=[True,False,False])

frequently_visit_stores_18_months=frequently_visit_stores_18_months[['customer_id_hashed','location_id']].drop_duplicates("customer_id_hashed")
print(frequently_visit_stores_18_months.shape)
frequently_visit_stores_18_months.to_csv(folder_write+"frequently_visit_stores_18_months.csv",index=False)
del frequently_visit_stores_18_months
gc.collect()


# In[9]:

del all_rewards_since_201606['transaction_id']
del all_rewards_since_201606['location_id']
gc.collect()


# In[10]:

###get recency
dfrecency=all_rewards_since_201606[['customer_id_hashed','transaction_date']].sort_values("transaction_date",ascending=False).drop_duplicates()#Allready combined

print (min(dfrecency['transaction_date']))
print (max(dfrecency['transaction_date']))
dfrecency = dfrecency.drop_duplicates('customer_id_hashed')
dfrecency.to_csv(folder_write + 'dfrecency.csv',index = False)


# In[11]:

dfrecency['transaction_date'] = pd.to_datetime(dfrecency['transaction_date'])
dfrecency['recency'] =  datetime.datetime.strptime(str(lastdate), '%Y-%m-%d').date() - dfrecency['transaction_date']
dfrecency['recency'] = dfrecency['recency'].apply(lambda x:x.days)
dfrecency['recency'] = np.ceil((dfrecency['recency']+1)/30)

dfrecency = dfrecency[['customer_id_hashed','recency']]
dfrecency = dfrecency.drop_duplicates('customer_id_hashed')
dfrecency.to_csv(folder_write + 'dfrecency2.csv',index = False)

dfrecency.shape


# In[12]:

all_rewards_since_201606['transactions'] = 1
dftotal = all_rewards_since_201606[['customer_id_hashed','total_transaction_amt','transactions']].groupby(['customer_id_hashed']).sum().reset_index().rename(columns={"total_transaction_amt":"sales"})

dftotal = pd.merge(dftotal,dfrecency,on = 'customer_id_hashed',how='outer')
del dfrecency

gc.collect()


# In[13]:

dftotal = dftotal.sort_values(['transactions','recency','sales'],ascending = [0,1,0])
dftotal.reset_index(drop = True, inplace = True)
dftotal.reset_index(inplace = True)
dftotal = dftotal.rename(columns = {'index':'Transindex'})

dftotal = dftotal.sort_values(['sales','recency','transactions'],ascending = [0,1,0])
dftotal.reset_index(drop = True, inplace = True)
dftotal.reset_index(inplace = True)
dftotal = dftotal.rename(columns = {'index':'Amtindex'})

dftotal = dftotal.sort_values(['recency','transactions','sales'],ascending = [1,0,0])
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
'''
dftotal = dftotal.sort_values(['RFM','recency','transactions',
                               'sales'],ascending = [1,1,0,0])
dftotal.reset_index(drop = True, inplace = True)
dftotal.reset_index(inplace = True)
dftotal = dftotal.rename(columns = {'index':'frmindex'})
c_ids = len(dftotal.index)
c_ids = np.ceil(c_ids/10.0)
dftotal['frmindex'] = np.ceil((dftotal['frmindex']+1)/c_ids)

dftotal.to_csv(folder_write + 'dfrfm.csv',index = False)

dftotal = pd.read_csv(folder_write + 'dfrfm.csv')
'''
dftotal = dftotal[['customer_id_hashed','RFM','recency','transactions','sales']]



# In[14]:

dfiddetail = pd.read_csv('/home/jian/Projects/Big_Lots/Loyal_members/loyalty_register_data/combined_masterids_up_to_20181229_JL.csv',nrows = samplerows)
dfiddetail = dfiddetail.drop_duplicates('customer_id_hashed')
#########

new_sign_ups_2019_list=list(recrusive_file_gen("/home/jian/BigLots/2019_by_weeks/"))
new_sign_ups_2019_list=sorted([x for x in new_sign_ups_2019_list if "ster" in x])

new_sign_ups_2019_df=pd.DataFrame({"file_path":new_sign_ups_2019_list})
new_sign_ups_2019_df['Date']=new_sign_ups_2019_df['file_path'].apply(lambda x: x.split("MediaStorm_")[1][:10])
new_sign_ups_2019_df=new_sign_ups_2019_df[new_sign_ups_2019_df['Date']<=lastdate]

for file_new_signups in new_sign_ups_2019_df['file_path'].tolist():
    df=pd.read_table(file_new_signups,dtype=str,usecols=['customer_id_hashed','email_address_hash','customer_zip_code'],sep="|",nrows=samplerows)
    dfiddetail=df.append(dfiddetail) # Already sorted and newest kept on the top
    print(datetime.datetime.now(),file_new_signups)
dfiddetail=dfiddetail.drop_duplicates("customer_id_hashed")

######
dfiddetail2 = pd.read_csv('/home/jian/Projects/Big_Lots/Loyal_members/loyalty_register_data/MediaStorm_Lapsed_Reward_Member_Master_from2014-08-26to2017-02-26.zip',
                     nrows = samplerows,dtype = 'str',sep = '|',
                       usecols = ['customer_id_hashed','email_address_hash','customer_zip_code'])
dfiddetail = dfiddetail.append(dfiddetail2,ignore_index = True)
dfiddetail = dfiddetail.drop_duplicates('customer_id_hashed')
dfiddetail = dfiddetail.drop_duplicates('email_address_hash')

del dfiddetail2
del df
gc.collect()

logging.info("CheckingPoint2")


# In[15]:


dfrecency = pd.read_csv(folder_write + 'dfrecency.csv')
dfrecency['active'] = np.where(dfrecency['transaction_date']>=active_Sunday,'active',
                               np.where(dfrecency['transaction_date']>=lapsed_Sunday,'lapsed','other')
                              )

print(dfrecency['active'].unique().tolist())

dftotal = pd.merge(dftotal,dfrecency[['customer_id_hashed','active']],on = 'customer_id_hashed')

logging.info(str(dfrecency['active'].unique().tolist()))
del dfrecency
gc.collect()


# In[16]:

logging.info("CheckingPoint1")


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
del dfiddetail
gc.collect()



# In[18]:

dftotal = dftotal.sort_values(['RFM','recency','transactions',
                               'sales'],ascending = [1,1,0,0])
dftotal.reset_index(drop = True, inplace = True)
dftotal.reset_index(inplace = True)
dftotal = dftotal.rename(columns = {'index':'frmindex'})
c_ids = len(dftotal.index)
c_ids = np.ceil(c_ids/10.0)
dftotal['frmindex'] = np.ceil((dftotal['frmindex']+1)/c_ids)


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


# In[19]:

# Extra segment for P|L|18+
# Remove the ids don't have transactions within 18 months
seg_P_L_18_plus=dftotal[(dftotal['frmindex']>7.5) & (dftotal['zipcodegroup']=="P") & (dftotal['active']=="other")]
print("seg_P_L_18_plus rest:", seg_P_L_18_plus.shape)

df_other_18_plus=dftotal[dftotal['active']=="other"]
df_other_18_plus['segment']="18_months_plus_back_201606"
print("df_other_18_plus all:", df_other_18_plus.shape)

dftotal=dftotal[dftotal['active']!="other"]

df_other_18_plus=df_other_18_plus[(df_other_18_plus['frmindex']<7.5) | (df_other_18_plus['zipcodegroup']!="P") | (df_other_18_plus['active']!="other")]
print("df_other_18_plus rest:", df_other_18_plus.shape)

# dftotal=dftotal.append(seg_P_L_18_plus) treat the new sperately

gc.collect()


# In[20]:

df_other_18_plus.to_csv(folder_write+"others_not_new_seg_PLother.csv",index=False)


# # Getting the primary stores for each member

# In[24]:

frequently_visit_stores_18_months=pd.read_csv(folder_write+"frequently_visit_stores_18_months.csv",dtype=str)
register_stores=pd.read_csv("/home/jian/Projects/Big_Lots/Loyal_members/loyalty_register_data/output_sing_up_location/BL_id_by_register_store_JL_2019-04-09.csv",
                            dtype=str,nrows=samplerows)
register_stores=register_stores[['customer_id_hashed','sign_up_location']].rename(columns={"sign_up_location":"location_id"})




# In[25]:

store_for_ids=frequently_visit_stores_18_months.append(register_stores)
store_for_ids=store_for_ids.drop_duplicates("customer_id_hashed")
store_for_ids.shape


# In[26]:

del frequently_visit_stores_18_months
del register_stores
gc.collect()


# In[27]:

dftotal=pd.merge(dftotal,store_for_ids,on="customer_id_hashed",how="left")
seg_P_L_18_plus=pd.merge(seg_P_L_18_plus,store_for_ids,on="customer_id_hashed",how="left")


# In[28]:

# read the quadrant by store for 2018 Q4

Q4_store_quadrant=pd.read_excel("/home/jian/Projects/Big_Lots/Live_Ramp/Quarterly_Update_2019Q2/Excel_BL_2018_Q4_post_YoY_small_JL_2019-03-04.xlsx",
                                dtype=str,sheetname="Q4_Store_Quadrant_Defination",usecols=['location_id','Quadrant'])
print(Q4_store_quadrant.shape)
print(len(Q4_store_quadrant['location_id'].unique()))

dftotal=pd.merge(dftotal,Q4_store_quadrant,on="location_id",how="left")
dftotal['Quadrant']=dftotal['Quadrant'].fillna("Quadrant III")

seg_P_L_18_plus=pd.merge(seg_P_L_18_plus,Q4_store_quadrant,on="location_id",how="left")
seg_P_L_18_plus['Quadrant']=seg_P_L_18_plus['Quadrant'].fillna("Quadrant III")


# In[29]:

del store_for_ids
gc.collect()


# # Summary

# In[30]:

dftotal['frmindex']=dftotal['frmindex'].apply(lambda x: str(int(float(x))).zfill(2))
dftotal['customer_zip_code']=dftotal['customer_zip_code'].apply(lambda x: x.zfill(5))
dftotal['frmindex']=dftotal['frmindex'].apply(lambda x:"D"+x)

seg_P_L_18_plus['frmindex']=seg_P_L_18_plus['frmindex'].apply(lambda x: str(int(float(x))).zfill(2))
seg_P_L_18_plus['customer_zip_code']=seg_P_L_18_plus['customer_zip_code'].apply(lambda x: x.zfill(5))
seg_P_L_18_plus['frmindex']=seg_P_L_18_plus['frmindex'].apply(lambda x:"D"+x)


# In[31]:

dftotal.to_csv(folder_write + 'dfrfm_final_details_wemail_zip_StoreQuad.csv',index = False)
seg_P_L_18_plus.to_csv(folder_write + 'seg_P_L_18_plus_details_wemail_zip_StoreQuad.csv',index = False)
gc.collect()


# In[32]:

df_H=pd.DataFrame({"frmindex":['D01','D02','D03','D04']})
df_H['HML_Group']="H"

df_M=pd.DataFrame({"frmindex":['D05','D06','D07']})
df_M['HML_Group']="M"

df_L=pd.DataFrame({"frmindex":['D08','D09','D10']})
df_L['HML_Group']="L"

df_HML=df_H.append(df_M).append(df_L)
gc.collect()


# In[34]:

dftotal=pd.merge(dftotal,df_HML,on='frmindex') # both 10

dftotal['segment_Decile']=dftotal['Quadrant']+"_"+dftotal['zipcodegroup']+"_"+dftotal['frmindex']+"_2019Q2"
dftotal['segment_2019Q2']=dftotal['Quadrant']+"_"+dftotal['zipcodegroup']+"_"+dftotal['HML_Group']+"_2019Q2"


seg_P_L_18_plus=pd.merge(seg_P_L_18_plus,df_HML,on='frmindex') # both 10

seg_P_L_18_plus['segment_Decile']=seg_P_L_18_plus['Quadrant']+"_"+seg_P_L_18_plus['zipcodegroup']+"_"+seg_P_L_18_plus['frmindex']+"_2019Q2PL18"
seg_P_L_18_plus['segment_2019Q2']=seg_P_L_18_plus['Quadrant']+"_"+seg_P_L_18_plus['zipcodegroup']+"_"+seg_P_L_18_plus['HML_Group']+"_2019Q2PL18"

gc.collect()


# In[35]:

logging.info("Checking Point before splitting T/C")

import random
random.seed(1)
total_rows=len(dftotal)

test_all_df=pd.DataFrame()
control_all_df=pd.DataFrame()

i_counter=0

dftotal=dftotal[['segment_Decile','segment_2019Q2','customer_id_hashed','email_address_hash','active']]

for seg,group in dftotal.groupby(['segment_Decile']):
    random_list=random.sample(range(len(group)), int(np.round(len(group)/total_rows*500000)))

    group=group.reset_index()
    del group['index']
    group=group.reset_index()
    df_control=group[group['index'].isin(random_list)]
    df_test=group[~group['index'].isin(random_list)]
    
    df_control['segment_Decile']="C_"+df_control['segment_Decile']
    df_test['segment_Decile']="T_"+df_test['segment_Decile']
    
    df_control['segment_2019Q2']="C_"+df_control['segment_2019Q2']
    df_test['segment_2019Q2']="T_"+df_test['segment_2019Q2']
    test_all_df=test_all_df.append(df_test)
    control_all_df=control_all_df.append(df_control)
    i_counter+=1
    print(i_counter,datetime.datetime.now())
    
logging.info("Checking Point after splitting T/C")


# In[36]:

del dftotal
gc.collect()


# In[37]:

logging.info("Checking Point before splitting T/C")

import random
random.seed(1)
total_rows=len(seg_P_L_18_plus)

test_seg_P_L_18_plus=pd.DataFrame()
control_seg_P_L_18_plus=pd.DataFrame()

i_counter=0

seg_P_L_18_plus=seg_P_L_18_plus[['segment_Decile','segment_2019Q2','customer_id_hashed','email_address_hash','active']]

for seg,group in seg_P_L_18_plus.groupby(['segment_Decile']):
    random_list=random.sample(range(len(group)), int(np.round(len(group)/total_rows*100000)))

    group=group.reset_index()
    del group['index']
    group=group.reset_index()
    df_control=group[group['index'].isin(random_list)]
    df_test=group[~group['index'].isin(random_list)]
    
    df_control['segment_Decile']="C_"+df_control['segment_Decile']
    df_test['segment_Decile']="T_"+df_test['segment_Decile']
    
    df_control['segment_2019Q2']="C_"+df_control['segment_2019Q2']
    df_test['segment_2019Q2']="T_"+df_test['segment_2019Q2']
    test_seg_P_L_18_plus=test_seg_P_L_18_plus.append(df_test)
    control_seg_P_L_18_plus=control_seg_P_L_18_plus.append(df_control)
    i_counter+=1
    print(i_counter,datetime.datetime.now())
    
logging.info("Checking Point after splitting T/C")


# In[43]:

del seg_P_L_18_plus
gc.collect()


# In[45]:

folder_write


# In[46]:

test_all_df.to_csv(folder_write+"all_test.csv",index=False)
control_all_df.to_csv(folder_write+"all_control.csv",index=False)

test_seg_P_L_18_plus.to_csv(folder_write+"test_seg_P_L_18_plus.csv",index=False)
control_seg_P_L_18_plus.to_csv(folder_write+"control_seg_P_L_18_plus.csv",index=False)

folder_write_inner_decile = folder_write+'by_decile_group/'
try:
    os.stat(folder_write_inner_decile)
except:
    os.mkdir(folder_write_inner_decile)
    
folder_write_inner_HML = folder_write+'by_HML_group/'
try:
    os.stat(folder_write_inner_HML)
except:
    os.mkdir(folder_write_inner_HML)


# In[47]:

# Test decile

i_counter=0
for seg,group in test_all_df.groupby(['segment_Decile']):
    group=group[['customer_id_hashed','email_address_hash','segment_Decile','segment_2019Q2']]
    group.to_csv(folder_write_inner_decile+seg+".csv",index=False)
    i_counter+=1
    print(i_counter,seg,datetime.datetime.now())
    
for seg,group in test_seg_P_L_18_plus.groupby(['segment_Decile']):
    group=group[['customer_id_hashed','email_address_hash','segment_Decile','segment_2019Q2']]
    group.to_csv(folder_write_inner_decile+seg+".csv",index=False)
    i_counter+=1
    print(i_counter,seg,datetime.datetime.now())


# In[48]:

# Control decile

i_counter=0
for seg,group in control_all_df.groupby(['segment_Decile']):
    group=group[['customer_id_hashed','email_address_hash','segment_Decile','segment_2019Q2']]
    group.to_csv(folder_write_inner_decile+seg+".csv",index=False)
    i_counter+=1
    print(i_counter,seg,datetime.datetime.now())
    
for seg,group in control_seg_P_L_18_plus.groupby(['segment_Decile']):
    group=group[['customer_id_hashed','email_address_hash','segment_Decile','segment_2019Q2']]
    group.to_csv(folder_write_inner_decile+seg+".csv",index=False)
    i_counter+=1
    print(i_counter,seg,datetime.datetime.now())    
    


# In[50]:

i_counter=0
for seg,group in test_all_df.groupby(['segment_2019Q2']):
    group=group[['customer_id_hashed','email_address_hash','segment_2019Q2']].rename(columns={"segment_2019Q2":"segment"})
    group.to_csv(folder_write_inner_HML+seg+".csv",index=False)
    i_counter+=1
    print(i_counter,seg,datetime.datetime.now())

    
for seg,group in test_seg_P_L_18_plus.groupby(['segment_2019Q2']):
    group=group[['customer_id_hashed','email_address_hash','segment_2019Q2']].rename(columns={"segment_2019Q2":"segment"})
    group.to_csv(folder_write_inner_HML+seg+".csv",index=False)
    i_counter+=1
    print(i_counter,seg,datetime.datetime.now())


# In[51]:

i_counter=0
for seg,group in control_all_df.groupby(['segment_2019Q2']):
    group=group[['customer_id_hashed','email_address_hash','segment_2019Q2']].rename(columns={"segment_2019Q2":"segment"})
    group.to_csv(folder_write_inner_HML+seg+".csv",index=False)
    i_counter+=1
    print(i_counter,seg,datetime.datetime.now())
    
for seg,group in control_seg_P_L_18_plus.groupby(['segment_2019Q2']):
    group=group[['customer_id_hashed','email_address_hash','segment_2019Q2']].rename(columns={"segment_2019Q2":"segment"})
    group.to_csv(folder_write_inner_HML+seg+".csv",index=False)
    i_counter+=1
    print(i_counter,seg,datetime.datetime.now())


# In[52]:

lapsed_trans=pd.read_table("/home/jian/Projects/Big_Lots/Loyal_members/loyalty_sales_data/lapsed20140826_20170226/MediaStormLapsedCustDtl.txt",
                     sep=",",usecols=['customer_id_hashed'],dtype=str).drop_duplicates() # Doesn't go to score at all, so no need to read all columns
lapsed_trans['lapsed_trans']=True

lapsed_master=pd.read_csv('/home/jian/Projects/Big_Lots/Loyal_members/loyalty_register_data/MediaStorm_Lapsed_Reward_Member_Master_from2014-08-26to2017-02-26.zip',
                     nrows = samplerows,dtype = 'str',sep = '|',
                       usecols = ['customer_id_hashed','email_address_hash','customer_zip_code'])

lapsed_master=lapsed_master.drop_duplicates("customer_id_hashed")
print(lapsed_master.shape)

lapsed_master=pd.merge(lapsed_master,lapsed_trans,on="customer_id_hashed",how="outer")
print(lapsed_master.shape)
lapsed_master=lapsed_master[~pd.isnull(lapsed_master['email_address_hash'])]

# remove the non-match email ids at the end and no calculation for the WD here


# In[53]:

summary_18_plus_back_20160626=df_other_18_plus.groupby(['segment','active'])['customer_id_hashed'].count().to_frame().reset_index().rename(columns={"customer_id_hashed":"id_count"})


# In[54]:

lapsed_master=lapsed_master[~lapsed_master['customer_id_hashed'].isin(test_all_df['customer_id_hashed'])]
lapsed_master=lapsed_master[~lapsed_master['customer_id_hashed'].isin(control_all_df['customer_id_hashed'])]
lapsed_master=lapsed_master[~lapsed_master['email_address_hash'].isin(test_all_df['email_address_hash'])]
lapsed_master=lapsed_master[~lapsed_master['email_address_hash'].isin(control_all_df['email_address_hash'])]

lapsed_master=lapsed_master[~lapsed_master['customer_id_hashed'].isin(test_seg_P_L_18_plus['customer_id_hashed'])]
lapsed_master=lapsed_master[~lapsed_master['customer_id_hashed'].isin(control_seg_P_L_18_plus['customer_id_hashed'])]
lapsed_master=lapsed_master[~lapsed_master['email_address_hash'].isin(test_seg_P_L_18_plus['email_address_hash'])]
lapsed_master=lapsed_master[~lapsed_master['email_address_hash'].isin(control_seg_P_L_18_plus['email_address_hash'])]

lapsed_master=lapsed_master[~lapsed_master['customer_id_hashed'].isin(df_other_18_plus['customer_id_hashed'])]
lapsed_master=lapsed_master[~lapsed_master['email_address_hash'].isin(df_other_18_plus['email_address_hash'])]


lapsed_master.shape


# In[55]:

lapsed_master=lapsed_master[['customer_id_hashed','email_address_hash']]
lapsed_master['segment']="WalkingDead_2019Q2"
lapsed_master['active']="other"


# In[56]:

lapsed_master.to_csv(folder_write_inner_decile+"WalkingDead_Group_before_20160626.csv",index=False)
lapsed_master.to_csv(folder_write_inner_HML+"WalkingDead_Group_before_20160626.csv",index=False)


# In[90]:

summary_test=test_all_df.groupby(['segment_2019Q2','active'])['customer_id_hashed'].count().to_frame().reset_index().rename(columns={"customer_id_hashed":"id_count","segment_2019Q2":"segment"})
summary_control=control_all_df.groupby(['segment_2019Q2','active'])['customer_id_hashed'].count().to_frame().reset_index().rename(columns={"customer_id_hashed":"id_count","segment_2019Q2":"segment"})

summary_test_seg_P_L_18=test_seg_P_L_18_plus.groupby(['segment_2019Q2','active'])['customer_id_hashed'].count().to_frame().reset_index().rename(columns={"customer_id_hashed":"id_count","segment_2019Q2":"segment"})
summary_control_seg_P_L_18=control_seg_P_L_18_plus.groupby(['segment_2019Q2','active'])['customer_id_hashed'].count().to_frame().reset_index().rename(columns={"customer_id_hashed":"id_count","segment_2019Q2":"segment"})


summary_WD=lapsed_master.groupby(['segment','active'])['customer_id_hashed'].count().to_frame().reset_index().rename(columns={"customer_id_hashed":"id_count"})

summary_overll=summary_test.append(summary_control).append(summary_WD).append(summary_18_plus_back_20160626).append(summary_test_seg_P_L_18).append(summary_control_seg_P_L_18)


summary_overll.to_csv(folder_write_inner_HML+"test_control_groups_summary_HLM_JL_"+str(datetime.datetime.now().date())+".csv",index=False)


# In[ ]:

summary_test=test_all_df.groupby(['segment_Decile','active'])['customer_id_hashed'].count().to_frame().reset_index().rename(columns={"customer_id_hashed":"id_count","segment_Decile":"segment"})
summary_control=control_all_df.groupby(['segment_Decile','active'])['customer_id_hashed'].count().to_frame().reset_index().rename(columns={"customer_id_hashed":"id_count","segment_Decile":"segment"})

summary_test_seg_P_L_18=test_seg_P_L_18_plus.groupby(['segment_Decile','active'])['customer_id_hashed'].count().to_frame().reset_index().rename(columns={"customer_id_hashed":"id_count","segment_Decile":"segment"})
summary_control_seg_P_L_18=control_seg_P_L_18_plus.groupby(['segment_Decile','active'])['customer_id_hashed'].count().to_frame().reset_index().rename(columns={"customer_id_hashed":"id_count","segment_Decile":"segment"})


summary_WD=lapsed_master.groupby(['segment','active'])['customer_id_hashed'].count().to_frame().reset_index().rename(columns={"customer_id_hashed":"id_count"})

summary_overll=summary_test.append(summary_control).append(summary_WD).append(summary_18_plus_back_20160626).append(summary_test_seg_P_L_18).append(summary_control_seg_P_L_18)


summary_overll.to_csv(folder_write_inner_decile+"test_control_groups_summary_decile_JL_"+str(datetime.datetime.now().date())+".csv",index=False)


# In[ ]:



