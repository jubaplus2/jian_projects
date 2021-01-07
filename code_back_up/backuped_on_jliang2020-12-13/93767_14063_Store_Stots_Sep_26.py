
# coding: utf-8

# In[1]:

import pandas as pd
import numpy as np
import datetime
import hashlib
import logging
import gc
logging.basicConfig(filename='BL_Store_Rewards_Stats'+str(datetime.datetime.now().date())+'.log', level=logging.INFO)


samplerows=None


# In[2]:

# To add

# 1. zip+4 rolled up to zip then ta, total HH 25-54 #Done
# 2. Q2 rewards members count up to end of Q2 2017 #Done
# 3. 2017 Q2 rewards all variables #Done
# 4. rewards week by week Q2, yoy #Done
# 5. Split the compariable stores


# In[3]:

EASI_C=pd.read_csv("/home/jian/Docs/EASI/2018-07-19/ZIP4_18_DATA_C2_CSV/ZIP4_18_DATA_C2.CSV",
                   dtype=str,skiprows=1,nrows=samplerows,usecols=['ZIP_CODE','ZIP4','HH18','PCTHH25_34','PCTHH35_44','PCTHH45_54'])
EASI_C['HH18']=EASI_C['HH18'].astype(float)

EASI_C['PCTHH25_34']=EASI_C['PCTHH25_34'].astype(float)
EASI_C['PCTHH35_44']=EASI_C['PCTHH35_44'].astype(float)
EASI_C['PCTHH45_54']=EASI_C['PCTHH45_54'].astype(float)

EASI_C['PCTHH25_54']=EASI_C['PCTHH25_34']+EASI_C['PCTHH35_44']+EASI_C['PCTHH45_54']

del EASI_C['PCTHH25_34']
del EASI_C['PCTHH35_44']
del EASI_C['PCTHH45_54']

EASI_C['HH_25_54']=EASI_C['HH18']*EASI_C['PCTHH25_54']/100
check_x=EASI_C['HH18'].sum()
EASI_C=EASI_C.groupby(['ZIP_CODE'])['HH_25_54'].sum().to_frame().reset_index()
EASI_C=EASI_C.rename(columns={"ZIP_CODE":"zip_cd"})


# # 1, 4

# In[4]:

store_info=pd.read_table("/home/jian/BigLots/static_files/Store_list/MediaStormStores20180901-133640-935.txt",sep="|",dtype=str)
store_info=store_info[['location_id','city_nm','state_nm','zip_cd','open_dt']]

store_info_2=pd.read_table("/home/jian/BigLots/static_files/Store_list/MediaStormStores20180801-133641-576.txt",sep="|",dtype=str)
store_info_2=store_info_2[['location_id','city_nm','state_nm','zip_cd','open_dt']]
store_info_2=store_info_2[~store_info_2['location_id'].isin(store_info['location_id'].tolist())]
store_info=store_info.append(store_info_2)

store_info_2=pd.read_table("/home/jian/BigLots/static_files/Store_list/MediaStormStores20180703.txt",sep="|",dtype=str)
store_info_2=store_info_2[['location_id','city_nm','state_nm','zip_cd','open_dt']]
store_info_2=store_info_2[~store_info_2['location_id'].isin(store_info['location_id'].tolist())]
store_info=store_info.append(store_info_2)

store_info_2=pd.read_table("/home/jian/BigLots/static_files/Store_list/MediaStormStores20171115.txt",sep="|",dtype=str)
store_info_2=store_info_2[['location_id','city_nm','state_nm','zip_cd','open_dt']]
store_info_2=store_info_2[~store_info_2['location_id'].isin(store_info['location_id'].tolist())]
store_info=store_info.append(store_info_2)

store_info_2=pd.read_table("/home/jian/BigLots/static_files/Store_list/MediaStormStores20170913.txt",sep="|",dtype=str)
store_info_2=store_info_2[['location_id','city_nm','state_nm','zip_cd','open_dt']]
store_info_2=store_info_2[~store_info_2['location_id'].isin(store_info['location_id'].tolist())]
store_info=store_info.append(store_info_2)
store_info['zip_cd']=store_info['zip_cd'].apply(lambda x: x.split("-")[0].zfill(5))


#

inclusion_stores_sales_data=pd.read_excel("/home/jian/BiglotsCode/outputs/Output_2018-09-01/wide_sales_date2018-09-01.xlsx",dtype=str,sheetname="sales")
inclusion_stores_trans_data=pd.read_excel("/home/jian/BiglotsCode/outputs/Output_2018-09-01/wide_sales_date2018-09-01.xlsx",dtype=str,sheetname="transactions")

inclusions_store_list=inclusion_stores_sales_data[inclusion_stores_sales_data['2018-09-01']!="0"]['location_id'].tolist()

inclusion_stores_sales_data=inclusion_stores_sales_data[inclusion_stores_sales_data['location_id'].isin(inclusions_store_list)]
inclusion_stores_trans_data=inclusion_stores_trans_data[inclusion_stores_trans_data['location_id'].isin(inclusions_store_list)]


#

Q2_Start_week_2018=datetime.date(2018,5,12)
Q2_End_week_2018=datetime.date(2018,8,4)
Q2_Start_week_2017=datetime.date(2017,5,13)
Q2_End_week_2017=datetime.date(2017,8,5)
Q2_2017_Weeks=[str(Q2_Start_week_2017+datetime.timedelta(days=7*i)) for i in range(13)]
Q2_2018_Weeks=[str(Q2_Start_week_2018+datetime.timedelta(days=7*i)) for i in range(13)]


#

inclusion_stores_sales_data=inclusion_stores_sales_data[['location_id']+Q2_2017_Weeks+Q2_2018_Weeks]
inclusion_stores_trans_data=inclusion_stores_trans_data[['location_id']+Q2_2017_Weeks+Q2_2018_Weeks]


#

for col in Q2_2017_Weeks+Q2_2018_Weeks:
    inclusion_stores_sales_data[col]=inclusion_stores_sales_data[col].astype(float)
    inclusion_stores_trans_data[col]=inclusion_stores_trans_data[col].astype(float)



#

inclusion_stores_trans_data['2017_Q2_Trans']=inclusion_stores_trans_data[Q2_2017_Weeks].sum(axis=1)
inclusion_stores_trans_data['2018_Q2_Trans']=inclusion_stores_trans_data[Q2_2018_Weeks].sum(axis=1)
inclusion_stores_sales_data['2017_Q2_Sales']=inclusion_stores_sales_data[Q2_2017_Weeks].sum(axis=1)
inclusion_stores_sales_data['2018_Q2_Sales']=inclusion_stores_sales_data[Q2_2018_Weeks].sum(axis=1)


#

Q2_Sales_Trans=pd.merge(inclusion_stores_trans_data[['location_id','2017_Q2_Trans','2018_Q2_Trans']],
                       inclusion_stores_sales_data[['location_id','2017_Q2_Sales','2018_Q2_Sales']],
                       on="location_id",how="outer")


#

DMA_Zip=pd.read_excel("/home/jian/Docs/Geo_mapping/Zips by DMA by County16-17 nielsen.xlsx",skiprows=1,dtype=str)
DMA_Zip=DMA_Zip.iloc[:,[0,2]]
DMA_Zip.columns=['zip_cd','DMA']
DMA_Zip=DMA_Zip.drop_duplicates(['zip_cd'])


#

store_info=pd.merge(store_info,DMA_Zip,on="zip_cd",how="left")


#

inclusion_store_list_set=set(Q2_Sales_Trans['location_id'].unique().tolist())

#

df_1=store_info.copy()
df_1=df_1[df_1['location_id'].isin(inclusion_store_list_set)]
df_4=Q2_Sales_Trans.copy()
del store_info
df_4['Q2_Sales_YoY']=(df_4['2018_Q2_Sales']-df_4['2017_Q2_Sales'])/df_4['2017_Q2_Sales']
df_4['Q2_Trans_YoY']=(df_4['2018_Q2_Trans']-df_4['2017_Q2_Trans'])/df_4['2017_Q2_Trans']
df_4=df_4[['location_id','Q2_Sales_YoY','Q2_Trans_YoY','2018_Q2_Sales','2017_Q2_Sales','2018_Q2_Trans','2017_Q2_Trans']]

gc.collect()


# # 2

# In[5]:

folderpath = '/home/jian/Projects/Big_Lots/Loyal_members/loyalty_register_data/' # usecols=[0,1,5],
selected_columns = ['customer_id_hashed','sign_up_location','customer_zip_code','sign_up_date']
dfiddetail = pd.read_table(folderpath+"/MediaStormCustTot-hashed-email.txt",
                       header=None,nrows = samplerows,sep = ',',dtype = str,usecols=[0,2,4,5])
dfiddetail.columns=['customer_id','sign_up_date','sign_up_location','customer_zip_code']
dfiddetail['customer_id_hashed'] = dfiddetail['customer_id'].apply(lambda x: hashlib.sha256(x.encode('utf-8')).hexdigest())
dfiddetail=dfiddetail[selected_columns]
dfiddetail=dfiddetail[dfiddetail['sign_up_location'].isin(inclusion_store_list_set)]
#
dfiddetail2 = pd.read_csv(folderpath+'MediaStormCustomerTransactionTotals_2018-01-09_2018-03-31.txt',
                          nrows = samplerows,sep = ',',usecols=[0,2,4,5],dtype = str)
dfiddetail2=dfiddetail2[selected_columns]
dfiddetail2['sign_up_location']=dfiddetail2['sign_up_location'].fillna("nan")
dfiddetail2=dfiddetail2[dfiddetail2['sign_up_location'].isin(inclusion_store_list_set)]
dfiddetail = dfiddetail.append(dfiddetail2)

#
dfiddetail2 = pd.read_csv(folderpath+'Existing Reward Member Master as of 2018-06-05.txt',
                          nrows = samplerows,sep = '|',usecols=[0,2,4,5],dtype = str)
dfiddetail2=dfiddetail2[selected_columns]
dfiddetail2['sign_up_location']=dfiddetail2['sign_up_location'].fillna("nan")
dfiddetail2=dfiddetail2[dfiddetail2['sign_up_location'].isin(inclusion_store_list_set)]
dfiddetail = dfiddetail.append(dfiddetail2)

#
dfiddetail2 = pd.read_csv(folderpath+'New Reward Member Master as of 2018-06-05.txt',
                          nrows = samplerows,sep = '|',usecols=[0,2,4,5],dtype = str)
dfiddetail2=dfiddetail2[selected_columns]
dfiddetail2['sign_up_location']=dfiddetail2['sign_up_location'].fillna("nan")
dfiddetail2=dfiddetail2[dfiddetail2['sign_up_location'].isin(inclusion_store_list_set)]
dfiddetail = dfiddetail.append(dfiddetail2)

#
dfiddetail2 = pd.read_csv(folderpath+'New Reward Member Master as of 2018-07-03.txt',
                          nrows = samplerows,sep = '|',usecols=[0,2,4,5],dtype = str)
dfiddetail2=dfiddetail2[selected_columns]
dfiddetail2['sign_up_location']=dfiddetail2['sign_up_location'].fillna("nan")
dfiddetail2=dfiddetail2[dfiddetail2['sign_up_location'].isin(inclusion_store_list_set)]
dfiddetail = dfiddetail.append(dfiddetail2)

#
dfiddetail2 = pd.read_csv(folderpath+'MediaStormMasterBiWeekly20180717-132337-377.txt',
                          nrows = samplerows,sep = '|',usecols=[0,2,4,5],dtype = str)
dfiddetail2=dfiddetail2[selected_columns]
dfiddetail2['sign_up_location']=dfiddetail2['sign_up_location'].fillna("nan")
dfiddetail2=dfiddetail2[dfiddetail2['sign_up_location'].isin(inclusion_store_list_set)]
dfiddetail = dfiddetail.append(dfiddetail2)

#
dfiddetail2 = pd.read_csv(folderpath+'MediaStormMasterBiWeekly20180731-130714-098.txt',
                          nrows = samplerows,sep = '|',usecols=[0,2,4,5],dtype = str)
dfiddetail2=dfiddetail2[selected_columns]
dfiddetail2['sign_up_location']=dfiddetail2['sign_up_location'].fillna("nan")
dfiddetail2=dfiddetail2[dfiddetail2['sign_up_location'].isin(inclusion_store_list_set)]
dfiddetail = dfiddetail.append(dfiddetail2)

#
dfiddetail2 = pd.read_csv(folderpath+'MediaStormMasterBiWeekly20180814-130703-491.txt',
                          nrows = samplerows,sep = '|',usecols=[0,2,4,5],dtype = str)
dfiddetail2=dfiddetail2[selected_columns]
dfiddetail2['sign_up_location']=dfiddetail2['sign_up_location'].fillna("nan")
dfiddetail2=dfiddetail2[dfiddetail2['sign_up_location'].isin(inclusion_store_list_set)]
dfiddetail = dfiddetail.append(dfiddetail2)
del dfiddetail2
#
dfiddetail['customer_zip_code']=dfiddetail['customer_zip_code'].astype(str)
dfiddetail['customer_zip_code']=dfiddetail['customer_zip_code'].apply(lambda x: x.replace(" ",""))
dfiddetail['customer_zip_code']=dfiddetail['customer_zip_code'].apply(lambda x: x.replace(" ",""))
dfiddetail['customer_zip_code']=dfiddetail['customer_zip_code'].apply(lambda x: x.replace(" ",""))
dfiddetail['zip_cd']=dfiddetail['customer_zip_code'].apply(lambda x: str(x).split("-")[0].zfill(5))
del dfiddetail['customer_zip_code']

dfiddetail=dfiddetail[dfiddetail['sign_up_date'].apply(lambda x: len(x))==10]

dfiddetail['sign_up_date']=dfiddetail['sign_up_date'].apply(lambda x: datetime.datetime.strptime(x,"%Y-%m-%d").date())
dfiddetail_2017_Q2=dfiddetail[dfiddetail['sign_up_date']<=Q2_End_week_2017]


dfiddetail=dfiddetail.sort_values(['sign_up_date'],ascending=False)
dfiddetail_2017_Q2=dfiddetail_2017_Q2.sort_values(['sign_up_date'],ascending=False)


dfiddetail=dfiddetail[['customer_id_hashed','sign_up_location','zip_cd','sign_up_date']].drop_duplicates('customer_id_hashed')
dfiddetail_2017_Q2=dfiddetail_2017_Q2[['customer_id_hashed','sign_up_location','zip_cd','sign_up_date']].drop_duplicates('customer_id_hashed')


logging.info("Finished read of singing up data, with rows of "+str(dfiddetail.shape[0]))

dfiddetail=dfiddetail[dfiddetail['zip_cd'].apply(lambda x: len(x)).isin([5,9])]
dfiddetail['zip_cd']=dfiddetail['zip_cd'].apply(lambda x: x[:5])

dfiddetail_2017_Q2=dfiddetail_2017_Q2[dfiddetail_2017_Q2['zip_cd'].apply(lambda x: len(x)).isin([5,9])]
dfiddetail_2017_Q2['zip_cd']=dfiddetail_2017_Q2['zip_cd'].apply(lambda x: x[:5])

#
gc.collect()


# In[6]:


sign_up_id_by_zip=dfiddetail.groupby(['zip_cd'])['customer_id_hashed'].count().to_frame().reset_index()
sign_up_id_by_store=dfiddetail.groupby(['sign_up_location'])['customer_id_hashed'].count().to_frame().reset_index()
sign_up_id_by_store_2017_Q2=dfiddetail_2017_Q2.groupby(['sign_up_location'])['customer_id_hashed'].count().to_frame().reset_index()
sign_up_id_by_store_2017_Q2=sign_up_id_by_store_2017_Q2.rename(columns={"sign_up_location":"location_id","customer_id_hashed":"id_counts_signed_up_Q2End_2017"})
df_2=sign_up_id_by_store.copy()

df_2=df_2.rename(columns={"sign_up_location":"location_id","customer_id_hashed":"id_counts_signed_up_Q2End_2018"})

dfiddetail_2017_Q2=dfiddetail_2017_Q2.sort_values(['sign_up_date'],ascending=False)
dfiddetail_2017_Q2=dfiddetail_2017_Q2[['customer_id_hashed','sign_up_location','zip_cd','sign_up_date']].drop_duplicates('customer_id_hashed')
sign_up_id_by_store_2017_Q2=dfiddetail_2017_Q2.groupby(['sign_up_location'])['customer_id_hashed'].count().to_frame().reset_index()
sign_up_id_by_store_2017_Q2=sign_up_id_by_store_2017_Q2.rename(columns={"sign_up_location":"location_id","customer_id_hashed":"id_counts_signed_up_Q2End_2017"})

df_2=pd.merge(df_2,sign_up_id_by_store_2017_Q2,on="location_id",how="left")
del dfiddetail_2017_Q2
del sign_up_id_by_store
del sign_up_id_by_store_2017_Q2

gc.collect()
df_2.head(2)


# # 3

# In[7]:

ta_by_zip=pd.read_excel("/home/jian/Projects/Big_Lots/New_TA/zips_in_new_ta/BL_Zips in new TA (TA level)_JL_20180330.xlsx",
                       dtype=str,usecols=['location_id','zip_cd','revenue_flag','TA_of_zip','TA_of_store'])
TA_Zips=ta_by_zip[['zip_cd','TA_of_zip']].drop_duplicates()
TA_Store=ta_by_zip[['location_id','TA_of_store']].drop_duplicates()
TA_Store=TA_Store[TA_Store['location_id'].isin(inclusion_store_list_set)]

stores_not_in_TA=[x for x in inclusion_store_list_set if x not in ta_by_zip['location_id'].tolist()]
zips_for_new_store=df_1[df_1['location_id'].isin(stores_not_in_TA)]['zip_cd'].unique().tolist()
# 8 new stores not in TA, 7 of the 8 zips are found in TA and allocated to the TA
# 4675|02719 missing TA info, so no population
TA_new_store=TA_Zips[TA_Zips['zip_cd'].isin(zips_for_new_store)]

given_TA_7_New_store=df_1[df_1['location_id'].isin(stores_not_in_TA)]
given_TA_7_New_store=pd.merge(given_TA_7_New_store,TA_Zips,on="zip_cd",how="left")
given_TA_7_New_store=given_TA_7_New_store[['location_id','TA_of_zip']].rename(columns={"TA_of_zip":"TA_of_store"})
TA_Store=TA_Store.append(given_TA_7_New_store)

demo_F_25_54=pd.read_csv("/home/jian/Docs/Household_and_Population/2016/Demo_Dataset_2018EASI.csv",
                         dtype=str,usecols=['ZIP_CODE','Estimate; Female: - 25 to 29 years','Estimate; Female: - 30 to 34 years',
                                                    'Estimate; Female: - 35 to 39 years','Estimate; Female: - 40 to 44 years',
                                                     'Estimate; Female: - 45 to 49 years','Estimate; Female: - 50 to 54 years'])
for col in demo_F_25_54.columns.tolist()[1:]:
    demo_F_25_54[col]=demo_F_25_54[col].astype(float)
demo_F_25_54['ZIP_CODE']=demo_F_25_54['ZIP_CODE'].apply(lambda x: x.zfill(5))
demo_F_25_54['Pop_F_25_54']=demo_F_25_54[demo_F_25_54.columns.tolist()[1:]].sum(axis=1)
demo_F_25_54=demo_F_25_54.rename(columns={"ZIP_CODE":"zip_cd"})
demo_F_25_54=demo_F_25_54[['zip_cd','Pop_F_25_54']]

TA_Zips_POP=pd.merge(TA_Zips,demo_F_25_54,on="zip_cd",how="left")
TA_POP=TA_Zips_POP.groupby(['TA_of_zip'])['Pop_F_25_54'].sum().to_frame().reset_index()
TA_POP=TA_POP.rename(columns={"TA_of_zip":"TA"})
TA_Store=TA_Store.rename(columns={"TA_of_store":"TA"})
df_3=pd.merge(TA_Store,TA_POP,on="TA",how="left")

TA_Zips_HH_EASI=pd.merge(TA_Zips,EASI_C,on="zip_cd",how="left")
TA_HH_EASI=TA_Zips_HH_EASI.groupby(['TA_of_zip'])['HH_25_54'].sum().to_frame().reset_index()
TA_HH_EASI=TA_HH_EASI.rename(columns={"TA_of_zip":"TA"})

df_3=pd.merge(df_3,TA_HH_EASI,on="TA",how="left")

del EASI_C
gc.collect()


# # 5, 6

# In[16]:
weird_id_removed="5efa406f90b87aee3110c8dcebfaddcc246364bc6817e83a8f2ef508ec42f597"
# store 5191
# date 2018-05-12
# sales: -480985.06


rewards_sales_from_SP=pd.read_csv("/home/jian/Projects/Big_Lots/Loyal_members/loyalty_sales_data/From_Sp/combinedtransactions_0811.csv",
                                 dtype=str,nrows=samplerows,usecols=['customer_id_hashed','transaction_date','transaction_id','location_id','total_transaction_amt'])
rewards_sales_from_SP=rewards_sales_from_SP[rewards_sales_from_SP['customer_id_hashed']!=weird_id_removed]

rewards_sales_from_SP=rewards_sales_from_SP[rewards_sales_from_SP['location_id'].isin(inclusion_store_list_set)]
rewards_sales_from_SP['transaction_date']=rewards_sales_from_SP['transaction_date'].apply(lambda x: datetime.datetime.strptime(x,"%Y-%m-%d").date())
rewards_sales_from_SP_2017Q2=rewards_sales_from_SP[(rewards_sales_from_SP['transaction_date']>=Q2_Start_week_2017) & (rewards_sales_from_SP['transaction_date']<=Q2_End_week_2017)]
rewards_sales_from_SP=rewards_sales_from_SP[(rewards_sales_from_SP['transaction_date']>=Q2_Start_week_2018) & (rewards_sales_from_SP['transaction_date']<=Q2_End_week_2018)]

rewards_sales_from_SP=rewards_sales_from_SP.drop_duplicates()
rewards_sales_from_SP_2017Q2=rewards_sales_from_SP_2017Q2.drop_duplicates()

rewards_sales_from_SP['total_transaction_amt']=rewards_sales_from_SP['total_transaction_amt'].astype(float)
rewards_sales_from_SP_2017Q2['total_transaction_amt']=rewards_sales_from_SP_2017Q2['total_transaction_amt'].astype(float)

del rewards_sales_from_SP['transaction_id']
del rewards_sales_from_SP_2017Q2['transaction_id']

gc.collect()

def add_week_end_date(x):
    weekday_num=x.weekday()
    if weekday_num==6:
        y=x+datetime.timedelta(days=6)
    else:
        y=x+datetime.timedelta(days=5-weekday_num)
    return y

rewards_by_store_Q2_sales=rewards_sales_from_SP.groupby(['location_id'])['total_transaction_amt'].sum().to_frame().reset_index()
rewards_by_store_Q2_sales=rewards_by_store_Q2_sales.rename(columns={"total_transaction_amt":"Q2_rewards_sales_2018"})
rewards_by_store_Q2_trans=rewards_sales_from_SP.groupby(['location_id'])['total_transaction_amt'].count().to_frame().reset_index()
rewards_by_store_Q2_trans=rewards_by_store_Q2_trans.rename(columns={"total_transaction_amt":"Q2_rewards_trans_2018"})

rewards_by_store_Q2_sales_2017Q2=rewards_sales_from_SP_2017Q2.groupby(['location_id'])['total_transaction_amt'].sum().to_frame().reset_index()
rewards_by_store_Q2_sales_2017Q2=rewards_by_store_Q2_sales_2017Q2.rename(columns={"total_transaction_amt":"Q2_rewards_sales_2017"})
rewards_by_store_Q2_trans_2017Q2=rewards_sales_from_SP_2017Q2.groupby(['location_id'])['total_transaction_amt'].count().to_frame().reset_index()
rewards_by_store_Q2_trans_2017Q2=rewards_by_store_Q2_trans_2017Q2.rename(columns={"total_transaction_amt":"Q2_rewards_trans_2017"})

df_5=pd.merge(rewards_by_store_Q2_sales,rewards_by_store_Q2_trans,on="location_id",how="outer")
df_5['Q2_rewards_sales_share_2018']=np.nan
df_5['Q2_rewards_trans_share_2018']=np.nan
df_5['Rewards_AOV_2018']=df_5['Q2_rewards_sales_2018']/df_5['Q2_rewards_trans_2018']

df_5_2017=pd.merge(rewards_by_store_Q2_sales_2017Q2,rewards_by_store_Q2_trans_2017Q2,on="location_id",how="outer")
df_5_2017['Q2_rewards_sales_share_2017']=np.nan
df_5_2017['Q2_rewards_trans_share_2017']=np.nan
df_5_2017['Rewards_AOV_2017']=df_5_2017['Q2_rewards_sales_2017']/df_5_2017['Q2_rewards_trans_2017']

df_5=pd.merge(df_5,df_5_2017,on="location_id",how="left")

gc.collect()


# In[18]:

existing_id_df_2018=rewards_sales_from_SP[['customer_id_hashed']].drop_duplicates()
existing_id_df_2018['id_shopped_2018']=1
existing_id_df_2017=rewards_sales_from_SP_2017Q2[['customer_id_hashed']].drop_duplicates()
existing_id_df_2017['id_shopped_2017']=1

existing_ind_df=pd.merge(existing_id_df_2018,existing_id_df_2017,on="customer_id_hashed",how="outer")
existing_ind_df=existing_ind_df.fillna(0)
existing_ind_df['both_existing']=existing_ind_df['id_shopped_2018']*existing_ind_df['id_shopped_2017']
existing_ind_df['new_2018']=existing_ind_df['id_shopped_2018']-existing_ind_df['id_shopped_2017']
existing_ind_df['new_2018']=np.where(existing_ind_df['new_2018']==1,1,0)
existing_ind_df=existing_ind_df[['customer_id_hashed','both_existing','new_2018']]

del existing_id_df_2018
del existing_id_df_2017


# In[25]:

rewards_sales_from_SP=pd.merge(rewards_sales_from_SP,existing_ind_df,on="customer_id_hashed",how="left")
rewards_sales_from_SP=rewards_sales_from_SP.fillna(0)
rewards_sales_from_SP_2017Q2=pd.merge(rewards_sales_from_SP_2017Q2,existing_ind_df,on="customer_id_hashed",how="left")
rewards_sales_from_SP_2017Q2=rewards_sales_from_SP_2017Q2.fillna(0)

rewards_sales_from_SP_existing=rewards_sales_from_SP[rewards_sales_from_SP['both_existing']==1]
rewards_sales_from_SP_new=rewards_sales_from_SP[rewards_sales_from_SP['new_2018']==1]

rewards_sales_from_SP_existing_2017=rewards_sales_from_SP_2017Q2[rewards_sales_from_SP_2017Q2['both_existing']==1]
rewards_sales_from_SP_new_2017=rewards_sales_from_SP_2017Q2[rewards_sales_from_SP_2017Q2['new_2018']==1]

rewards_by_store_Q2_sales_existing=rewards_sales_from_SP_existing.groupby(['location_id'])['total_transaction_amt'].sum().to_frame().reset_index()
rewards_by_store_Q2_sales_existing=rewards_by_store_Q2_sales_existing.rename(columns={"total_transaction_amt":"Q2_existing_rewards_sales_2018"})
rewards_by_store_Q2_trans_existing=rewards_sales_from_SP_existing.groupby(['location_id'])['total_transaction_amt'].count().to_frame().reset_index()
rewards_by_store_Q2_trans_existing=rewards_by_store_Q2_trans_existing.rename(columns={"total_transaction_amt":"Q2_existing_rewards_trans_2018"})

rewards_by_store_Q2_sales_existing_2017Q2=rewards_sales_from_SP_existing_2017.groupby(['location_id'])['total_transaction_amt'].sum().to_frame().reset_index()
rewards_by_store_Q2_sales_existing_2017Q2=rewards_by_store_Q2_sales_existing_2017Q2.rename(columns={"total_transaction_amt":"Q2_existing_rewards_sales_2017"})
rewards_by_store_Q2_trans_existing_2017Q2=rewards_sales_from_SP_existing_2017.groupby(['location_id'])['total_transaction_amt'].count().to_frame().reset_index()
rewards_by_store_Q2_trans_existing_2017Q2=rewards_by_store_Q2_trans_existing_2017Q2.rename(columns={"total_transaction_amt":"Q2_existing_rewards_trans_2017"})

rewards_by_store_Q2_sales_new=rewards_sales_from_SP_new.groupby(['location_id'])['total_transaction_amt'].sum().to_frame().reset_index()
rewards_by_store_Q2_sales_new=rewards_by_store_Q2_sales_new.rename(columns={"total_transaction_amt":"Q2_new_rewards_sales_2018"})
rewards_by_store_Q2_trans_new=rewards_sales_from_SP_new.groupby(['location_id'])['total_transaction_amt'].count().to_frame().reset_index()
rewards_by_store_Q2_trans_new=rewards_by_store_Q2_trans_new.rename(columns={"total_transaction_amt":"Q2_new_rewards_trans_2018"})

rewards_by_store_Q2_sales_new_2017Q2=rewards_sales_from_SP_new_2017.groupby(['location_id'])['total_transaction_amt'].sum().to_frame().reset_index()
rewards_by_store_Q2_sales_new_2017Q2=rewards_by_store_Q2_sales_new_2017Q2.rename(columns={"total_transaction_amt":"Q2_new_rewards_sales_2017"})
rewards_by_store_Q2_trans_new_2017Q2=rewards_sales_from_SP_new_2017.groupby(['location_id'])['total_transaction_amt'].count().to_frame().reset_index()
rewards_by_store_Q2_trans_new_2017Q2=rewards_by_store_Q2_trans_new_2017Q2.rename(columns={"total_transaction_amt":"Q2_new_rewards_trans_2017"})


del rewards_sales_from_SP['both_existing']
del rewards_sales_from_SP['new_2018']

del rewards_sales_from_SP_2017Q2['both_existing']
del rewards_sales_from_SP_2017Q2['new_2018']


# In[27]:

def unique_id_count(x):
    y=len(set(x))
    return y


# In[28]:

rewards_by_store_Q2_unique_id_existing=rewards_sales_from_SP_existing.groupby(['location_id'])['customer_id_hashed'].agg(unique_id_count).to_frame().reset_index()
rewards_by_store_Q2_unique_id_existing=rewards_by_store_Q2_unique_id_existing.rename(columns={"customer_id_hashed":"Q2_existing_id_counts_2018"})
rewards_by_store_Q2_unique_id_existing_2017=rewards_sales_from_SP_existing_2017.groupby(['location_id'])['customer_id_hashed'].agg(unique_id_count).to_frame().reset_index()
rewards_by_store_Q2_unique_id_existing_2017=rewards_by_store_Q2_unique_id_existing_2017.rename(columns={"customer_id_hashed":"Q2_existing_id_counts_2017"})

rewards_by_store_Q2_unique_id_new=rewards_sales_from_SP_new.groupby(['location_id'])['customer_id_hashed'].agg(unique_id_count).to_frame().reset_index()
rewards_by_store_Q2_unique_id_new=rewards_by_store_Q2_unique_id_new.rename(columns={"customer_id_hashed":"Q2_new_id_counts_2018"})
rewards_by_store_Q2_unique_id_new_2017=rewards_sales_from_SP_new_2017.groupby(['location_id'])['customer_id_hashed'].agg(unique_id_count).to_frame().reset_index()
rewards_by_store_Q2_unique_id_new_2017=rewards_by_store_Q2_unique_id_new_2017.rename(columns={"customer_id_hashed":"Q2_new_id_counts_2017"})


# In[31]:

df_5=pd.merge(df_5,rewards_by_store_Q2_sales_existing,on="location_id",how="left")
df_5=pd.merge(df_5,rewards_by_store_Q2_trans_existing,on="location_id",how="left")
df_5=pd.merge(df_5,rewards_by_store_Q2_sales_existing_2017Q2,on="location_id",how="left")
df_5=pd.merge(df_5,rewards_by_store_Q2_trans_existing_2017Q2,on="location_id",how="left")
df_5=pd.merge(df_5,rewards_by_store_Q2_unique_id_existing,on="location_id",how="left")
df_5=pd.merge(df_5,rewards_by_store_Q2_unique_id_existing_2017,on="location_id",how="left")

df_5=pd.merge(df_5,rewards_by_store_Q2_sales_new,on="location_id",how="left")
df_5=pd.merge(df_5,rewards_by_store_Q2_trans_new,on="location_id",how="left")
df_5=pd.merge(df_5,rewards_by_store_Q2_sales_new_2017Q2,on="location_id",how="left")
df_5=pd.merge(df_5,rewards_by_store_Q2_trans_new_2017Q2,on="location_id",how="left")
df_5=pd.merge(df_5,rewards_by_store_Q2_unique_id_new,on="location_id",how="left")
df_5=pd.merge(df_5,rewards_by_store_Q2_unique_id_new_2017,on="location_id",how="left")


# # 7

# In[ ]:

df_7=inclusion_stores_sales_data[['location_id']+Q2_2017_Weeks+Q2_2018_Weeks]
for col in Q2_2018_Weeks:
    week_2018=col
    week_2017=str(datetime.datetime.strptime(col,"%Y-%m-%d").date()-datetime.timedelta(days=52*7))
    df_7["YoY_"+col]=np.round((df_7[week_2018]-df_7[week_2017])/df_7[week_2017],6)
df_7=df_7[["location_id"]+["YoY_"+x for x in Q2_2018_Weeks]]

# In[8]:

store_level_output=pd.merge(df_1,df_2,on="location_id",how="left")
store_level_output=pd.merge(store_level_output,df_3,on="location_id",how="left")
store_level_output=pd.merge(store_level_output,df_4,on="location_id",how="left")
store_level_output=pd.merge(store_level_output,df_5,on="location_id",how="left")
store_level_output=pd.merge(store_level_output,df_7,on="location_id",how="left")


# In[9]:

store_level_output['Q2_rewards_sales_share_2018']=store_level_output['Q2_rewards_sales_2018']/store_level_output['2018_Q2_Sales']
store_level_output['Q2_rewards_trans_share_2018']=store_level_output['Q2_rewards_trans_2018']/store_level_output['2018_Q2_Trans']

store_level_output['Q2_rewards_sales_share_2017']=store_level_output['Q2_rewards_sales_2017']/store_level_output['2017_Q2_Sales']
store_level_output['Q2_rewards_trans_share_2017']=store_level_output['Q2_rewards_trans_2017']/store_level_output['2017_Q2_Trans']




# # 8

# In[ ]:

# New from YG request
# Week by week of rewards members YoY


# In[ ]:

count_k=0
rewards_week_by_week=store_level_output[['location_id']]
for week in Q2_2017_Weeks:
    week_end=datetime.datetime.strptime(week,"%Y-%m-%d").date()
    week_start=week_end-datetime.timedelta(days=6)
    df=rewards_sales_from_SP_2017Q2[(rewards_sales_from_SP_2017Q2['transaction_date']>=week_start) &                                   (rewards_sales_from_SP_2017Q2['transaction_date']<=week_end)]
    df=df.groupby(['location_id'])['total_transaction_amt'].sum().to_frame().reset_index()
    df=df.rename(columns={"total_transaction_amt":"Rewards_Sales_"+week})
    rewards_week_by_week=pd.merge(rewards_week_by_week,df,on="location_id",how="left")
    
    count_k+=1
    print(count_k,week,datetime.datetime.now())


# In[ ]:

count_k=0
for week in Q2_2018_Weeks:
    week_end=datetime.datetime.strptime(week,"%Y-%m-%d").date()
    week_start=week_end-datetime.timedelta(days=6)
    df=rewards_sales_from_SP[(rewards_sales_from_SP['transaction_date']>=week_start) &                                   (rewards_sales_from_SP['transaction_date']<=week_end)]
    df=df.groupby(['location_id'])['total_transaction_amt'].sum().to_frame().reset_index()
    df=df.rename(columns={"total_transaction_amt":"Rewards_Sales_"+week})
    rewards_week_by_week=pd.merge(rewards_week_by_week,df,on="location_id",how="left")
    
    count_k+=1
    print(count_k,week,datetime.datetime.now())


# In[ ]:

selected_YoY=["location_id"]
for week in Q2_2018_Weeks:
    col_2018="Rewards_Sales_"+week
    col_2017="Rewards_Sales_"+str(datetime.datetime.strptime(week,"%Y-%m-%d").date()-datetime.timedelta(days=52*7))
    new_col="Rewards_Sales_YoY_"+week
    rewards_week_by_week[new_col]=(rewards_week_by_week[col_2018]-rewards_week_by_week[col_2017])/rewards_week_by_week[col_2017]
    selected_YoY=selected_YoY+[new_col]
    
df_8=rewards_week_by_week[selected_YoY] #df_8 not used, using the whole rewards_week_by_week


# In[ ]:

store_level_output=pd.merge(store_level_output,rewards_week_by_week,on="location_id",how="left")


# # 9

# In[ ]:

# Zip level
del dfiddetail['sign_up_date']
rewards_sales_from_SP=pd.merge(rewards_sales_from_SP,dfiddetail,on="customer_id_hashed",how="left")
rewards_sales_from_SP['zip_cd']=rewards_sales_from_SP['zip_cd'].fillna("zip_missing")

Store_level_PST=pd.read_csv("/home/jian/Projects/Big_Lots/New_TA/zips_in_new_ta/sales_by_zip (Store level).csv",dtype=str)
Store_level_PST=Store_level_PST[Store_level_PST['location_id'].isin(inclusion_store_list_set)]

Store_level_P_Zips=Store_level_PST[Store_level_PST['revenue_flag']=="P"].groupby(['location_id'])['zip'].apply(set).to_frame().reset_index()
Store_level_S_Zips=Store_level_PST[Store_level_PST['revenue_flag']=="S"].groupby(['location_id'])['zip'].apply(set).to_frame().reset_index()
Store_level_T_Zips=Store_level_PST[Store_level_PST['revenue_flag']=="T"].groupby(['location_id'])['zip'].apply(set).to_frame().reset_index()

Store_level_P_Zips_Dict=Store_level_P_Zips.set_index(['location_id']).to_dict()['zip']
Store_level_S_Zips_Dict=Store_level_S_Zips.set_index(['location_id']).to_dict()['zip']
Store_level_T_Zips_Dict=Store_level_T_Zips.set_index(['location_id']).to_dict()['zip']

store_list_with_PST=Store_level_PST['location_id'].unique().tolist()


df_SP_July_Decile=pd.read_csv("/home/jian/Projects/Big_Lots/Loyal_members/Segment_Movement_analysis/Data_From_Sp/df_crm_finalscore_0714data.csv",
                              dtype=str,usecols=['customer_id_hashed','frmindex','zipcodegroup','customer_zip_code'])
df_SP_July_Decile=df_SP_July_Decile.drop_duplicates(['customer_id_hashed'])
decile_list=df_SP_July_Decile['frmindex'].unique().tolist()
len(df_SP_July_Decile['customer_id_hashed'].unique())


# In[16]:

dfiddetail=pd.merge(dfiddetail,df_SP_July_Decile[['customer_id_hashed','frmindex']],on="customer_id_hashed",how="left")
dfiddetail_id_count_by_signup_zip_decile=dfiddetail.groupby(['sign_up_location','zip_cd','frmindex'])['customer_id_hashed'].count().to_frame().reset_index()
dfiddetail_id_count_by_signup_zip_decile=dfiddetail_id_count_by_signup_zip_decile.rename(columns={"customer_id_hashed":"id_count"})


# In[17]:

rewards_sales_by_store=rewards_sales_from_SP.groupby('location_id')['total_transaction_amt'].sum().to_frame().reset_index()
rewards_sales_by_store_dict=rewards_sales_by_store.set_index(['location_id']).to_dict()['total_transaction_amt']

rewards_sales_by_customer_zip_store=rewards_sales_from_SP.groupby(['location_id','zip_cd'])['total_transaction_amt'].sum().to_frame().reset_index()
rewards_trans_by_customer_zip_store=rewards_sales_from_SP.groupby(['location_id','zip_cd'])['total_transaction_amt'].count().to_frame().reset_index()

sign_up_id_by_store_zip=dfiddetail.groupby(['sign_up_location','zip_cd'])['customer_id_hashed'].count().to_frame().reset_index()


# In[ ]:

output_zip_level=pd.DataFrame()
k=0
for store in store_list_with_PST:
    total_rewards_sales_store=rewards_sales_by_store_dict[store]
    store_rewards_sales_by_customer_zip_store=rewards_sales_by_customer_zip_store[rewards_sales_by_customer_zip_store['location_id']==store]
    store_rewards_trans_by_customer_zip_store=rewards_trans_by_customer_zip_store[rewards_trans_by_customer_zip_store['location_id']==store]
    store_dfiddetail_id_count_by_signup_zip_decile=dfiddetail_id_count_by_signup_zip_decile[dfiddetail_id_count_by_signup_zip_decile['sign_up_location']==store]
    store_sign_up_id_by_store_zip=sign_up_id_by_store_zip[sign_up_id_by_store_zip['sign_up_location']==store]
    
    for label in ["P","S","T"]:
        zips_store_label=locals()["Store_level_"+label+"_Zips_Dict"][store]
        
        id_count_store_label_all_zips=sign_up_id_by_zip[sign_up_id_by_zip['zip_cd'].isin(zips_store_label)]['customer_id_hashed'].sum()
        id_count_store_label_store_only=store_sign_up_id_by_store_zip[store_sign_up_id_by_store_zip['zip_cd'].isin(zips_store_label)]['customer_id_hashed'].sum()
        
        sales_store_label=store_rewards_sales_by_customer_zip_store[store_rewards_sales_by_customer_zip_store['zip_cd'].isin(zips_store_label)]['total_transaction_amt'].sum()
        trans_store_label=store_rewards_trans_by_customer_zip_store[store_rewards_trans_by_customer_zip_store['zip_cd'].isin(zips_store_label)]['total_transaction_amt'].sum()
        
        AOV_store_label=sales_store_label/trans_store_label
        sales_share_label=sales_store_label/total_rewards_sales_store
        
        trans_to_id=trans_store_label/id_count_store_label_store_only
        
        for D_decile in decile_list:
            locals()[D_decile+"_store_label"]=store_dfiddetail_id_count_by_signup_zip_decile[(store_dfiddetail_id_count_by_signup_zip_decile['frmindex']==D_decile) &                                                                                             (store_dfiddetail_id_count_by_signup_zip_decile['zip_cd'].isin(zips_store_label))]['id_count'].sum()
            
        locals()['df_app_'+label]=pd.DataFrame({"location_id":store,label+"_id_counts":id_count_store_label_all_zips,
                                                label+"_id_counts_singed_store":id_count_store_label_store_only,label+"_sales_share":sales_share_label,
                                                label+"_AOV":AOV_store_label,label+"_trans_per_id_avg":trans_to_id,
                                                label+"_D1":locals()["D01_store_label"],label+"_D2":locals()["D02_store_label"],
                                                label+"_D3":locals()["D03_store_label"],label+"_D4":locals()["D04_store_label"],
                                                label+"_D5":locals()["D05_store_label"],label+"_D6":locals()["D06_store_label"],
                                                label+"_D7":locals()["D07_store_label"],label+"_D8":locals()["D08_store_label"],
                                                label+"_D9":locals()["D09_store_label"],label+"_D10":locals()["D10_store_label"],
                                               },index=[store])
    df_app_3_lable=pd.merge(df_app_P,df_app_S,on="location_id",how="left")
    df_app_3_lable=pd.merge(df_app_3_lable,df_app_T,on="location_id",how="left")
    df_app_3_lable=df_app_3_lable[['location_id','P_id_counts','P_id_counts_singed_store','P_sales_share','P_AOV','P_trans_per_id_avg','P_D1','P_D2','P_D3','P_D4','P_D5','P_D6','P_D7','P_D8','P_D9','P_D10',
                                   'S_id_counts','S_id_counts_singed_store','S_sales_share','S_AOV','S_trans_per_id_avg','S_D1','S_D2','S_D3','S_D4','S_D5','S_D6','S_D7','S_D8','S_D9','S_D10',
                                   'T_id_counts','T_id_counts_singed_store','T_sales_share','T_AOV','T_trans_per_id_avg','T_D1','T_D2','T_D3','T_D4','T_D5','T_D6','T_D7','T_D8','T_D9','T_D10']]
    output_zip_level=output_zip_level.append(df_app_3_lable)
    k+=1
    if k %100==1:
        print(store,datetime.datetime.now())
        logging.info(str(k)+"|"+store+"|"+str(datetime.datetime.now()))

    if k %100==23:
        print(store,datetime.datetime.now())
        logging.info(str(k)+"|"+store+"|"+str(datetime.datetime.now()))


# # All_Rewards_regardless_of_Zips_New_Columns
# 

# In[ ]:

def unique_id_count(x):
    y=len(set(x))
    return y
    
Sep_12_new_request=rewards_sales_from_SP.groupby(['location_id'])['customer_id_hashed'].apply(unique_id_count).to_frame().reset_index()
Sep_12_new_request=Sep_12_new_request.rename(columns={"customer_id_hashed":"2018_Q2_rewards_id_shopped"})

Sep_12_new_request_2017=rewards_sales_from_SP_2017Q2.groupby(['location_id'])['customer_id_hashed'].apply(unique_id_count).to_frame().reset_index()
Sep_12_new_request_2017=Sep_12_new_request_2017.rename(columns={"customer_id_hashed":"2017_Q2_rewards_id_shopped"})

Sep_12_new_request=pd.merge(Sep_12_new_request,Sep_12_new_request_2017,on="location_id",how="left")
Sep_12_new_request['Q2_rewards_trans_per_id']=np.nan

# In[61]:

rewards_sales_from_SP_Decile=rewards_sales_from_SP[['customer_id_hashed','location_id']].drop_duplicates()
sp_decile=df_SP_July_Decile[['customer_id_hashed','frmindex']]
rewards_sales_from_SP_Decile=pd.merge(rewards_sales_from_SP_Decile,sp_decile,on="customer_id_hashed",how="left")


# In[66]:

count_k=0
Sep_12_new_request_Decile=pd.DataFrame()
for store,group in rewards_sales_from_SP_Decile.groupby(['location_id']):
    for i in range(10):
        frmindex_str="D"+str(i+1).zfill(2)
        locals()[frmindex_str]=len(group[group['frmindex']==frmindex_str])
    df_app=pd.DataFrame({"location_id":store,"D01_July":D01,"D02_July":D02,"D03_July":D03,"D04_July":D04,"D05_July":D05,
                        "D06_July":D06,"D07_July":D07,"D08_July":D08,"D09_July":D09,"D10_July":D10},index=[store])
    Sep_12_new_request_Decile=Sep_12_new_request_Decile.append(df_app)
    count_k+=1
    if count_k %100==1:
        print(count_k,store,datetime.datetime.now())
        logging.info(str(count_k)+"|"+store+"|"+str(datetime.datetime.now()))

    if count_k %100==23:
        print(count_k,store,datetime.datetime.now())
        logging.info(str(count_k)+"|"+store+"|"+str(datetime.datetime.now()))
        
    
Sep_12_new_request=pd.merge(Sep_12_new_request,Sep_12_new_request_Decile,on="location_id",how="left")

logging.info("Done of the new request | "+str(datetime.datetime.now()))


# In[ ]:

'''
# QC
rewards_sales_from_SP=pd.read_csv("/home/jian/Projects/Big_Lots/Loyal_members/loyalty_sales_data/From_Sp/combinedtransactions_0811.csv",
                                 dtype=str,nrows=samplerows,usecols=['customer_id_hashed','transaction_date','transaction_id','location_id','total_transaction_amt'])
rewards_sales_from_SP=rewards_sales_from_SP[rewards_sales_from_SP['location_id'].isin(inclusion_store_list_set)]
rewards_sales_from_SP['transaction_date']=rewards_sales_from_SP['transaction_date'].apply(lambda x: datetime.datetime.strptime(x,"%Y-%m-%d").date())
'''


# In[ ]:




# In[ ]:

rewards_sales_from_SP_2017Q2['transaction_date'].unique()


# In[ ]:

df_qc=rewards_sales_from_SP_2017Q2[rewards_sales_from_SP_2017Q2['location_id']=="1553"]


# In[ ]:

df_qc=df_qc.drop_duplicates()


# In[ ]:

df_qc.head(2)


# In[ ]:

df_qc['total_transaction_amt'].astype(float).sum()


# In[ ]:

1+1


# In[ ]:




# In[ ]:




# In[ ]:




# # Final_Output

# In[19]:

final_output=pd.merge(store_level_output,output_zip_level,on="location_id",how="left")
logging.info("Merge1 | "+str(datetime.datetime.now()))
final_output=pd.merge(final_output,Sep_12_new_request,on="location_id",how="left")
logging.info("Merge2 | "+str(datetime.datetime.now()))
final_output['Q2_rewards_AOV_2018']=final_output['Q2_rewards_sales_2018']/final_output['Q2_rewards_trans_2018']
logging.info("3 | "+str(datetime.datetime.now()))
final_output['Q2_rewards_trans_per_id']=final_output['Q2_rewards_trans_2018']/final_output['2018_Q2_rewards_id_shopped']
logging.info("4 | "+str(datetime.datetime.now()))



# In[ ]:




# In[20]:

sales_weekly=pd.read_excel("/home/jian/BiglotsCode/outputs/Output_2018-09-08/wide_sales_date2018-09-08.xlsx",
                          dtype=str,sheetname="sales")
sales_weekly=sales_weekly[['location_id']+Q2_2017_Weeks+Q2_2018_Weeks]
for col in sales_weekly.columns.tolist()[1:]:
    sales_weekly[col]=sales_weekly[col].astype(float)


# In[21]:

sales_weekly['inclusion_index']=np.where(((sales_weekly['2017-05-13']>0) &                                          (sales_weekly['2017-05-20']>0) &                                          (sales_weekly['2017-05-27']>0) &                                          (sales_weekly['2017-06-03']>0) &                                          (sales_weekly['2017-06-10']>0) &                                          (sales_weekly['2017-06-17']>0) &                                          (sales_weekly['2017-06-24']>0) &                                          (sales_weekly['2017-07-01']>0) &                                          (sales_weekly['2017-07-08']>0) &                                          (sales_weekly['2017-07-15']>0) &                                          (sales_weekly['2017-07-22']>0) &                                          (sales_weekly['2017-07-29']>0) &                                          (sales_weekly['2017-08-05']>0) &                                          (sales_weekly['2018-05-12']>0) &                                          (sales_weekly['2018-05-19']>0) &                                          (sales_weekly['2018-05-26']>0) &                                          (sales_weekly['2018-06-02']>0) &                                          (sales_weekly['2018-06-09']>0) &                                          (sales_weekly['2018-06-16']>0) &                                          (sales_weekly['2018-06-23']>0) &                                          (sales_weekly['2018-06-30']>0) &                                          (sales_weekly['2018-07-07']>0) &                                          (sales_weekly['2018-07-14']>0) &                                          (sales_weekly['2018-07-21']>0) &                                          (sales_weekly['2018-07-28']>0) &                                          (sales_weekly['2018-08-04']>0)),1,0)
sales_weekly_inclusion_df=sales_weekly[['location_id','inclusion_index']]


# In[22]:

final_output=pd.merge(final_output,sales_weekly_inclusion_df,on="location_id",how="left")


# In[23]:

final_output.to_csv("/home/jian/Projects/Big_Lots/Analysis/Store_Stats_201809/BL_Store_Rewards_Stats_JL_"+str(datetime.datetime.now().date())+".csv",index=False)


# In[ ]:




# In[24]:

writer=pd.ExcelWriter("/home/jian/Projects/Big_Lots/Analysis/Store_Stats_201809/BL_Store_Rewards_Stats_JL_"+str(datetime.datetime.now().date())+".xlsx",engine="xlsxwriter")


# In[25]:

inclusion_df=final_output[final_output['inclusion_index']==1]
exclusion_df=final_output[final_output['inclusion_index']==0]

inclusion_df.to_excel(writer,"inclusion",index=False)
exclusion_df.to_excel(writer,"exclusion",index=False)
writer.save()


# In[ ]:



