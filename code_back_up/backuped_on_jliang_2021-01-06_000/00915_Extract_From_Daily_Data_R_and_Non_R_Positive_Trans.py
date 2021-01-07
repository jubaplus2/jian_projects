
# coding: utf-8

# In[1]:

import pandas as pd
import numpy as np
import datetime
import os
import glob
import gc
os.getcwd()


# In[2]:

store_info=pd.read_table("/home/jian/BigLots/static_files/Store_list/MediaStormStores20190201-133832-957.txt",sep="|",dtype=str)
store_info=store_info[['location_id','city_nm','state_nm','zip_cd','open_dt']]

store_info_2=pd.read_table("/home/jian/BigLots/static_files/Store_list/MediaStormStores20190101-135843-638.txt",sep="|",dtype=str)
store_info_2=store_info_2[['location_id','city_nm','state_nm','zip_cd','open_dt']]
store_info_2=store_info_2[~store_info_2['location_id'].isin(store_info['location_id'].tolist())]
store_info=store_info.append(store_info_2)

store_info_2=pd.read_table("/home/jian/BigLots/static_files/Store_list/MediaStormStores20181201-135231-415.txt",sep="|",dtype=str)
store_info_2=store_info_2[['location_id','city_nm','state_nm','zip_cd','open_dt']]
store_info_2=store_info_2[~store_info_2['location_id'].isin(store_info['location_id'].tolist())]
store_info=store_info.append(store_info_2)

store_info_2=pd.read_table("/home/jian/BigLots/static_files/Store_list/MediaStormStores20181101-134628-331.txt",sep="|",dtype=str)
store_info_2=store_info_2[['location_id','city_nm','state_nm','zip_cd','open_dt']]
store_info_2=store_info_2[~store_info_2['location_id'].isin(store_info['location_id'].tolist())]
store_info=store_info.append(store_info_2)

store_info_2=pd.read_table("/home/jian/BigLots/static_files/Store_list/MediaStormStores20181001-135417-132.txt",sep="|",dtype=str)
store_info_2=store_info_2[['location_id','city_nm','state_nm','zip_cd','open_dt']]
store_info_2=store_info_2[~store_info_2['location_id'].isin(store_info['location_id'].tolist())]
store_info=store_info.append(store_info_2)

store_info_2=pd.read_table("/home/jian/BigLots/static_files/Store_list/MediaStormStores20180901-133640-935.txt",sep="|",dtype=str)
store_info_2=store_info_2[['location_id','city_nm','state_nm','zip_cd','open_dt']]
store_info_2=store_info_2[~store_info_2['location_id'].isin(store_info['location_id'].tolist())]
store_info=store_info.append(store_info_2)

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


DMA_Zip=pd.read_excel("/home/jian/Docs/Geo_mapping/Zips by DMA by County16-17 nielsen.xlsx",skiprows=1,dtype=str)
DMA_Zip=DMA_Zip.iloc[:,[0,2]]
DMA_Zip.columns=['zip_cd','DMA']
DMA_Zip=DMA_Zip.drop_duplicates(['zip_cd'])


store_info=pd.merge(store_info,DMA_Zip,on="zip_cd",how="left")


# In[3]:

weeks_2018Q4=[datetime.date(2018,11,10)+datetime.timedelta(days=x*7) for x in range(13)]
weeks_2017Q4=[x-datetime.timedelta(days=52*7) for x in weeks_2018Q4]


# In[4]:

def recursive_file_gen(my_root_dir):
    for root, dirs, files in os.walk(my_root_dir):
        for file in files:
            yield os.path.join(root, file)


# In[5]:

Q4_files_2017="/home/jian/BigLots/2017_Daily_Sales_Weeks_Quarter_as_Transfer/2017_Q4/*.txt"
Q4_files_2017=glob.glob(Q4_files_2017)
Q4_files_2017.remove("/home/jian/BigLots/2017_Daily_Sales_Weeks_Quarter_as_Transfer/2017_Q4/MediaStormDailySales17Q4W120181204-052708-319.txt")



# In[6]:

def week_end_dt(x):
    if x.weekday()==6:
        y=x+datetime.timedelta(days=6)
    else:
        y=x+datetime.timedelta(days=(5-x.weekday()))
    return y

def count_unique(x):
    return len(set(x))


# In[7]:

def load_and_agg_df(file_list):
    
    i_counter=0

    sales_agg_df=pd.DataFrame()
    ids_by_week_store=pd.DataFrame()

    for file_path in file_list:
        df=pd.read_table(file_path,dtype=str,sep="|",usecols=None,nrows=None)
        df=df[df['location_id']!="6990"]
        # print(df.shape)
        df=df.drop_duplicates()
        # print(df.shape)
        df['transaction_dt']=df['transaction_dt'].apply(lambda x: datetime.datetime.strptime(x, "%Y-%m-%d").date())
        df['subclass_transaction_amt']=df['subclass_transaction_amt'].astype(float)

        df['rewards_label']=np.where(pd.isnull(df['customer_id_hashed']),"Non_Rewards","Rewards")

        date_max=df['transaction_dt'].max()
        date_min=df['transaction_dt'].min()

        # print(i_counter,date_max,date_min,datetime.datetime.now())

        if ((date_max-date_min).days==6) & (date_max.weekday()==5):
            df['week_end_dt']=date_max
            df_agg_sales=df.groupby(['location_id','week_end_dt','rewards_label'])['subclass_transaction_amt'].sum().to_frame().reset_index()
            df_agg_sales=df_agg_sales.rename(columns={"subclass_transaction_amt":"sales"})
            
            df=df[df['subclass_transaction_amt']>0]
            df_agg_trans=df[['location_id','transaction_dt','week_end_dt','transaction_id','customer_id_hashed','rewards_label']].drop_duplicates()
            df_agg_trans['transactions']=1
            df_agg_trans=df_agg_trans.groupby(['location_id','week_end_dt','rewards_label'])['transactions'].sum().to_frame().reset_index()
            
            df_agg_sales=pd.merge(df_agg_sales,df_agg_trans,on=['location_id','week_end_dt','rewards_label'],how="outer")
            
            df=df[df['rewards_label']=="Rewards"]
            df=df[['location_id','week_end_dt','customer_id_hashed']].drop_duplicates()

        else:
            print("Date in the data not 7 days",file_path)
            df=pd.DataFrame()
            df_agg_sales=pd.DataFrame()

        sales_agg_df=sales_agg_df.append(df_agg_sales)
        ids_by_week_store=ids_by_week_store.append(df)
        i_counter+=1

    return sales_agg_df,ids_by_week_store


# In[8]:

possible_folder_1="/home/jian/BigLots/2018_by_weeks/"
possible_folder_2="/home/jian/BigLots/2019_by_weeks/"

Q4_files_2018_all_types=list(recursive_file_gen(possible_folder_1))+list(recursive_file_gen(possible_folder_2))
Q4_files_2018_all_types=[x for x in Q4_files_2018_all_types if "aily" in x]
Q4_files_2018=[]
for week_end_date in weeks_2018Q4:
    file_path=[x for x in Q4_files_2018_all_types if str(week_end_date) in x]
    Q4_files_2018=Q4_files_2018+file_path
    
print(len(Q4_files_2018))


# In[9]:

sales_agg_2017, ids_by_week_store_2017 = load_and_agg_df(Q4_files_2017)

sales_agg_2018, ids_by_week_store_2018 = load_and_agg_df(Q4_files_2018)



# In[ ]:

unique_id_2017_by_store=ids_by_week_store_2017.groupby('location_id')['customer_id_hashed'].apply(count_unique).to_frame().reset_index().rename(columns={"customer_id_hashed":"Q4_unique_id"})
unique_id_2018_by_store=ids_by_week_store_2018.groupby('location_id')['customer_id_hashed'].apply(count_unique).to_frame().reset_index().rename(columns={"customer_id_hashed":"Q4_unique_id"})
unique_id_2017_by_store['rewards_label']="Rewards"
unique_id_2018_by_store['rewards_label']="Rewards"


unique_id_2017_by_store_week=ids_by_week_store_2017.groupby(['location_id','week_end_dt'])['customer_id_hashed'].apply(count_unique).to_frame().reset_index().rename(columns={"customer_id_hashed":"Q4_unique_id"})
unique_id_2018_by_store_week=ids_by_week_store_2018.groupby(['location_id','week_end_dt'])['customer_id_hashed'].apply(count_unique).to_frame().reset_index().rename(columns={"customer_id_hashed":"Q4_unique_id"})
unique_id_2017_by_store_week['week_end_dt']=unique_id_2017_by_store_week['week_end_dt'].apply(lambda x: x.date())
unique_id_2018_by_store_week['week_end_dt']=unique_id_2018_by_store_week['week_end_dt'].apply(lambda x: x.date())
unique_id_2017_by_store_week['rewards_label']="Rewards"
unique_id_2018_by_store_week['rewards_label']="Rewards"


# In[ ]:

sales_agg_2017_with_id_by_week=pd.merge(sales_agg_2017,unique_id_2017_by_store_week,on=['location_id','week_end_dt','rewards_label'],how="outer")
sales_agg_2018_with_id_by_week=pd.merge(sales_agg_2018,unique_id_2018_by_store_week,on=['location_id','week_end_dt','rewards_label'],how="outer")


# In[ ]:

sales_agg_2017_with_id_by_week['location_id']=sales_agg_2017_with_id_by_week['location_id'].astype(int)
sales_agg_2018_with_id_by_week['location_id']=sales_agg_2018_with_id_by_week['location_id'].astype(int)
unique_id_2017_by_store['location_id']=unique_id_2017_by_store['location_id'].astype(int)
unique_id_2018_by_store['location_id']=unique_id_2018_by_store['location_id'].astype(int)


# In[ ]:

writer=pd.ExcelWriter("/home/jian/Projects/Big_Lots/Analysis/2018_Q4/Post_YoY/BL_2018_Q4_post_data_JL_"+str(datetime.datetime.now().date())+".xlsx",engine="xlsxwriter")
sales_agg_2017_with_id_by_week.to_excel(writer,"sales_by_week_2017",index=False)
sales_agg_2018_with_id_by_week.to_excel(writer,"sales_by_week_2018",index=False)
unique_id_2017_by_store.to_excel(writer,"ids_by_store_quarter_2017",index=False)
unique_id_2018_by_store.to_excel(writer,"ids_by_store_quarter_2018",index=False)
writer.save()


# In[ ]:

ids_by_week_store_2017.to_csv("/home/jian/Projects/Big_Lots/Analysis/2018_Q4/Post_YoY/BL_2018_Q4_ids_2017Q4_by_store_JL_"+str(datetime.datetime.now().date())+".csv",index=False)
ids_by_week_store_2018.to_csv("/home/jian/Projects/Big_Lots/Analysis/2018_Q4/Post_YoY/BL_2018_Q4_ids_2018Q4_by_store_JL_"+str(datetime.datetime.now().date())+".csv",index=False)


# In[ ]:



