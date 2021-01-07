
# coding: utf-8

# In[28]:

import geocoder
import pandas as pd
import numpy as np
import datetime
import time
import logging
from haversine import haversine
# logging.basicConfig(filename="celery"+ str(datetime.datetime.now().date())+".log", level=logging.INFO)


# In[29]:

rewards_data=pd.read_table("/home/jian/Projects/Smoothie_King/data/LoyaltyUser_103018.txt",dtype=str,sep=",")


# In[30]:

rewards_data_df_TA=rewards_data[['user_id','zipcode','latitude','longitude']]
rewards_data_df_TA['zipcode']=rewards_data_df_TA['zipcode'].fillna("0")
rewards_data_df_TA['zipcode']=rewards_data_df_TA['zipcode'].apply(lambda x: x.split("-")[0].zfill(5))
rewards_data_df_TA['latitude']=rewards_data_df_TA['latitude'].astype(float).apply(lambda x: np.round(x,6))
rewards_data_df_TA['longitude']=rewards_data_df_TA['longitude'].astype(float).apply(lambda x: np.round(x,6))

# Round to 6 digits which result in error about 0.14 meter


# In[31]:

zips_TA=pd.read_excel("/home/jian/Projects/Smoothie_King/TA/SmoothieKing_TA_revised_3_miles_JL_2018-10-31.xlsx",dtype=str,sheetname="zip_TA")
store_TA=pd.read_excel("/home/jian/Projects/Smoothie_King/TA/SmoothieKing_TA_revised_3_miles_JL_2018-10-31.xlsx",dtype=str,sheetname="output_TA_by_store")
store_TA=store_TA[store_TA['status']!="NonComp"]
zips_TA=zips_TA[zips_TA['store'].isin(store_TA['storenumber'])]
zips_in_TA_list=zips_TA['zip'].unique().tolist()
stores_in_TA_list=store_TA['storenumber'].unique().tolist()


# # Step 1: Use zipcode in the rewards data to check in

# In[32]:

rewards_data_df_TA_in=rewards_data_df_TA[(rewards_data_df_TA['zipcode'].isin(zips_in_TA_list))]
rewards_data_df_TA_in['TA']="In"
rewards_data_df_TA_out=rewards_data_df_TA[~(rewards_data_df_TA['zipcode'].isin(zips_in_TA_list))]
rewards_data_df_TA_out['TA']="Out"
unique_lat_lng_pair_in_out=rewards_data_df_TA_out[['latitude','longitude']].drop_duplicates()


# # Step 2: For those zipcodes not in, check lat lng

# In[33]:

unique_lat_lng_pair_in_out=rewards_data_df_TA_out[['latitude','longitude']].drop_duplicates()
unique_lat_lng_pair_in_out=unique_lat_lng_pair_in_out.reset_index()
del unique_lat_lng_pair_in_out['index']
unique_lat_lng_pair_in_out['Bing_zip']=np.nan
unique_lat_lng_pair_in_out['Bing_lat']=np.nan
unique_lat_lng_pair_in_out['Bing_lng']=np.nan
unique_lat_lng_pair_in_out['Bing_return_dist']=np.nan
unique_lat_lng_pair_in_out['Bing_Address']=np.nan


# In[34]:

key_1="AuR1ORKdndGqHaD4jiql1C5Inwmc5BOqJrlo5joEAb2Yljp5aizGIN7IVoxJNcim"
key_2="AqKD9pWS14y4TYe2SHAiss2hgu2g8WbH4jqHE2fkFFh_TwMJVzyh2s51ZYhUvDjd"
key_3="AgkMFg3p8ay9X30Ki1BUBaEm5I9Q7aYeevjrgthjqZCwNXR_5wJjWpgA8rlR13Lb"
key_4="AjFG9KoflGW48sPgVbrD6mnfdhQvh11fHQW74h4LcVrYbTiKOH8X2j9rDee7m5nt"
key_5="Atad91C2OX9gdh8CpuyWU4cBFOsqi1WF_S3GlaNU5PY4txgcvX2oQXeZ98AJ6OhY"
key_6="Ags3k581BNyCFut0Vo4YvlUP1ccQU1AgWjVawe7FrReB-I_jL-ck-xxDp1c4DyBn"
key_7="AgvsfKzpX7lz1jfQOUV5URxhMwdNWa0w3gHwUZjmekfHBj-dg3I9yNbwZErlzSW-"
key_8="AvN6wkXmVKIhUw5NsqoyxKoRZ_u9ZvEnSBQEoscOiBlNQg23RIPaA1gDRszp9W2N"

key_list=[key_1,key_2,key_3,key_4,key_5,key_6,key_7,key_8]
# All tested to be workable
print(str(datetime.datetime.now())+": Start to run Bing Map")
output_lat_long=pd.DataFrame()

# Can be optimised later through "batch_reverse" method


# In[36]:

for i in range(int(np.ceil(unique_lat_lng_pair_in_out.shape[0]/100000))):
    df=unique_lat_lng_pair_in_out.iloc[i*100000:(i+1)*100000,].reset_index()
    key_bing=key_list[i]
    del df['index']
    for j in range(len(df)):
        coordinate_pair=[df['latitude'][j],df['longitude'][j]]
        try:
            response=geocoder.bing(coordinate_pair,key=key_bing,method="reverse")
        except:
            response=[]
            
            
        try:
            bing_lat=response.latlng[0]
        except:
            bing_lat=np.nan
            
        try:
            bing_lng=response.latlng[1]
        except:
            bing_lng=np.nan
        
        try:
            bing_address=response.address
        except:
            bing_address=np.nan
        
        try:
            bing_postal=response.postal
        except:
            bing_postal=np.nan    
            
        try:
            dist=haversine(coordinate_pair,[bing_lat,bing_lng],miles=True)
        except:
            dist=np.nan
            
        df['Bing_lat'][j]=bing_lat
        df['Bing_lng'][j]=bing_lng
        df['Bing_zip'][j]=bing_postal
        df['Bing_Address'][j]=bing_address
        df['Bing_return_dist'][j]=dist
        time.sleep(1)
        
        if j%33==1:
            time.sleep(0.1)
        if j%1000==999:
            time.sleep(1.2)
            print("i: "+str(i)+"|j: "+str(j)+" "+str(datetime.datetime.now()))
            
    time.sleep(61)
    print("Finished of i: "+str(i)+" "+str(datetime.datetime.now()))
    df.to_csv("/home/jian/Projects/Smoothie_King/Rewards_Zip/SK_Unique_lat_lng_pair_JL_key_"+str(i)+"_"+str(datetime.datetime.now().date())+".csv",index=False)
    output_lat_long=output_lat_long.append(df)


# In[37]:

output_lat_long.to_csv("/home/jian/Projects/Smoothie_King/Rewards_Zip/SK_Unique_lat_lng_pair_JL_"+str(datetime.datetime.now())+".csv",index=False)


# In[38]:

output_lat_long['Bing_zip']=output_lat_long['Bing_zip'].fillna("0")
print("1")

output_lat_long['Bing_zip']=output_lat_long['Bing_zip'].apply(lambda x: str(x).split("-")[0].split(".")[0].zfill(5))
print("2")
rewards_data_df_TA_out=pd.merge(rewards_data_df_TA_out,output_lat_long,on=['latitude','longitude'],how="left")
print("3")
rewards_data_df_TA_out['TA'][rewards_data_df_TA_out['Bing_zip'].isin(zips_in_TA_list)]="In"
print("4")
rewards_output_with_ind=rewards_data_df_TA_in.append(rewards_data_df_TA_out)
print("5")
rewards_output_with_ind.to_csv("/home/jian/Projects/Smoothie_King/Rewards_Zip/SK_Rewards_in_or_out_TA_JL_"+str(datetime.datetime.now().date())+".csv",index=False)
print("6")


# In[41]:

df_to_merge=rewards_output_with_ind[['user_id','TA','Bing_zip']]
final_output=pd.merge(rewards_data,df_to_merge,on="user_id",how="left")
final_output.to_csv("/home/jian/Projects/Smoothie_King/Rewards_Zip/SK_Rewards_with_TA_index_JL_"+str(datetime.datetime.now().date())+".csv",index=False)
print("Done"+str(datetime.datetime.now()))


# In[ ]:



