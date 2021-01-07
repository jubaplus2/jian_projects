
# coding: utf-8

# In[8]:

import pandas as pd
import numpy as np
from haversine import haversine
import json
import googlemaps
import datetime


# In[2]:

Bing_Zip_0=pd.read_csv("/home/jian/Projects/Smoothie_King/Rewards_Zip/output_108/SK_Unique_lat_lng_pair_JL_key_0_2018-11-11.csv",dtype=str)
Bing_Zip_1=pd.read_csv("/home/jian/Projects/Smoothie_King/Rewards_Zip/output_108/SK_Unique_lat_lng_pair_JL_key_1_2018-11-11.csv",dtype=str)
Bing_Zip_2=pd.read_csv("/home/jian/Projects/Smoothie_King/Rewards_Zip/output_108/SK_Unique_lat_lng_pair_JL_key_2_2018-11-11.csv",dtype=str)
Bing_Zip_3=pd.read_csv("/home/jian/Projects/Smoothie_King/Rewards_Zip/output_108/SK_Unique_lat_lng_pair_JL_key_3_2018-11-11.csv",dtype=str)
Bing_Zip_4=pd.read_csv("/home/jian/Projects/Smoothie_King/Rewards_Zip/output_108/SK_Unique_lat_lng_pair_JL_key_4_2018-11-11.csv",dtype=str)
Bing_Zip_5=pd.read_csv("/home/jian/Projects/Smoothie_King/Rewards_Zip/output_108/SK_Unique_lat_lng_pair_JL_key_5_2018-11-11.csv",dtype=str)
Bing_Zip_6=pd.read_csv("/home/jian/Projects/Smoothie_King/Rewards_Zip/output_108/SK_Unique_lat_lng_pair_JL_key_6_2018-11-11.csv",dtype=str)
Bing_Zip_7=pd.read_csv("/home/jian/Projects/Smoothie_King/Rewards_Zip/output_108/SK_Unique_lat_lng_pair_JL_key_7_2018-11-11.csv",dtype=str)

Bing_Zip=Bing_Zip_0.append(Bing_Zip_1).append(Bing_Zip_2).append(Bing_Zip_3).append(Bing_Zip_4).append(Bing_Zip_5).append(Bing_Zip_6).append(Bing_Zip_7)
Bing_Zip=Bing_Zip.reset_index()
del Bing_Zip['index']
Bing_Zip['Bing_return_dist']=Bing_Zip['Bing_return_dist'].astype(float)
Bing_Zip_valid=Bing_Zip[Bing_Zip['Bing_return_dist']<=0.1]
Bing_Zip['Source']="Bing"

Bing_Zip_to_be_Google=Bing_Zip[Bing_Zip['Bing_return_dist']>0.1]
Bing_Zip_to_be_Google['Source']="Google"



# In[3]:

Bing_Zip_to_be_Google=Bing_Zip_to_be_Google[['latitude','longitude']].reset_index()
del Bing_Zip_to_be_Google['index']

Bing_Zip_to_be_Google['Google_zip']=np.nan
Bing_Zip_to_be_Google['Google_lat']=np.nan
Bing_Zip_to_be_Google['Google_lng']=np.nan
Bing_Zip_to_be_Google['Google_return_dist']=np.nan
Bing_Zip_to_be_Google['Google_Address']=np.nan
Bing_Zip_to_be_Google['Source']=np.nan


# In[4]:

key='AIzaSyDxp8O8JKOvbuB6F5DfqyyJMYPPKwIXLdY'
gmaps = googlemaps.Client(key=key)


# In[78]:

for i in range(len(Bing_Zip_to_be_Google)):
    lat=float(Bing_Zip_to_be_Google['latitude'][i])
    lng=float(Bing_Zip_to_be_Google['longitude'][i])
    
    Google_lat=np.nan
    Google_lng=np.nan
    Google_Address=np.nan
    Google_zip=np.nan
         
    response_original=gmaps.reverse_geocode((lat,lng))
    if len(response_original)>0:   
        response=response_original[0]
        Google_lat=response['geometry']['location']['lat']
        Google_lng=response['geometry']['location']['lng']
        Google_Address=response['formatted_address']
        dist=haversine((lat,lng),(Google_lat,Google_lng),miles=True)
        
        for j in range(len(response['address_components'])):
            if response['address_components'][j]['types']==['postal_code']:
                Google_zip=response['address_components'][j]['long_name']
        
    Bing_Zip_to_be_Google.loc[i,'Google_zip']=Google_zip
    Bing_Zip_to_be_Google.loc[i,'Google_lat']=Google_lat
    Bing_Zip_to_be_Google.loc[i,'Google_lng']=Google_lng
    Bing_Zip_to_be_Google.loc[i,'Google_return_dist']=dist
    Bing_Zip_to_be_Google.loc[i,'Google_Address']=Google_Address
    if i%1000==2:
        print(i,datetime.datetime.now())
Bing_Zip_to_be_Google['Source']="Google"


# In[9]:

Bing_Zip_to_be_Google.to_csv("/home/jian/Projects/Smoothie_King/Rewards_Zip/Run_pairs_on_Google_JL_"+str(datetime.datetime.now().date())+".csv",index=False)


# In[ ]:



