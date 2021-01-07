import pandas as pd
from haversine import haversine
import numpy as np
import json
import logging
import datetime
logging.basicConfig(filename='Zip diatance to closedt store.log', level=logging.INFO)
start_running=str(datetime.datetime.now())
logging.info("Start to run: "+ start_running)

data=pd.read_excel("/home/jian/Projects/Big_Lots/New_TA/Zip_Distance_to_nearest_store/BL_Zips in new TA (TA level)_JL_20180330.xlsx")

store_df=data[~pd.isnull(data['location_id'])]
store_df=store_df[['location_id','latitude','longitude']].drop_duplicates().reset_index()

zip_centers=json.load(open("/home/jian/Docs/Geo mapping/center_of_rentrak_zip.json"))
zip_all=[str(x).zfill(5) for x in list(set(data['zip_cd']))]

i=0
zip_i=0
result=pd.DataFrame()
only_nearest_store=pd.DataFrame()
for zip_cd in zip_all:

    for store in store_df['location_id']:
        x=store_df[store_df['location_id']==store]
        lat=x['latitude'].unique().tolist()[0]
        long=x['longitude'].unique().tolist()[0]
        try:
            dist=haversine(zip_centers[zip_cd],(lat,long),miles=True)
        except:
            dist=987654321 # No Zip Center
        df_app=pd.DataFrame({"store":int(store),"zip_cd":zip_cd,"store_lat":lat,"store_long":long,"Distance":dist},index=[i])
        result=result.append(df_app)
        i=i+1
        
    closet_result=result.sort_values(["zip_cd","Distance"],ascending=[True,True])
    closet_result_1=closet_result.groupby(['zip_cd']).head(1)
    only_nearest_store=only_nearest_store.append(closet_result_1)
        
    zip_i=zip_i+1
    if zip_i % 300 ==1:
        # print(zip_i)
        logging.info(str(datetime.datetime.now()))
        logging.info(zip_i)
        
# result.to_csv("/home/jian/Projects/Big_Lots/New_TA/Zip_Distance_to_nearest_store/all_distance.csv")
# closet_result=result.sort_values(["zip_cd","Distance"],ascending=[True,True])
# closet_result_1=closet_result.groupby(['zip_cd']).head(1)
closet_result_1.to_csv("/home/jian/Projects/Big_Lots/New_TA/Zip_Distance_to_nearest_store/zip_with_closest_store_in_TA.csv")