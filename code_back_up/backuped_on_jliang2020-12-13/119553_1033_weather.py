import json
import pandas as pd
import os
import glob
import codecs
import datetime
json_list=glob.glob("/home/jian/Projects/Big_Lots/Weather/Json_data/api_response till 20180322/*.json")

store_list=pd.read_csv("/home/jian/Projects/Big_Lots/Weather/OtherInput/MediaStormStoreList_Nov15.txt",sep="|",dtype=str)
store_list=store_list[['location_id','city_nm','state_nm','zip_cd']]
store_list['zip_cd']=store_list['zip_cd'].apply(lambda x: str.split(x,'-')[0])
store_list['zip_cd']=store_list['zip_cd'].astype(int)

zip_dma=pd.read_csv("/home/jian/Projects/Big_Lots/Weather/OtherInput/zipdmamapping.csv")
zip_dma.columns=['zip_cd','DMA']
zip_dma['zip_cd']=zip_dma['zip_cd'].astype(int)
store_list=pd.merge(store_list,zip_dma,on='zip_cd',how='left')

date_writer=[]
for date_json in json_list:
    date_json_time=datetime.datetime.strptime(date_json[len(date_json)-15:len(date_json)-5],"%Y-%m-%d").date()
    date_str=str(date_json_time)
    date_writer=date_writer+[date_json_time]
    
response_list=[]
date_list=[]
output=pd.DataFrame()


for f in json_list:
    response=json.load(open(f,'r'))
    # date=f.split('.')[0][(len(f.split('.')[0])-10):len(f.split('.')[0])]
    df_day=pd.DataFrame(response)
    df=pd.DataFrame()
    df_date=pd.DataFrame()
    for i in range(len(df_day.columns)):
        zip_cd=df_day.columns[i]   
        temp=response[zip_cd]['main']['temp']
        temp_max=response[zip_cd]['main']['temp_max']
        temp_min=response[zip_cd]['main']['temp_min']
        humidity=str(response[zip_cd]['main']['humidity'])+"%"
        pressure=response[zip_cd]['main']['pressure']
        weather_types=len(response[zip_cd]['weather'])
        wind_speed=response[zip_cd]['wind']['speed']
        name=response[zip_cd]['name']
        country=response[zip_cd]['sys']['country']
        sunrise=pd.to_datetime(response[zip_cd]['sys']['sunrise'],unit='s')
        sunset=pd.to_datetime(response[zip_cd]['sys']['sunset'],unit='s')
        
        time=str(datetime.datetime.fromtimestamp(response[zip_cd]['dt']).time())
        date=str(datetime.datetime.fromtimestamp(response[zip_cd]['dt']).date())
        
        if 'deg' in response[zip_cd]['wind'].keys():
            wind_deg=response[zip_cd]['wind']['deg']
        else:
            wind_deg=float('NaN')
        
        if 'visibility' in response[zip_cd].keys():
            visibility=response[zip_cd]['visibility']
        else: 
            visibility= float('NaN')
            
        if 'clouds' in response[zip_cd].keys():
            clouds=str(response[zip_cd]['clouds']['all'])+"%"
        else: 
            clouds= float('NaN')
        '''    
        if 'rain' in response[zip_cd].keys():
            rain_3h=response[zip_cd]['rain']['3h']
        else: 
            rain_3h= float('NaN')
        
        if 'snow' in response[zip_cd].keys():
            snow_3h=response[zip_cd]['snow']['3h']
        else: 
            snow_3h= float('NaN')
        '''    
        lat=response[zip_cd]['coord']['lat']
        lon=response[zip_cd]['coord']['lon']
        # df_weather=pd.DataFrame({'index':i},{'zip':zip_cd},{'weather':weather}).T    
        weather = []
        for j in range(weather_types):
            weather = weather+[str(response[zip_cd]['weather'][j]['main'])]

        # df_weather=pd.DataFrame([i,zip_cd,temp,temp_max,temp_min,name,weather,weather_types,pressure,humidity,wind_speed,wind_deg,visibility,lat,lon]).T
        # df_weather.columns=['index','zip','temperature','temp_max','temp_min','city_name','weather','weather_types','pressure','humidity','wind_speed','wind_deg','visibility','lat_weather_record','lon_weather_record']
        
        df_weather=pd.DataFrame([{'date':date,'time':time,'zip_cd':zip_cd,'city_name_weather':name,'temperature':temp,'temp_max':temp_max,'temp_min':temp_min,
                                 'weather':weather,'weather_types':weather_types,'pressure':pressure,'humidity':humidity,'clouds':clouds,
                                  'wind_speed':wind_speed,'wind_deg':wind_deg,'visibility':visibility,'longitude':lon,'latitude':lat,
                                  'utc_sunrise':sunrise,'utc_sunset':sunset}]) #'rain_3h':rain_3h,'snow_3h':snow_3h,
        
        df_weather.reset_index(inplace=True)

        df=df.append(df_weather)
        # df=df[df_weather.columns.tolist()]

    df['zip_cd']=df['zip_cd'].astype(int)
    df_date=pd.merge(store_list,df,on='zip_cd',how='left')

    # del df_date['index']
    output=output.append(df_date)
    
output_reshape=output[['date','time','zip_cd','location_id','city_nm','state_nm','DMA','city_name_weather','latitude','longitude','temperature','temp_max',
                      'temp_min','weather','weather_types','clouds','humidity','pressure','visibility','wind_speed','wind_deg',
                      'utc_sunrise','utc_sunset']]

output_reshape.to_csv("/home/jian/Projects/Big_Lots/Weather/output/BL_whether_output_JL_" +str(max(date_writer))+".csv",index=False)