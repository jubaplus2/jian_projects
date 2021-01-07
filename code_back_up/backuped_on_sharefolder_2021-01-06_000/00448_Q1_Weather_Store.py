
# coding: utf-8

# In[1]:

import numpy as np
import pandas as pd
import json
import datetime
import glob


# In[2]:

folder="/home/jian/Projects/Big_Lots/Weather/Json_data/daily/api_response_20180524/"
json_list_all=glob.glob(folder+"*.json")


# In[3]:

inclusion_stores=pd.read_excel("/home/jian/Projects/Big_Lots/Q1_Post/BL_Sales YoY_JL_20180514.xlsx",sheetname="Inclusion Stores",skiprows=1,dtype=str)
store_zips=pd.read_excel("/home/jian/Projects/Big_Lots/Other_Input/all_store_DMA.xlsx",dtype=str)[['location_id','zip']]
store_zips['zip']=store_zips['zip'].apply(lambda x: x.zfill(5))
inclusion_stores=pd.merge(inclusion_stores,store_zips,on="location_id",how="left")
inclusion_stores=inclusion_stores[['location_id','zip']].rename(columns={"zip":"zip_cd"})


# In[4]:

df_files=pd.DataFrame({"file":json_list_all},index=range(len(json_list_all)))
df_files['Date']=df_files['file'].apply(lambda x: datetime.datetime.strptime(x[len(x)-15:len(x)-5],"%Y-%m-%d"))
df_files=df_files[(df_files['Date']>=datetime.datetime(2018,2,4)) & (df_files['Date']<=datetime.datetime(2018,5,5))]


# In[5]:

all_weather_groups=[]
all_weather_desc=[]
all_weather_id=[]
df_missing_weather=pd.DataFrame()
for file in df_files['file']:
    response=json.load(open(file,"r"))
    for zip_cd in inclusion_stores['zip_cd']:
        try:        
            data_zip_weather=response[zip_cd]['weather']
            for i in range(len(data_zip_weather)):            
                weather_group=data_zip_weather[i]['main']
                weather_desc=data_zip_weather[i]['description']
                
                all_weather_groups=list(set(all_weather_groups+[weather_group]))
                all_weather_desc=list(set(all_weather_desc+[weather_desc]))
                
        except:
            df_missing_weather=df_missing_weather.append(pd.DataFrame({"Date":file[len(file)-15:len(file)-5],"zips":zip_cd},index=[0]))
            # print("zip data not available: " + zip_cd+" | "+file[len(file)-15:len(file)-5])


# In[6]:

df_missing_weather=df_missing_weather.sort_values(["Date","zips"])
writer_folder="/home/jian/Projects/Big_Lots/Weather/Q1_Weather_Counts/"

writer=pd.ExcelWriter(writer_folder+"Q1_inclusion_store_all_weather_type_JL_"+str(datetime.datetime.now().date())+".xlsx",engine="xlsxwriter")


# In[7]:

pd.DataFrame({"all_type_group":all_weather_groups}).to_excel(writer,"all_weather_group_list",index=False)
pd.DataFrame({"all_type_desc":all_weather_desc}).to_excel(writer,"all_weather_desc_list",index=False)
df_missing_weather.to_excel(writer,"missing_zips",index=False)


# In[8]:

writer.save()


# In[ ]:




# In[ ]:




# In[ ]:




# In[9]:

ranked_rain_snow_df=pd.read_excel("/home/jian/Projects/Big_Lots/Weather/Q1_Weather_Counts/Q1_inclusion_store_all_weather_type_ranked.xlsx",sheetname="ranked rain snow")

ranked_rain=ranked_rain_snow_df.iloc[:,[4,5]]
ranked_rain.columns=[['rain_desc','rank']]
ranked_rain=ranked_rain.sort_values("rank")

ranked_snow=ranked_rain_snow_df.iloc[:,[0,1]]
ranked_snow.columns=[['snow_desc','rank']]
ranked_snow=ranked_snow[~pd.isnull(ranked_snow['snow_desc'])]
ranked_snow=ranked_snow.sort_values("rank")


# In[ ]:




# # Rain

# In[10]:

df_output_rain=pd.DataFrame(columns=["Store","zip_cd","Date","Collected","No_Rain"]+ranked_rain['rain_desc'].tolist())
df_output_snow=pd.DataFrame(columns=["Store","zip_cd","Date","Collected","No_Snow"]+ranked_snow['snow_desc'].tolist())
df_output_temp=pd.DataFrame(columns=["Store","zip_cd","Date","Collected","Temp","Temp_Max","Temp_Min"])


# In[ ]:

for file in df_files['file']:
    date=datetime.datetime.strptime(file[len(file)-15:len(file)-5],"%Y-%m-%d").date()
    response=json.load(open(file,"r"))
    
    df_rain=pd.DataFrame(columns=["Store","zip_cd","Date","Collected","No_Rain"]+ranked_rain['rain_desc'].tolist())
    df_snow=pd.DataFrame(columns=["Store","zip_cd","Date","Collected","No_Snow"]+ranked_snow['snow_desc'].tolist())
    df_temp=pd.DataFrame(columns=["Store","zip_cd","Date","Collected","Temp","Temp_Max","Temp_Min"])
    df_rain['Store']=inclusion_stores['location_id']
    df_rain['zip_cd']=inclusion_stores['zip_cd']
    df_rain['Date']=date
    df_snow['Store']=inclusion_stores['location_id']
    df_snow['zip_cd']=inclusion_stores['zip_cd']
    df_snow['Date']=date
    df_temp['Store']=inclusion_stores['location_id']
    df_temp['zip_cd']=inclusion_stores['zip_cd']   
    df_temp['Date']=date
    

        
    for zip_cd in inclusion_stores['zip_cd'].unique().tolist():
        if zip_cd in list(response.keys()):
            df_rain[df_rain['zip_cd']==zip_cd]["Collected"]="Recorded"
            df_snow[df_snow['zip_cd']==zip_cd]["Collected"]="Recorded"
            df_temp[df_temp['zip_cd']==zip_cd]["Collected"]="Recorded"
            temp=response[zip_cd]['main']['temp']*9/5 - 459.67
            temp_Max=response[zip_cd]['main']['temp_max']*9/5 - 459.67
            temp_Min=response[zip_cd]['main']['temp_min']*9/5 - 459.67

            df_output_temp[df_output_temp['zip_cd']==zip_cd]['Temp']=temp
            df_output_temp[df_output_temp['zip_cd']==zip_cd]['Temp_Max']=temp_Max
            df_output_temp[df_output_temp['zip_cd']==zip_cd]['Temp_Min']=temp_Min

            for desc_rain in ranked_rain['rain_desc'].tolist():
                for j in range(len(response[zip_cd]['weather'])):
                    if desc_rain in response[zip_cd]['weather'][j]['description']:
                        df_rain[df_rain['zip_cd']==zip_cd][desc_rain]=1
            for desc_snow in ranked_snow['snow_desc'].tolist():
                for j in range(len(response[zip_cd]['weather'])):
                    if desc_snow in response[zip_cd]['weather'][j]['description']:
                        df_snow[df_snow['zip_cd']==zip_cd][desc_snow]=1
        else:
            df_rain[df_rain['zip_cd']==zip_cd]["Collected"]="Not_recorded"
            df_snow[df_snow['zip_cd']==zip_cd]["Collected"]="Not_recorded"
            df_temp[df_temp['zip_cd']==zip_cd]["Collected"]="Not_recorded"
            
    df_output_rain=df_output_rain.append(df_rain)
    df_output_snow=df_output_snow.append(df_snow)
    df_output_temp=df_output_temp.append(df_temp)                
    print( datetime.datetime.now(),"finished of the date: ",date)
                


# In[ ]:

df_output_rain['sum']=df_output_rain[ranked_rain['rain_desc'].tolist()].sum(axis=1)
df_output_snow['sum']=df_output_snow[ranked_snow['snow_desc'].tolist()].sum(axis=1)
df_output_rain['No_Rain']=np.where(pd.isnull(df_output_rain['sum']),1,np.nan)
df_output_snow['No_Snow']=np.where(pd.isnull(df_output_snow['sum']),1,np.nan)

df_output_rain['No_Rain']=np.where(df_output_rain['Collected']=="Not_recorded",np.nan,df_output_rain['No_Rain'])
df_output_snow['No_Snow']=np.where(df_output_snow['Collected']=="Not_recorded",np.nan,df_output_rain['No_Rain'])

del df_output_rain['sum']
del df_output_snow['sum']


# In[ ]:

writer=pd.ExcelWriter(writer_folder+"Q1_Snow_Rain_Desc_"+str(datetime.datetime.now().date())+".xlsx",engine="xlsxwriter")
df_output_temp.to_excel(writer,"Temp",index=False)
df_output_rain.to_excel(writer,"Rain",index=False)
df_output_snow.to_excel(writer,"Snow",index=False)

writer.save()

