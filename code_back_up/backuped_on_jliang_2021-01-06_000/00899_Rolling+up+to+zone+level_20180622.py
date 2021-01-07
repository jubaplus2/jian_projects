
# coding: utf-8

# In[78]:

# Objective: Keep the dimension of zone (combination of newspaper and zone_id) and remove the dimension of target stores
# With the same exclusion cretioria: 
#   a) 1 event only - removed 
#   b) keep all the stores when it is/was open
#   c) total circ != 0
#   d) the same inclusion stores (recently open in April 2018) & circ stores are before the disappare of the weekly sales
import pandas as pd
import glob
import numpy as np
import datetime
import logging
import re
import os
import gc
today_str=str(datetime.datetime.now().date())
writer_folder="/home/jian/Projects/Big_Lots/Newspaper/zone_zip_level_output_"+today_str+"/"
try:
    os.stat(writer_folder)
except:
    os.mkdir(writer_folder)
    
logging.basicConfig(filename='read_all_newspaper_'+today_str+'_files.log', level=logging.INFO)


# In[61]:

start_running=str(datetime.datetime.now())
logging.info("Newspaper level - Start to run: "+ start_running)

folder="/home/jian/Projects/Big_Lots/Newspaper/All newspaper files for reading JL/"

file_list=glob.glob(folder+"*")
logging.info("Newspaper level - Total files: "+str(len(file_list)))

Selected_Col=['projectname','storeid','productid','zoneid','zips','totalcirc']
output_combined_all_date=pd.DataFrame()
check_df=pd.DataFrame()
finished=[]


# In[62]:

recent_52_weeks_files_list=[]
for file in file_list:           
    date=file.split("/")[len(file.split("/"))-1].split(" ")[0]+" "+file.split("/")[len(file.split("/"))-1].split(" ")[1]+" "+    file.split("/")[len(file.split("/"))-1].split(" ")[2]
    date=date.replace("Sept","Sep")

    if len(date.split(" ")[0])==3:
        date=datetime.datetime.strptime(date,"%b %d %Y").date()
    else:
        date=datetime.datetime.strptime(date,"%B %d %Y").date()
    if date>datetime.date(2017,5,10):
    
        recent_52_weeks_files_list=recent_52_weeks_files_list+[file]
file_list=recent_52_weeks_files_list
# Update the file list into the most recent 52 weeks events


# In[63]:

zip_circ_analysis=pd.read_csv("/home/jian/Projects/Big_Lots/Newspaper/Analysis/zip_level_information.csv")
zip_exclude_1=zip_circ_analysis[(zip_circ_analysis['zip_cd']==0) | (zip_circ_analysis['zip_cd']==99999)]
zip_exclude_2=zip_circ_analysis[(zip_circ_analysis['Event_Count']==1) | (zip_circ_analysis['total_circ']==0)]
zip_exclude=zip_exclude_1.append(zip_exclude_2)[['zip_cd','total_circ']]


# In[64]:

sales_data=pd.read_excel("/home/jian/BiglotsCode/outputs/Output_2018-05-05/wide_sales_date2018-05-05.xlsx")
# Any one week sales >0 in the most recent month, it indicated an "Open" status up to 2018-05-05
recent_month=sales_data.columns.tolist()[len(sales_data.columns.tolist())-4:len(sales_data.columns.tolist())]
most_recent_open_stores=sales_data[['location_id']+recent_month]
most_recent_open_stores['4_week_total_sales']=most_recent_open_stores[most_recent_open_stores.columns[1]]+most_recent_open_stores[most_recent_open_stores.columns[2]]+                                                                  most_recent_open_stores[most_recent_open_stores.columns[3]]+most_recent_open_stores[most_recent_open_stores.columns[4]]
                                                 
most_recent_open_stores=most_recent_open_stores[most_recent_open_stores['4_week_total_sales']>0]
recent_open_stores=most_recent_open_stores['location_id'].unique().tolist()
recent_open_stores=[str(x) for x in recent_open_stores]


# In[65]:

store_latest_non_zero_week=pd.read_csv("/home/jian/BiglotsCode/outputs/combined_sales_long_2018-05-12.csv")
store_latest_non_zero_week=store_latest_non_zero_week[['location_id','week_end_date','sales']]
store_latest_non_zero_week['week_end_date']=store_latest_non_zero_week['week_end_date'].apply(lambda x:datetime.datetime.strptime(x,"%Y-%m-%d").date())
store_latest_non_zero_week=store_latest_non_zero_week[store_latest_non_zero_week['sales']!=0]
store_latest_non_zero_week=store_latest_non_zero_week.sort_values(["location_id",'week_end_date'],ascending=[True,False]).drop_duplicates(['location_id'])
del store_latest_non_zero_week['sales']
store_latest_non_zero_week.columns=['storeid','latest_date']
store_latest_non_zero_week['storeid']=store_latest_non_zero_week['storeid'].astype(str)
store_latest_non_zero_week.reset_index(inplace=True)
store_latest_non_zero_week['storeid']=store_latest_non_zero_week['storeid'].astype(int)

del store_latest_non_zero_week['index']
store_latest_non_zero_week['storeid']=store_latest_non_zero_week['storeid'].astype(str)


# In[67]:

for file in file_list:
    date=file.split("/")[len(file.split("/"))-1].split(" ")[0]+" "+file.split("/")[len(file.split("/"))-1].split(" ")[1]+" "+    file.split("/")[len(file.split("/"))-1].split(" ")[2]
    date=date.replace("Sept","Sep")

    if len(date.split(" ")[0])==3:
        date=datetime.datetime.strptime(date,"%b %d %Y").date()
    else:
        date=datetime.datetime.strptime(date,"%B %d %Y").date()
        
    # if date>=datetime.datetime(2017,5,10).date(): #Most recent 12 months to reduce stores without information

    df=pd.read_excel(file,sheetname="to_read",dtype=str,na_values=['NULL','null','Null',""," "])
    df=df[Selected_Col]
    df['zips']=df['zips'].fillna('null')
    df['zips']=df['zips'].replace('nan','null')
    df['totalcirc']=df['totalcirc'].astype(float)

    df['Date']=date
    df=df[df['storeid'].isin(recent_open_stores)]
    df=pd.merge(df,store_latest_non_zero_week,on="storeid",how="left")
    df=df[df['latest_date']>=date]
    del df['latest_date']
    # check_df=check_df.append(df)



    df_combine_date=pd.DataFrame()
    #Use only product id to adjust null
    for productid,group in df.groupby(['productid']):
        # df_product_store = group.copy()
        df_product_store_zone_NA=group[group['zips']=='null']
        df_product_store_zone_NA.reset_index(inplace=True)
        del df_product_store_zone_NA['index']
        
        df_product_store_zone_zip=group[group['zips']!='null']
        df_product_store_zone_zip.reset_index(inplace=True)
        del df_product_store_zone_zip['index']        

        if len(df_product_store_zone_NA)==0:
            df_product_store_zone_zip=group.groupby(['storeid','productid','zoneid','zips'])['totalcirc'].sum().to_frame()
            df_product_store_zone_zip['circ_proportion_of_Null']=0
            df_product_store_zone_zip['adjusted_circ_about_Null']=df_product_store_zone_zip['totalcirc']
            df_product_store_zone_zip.reset_index(inplace=True)
            #V del df_product_store_zone_zip['index']

        elif len(df_product_store_zone_zip)==0:              
            df_product_store_zone_NA['extract_from_zone 5 digit']=df_product_store_zone_NA['zoneid'].apply(lambda x: re.findall(r"\b\d{5}\b", x))
            unique_len_from_zone=len(df_product_store_zone_NA['extract_from_zone 5 digit'].apply(lambda x :len(x)).unique())
            if unique_len_from_zone==0:
                df_product_store_zone_zip=group.groupby(['storeid','productid','zoneid','zips'])['totalcirc'].sum().to_frame()
                df_product_store_zone_zip['circ_proportion_of_Null']=1
                df_product_store_zone_zip['adjusted_circ_about_Null']=df_product_store_zone_zip['totalcirc']
                df_product_store_zone_zip.reset_index(inplace=True)
                # del df_product_store_zone_zip['index']
            else:                                                                                                    
                group['extract_from_zone 5 digit']=group['zoneid'].apply(lambda x: re.findall(r"\b\d{5}\b", x))                                                                        
                group['zips']=group['extract_from_zone 5 digit'].apply(lambda x: str(x)[1:(len(str(x))-1)].replace("'","").replace("'",""))
                df_product_store_zone_zip=group.groupby(['storeid','productid','zoneid','zips'])['totalcirc'].sum().to_frame()
                df_product_store_zone_zip['circ_proportion_of_Null']=1
                df_product_store_zone_zip['adjusted_circ_about_Null']=df_product_store_zone_zip['totalcirc']
                df_product_store_zone_zip.reset_index(inplace=True)
                # del df_product_store_zone_zip['index']

        elif len(df_product_store_zone_NA)>=1:
            na_zip_circ=df_product_store_zone_NA['totalcirc'].sum()
            df_product_store_zone_zip=df_product_store_zone_zip.groupby(['storeid','productid','zoneid','zips'])['totalcirc'].sum().to_frame()
            df_product_store_zone_zip.reset_index(inplace=True)
            
            df_product_store_zone_zip['circ_proportion_of_Null']=df_product_store_zone_zip['totalcirc'].apply(lambda x:x/sum(df_product_store_zone_zip['totalcirc']))
            df_product_store_zone_zip['adjusted_circ_about_Null']=df_product_store_zone_zip['totalcirc']+df_product_store_zone_zip['circ_proportion_of_Null']*na_zip_circ
        df_combine_date=df_combine_date.append(df_product_store_zone_zip)

    df_combine_date['Date']=date
    finished=finished+[str(date)]
    logging.info(str(date)+ " : Done of "+str(len(finished)))
    print(str(date)+ " : Done of "+str(len(finished)))

    output_combined_all_date=output_combined_all_date.append(df_combine_date) 
output_combined_all_date.reset_index(inplace=True)
del output_combined_all_date['index']
output_combined_all_date.to_excel(writer_folder+"BL_combined newspaper data_JL_"+today_str+".xlsx",index=False)


# In[68]:

household=pd.read_csv("/home/jian/Docs/Household_and_Population/2016/ZipcodePopulation_FromSP.csv",dtype=str)
household=household[['ZIP_CODE',"HH15"]]
household.columns=['zip_cd','Households']
household['Households']=household['Households'].fillna(0)
household['Households']=household['Households'].astype(float)
gc.collect()


# In[79]:




# In[70]:




# In[71]:

output_combined_all_date['Date_Str']=output_combined_all_date['Date'].apply(lambda x: str(x))
output_combined_all_date['key']="S_"+output_combined_all_date['storeid']+" | P_"+output_combined_all_date['productid']+" | Z"+                                output_combined_all_date['zoneid']+" | "+output_combined_all_date['Date_Str']
del output_combined_all_date['Date_Str']

output_combined_all_date['zip_list']=output_combined_all_date['zips'].apply(lambda x: [ zip_str.strip().zfill(5) for zip_str in x.split(",")])
output_combined_all_date['zip_count']=output_combined_all_date['zip_list'].apply(lambda x: len(x))

output_combined_all_date_1=output_combined_all_date[output_combined_all_date['zip_count']==1]
output_combined_all_date_M=output_combined_all_date[output_combined_all_date['zip_count']!=1]
output_combined_all_date_1['zip_cd']=output_combined_all_date_1['zips'].apply(lambda x: str(x).zfill(5))
output_combined_all_date_1['zip_list_proportion']=1
output_combined_all_date_1['adjusted_circ_with_list']=output_combined_all_date_1['adjusted_circ_about_Null']
output_combined_all_date_M.reset_index(inplace= True)
del output_combined_all_date_M['index']
output_combined_all_date_M_agg=pd.DataFrame()


# In[75]:

for i in range(len(output_combined_all_date_M)):
    df=output_combined_all_date_M.iloc[i:(i+1),:]
    df.reset_index(inplace=True)
    del df['index']
    df_to_app=pd.DataFrame()
    for zip_i in list(set(df['zip_list'][0])):
        df_1_zip=df.copy()
        df_1_zip['zip_cd']=zip_i
        df_to_app=df_to_app.append(df_1_zip)
    df_to_app=pd.merge(df_to_app,household,on="zip_cd",how="left")
    df_to_app['Households']=df_to_app['Households'].fillna(0)
    if df_to_app['Households'].sum()!=0:
        df_to_app['zip_list_proportion']=df_to_app['Households']/df_to_app['Households'].sum()
        df_to_app['adjusted_circ_with_list']=df_to_app['adjusted_circ_about_Null']*df_to_app['zip_list_proportion']
    else:
        logging.info("All 0 population: "+str(df_to_app['Date'].unique()[0])+" "+str(df_to_app['storeid'].unique()[0])+" "+str(df_to_app['productid'].unique()[0]))
        logging.info(df_to_app['zip_cd'])
        df_to_app['zip_list_proportion']=1/len(df_to_app)
        df_to_app['adjusted_circ_with_list']=df_to_app['adjusted_circ_about_Null']*df_to_app['zip_list_proportion']
        
    output_combined_all_date_M_agg=output_combined_all_date_M_agg.append(df_to_app)
    if i % 1000 == 100:
        logging.info(str(datetime.datetime.now())+" :")
        logging.info("output_combined_all_date_M_agg done of the row: "+str(i))
        
        # print(str(datetime.datetime.now())+"output_combined_all_date_M_agg done of the row: "+str(i))
output_adjusted_final=output_combined_all_date_M_agg.append(output_combined_all_date_1)
output_adjusted_final.to_csv(writer_folder+"BL_combined newspaper final detailed_JL_"+today_str+".csv",index=False)


# In[ ]:



