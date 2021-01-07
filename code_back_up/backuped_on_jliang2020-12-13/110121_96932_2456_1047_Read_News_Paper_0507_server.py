import pandas as pd
import glob
import numpy as np
import datetime
import logging
logging.basicConfig(filename='read all newspaper files.log', level=logging.INFO)

start_running=str(datetime.datetime.now())
logging.info("Start to run: "+ start_running)

folder="/home/jian/Projects/Big_Lots/Newspaper/All newspaper files for reading JL/"

file_list=glob.glob(folder+"*")
logging.info("Total files: "+str(len(file_list)))

Selected_Col=['projectname','storeid','productname','productid','dayname','zips','totalcirc']
output_combined_all_date=pd.DataFrame()
check_df=pd.DataFrame()
finished=[]

for file in file_list:

    date=file.split("/")[len(file.split("/"))-1].split(" ")[0]+" "+file.split("/")[len(file.split("/"))-1].split(" ")[1]+" "+\
    file.split("/")[len(file.split("/"))-1].split(" ")[2]
    date=date.replace("Sept","Sep")

    if len(date.split(" ")[0])==3:
        date=datetime.datetime.strptime(date,"%b %d %Y").date()
    else:
        date=datetime.datetime.strptime(date,"%B %d %Y").date()
    df=pd.read_excel(file,sheetname="to_read",dtype=str,na_values=['NULL',""," "])
    df=df[Selected_Col]
    df['zips']=df['zips'].fillna('null')
    df['zips']=df['zips'].replace('nan','null')
    df['totalcirc']=df['totalcirc'].astype(float)
    df['keys']=df['productid'] #Use only product id to adjust null
    df['Date']=date
    check_df=check_df.append(df)
    
    
    
    df_combine_date=pd.DataFrame()
    for key in df['keys'].unique():
        df_product_store=df[df['keys']==key]
        df_product_store_NA=df_product_store[df_product_store['zips']=='null']
        df_product_store_NA.reset_index(inplace=True)
        del df_product_store_NA['index']
        df_product_store_zip=df_product_store[df_product_store['zips']!='null']
        df_product_store_zip.reset_index(inplace=True)
        del df_product_store_zip['index']        

        if len(df_product_store_NA)==0:
            df_product_store_zip=df_product_store.groupby(['storeid','productname','productid','dayname','zips'])['totalcirc'].sum().to_frame()
            df_product_store_zip['circ_proportion_of_Null']=0
            df_product_store_zip['adjusted_circ_about_Null']=df_product_store_zip['totalcirc']
            df_product_store_zip.reset_index(inplace=True)
        elif len(df_product_store_zip)==0:

            df_product_store_zip=df_product_store.groupby(['storeid','productname','productid','dayname','zips'])['totalcirc'].sum().to_frame()
            df_product_store_zip['circ_proportion_of_Null']=1
            df_product_store_zip['adjusted_circ_about_Null']=df_product_store_zip['totalcirc']
            df_product_store_zip.reset_index(inplace=True)
        elif len(df_product_store_NA)>=1:
            na_zip_circ=df_product_store_NA['totalcirc'].sum()
            df_product_store_zip=df_product_store_zip.groupby(['storeid','productname','productid','dayname','zips'])['totalcirc'].sum().to_frame()
            df_product_store_zip.reset_index(inplace=True)
            df_product_store_zip['circ_proportion_of_Null']=df_product_store_zip['totalcirc'].apply(lambda x:x/sum(df_product_store_zip['totalcirc']))
            df_product_store_zip['adjusted_circ_about_Null']=df_product_store_zip['totalcirc']+df_product_store_zip['circ_proportion_of_Null']*na_zip_circ
        df_combine_date=df_combine_date.append(df_product_store_zip)

    df_combine_date['Date']=date
    finished=finished+[str(date)]
    logging.info(str(date)+ " : Done of "+str(len(finished)))

    output_combined_all_date=output_combined_all_date.append(df_combine_date) 
output_combined_all_date.reset_index(inplace=True)
del output_combined_all_date['index']
output_combined_all_date.to_excel("/home/jian/Projects/Big_Lots/Newspaper/Newspaper_0507/BL_combined newspaper data_JL_20180507.xlsx",index=False)

household=pd.read_csv("/home/jian/Docs/Household and Population/2016/ZipcodePopulation_FromSP.csv",dtype=str)
household=household[['ZIP_CODE',"HH15"]]
household.columns=['zip_cd','Households']
household['Households']=household['Households'].fillna(0)
household['Households']=household['Households'].astype(float)

output_combined_all_date['zip_list']=output_combined_all_date['zips'].apply(lambda x: [ zip.strip().zfill(5) for zip in x.split(",")])
output_combined_all_date['zip_count']=output_combined_all_date['zip_list'].apply(lambda x: len(x))

output_combined_all_date_1=output_combined_all_date[output_combined_all_date['zip_count']==1]
output_combined_all_date_M=output_combined_all_date[output_combined_all_date['zip_count']!=1]
output_combined_all_date_1['zip_cd']=output_combined_all_date_1['zips'].str.zfill(5)
output_combined_all_date_1['zip_list_proportion']=1
output_combined_all_date_1['adjusted_circ_with_list']=output_combined_all_date_1['adjusted_circ_about_Null']
output_combined_all_date_M.reset_index(inplace= True)
del output_combined_all_date_M['index']
output_combined_all_date_M_agg=pd.DataFrame()

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
    if i % 1000 == 1:
        logging.info(str(datetime.datetime.now())+" :")
        logging.info("output_combined_all_date_M_agg done of the row: "+str(i))
        
        # print("output_combined_all_date_M_agg done of the row: "+str(i))
output_adjusted_final=output_combined_all_date_M_agg.append(output_combined_all_date_1)
output_adjusted_final.to_csv("/home/jian/Projects/Big_Lots/Newspaper/Newspaper_0507/BL_combined newspaper final detailed_JL_20180507.csv",index=False)

output_no_detail=output_adjusted_final.groupby(['zip_cd','Date'])['adjusted_circ_with_list'].sum().to_frame()
output_no_detail.reset_index(inplace=True)
output_no_detail=output_no_detail.sort_values(['Date','zip_cd'])
output_no_detail.to_csv("/home/jian/Projects/Big_Lots/Newspaper/Newspaper_0507/BL_newspaper count by zip by date_JL_20180507.csv",index=False)

output_no_detail=pd.read_csv("/home/jian/Projects/Big_Lots/Newspaper/Newspaper_0507/BL_newspaper count by zip by date_JL_20180507.csv")
test_agg_wide=output_no_detail.pivot(index="zip_cd",columns='Date',values='adjusted_circ_with_list')
test_agg_wide.reset_index(inplace=True)
test_agg_wide_columns=test_agg_wide.columns.tolist()[1:]
test_agg_wide_columns=["newspaper_"+x for x in test_agg_wide_columns]
test_agg_wide.columns=[['zip_cd']+test_agg_wide_columns]
test_agg_wide=test_agg_wide.fillna(0)
test_agg_wide.to_csv("/home/jian/Projects/Big_Lots/Newspaper/Newspaper_0507/BL_newspaper wide of date by zip_JL_20180507.csv",index=False)


