import pandas as pd
import glob
import numpy as np
import datetime
import logging
import re
import os
today_str=str(datetime.datetime.now().date())
writer_folder="/home/jian/Projects/Big_Lots/Newspaper/output_"+today_str+"/"
try:
    os.stat(writer_folder)
except:
    os.mkdir(writer_folder)
    

logging.basicConfig(filename='read_all_newspaper_'+today_str+'_files.log', level=logging.INFO)

sales_data=pd.read_excel("/home/jian/BiglotsCode/outputs/Output_2018-05-05/wide_sales_date2018-05-05.xlsx")
# Any one week sales >0 in the most recent month, it indicated an "Open" status up to 2018-05-05
recent_month=sales_data.columns.tolist()[len(sales_data.columns.tolist())-4:len(sales_data.columns.tolist())]
most_recent_open_stores=sales_data[['location_id']+recent_month]
most_recent_open_stores['4_week_total_sales']=most_recent_open_stores[most_recent_open_stores.columns[1]]+most_recent_open_stores[most_recent_open_stores.columns[2]]+\
                                                                  most_recent_open_stores[most_recent_open_stores.columns[3]]+most_recent_open_stores[most_recent_open_stores.columns[4]]
                                                 
most_recent_open_stores=most_recent_open_stores[most_recent_open_stores['4_week_total_sales']>0]
recent_open_stores=most_recent_open_stores['location_id'].unique().tolist()

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
store_latest_non_zero_week=store_latest_non_zero_week.rename(columns={'storeid':'location_id'})
del store_latest_non_zero_week['index']

all_week_list=[datetime.datetime.strptime(x,"%Y-%m-%d").date() for x in sales_data.columns.tolist()[1:]]
year_2017_weeks=[str(x) for x in all_week_list if (x>=datetime.datetime(2016,5,8).date()) & (x<datetime.datetime(2017,5,8).date())]
year_2018_weeks=[str(x) for x in all_week_list if (x>=datetime.datetime(2017,5,8).date()) & (x<datetime.datetime(2018,5,8).date())]

sales_data_2017=sales_data[['location_id']+year_2017_weeks]
sales_data_2018=sales_data[['location_id']+year_2018_weeks]
sales_data_2017_existing=sales_data_2017[sales_data_2017['location_id'].isin(recent_open_stores)].sort_values('location_id')
sales_data_2018_existing=sales_data_2018[sales_data_2018['location_id'].isin(recent_open_stores)].sort_values('location_id')
sales_data_2017_existing.reset_index(inplace=True)
sales_data_2018_existing.reset_index(inplace=True)
del sales_data_2017_existing['index']
del sales_data_2018_existing['index']

for i in range(len(sales_data_2018_existing)):
    for j in range(1,53):
            if sales_data_2018_existing.iloc[i,j]==0:
                sales_data_2017_existing.iloc[i,j]=0
            if sales_data_2017_existing.iloc[i,j]==0:
                sales_data_2018_existing.iloc[i,j]=0
  
sales_wide_2_years=pd.merge(sales_data_2017_existing,sales_data_2018_existing,on='location_id',how='left')

sales_data_2017_existing['sales_2017']=sales_data_2017_existing[year_2017_weeks].sum(axis=1)
sales_data_2018_existing['sales_2018']=sales_data_2018_existing[year_2018_weeks].sum(axis=1)
df_2017_sales=sales_data_2017_existing[['location_id','sales_2017']]
df_2018_sales=sales_data_2018_existing[['location_id','sales_2018']]
df_2_year_sales=pd.merge(df_2017_sales,df_2018_sales,on='location_id',how='left')
df_2_year_sales=df_2_year_sales[df_2_year_sales['sales_2017']!=0]
df_2_year_sales=df_2_year_sales[df_2_year_sales['sales_2018']!=0]

writer=pd.ExcelWriter(writer_folder+"sales_data_2_year_compariable.xlsx",engine='xlsxwriter')

df_2_year_sales.to_excel(writer,"rencet_open_stores_compariable",index=False)
sales_wide_2_years.to_excel(writer,"yearly_sales",index=False)
writer.save()   

recent_open_stores=[str(x) for x in recent_open_stores]
store_latest_non_zero_week['location_id']=store_latest_non_zero_week['location_id'].astype(str)
store_latest_non_zero_week=store_latest_non_zero_week.rename(columns={"location_id":"storeid"})

start_running=str(datetime.datetime.now())
logging.info("Newspaper level - Start to run: "+ start_running)

folder="/home/jian/Projects/Big_Lots/Newspaper/All newspaper files for reading JL/"

file_list=glob.glob(folder+"*")
logging.info("Newspaper level - Total files: "+str(len(file_list)))

Selected_Col=['projectname','storeid','productname','productid','zoneid','dayname','zips','totalcirc']
output_combined_all_date=pd.DataFrame()
check_df=pd.DataFrame()
finished=[]

wide_newspaper_week=pd.DataFrame({"productid":["-1"],"to_be_deleted":[-1]},index=[0])
i=0
all_date_list=[]
newspapaer_name=pd.read_excel('/home/jian/Projects/Big_Lots/Newspaper/All newspaper files for reading JL/August 28 2016 4 STD.xlsx',sheetname="to_read",dtype=str,na_values=['NULL','null','Null',""," "])[["productid","productname"]].drop_duplicates()
for file in file_list:
    date=file.split("/")[len(file.split("/"))-1].split(" ")[0]+" "+file.split("/")[len(file.split("/"))-1].split(" ")[1]+" "+\
    file.split("/")[len(file.split("/"))-1].split(" ")[2]
    date=date.replace("Sept","Sep")

    if len(date.split(" ")[0])==3:
        date=datetime.datetime.strptime(date,"%b %d %Y").date()
    else:
        date=datetime.datetime.strptime(date,"%B %d %Y").date()
        
    # if date>=datetime.datetime(2017,4,1).date(): #Most recent 12 months to reduce stores without information

    df=pd.read_excel(file,sheetname="to_read",dtype=str,na_values=['NULL','null','Null',""," "])
    df=df[['storeid','productid','totalcirc']]

    df['totalcirc']=df['totalcirc'].astype(float)
    #df['keys']=df['productid'] 
    #Use only product id to adjust null

    df=df[df['storeid'].isin(recent_open_stores)]
    df=pd.merge(df,store_latest_non_zero_week,on="storeid",how="left")
    df=df[df['latest_date']>=date]
    del df['latest_date']
    df_agg_newspaper=df.groupby(['productid'])['totalcirc'].sum().to_frame().reset_index()
    df_agg_newspaper=df_agg_newspaper.rename(columns={"totalcirc":str(date)})
    wide_newspaper_week=pd.merge(wide_newspaper_week,df_agg_newspaper,on='productid',how='outer')
            
    i=i+1
    if i %10 ==0:
        print(i)
        
    all_date_list=all_date_list+[date]
    
    newspapaer_name_newfile=df=pd.read_excel(file,sheetname="to_read",dtype=str,na_values=['NULL','null','Null',""," "])[["productid","productname"]].drop_duplicates()
    newspapaer_name_newfile.columns=["productid","product_new_name"]
    newspapaer_name=pd.merge(newspapaer_name,newspapaer_name_newfile,on="productid",how="outer")
    newspapaer_name=newspapaer_name.fillna("NA_NAME")
    newspapaer_name.reset_index(inplace=True)
    del newspapaer_name['index']
    for j in range(len(newspapaer_name)):
        if newspapaer_name['product_new_name'][j]!="NA_NAME":
            newspapaer_name['productname'][j]=newspapaer_name['product_new_name'][j]
    newspapaer_name=newspapaer_name[['productid','productname']]
        

    
all_date_list=[str(x) for x in sorted(all_date_list)]
del wide_newspaper_week['to_be_deleted']
wide_newspaper_week=wide_newspaper_week[wide_newspaper_week['productid']!="-1"]
wide_newspaper_week['productid']=wide_newspaper_week['productid'].astype(int)
wide_newspaper_week=wide_newspaper_week.sort_values('productid')
wide_newspaper_week.reset_index(inplace=True)
del wide_newspaper_week['index']
wide_newspaper_week=wide_newspaper_week.fillna(0)
wide_newspaper_week['mean']=wide_newspaper_week.iloc[:,1:].mean(axis=1)
newspapaer_name['productid']=newspapaer_name['productid'].astype(int)
wide_newspaper_week=pd.merge(wide_newspaper_week,newspapaer_name,on='productid',how="left")
wide_newspaper_week=wide_newspaper_week[['productid','productname']+all_date_list+['mean']]

writer=pd.ExcelWriter(writer_folder+"wide_news_with_week.xlsx",engine='xlsxwriter')
wide_newspaper_week.to_excel(writer,"newspaper_type_by_week",index=False)

workbook  = writer.book
worksheet = writer.sheets['newspaper_type_by_week']

format_high_color=workbook.add_format({'bg_color': '3AFA8B'})
format_low_color=workbook.add_format({'bg_color': 'FC9B53'})

for i in range(len(wide_newspaper_week)):
    range_str="C"+str(i+2)+":BH"+str(i+2)
    worksheet.conditional_format(range_str, {'type':  'cell',
                                            'criteria': '>',
                                            'value':    wide_newspaper_week['mean'][i]*1.2, #+20%
                                            'format':   format_high_color})
    worksheet.conditional_format(range_str, {'type':  'cell',
                                            'criteria': '<',
                                            'value':    wide_newspaper_week['mean'][i]*0.8, #-20%
                                            'format':   format_low_color})
    
writer.save()

for file in file_list:
    date=file.split("/")[len(file.split("/"))-1].split(" ")[0]+" "+file.split("/")[len(file.split("/"))-1].split(" ")[1]+" "+\
    file.split("/")[len(file.split("/"))-1].split(" ")[2]
    date=date.replace("Sept","Sep")

    if len(date.split(" ")[0])==3:
        date=datetime.datetime.strptime(date,"%b %d %Y").date()
    else:
        date=datetime.datetime.strptime(date,"%B %d %Y").date()
        
    # if date>=datetime.datetime(2017,4,1).date(): #Most recent 12 months to reduce stores without information

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
    for productid in df['productid'].unique():
        df_product_store=df[df['productid']==productid]
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
            df_product_store_NA['extract_from_zone 5 digit']=df_product_store_NA['zoneid'].apply(lambda x: re.findall(r"\b\d{5}\b", x))
            unique_len_from_zone=len(df_product_store_NA['extract_from_zone 5 digit'].apply(lambda x :len(x)).unique())
            if unique_len_from_zone==0:
                df_product_store_zip=df_product_store.groupby(['storeid','productname','productid','dayname','zips'])['totalcirc'].sum().to_frame()
                df_product_store_zip['circ_proportion_of_Null']=1
                df_product_store_zip['adjusted_circ_about_Null']=df_product_store_zip['totalcirc']
                df_product_store_zip.reset_index(inplace=True)
            else:                                                                                                    
                df_product_store['extract_from_zone 5 digit']=df_product_store['zoneid'].apply(lambda x: re.findall(r"\b\d{5}\b", x))                                                                        
                df_product_store['zips']=df_product_store['extract_from_zone 5 digit'].apply(lambda x: str(x)[1:(len(str(x))-1)].replace("'",""))
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
output_combined_all_date.to_excel(writer_folder+"BL_combined newspaper data_JL_"+today_str+".xlsx",index=False)

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
output_adjusted_final.to_csv(writer_folder+"BL_combined newspaper final detailed_JL_"+today_str+".csv",index=False)

all_store_in_newspaper=output_adjusted_final['storeid'].unique().tolist()                                                                                                    
                                                                                                     
output_no_detail=output_adjusted_final.groupby(['zip_cd','Date'])['adjusted_circ_with_list'].sum().to_frame()
output_no_detail.reset_index(inplace=True)
output_no_detail=output_no_detail.sort_values(['Date','zip_cd'])
output_no_detail.to_csv(writer_folder+"BL_newspaper count by zip by date_JL_"+today_str+".csv",index=False)

output_no_detail=pd.read_csv(writer_folder+"BL_newspaper count by zip by date_JL_"+today_str+".csv")
test_agg_wide=output_no_detail.pivot(index="zip_cd",columns='Date',values='adjusted_circ_with_list')
test_agg_wide.reset_index(inplace=True)
test_agg_wide_columns=test_agg_wide.columns.tolist()[1:]
test_agg_wide_columns=["newspaper_"+x for x in test_agg_wide_columns]
test_agg_wide.columns=[['zip_cd']+test_agg_wide_columns]
test_agg_wide=test_agg_wide.fillna(0)
test_agg_wide.to_csv(writer_folder+"BL_newspaper wide of date by zip_JL_"+today_str+".csv",index=False)
















  

