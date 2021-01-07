import pandas as pd
import gc
gc.collect()
import datetime
import hashlib
import numpy as np
import logging

logging.basicConfig(filename='merge_sales_with_signing_up.log', level=logging.INFO)
logging.info("Start Running: "+str(datetime.datetime.now()))


df_id_zip=pd.read_csv("/home/jian/Projects/Big_Lots/Loyal_members/Email_Zips/output_loyalty_member_by_id.csv",dtype=str)
del df_id_zip['sign_up_date']
df_id_zip['customer_zip_code']=df_id_zip['customer_zip_code'].fillna("00000")
df_id_zip['customer_zip_code']=df_id_zip['customer_zip_code'].apply(lambda x: x.zfill(5)[0:5])
logging.info("Finished Reading Zip by member: "+str(datetime.datetime.now()))

data_1=pd.read_csv("/home/jian/Projects/Big_Lots/Loyal_members/sales_of_loyalty_member/MediaStormCustDtl.txt",header=None,dtype=str)
logging.info("Finished Reading Sales Data 1: "+str(datetime.datetime.now()))

data_2=pd.read_csv("/home/jian/Projects/Big_Lots/Loyal_members/sales_of_loyalty_member/MediaStorm customer transaction details - 2018-01-09 - 2018-03-31.txt",dtype=str)
logging.info("Finished Reading Sales Data 2: "+str(datetime.datetime.now()))

data_3=pd.read_csv("/home/jian/Projects/Big_Lots/Loyal_members/sales_of_loyalty_member/MediaStorm customer transaction details - 2018-04-01 - 2018-04-15.txt",dtype=str)
logging.info("Finished Reading Sales Data 3: "+str(datetime.datetime.now()))

data_1.columns=data_2.columns.tolist()  
data_1['customer_id_hashed']=data_1['customer_id_hashed'].apply(lambda x: hashlib.sha256(x.encode('UTF-8')).hexdigest())
logging.info("Finished Hashing: "+str(datetime.datetime.now()))
gc.collect()

def clean_data(df):
    del df['merch_cat']

    df['total_transaction_amt']=df['total_transaction_amt'].astype(float)
    df=df.drop_duplicates()    
    df['transaction_date']=df['transaction_date'].apply(lambda x: datetime.datetime.strptime(x,"%Y-%m-%d").date())
    df=df[['customer_id_hashed','transaction_date','total_transaction_amt']]
    df_sales=df.groupby(['customer_id_hashed','transaction_date'])['total_transaction_amt'].sum().to_frame().reset_index()
    df_trans=df[df['total_transaction_amt']>0]
    df_trans=df.groupby(['customer_id_hashed','transaction_date'])['total_transaction_amt'].count().to_frame().reset_index()
    df_trans=df_trans.rename(columns={"total_transaction_amt":"total_transaction_count"})
    df=pd.merge(df_sales,df_trans,on=['customer_id_hashed','transaction_date'],how="left")
    return df
data_1=clean_data(data_1)
data_2=clean_data(data_2)
data_3=clean_data(data_3)
data_all=data_1.append(data_2).append(data_3).drop_duplicates()
data_all=data_all.drop_duplicates()

del data_1
del data_2
del data_3
gc.collect()

data_all=data_all[data_all['transaction_date']>=datetime.datetime(2017,5,7).date()]
data_all['weekday']=data_all['transaction_date'].apply(lambda x: x.weekday())

data_all['week_end_date']=np.where(data_all['weekday']==6,
                                   data_all['transaction_date'].apply(lambda x: x+datetime.timedelta(days=6)),
                                  data_all['transaction_date'].apply(lambda x: x+datetime.timedelta(days=5-x.weekday()))
                                  )
del data_all['transaction_date']
del data_all['weekday']
gc.collect()
logging.info("Finished Reading Sales by member: "+str(datetime.datetime.now()))


data_all=data_all.groupby(['customer_id_hashed','week_end_date'])['total_transaction_amt','total_transaction_count'].sum()

try:
    data_all=data_all.to_frame()
except:
    logging.info("No need to do 'to_frame' in data_all: "+str(datetime.datetime.now()))


data_all=data_all.reset_index()
    
try:
    del data_all['index']
except:
    logging.info("No index deleted: in data_all"+str(datetime.datetime.now()))
    
logging.info("Finished aggregating by zip by week: "+str(datetime.datetime.now()))
gc.collect()


logging.info("Start Merging: "+str(datetime.datetime.now()))
data_all=pd.merge(data_all,df_id_zip,on="customer_id_hashed",how="left")
logging.info("Finished merging: "+str(datetime.datetime.now()))
data_all.to_csv("/home/jian/Projects/Big_Lots/Loyal_members/sales_of_loyalty_member/sales_by_zip_20180529.csv",index=False)


gc.collect()
logging.info("Start Groupby on zip and week: "+str(datetime.datetime.now()))
data_all_46_full_weeks=data_all.groupby(['customer_zip_code','week_end_date'])['total_transaction_amt','total_transaction_count'].sum().reset_index()

data_all_46_full_weeks=data_all_46_full_weeks.reset_index()
    
try:
    del data_all['index']
except:
    logging.info("No index deleted in data_all_46_full_weeks: "+str(datetime.datetime.now()))
    
gc.collect()
data_all_46_full_weeks.to_csv("/home/jian/Projects/Big_Lots/Loyal_members/sales_of_loyalty_member/sales_by_zip_agg_20180529.csv",index=False)

logging.info("Done: "+str(datetime.datetime.now()))

import pandas as pd
import datetime
import numpy as np
import glob
import logging
import os
import gc

today_str=str(datetime.datetime.now().date())
writer_folder="/home/jian/Projects/Big_Lots/Newspaper/output_"+today_str+"/"
try:
    os.stat(writer_folder)
except:
    os.mkdir(writer_folder)
    
sales_data=pd.read_excel("/home/jian/BiglotsCode/outputs/Output_2018-05-05/wide_sales_date2018-05-05.xlsx")
'''
# Any one week sales >0 in the most recent month, it indicated an "Open" status up to 2018-05-05
recent_month=sales_data.columns.tolist()[len(sales_data.columns.tolist())-4:len(sales_data.columns.tolist())]
most_recent_open_stores=sales_data[['location_id']+recent_month]
most_recent_open_stores['4_week_total_sales']=most_recent_open_stores[most_recent_open_stores.columns[1]]+most_recent_open_stores[most_recent_open_stores.columns[2]]+\
                                                                  most_recent_open_stores[most_recent_open_stores.columns[3]]+most_recent_open_stores[most_recent_open_stores.columns[4]]


most_recent_open_stores=most_recent_open_stores[most_recent_open_stores['4_week_total_sales']>0]
'''

store_list_df_Dom=pd.read_excel("/home/jian/Projects/Big_Lots/Newspaper/Analysis/Store list 5-4-18 215p.xlsx",sheetname="oepn_stores",dtype=str) 
recent_open_stores=store_list_df_Dom['Store_list_recent_open'].astype(int)


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

store_latest_non_zero_week_in=store_latest_non_zero_week[store_latest_non_zero_week['location_id'].isin(recent_open_stores)]
store_latest_non_zero_week_out=store_latest_non_zero_week[~store_latest_non_zero_week['location_id'].isin(recent_open_stores)]
store_latest_non_zero_week_in['latest_date']=datetime.datetime(2020,12,31).date() # if in the list from Dom 5-24, it is labeled as 2020-12-31

store_latest_non_zero_week=store_latest_non_zero_week_in.append(store_latest_non_zero_week_out).sort_values('location_id').reset_index()
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

writer=pd.ExcelWriter(writer_folder+"sales_data_2_year_compariable_0525.xlsx",engine='xlsxwriter')

df_2_year_sales.to_excel(writer,"rencet_open_stores_compariable",index=False)
sales_wide_2_years.to_excel(writer,"yearly_sales",index=False)
writer.save()   


newspaper_detail=pd.read_csv("/home/jian/Projects/Big_Lots/Newspaper/output_2018-05-22/BL_combined newspaper final detailed_JL_2018-05-22.csv",dtype=str)
newspaper_detail['adjusted_circ_with_list']=newspaper_detail['adjusted_circ_with_list'].astype(float)
newspaper_detail_long=newspaper_detail[['Date','adjusted_circ_with_list','storeid','zip_cd','productid']]
newspaper_detail_long['Date']=newspaper_detail_long['Date'].apply(lambda x: datetime.datetime.strptime(x,"%Y-%m-%d").date())

# Replace the date for 2017-11-22 with 2017-11-23
# Replace the date for 2016-11-23 with 2016-11-24

newspaper_detail_long['Date']=newspaper_detail_long['Date'].replace(datetime.datetime(2017,11,22).date(),datetime.datetime(2017,11,23).date())
newspaper_detail_long['Date']=newspaper_detail_long['Date'].replace(datetime.datetime(2016,11,23).date(),datetime.datetime(2016,11,24).date())

newspaper_detail_long['storeid']=newspaper_detail_long['storeid'].astype(int)
newspaper_detail_long['productid']=newspaper_detail_long['productid'].astype(int)
newspaper_detail_long.shape

# Extrac the cost PM from the RecbyStore
RecbyStore_folder="/home/jian/Projects/Big_Lots/Newspaper/RecbyStore/"
file_list=glob.glob(RecbyStore_folder+"*")


cost_df=pd.DataFrame()
check_cost_df=pd.DataFrame()
for file in file_list:
    df=pd.read_excel(file,sheetname=None,dtype=str)
    date_name=file.split("/")[len(file.split("/"))-1].split(" ")[0]+" "+file.split("/")[len(file.split("/"))-1].split(" ")[1]+" "+\
    file.split("/")[len(file.split("/"))-1].split(" ")[2]
    if len(date_name.split(" ")[0])==3:
        date_name=datetime.datetime.strptime(date_name,"%b %d %Y").date()
    else:
        date_name=datetime.datetime.strptime(date_name,"%B %d %Y").date()
    
    # print(str(date_name),df.keys())
    
    try:
        df=df[list(df.keys())[0]]
        df=df[~pd.isnull(df['DMA'])]
        df=df[~pd.isnull(df['PRODUCT ID'])]
        df=df[df['DMA']!='nan']
        df=df[df['PRODUCT ID']!='nan']
        df=df[['PRODUCT ID','STORE','CONFIRM MEDIA CPM','PRINT CPM']].drop_duplicates()
        df['Date']=date_name
    except:
        print(str(date_name))
    cost_df=cost_df.append(df)
    
    
    check_cost_df_app=pd.read_excel(file,sheetname=None,dtype=str)
    check_cost_df_app=check_cost_df_app[list(check_cost_df_app.keys())[0]]
    check_cost_df_app=check_cost_df_app[~pd.isnull(check_cost_df_app['DMA'])]
    check_cost_df_app=check_cost_df_app[~pd.isnull(check_cost_df_app['PRODUCT ID'])]
    check_cost_df_app=check_cost_df_app[check_cost_df_app['DMA']!='nan']
    check_cost_df_app=check_cost_df_app[check_cost_df_app['PRODUCT ID']!='nan']
    check_cost_df_app=check_cost_df_app[['PRODUCT ID','STORE','MEDIA COST','PRINT COST']]
    check_cost_df_app['Date']=date_name
    check_cost_df=check_cost_df.append(check_cost_df_app)
    
cost_df=cost_df.rename(columns={"PRODUCT ID":"productid",'STORE':'storeid',"CONFIRM MEDIA CPM":"media_cpm","PRINT CPM":"print_cpm"})
cost_df['media_cpm']=cost_df['media_cpm'].astype(float)
cost_df['print_cpm']=cost_df['print_cpm'].astype(float)
cost_df['productid']=cost_df['productid'].astype(int)
cost_df['storeid']=cost_df['storeid'].astype(int)
cost_df.shape

check_cost_df['MEDIA COST']=check_cost_df['MEDIA COST'].astype(float)
check_cost_df['PRINT COST']=check_cost_df['PRINT COST'].astype(float)
check_cost_df['cost']=check_cost_df['MEDIA COST']+check_cost_df['PRINT COST']

# Deal with one duplication with different media cost
cost_df_1=cost_df[(cost_df['productid']!=400) | (cost_df['storeid']!=555) | (cost_df['Date']!=datetime.datetime(2017,11,23).date())]
cost_df_2=cost_df[(cost_df['productid']==400) & (cost_df['storeid']==555) & (cost_df['Date']==datetime.datetime(2017,11,23).date())]

cost_df_2=pd.read_excel("/home/jian/Projects/Big_Lots/Newspaper/RecbyStore/November 23 2017 RecByStore 6 STD for BL REPORTS.xlsx",dtype=str)
cost_df_2=cost_df_2[['PRODUCT ID','STORE','CONFIRMED CIRCULATION','MEDIA COST',"PRINT CPM"]]
cost_df_2=cost_df_2[(cost_df_2['PRODUCT ID']=="400") & (cost_df_2['STORE']=="555")]
cost_df_2['STORE']=cost_df_2['STORE'].astype(int)
cost_df_2['PRODUCT ID']=cost_df_2['PRODUCT ID'].astype(int)
cost_df_2['MEDIA COST']=cost_df_2['MEDIA COST'].astype(float)
cost_df_2['PRINT CPM']=cost_df_2['PRINT CPM'].astype(float)
cost_df_2['CONFIRMED CIRCULATION']=cost_df_2['CONFIRMED CIRCULATION'].astype(float)
cost_df_2['media_cpm']=cost_df_2['MEDIA COST'].sum()/cost_df_2['CONFIRMED CIRCULATION'].sum()*1000
cost_df_2=cost_df_2.rename(columns={"PRODUCT ID":"productid",'STORE':'storeid',"PRINT CPM":"print_cpm"})
cost_df_2=cost_df_2[['productid','storeid','media_cpm','print_cpm']].drop_duplicates()
cost_df_2['Date']=datetime.datetime(2017,11,23).date()

cost_df_adjusted=cost_df_1.append(cost_df_2).sort_values(['Date','storeid','productid']).reset_index()
del cost_df_adjusted['index']

newspaper_detail_long=pd.merge(newspaper_detail_long,cost_df_adjusted,on=['productid','storeid','Date'],how="left")

nan_cpm=newspaper_detail_long[pd.isnull(newspaper_detail_long['media_cpm']) | pd.isnull(newspaper_detail_long['print_cpm'])]
newspaper_detail_long=newspaper_detail_long[~pd.isnull(newspaper_detail_long['media_cpm']) & ~pd.isnull(newspaper_detail_long['print_cpm'])]
nan_cpm_key=nan_cpm[['Date','productid']].drop_duplicates().reset_index()
cost_df_for_nan_agg=pd.DataFrame()
del nan_cpm_key['index']
for i in range(len(nan_cpm_key)):
    df_i=nan_cpm_key.iloc[i,:].to_frame().T.reset_index()
    del df_i['index']
    cost_df_for_nan=cost_df_adjusted[(cost_df_adjusted['Date']==df_i['Date'][0]) & (cost_df_adjusted['productid']==df_i['productid'][0])]
    cost_df_for_nan=cost_df_for_nan.groupby(['Date','productid','media_cpm','print_cpm'])['storeid'].count().reset_index()
    cost_df_for_nan=cost_df_for_nan.sort_values('storeid',ascending=False).reset_index()
    del cost_df_for_nan['index']
    try:
        cost_df_for_nan=cost_df_for_nan.iloc[0,:].to_frame().T.reset_index()    
        del cost_df_for_nan['index']

        del cost_df_for_nan['storeid']
        cost_df_for_nan_agg=cost_df_for_nan_agg.append(cost_df_for_nan)
    except:
        print(nan_cpm_key['Date'][i],nan_cpm_key['productid'][i])
        
del nan_cpm['media_cpm']
del nan_cpm['print_cpm']
nan_cpm=pd.merge(nan_cpm,cost_df_for_nan_agg,on=["Date","productid"],how="left")
newspaper_detail_long=newspaper_detail_long.append(nan_cpm).reset_index()
del newspaper_detail_long['index']
newspaper_detail_long=newspaper_detail_long.sort_values(['Date','storeid','productid']).reset_index()
del newspaper_detail_long['index']

newspaper_detail_long['media_cpm']=newspaper_detail_long['media_cpm'].astype(float)
newspaper_detail_long['print_cpm']=newspaper_detail_long['print_cpm'].astype(float)
newspaper_detail_long['cost']=newspaper_detail_long['adjusted_circ_with_list']*(newspaper_detail_long['media_cpm']+newspaper_detail_long['print_cpm'])/1000

check_cost_df=check_cost_df[check_cost_df['Date'].isin(newspaper_detail_long['Date'])]

sales_data_with_wholes=pd.read_excel("/home/jian/Projects/Big_Lots/Newspaper/output_2018-05-25/sales_data_2_year_compariable_0525.xlsx",sheetname=None,dtype=str)
compariable_all_sales=sales_data_with_wholes[list(sales_data_with_wholes.keys())[0]]
compariable_store_week_sales=sales_data_with_wholes[list(sales_data_with_wholes.keys())[1]]
compariable_store_week_sales['location_id']=compariable_store_week_sales['location_id'].astype(int)
for col in compariable_store_week_sales.columns.tolist()[1:]:
    compariable_store_week_sales[col]=compariable_store_week_sales[col].astype(float)
    
compariable_all_sales['location_id']=compariable_all_sales['location_id'].astype(int)
compariable_all_sales['sales_2017']=compariable_all_sales['sales_2017'].astype(float)
compariable_all_sales['sales_2018']=compariable_all_sales['sales_2018'].astype(float)

loyalty_sales=pd.read_csv("/home/jian/Projects/Big_Lots/Loyal_members/sales_of_loyalty_member/output_of_loyalty_consumption_by_location.csv",dtype=str)
loyalty_sales['transaction_date']=loyalty_sales['transaction_date'].apply(lambda x: datetime.datetime.strptime(x,"%Y-%m-%d").date())
loyalty_sales['total_transaction_amt']=loyalty_sales['total_transaction_amt'].astype(float)
loyalty_sales['location_id']=loyalty_sales['location_id'].astype(int)
loyalty_sales['weekday']=loyalty_sales['transaction_date'].apply(lambda x: x.weekday()) # Sunday=6 ; Monday=0
loyalty_sales['weekday']=loyalty_sales['transaction_date'].apply(lambda x: x.weekday())
loyalty_sales['week_end_date']=np.where(loyalty_sales['weekday']==6,
                                        loyalty_sales['transaction_date']+datetime.timedelta(days=6),
                                        loyalty_sales['transaction_date'].apply(lambda x: x+datetime.timedelta(days=5-(x.weekday())))
                                        )
count_weekdays=loyalty_sales[['transaction_date','week_end_date']]

count_weekdays=count_weekdays.drop_duplicates().reset_index()
del count_weekdays['index']
count_weekdays=count_weekdays.groupby(['week_end_date'])['transaction_date'].count().to_frame().reset_index()
count_weekdays=count_weekdays[count_weekdays['transaction_date']==7]

recent_1_year_weekends=compariable_store_week_sales.columns.tolist()
recent_1_year_weekends.remove("location_id")                                       
cols=[datetime.datetime.strptime(x,"%Y-%m-%d").date() for x in recent_1_year_weekends]                                  
cols_date=[x for x in cols if x>=datetime.datetime(2017,5,13).date()]
count_weekdays=count_weekdays[count_weekdays['week_end_date'].isin(cols_date)]
loyal_dates=sorted(count_weekdays['week_end_date'].unique().tolist())

loyalty_sales_full_weeks_1_year=loyalty_sales[loyalty_sales['week_end_date'].isin(loyal_dates)] # 46 weeks in total
loyalty_sales_full_weeks_1_year=loyalty_sales_full_weeks_1_year[loyalty_sales_full_weeks_1_year['location_id'].isin(compariable_all_sales['location_id'])]
loyalty_sales_full_weeks_46_weeks=loyalty_sales_full_weeks_1_year.groupby(['location_id','week_end_date'])['total_transaction_amt'].sum().to_frame().reset_index()
loyalty_sales_full_weeks_46_weeks.columns=['location_id','week_end_date','loyalty_sales_46_weeks']

total_sales_no_exclusion=pd.read_csv("/home/jian/BiglotsCode/outputs/combined_sales_long_2018-05-19.csv",dtype=str)
total_sales_no_exclusion=total_sales_no_exclusion[['location_id','week_end_date','sales']]
total_sales_no_exclusion['location_id']=total_sales_no_exclusion['location_id'].astype(int)
total_sales_no_exclusion['week_end_date']=total_sales_no_exclusion['week_end_date'].apply(lambda x:datetime.datetime.strptime(x,"%Y-%m-%d").date())
total_sales_no_exclusion['sales']=total_sales_no_exclusion['sales'].astype(float)
total_sales_no_exclusion=total_sales_no_exclusion.rename(columns={"sales":"total_sales_46_weeks"})

loyal_total_sales_long_by_week=pd.merge(loyalty_sales_full_weeks_46_weeks,total_sales_no_exclusion,on=['location_id','week_end_date'],how='left')
loyal_total_sales_long_by_week=loyal_total_sales_long_by_week.fillna(0)
loyal_total_sales_long_by_week['loyalty_sales_Pctg']=loyal_total_sales_long_by_week['loyalty_sales_46_weeks']/loyal_total_sales_long_by_week['total_sales_46_weeks']

loyal_total_sales_by_store=loyal_total_sales_long_by_week.groupby('location_id')['loyalty_sales_46_weeks','total_sales_46_weeks'].sum().reset_index()
loyal_total_sales_by_store['loyalty_overall_46_weeks_pctg']=loyal_total_sales_by_store['loyalty_sales_46_weeks']/loyal_total_sales_by_store['total_sales_46_weeks']
loyal_total_sales_by_store=pd.merge(compariable_all_sales,loyal_total_sales_by_store,on="location_id",how="left")
total_sales_52_weeks=compariable_store_week_sales[compariable_store_week_sales['location_id'].isin(compariable_all_sales['location_id'])]
total_sales_52_weeks=total_sales_52_weeks.set_index('location_id').stack().to_frame().reset_index()
total_sales_52_weeks.columns=['location_id','week_end_date','total_sales']
total_sales_52_weeks['week_end_date']=total_sales_52_weeks['week_end_date'].apply(lambda x:datetime.datetime.strptime(x,'%Y-%m-%d').date())
total_sales_52_weeks_2017=total_sales_52_weeks[total_sales_52_weeks['week_end_date']<datetime.datetime(2017,5,10).date()]
total_sales_52_weeks_2018=total_sales_52_weeks[total_sales_52_weeks['week_end_date']>datetime.datetime(2017,5,10).date()]

total_sales_52_weeks_2017=total_sales_52_weeks_2017.rename(columns={"total_sales":"sales_compariable_2017"})
total_sales_52_weeks_2018=total_sales_52_weeks_2018.rename(columns={"total_sales":"sales_compariable_2018"})
total_sales_52_weeks_2017['week_end_date']=total_sales_52_weeks_2017['week_end_date'].apply(lambda x: x+datetime.timedelta(days=52*7))
total_sales_52_weeks_compariable=pd.merge(total_sales_52_weeks_2017,total_sales_52_weeks_2018,on=['location_id','week_end_date'],how="left")

total_sales_52_weeks_compariable=pd.merge(total_sales_52_weeks_compariable,loyal_total_sales_long_by_week,on=['location_id','week_end_date'],how="left")
writer_store_level=pd.ExcelWriter("/home/jian/Projects/Big_Lots/Newspaper/Analysis/store_level_information.xlsx",engine="xlsxwriter")
zip_ta=pd.read_excel("/home/jian/Projects/Big_Lots/Newspaper/Analysis/BL_Zips in new TA (TA level)_JL_20180330.xlsx",dtype=str)
zip_ta=zip_ta[['location_id','zip_cd','trade_area_code','TA_of_zip','TA_of_store']].drop_duplicates()
ta_store_zip=zip_ta[zip_ta['location_id']!="nan"][['location_id','zip_cd','trade_area_code','TA_of_store']].drop_duplicates()
ta_store_zip['location_id']=ta_store_zip['location_id'].astype(int)
total_sales_52_weeks_compariable=pd.merge(total_sales_52_weeks_compariable,ta_store_zip,on="location_id",how="left")
loyal_total_sales_by_store=pd.merge(loyal_total_sales_by_store,ta_store_zip,on="location_id",how="left")

loyal_total_sales_by_store.to_excel(writer_store_level,"sales_by_store",index=False)
total_sales_52_weeks_compariable.to_excel(writer_store_level,"sales_by_store_by_week",index=False)
compariable_store_week_sales.to_excel(writer_store_level,"compariable_sales_data",index=False)
writer_store_level.save()
gc.collect()
newspaper_detail_long_1year=newspaper_detail_long[newspaper_detail_long['Date']>datetime.datetime(2017,5,10).date()].reset_index()
newspaper_zip_total_circ=newspaper_detail_long_1year.groupby(['zip_cd'])['adjusted_circ_with_list'].sum().to_frame().reset_index()
newspaper_zip_total_week_count=newspaper_detail_long_1year[['zip_cd','Date']].drop_duplicates().groupby(['zip_cd'])['Date'].count().to_frame().reset_index()
newspaper_zip_total_cost=newspaper_detail_long_1year.groupby(['zip_cd'])['cost'].sum().to_frame().reset_index()
newspaper_zip_total_traget_store=newspaper_detail_long_1year[['zip_cd','storeid']].drop_duplicates().groupby(['zip_cd'])['storeid'].apply(list).to_frame().reset_index()
newspaper_zip_total_traget_store['len']=[len(x) for x in newspaper_zip_total_traget_store['storeid']]
newspaper_zip_total_traget_store.reset_index(inplace=True)
del newspaper_zip_total_traget_store['index']
gc.collect()
loyalty_sales=loyal_total_sales_by_store[['location_id','loyalty_sales_46_weeks','total_sales_46_weeks']]
loyalty_sales_pctg_result=pd.DataFrame()
for i in range(len(newspaper_zip_total_traget_store)): 
    df=newspaper_zip_total_traget_store.iloc[i,:].to_frame().T.reset_index()
    zipcode=df['zip_cd'][0]
    store_list=df['storeid'][0]
    loyalty_pctg_df=loyalty_sales[loyalty_sales['location_id'].isin(store_list)]
    loyalty_pctg_df['zip_cd']=zipcode
    
    loyalty_pctg_df=loyalty_pctg_df.groupby(['zip_cd'])['loyalty_sales_46_weeks','total_sales_46_weeks'].sum().reset_index()
    loyalty_pctg_df['loyalty_pctg_zip_sales']=loyalty_pctg_df['loyalty_sales_46_weeks']/loyalty_pctg_df['total_sales_46_weeks']
    loyalty_pctg_df=loyalty_pctg_df[['zip_cd','loyalty_pctg_zip_sales']]
    loyalty_sales_pctg_result=loyalty_sales_pctg_result.append(loyalty_pctg_df)   
    if i %1000 ==1:
        print(i)
demo_data=pd.read_csv("/home/jian/Projects/Big_Lots/Newspaper/Analysis/Demo_Dataset.csv",dtype=str)
demo_data=demo_data[['ZIP_CODE','HH15','Estimate; SEX AND AGE - Total population','Estimate; Female: - 25 to 29 years','Estimate; Female: - 30 to 34 years',
                    'Estimate; Female: - 35 to 39 years','Estimate; Female: - 40 to 44 years','Estimate; Female: - 45 to 49 years','Estimate; Female: - 50 to 54 years']]
for col in demo_data.columns.tolist()[1:]:
    demo_data[col]=demo_data[col].astype(float)
demo_data['ZIP_CODE']=demo_data['ZIP_CODE'].apply(lambda x:x.zfill(5))

demo_data['F_25to54']=demo_data[['Estimate; Female: - 25 to 29 years','Estimate; Female: - 30 to 34 years',
                    'Estimate; Female: - 35 to 39 years','Estimate; Female: - 40 to 44 years','Estimate; Female: - 45 to 49 years','Estimate; Female: - 50 to 54 years']].sum(axis=1)
demo_data=demo_data[['ZIP_CODE','HH15','Estimate; SEX AND AGE - Total population','F_25to54']]
demo_data=demo_data.rename(columns={"ZIP_CODE":"zip_cd","Estimate; SEX AND AGE - Total population":"total_pop"})
newspaper_zip_total_circ=newspaper_zip_total_circ.rename(columns={"adjusted_circ_with_list":"total_circ"})
newspaper_zip_total_week_count=newspaper_zip_total_week_count.rename(columns={"Date":"Event_Count"})
result_zip_level=pd.merge(newspaper_zip_total_circ,newspaper_zip_total_week_count,on="zip_cd",how="left")
result_zip_level['Circ per Event']=result_zip_level['total_circ']/result_zip_level['Event_Count']
result_zip_level=pd.merge(result_zip_level,newspaper_zip_total_cost,on="zip_cd",how="left")
result_zip_level=pd.merge(result_zip_level,loyalty_sales_pctg_result,on="zip_cd",how="left")
result_zip_level=pd.merge(result_zip_level,demo_data,on='zip_cd',how="left")
result_zip_level['Circ Penetration of F25_54']=result_zip_level['Circ per Event']/result_zip_level['F_25to54']
zip_ta_dedup_store=ta_store_zip[['zip_cd','TA_of_store','trade_area_code']]
zip_ta_dedup_store=zip_ta_dedup_store.rename(columns={"TA_of_store":"selected_TA"})
zip_ta_dedup_store=zip_ta_dedup_store[['zip_cd','selected_TA','trade_area_code']].drop_duplicates()

zip_ta_dedup=zip_ta[['zip_cd','trade_area_code','TA_of_zip']].drop_duplicates().reset_index()
del zip_ta_dedup['index']
zip_ta_dedup=zip_ta_dedup.drop_duplicates(['zip_cd'])
zip_ta_dedup_non_store=zip_ta_dedup[~zip_ta_dedup['zip_cd'].isin(zip_ta_dedup_store['zip_cd'])]
zip_ta_dedup_non_store=zip_ta_dedup_non_store.rename(columns={"TA_of_zip":"selected_TA"})
zip_ta_dedup_non_store=zip_ta_dedup_non_store[['zip_cd','selected_TA','trade_area_code']]
zip_ta_dedup=zip_ta_dedup_store.append(zip_ta_dedup_non_store)
result_zip_level_left_merge=pd.merge(result_zip_level,zip_ta_dedup,on="zip_cd",how="left")
result_zip_level_keep_all_zip_in_TA=pd.merge(result_zip_level,zip_ta_dedup,on="zip_cd",how="outer")
loyalty_member_by_zip=pd.read_csv("/home/jian/Projects/Big_Lots/Loyal_members/Idsbyzipcode.csv",dtype=str)
loyalty_member_by_zip.columns=['zip_cd','loyalty_mem_count']
loyalty_member_by_zip['zip_cd']=loyalty_member_by_zip['zip_cd'].apply(lambda x:x.zfill(5))
loyalty_member_by_zip['loyalty_mem_count']=loyalty_member_by_zip['loyalty_mem_count'].astype(int)

result_zip_level_left_merge=pd.merge(result_zip_level_left_merge,loyalty_member_by_zip,on="zip_cd",how="left")
result_zip_level_left_merge['loyalty_mem_count']=result_zip_level_left_merge['loyalty_mem_count'].fillna(0)
result_zip_level_left_merge['loyalty_mem_count']=result_zip_level_left_merge['loyalty_mem_count'].astype(int)
result_zip_level_left_merge['loyalty_mem_penetration_F25_54']=result_zip_level_left_merge['loyalty_mem_count']/result_zip_level_left_merge['F_25to54']
store_zip=ta_store_zip[['location_id','zip_cd']].drop_duplicates().reset_index()
del store_zip['index']

compariable_store_week_sales['sum']=compariable_store_week_sales[compariable_store_week_sales.columns.tolist()[1:]].sum(axis=1)
compariable_store_week_sales=compariable_store_week_sales[compariable_store_week_sales['sum']!=0]
del compariable_store_week_sales['sum']
compariable_store_week_sales.shape
columns_dates=[datetime.datetime.strptime(x,"%Y-%m-%d").date() for x in compariable_store_week_sales.columns.tolist()[1:]]
columns_dates_2017=[str(x) for x in columns_dates if x<datetime.datetime(2017,5,10).date()]
columns_dates_2018=[str(x) for x in columns_dates if x>=datetime.datetime(2017,5,10).date()]
df_sales_2017=compariable_store_week_sales[['location_id']+columns_dates_2017]
df_sales_2018=compariable_store_week_sales[['location_id']+columns_dates_2018]
df_sales_2017=pd.merge(df_sales_2017,store_zip,on="location_id",how="left")
df_sales_2018=pd.merge(df_sales_2018,store_zip,on="location_id",how="left")


df_sales_2017=df_sales_2017[['location_id','zip_cd']+columns_dates_2017]
df_sales_2018=df_sales_2018[['location_id','zip_cd']+columns_dates_2018]
df_sales_2017['2017_compariable_sales']=df_sales_2017[columns_dates_2017].sum(axis=1)
df_sales_2018['2018_compariable_sales']=df_sales_2018[columns_dates_2018].sum(axis=1)
df_sales_2017_store=df_sales_2017.groupby('zip_cd')['location_id'].apply(list).to_frame().reset_index()
# df_sales_2018_store=df_sales_2018.groupby('zip_cd')['location_id'].apply(list).to_frame().reset_index() #The same
df_sales_2017_sales=df_sales_2017.groupby('zip_cd')['2017_compariable_sales'].sum().to_frame().reset_index()
df_sales_2018_sales=df_sales_2018.groupby('zip_cd')['2018_compariable_sales'].sum().to_frame().reset_index()
sales_by_zip_df=pd.merge(df_sales_2017_store,df_sales_2017_sales,on="zip_cd",how="left")
sales_by_zip_df=pd.merge(sales_by_zip_df,df_sales_2018_sales,on="zip_cd",how="left")
sales_by_zip_df['YoY']=(sales_by_zip_df['2018_compariable_sales']-sales_by_zip_df['2017_compariable_sales'])/sales_by_zip_df['2017_compariable_sales']
result_zip_level_left_merge=pd.merge(result_zip_level_left_merge,sales_by_zip_df,on="zip_cd",how="left")

Revenue_Label=pd.read_excel("/home/jian/Projects/Big_Lots/Newspaper/Analysis/BL_Zips in new TA (TA level)_JL_20180330.xlsx",dtype=str)
Revenue_Label=Revenue_Label[['zip_cd','revenue_flag']].drop_duplicates().reset_index()
del Revenue_Label['index']
result_zip_level_left_merge=pd.merge(result_zip_level_left_merge,Revenue_Label,on="zip_cd",how="left")

store_list_for_zip=pd.read_csv("/home/jian/Projects/Big_Lots/New_TA/zips in new ta/sales_by_zip (Store level).csv",dtype=str,na_values=[""])
store_list_for_zip=store_list_for_zip[['zip','location_id']].drop_duplicates().reset_index()
del store_list_for_zip['index']
store_list_for_zip=store_list_for_zip.groupby(['zip'])['location_id'].apply(set).to_frame().reset_index()
store_list_for_zip=store_list_for_zip.rename(columns={"zip":"zip_cd","location_id":"store_list"})
result_zip_level_left_merge=pd.merge(result_zip_level_left_merge,store_list_for_zip,on="zip_cd",how="left")

sales_by_zip_matched=pd.read_csv("/home/jian/Projects/Big_Lots/Loyal_members/sales_of_loyalty_member/sales_by_zip_agg_20180529.csv",dtype=str)
sales_by_zip_matched['customer_zip_code']=sales_by_zip_matched['customer_zip_code'].apply(lambda x: x.replace(" ",""))
sales_by_zip_matched['customer_zip_code']=sales_by_zip_matched['customer_zip_code'].apply(lambda x: x.replace(" ",""))
sales_by_zip_matched['customer_zip_code']=sales_by_zip_matched['customer_zip_code'].apply(lambda x: x.replace(" ",""))
sales_by_zip_matched['customer_zip_code']=sales_by_zip_matched['customer_zip_code'].apply(lambda x: x.zfill(5))
sales_by_zip_matched['total_transaction_amt']=sales_by_zip_matched['total_transaction_amt'].astype(float)
sales_by_zip_matched['week_end_date']=sales_by_zip_matched['week_end_date'].apply(lambda x: datetime.datetime.strptime(x,"%Y-%m-%d").date())
sales_by_zip_matched=sales_by_zip_matched[sales_by_zip_matched['week_end_date'].isin(loyalty_sales_full_weeks_46_weeks['week_end_date'].unique().tolist())]

sales_by_zip_matched=sales_by_zip_matched.rename(columns={"customer_zip_code":"zip_cd","total_transaction_amt":"loyalty_sales_by_zip","total_transaction_count":"loyalty_transactions_by_zip"})
result_zip_level_left_merge=pd.merge(result_zip_level_left_merge,sales_by_zip_matched,on='zip_cd',how="left")

result_zip_level_left_merge['loyalty_label']=np.where(result_zip_level_left_merge['loyalty_mem_count']>np.percentile(result_zip_level_left_merge['loyalty_mem_count'], 80),"Loyalty_5",
                                                     np.where(result_zip_level_left_merge['loyalty_mem_count']>np.percentile(result_zip_level_left_merge['loyalty_mem_count'], 60),"Loyalty_4",
                                                              np.where(result_zip_level_left_merge['loyalty_mem_count']>np.percentile(result_zip_level_left_merge['loyalty_mem_count'], 40),"Loyalty_3",
                                                                       np.where(result_zip_level_left_merge['loyalty_mem_count']>np.percentile(result_zip_level_left_merge['loyalty_mem_count'], 20),"Loyalty_2",
                                                                                "Loyalty_1")
                                                                      )
                                                             )
                                                     )
result_zip_level_left_merge['circ_label']=np.where(result_zip_level_left_merge['total_circ']>np.percentile(result_zip_level_left_merge['total_circ'], 80),"Total_Circ_5",
                                                     np.where(result_zip_level_left_merge['total_circ']>np.percentile(result_zip_level_left_merge['total_circ'], 60),"Total_Circ_4",
                                                              np.where(result_zip_level_left_merge['total_circ']>np.percentile(result_zip_level_left_merge['total_circ'], 40),"Total_Circ_3",
                                                                       np.where(result_zip_level_left_merge['total_circ']>np.percentile(result_zip_level_left_merge['total_circ'], 20),"Total_Circ_2",
                                                                                "Total_Circ_1")
                                                                      )
                                                             )
                                                     )
result_zip_level_left_merge['Split_of_weekly_circ']=np.nan
newspaper_wide=newspaper_detail_long_1year.groupby(['zip_cd','Date'])['adjusted_circ_with_list'].sum().to_frame().reset_index()
newspaper_wide=newspaper_wide.sort_values(['zip_cd','Date']).reset_index()
newspaper_wide['Date']=newspaper_wide['Date'].apply(lambda x:str(x))
del newspaper_wide['index']
newspaper_wide=newspaper_wide.pivot(index="zip_cd",columns="Date",values="adjusted_circ_with_list").reset_index()
result_zip_level_left_merge=pd.merge(result_zip_level_left_merge,newspaper_wide,on="zip_cd",how="left")

result_zip_level_left_merge.to_csv("/home/jian/Projects/Big_Lots/Newspaper/Analysis/zip_level_information.csv",index=False)
gc.collect()
matrix_excel_writer=pd.ExcelWriter("/home/jian/Projects/Big_Lots/Newspaper/Analysis/BL_Newspaper Matrix_JL_20180525.xlsx",engine="xlsxwriter")
matrix_of_zip_list=result_zip_level_left_merge[['zip_cd','loyalty_label','circ_label']].groupby(['loyalty_label','circ_label'])['zip_cd'].apply(list).to_frame().reset_index()
matrix_of_zip_list.columns=matrix_of_zip_list.columns.tolist()[0:2]+["zip_cd_list"]
matrix_of_zip_list=matrix_of_zip_list.pivot(index="loyalty_label",columns="circ_label",values="zip_cd_list").reset_index()

matrix_of_zip_list.to_excel(matrix_excel_writer,"zip_list",index=False)

matrix_of_totol_zip_count=matrix_of_zip_list.copy()
for col in matrix_of_totol_zip_count.columns.tolist()[1:]:
    matrix_of_totol_zip_count[col]=[len(x) for x in matrix_of_totol_zip_count[col]]
matrix_of_totol_zip_count.to_excel(matrix_excel_writer,"zip_count",index=False)

matrix_of_store_sales_2017=result_zip_level_left_merge[['circ_label','loyalty_label','2017_compariable_sales']].groupby(['loyalty_label','circ_label'])['2017_compariable_sales'].sum().to_frame().reset_index()
matrix_of_store_sales_2017=matrix_of_store_sales_2017.pivot(index="loyalty_label",columns="circ_label",values="2017_compariable_sales").reset_index()
matrix_of_store_sales_2017.to_excel(matrix_excel_writer,"store_sales_2017",index=False)

matrix_of_store_sales_2018=result_zip_level_left_merge[['circ_label','loyalty_label','2018_compariable_sales']].groupby(['loyalty_label','circ_label'])['2018_compariable_sales'].sum().to_frame().reset_index()
matrix_of_store_sales_2018=matrix_of_store_sales_2018.pivot(index="loyalty_label",columns="circ_label",values="2018_compariable_sales").reset_index()
matrix_of_store_sales_2018.to_excel(matrix_excel_writer,"store_sales_2018",index=False)

matrix_of_store_sales_YoY=pd.DataFrame({"Total_Circ_1":[np.nan]*5,"Total_Circ_2":[np.nan]*5,"Total_Circ_3":[np.nan]*5,
                                       "Total_Circ_4":[np.nan]*5,"Total_Circ_5":[np.nan]*5},index=matrix_of_store_sales_2018['loyalty_label']).reset_index()

for i in range(5):
    for col in matrix_of_store_sales_2018.columns.tolist()[1:]:
        matrix_of_store_sales_YoY[col][i]=(matrix_of_store_sales_2018[col][i]-matrix_of_store_sales_2017[col][i])/matrix_of_store_sales_2017[col][i]
matrix_of_store_sales_YoY.to_excel(matrix_excel_writer,"store_sales_YoY",index=False)

def matrix_of_sum(df,col):
    df_pivot=df[['circ_label','loyalty_label',col]].groupby(['loyalty_label','circ_label'])[col].sum().to_frame().reset_index()
    result=df_pivot.pivot(index="loyalty_label",columns="circ_label",values=col).reset_index()
    return result

matrix_of_total_pop=matrix_of_sum(result_zip_level_left_merge,"total_pop")
matrix_of_total_female25_54=matrix_of_sum(result_zip_level_left_merge,"F_25to54")
matrix_of_total_households=matrix_of_sum(result_zip_level_left_merge,"HH15")
matrix_of_total_loyalty_mem=matrix_of_sum(result_zip_level_left_merge,"loyalty_mem_count")
matrix_of_circ_per_event=matrix_of_sum(result_zip_level_left_merge,"Circ per Event")
matrix_of_lotyalty_sales=matrix_of_sum(result_zip_level_left_merge,"loyalty_sales_by_zip")
matrix_of_lotyalty_trans=matrix_of_sum(result_zip_level_left_merge,"loyalty_transactions_by_zip")

matrix_of_total_pop.to_excel(matrix_excel_writer,"total_population",index=False)
matrix_of_total_female25_54.to_excel(matrix_excel_writer,"female_25_54",index=False)
matrix_of_total_households.to_excel(matrix_excel_writer,"total_households",index=False)
matrix_of_total_loyalty_mem.to_excel(matrix_excel_writer,"loyalty_members",index=False)
matrix_of_circ_per_event.to_excel(matrix_excel_writer,"circ_per_event",index=False)
matrix_of_lotyalty_sales.to_excel(matrix_excel_writer,"loyalty_sales",index=False)
matrix_of_lotyalty_trans.to_excel(matrix_excel_writer,"loyalty_transaction",index=False)

matrix_excel_writer.save()