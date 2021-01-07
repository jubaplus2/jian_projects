
# coding: utf-8

# In[1]:

import pandas as pd
import datetime
import numpy as np
import os
import glob
import gc
import logging
logging.basicConfig(filename='weekly_audience_tracker.log', level=logging.INFO)

def recursive_file_gen(root_folder):
    for root, dirs, files in os.walk(root_folder):
        for file in files:
            yield os.path.join(root, file)
            
os.getcwd()
print("Worker Start: "+str(datetime.datetime.now()))


# In[2]:

last_sturday = (datetime.datetime.now()-datetime.timedelta(days=(datetime.datetime.now().weekday()+2))).date()
print("last_sturday: "+str(last_sturday))

last_day_of_2018Q4=datetime.date(2019,2,2)

year_of_quarter=(last_sturday-last_day_of_2018Q4).days/(52*7)
year_of_quarter=str(int(2019+np.floor(year_of_quarter)))
print("Year",year_of_quarter)


quarter_of_quarter=(last_sturday-last_day_of_2018Q4).days/7
quarter_of_quarter=np.floor(quarter_of_quarter/13)%4
quarter_of_quarter=str(int(1+quarter_of_quarter))
print("Quarter",quarter_of_quarter)

str_current_quarter=year_of_quarter+"_Q"+quarter_of_quarter

print(str_current_quarter)

current_week=int((last_sturday-last_day_of_2018Q4).days/7%13)
print("current_week",current_week)

if current_week==0:
    quarter_of_quarter=int(quarter_of_quarter)-1
    str_current_quarter=year_of_quarter+"_Q"+str(quarter_of_quarter)
    current_week=13
    print("Quarter",quarter_of_quarter)
    print(str_current_quarter)
    print("current_week",current_week)
    
    
quarter_of_quarter


# In[3]:

current_quarter_beginning=last_day_of_2018Q4+datetime.timedelta(days=(int(year_of_quarter)-2019)*52*7+                                                                (int(quarter_of_quarter)-1)*13*7+1)
print("current_quarter_beginning: "+str(current_quarter_beginning))

current_quarter_beginning_last_year=current_quarter_beginning-datetime.timedelta(days=52*7)
print("current_quarter_beginning_last_year: "+str(current_quarter_beginning_last_year))


# In[4]:

all_weekly_new_sign_ups=list(recursive_file_gen("/home/jian/BigLots/"))
all_weekly_new_sign_ups=[x for x in all_weekly_new_sign_ups if "aster" in x.lower()]
all_weekly_new_sign_ups=[x for x in all_weekly_new_sign_ups if x[-4:]==".txt"]
all_weekly_new_sign_ups=[x for x in all_weekly_new_sign_ups if x.split("/MediaStorm_")[1][:10]>=str(current_quarter_beginning_last_year)]
all_weekly_new_sign_ups.sort()
print(len(all_weekly_new_sign_ups))

new_sign_ups_piece_1="/home/jian/BigLots/New_Sing_Ups_2018_Fiscal_Year/All Rewards Members 2018-02-04 - 2019-05-04.zip"
new_sign_ups_piece_2="/home/jian/BigLots/New_Sing_Ups_2018_Fiscal_Year/MediaStorm Rewards Master P4 2019 - no transaction info.zip"
all_weekly_new_sign_ups.append(new_sign_ups_piece_1)
all_weekly_new_sign_ups.append(new_sign_ups_piece_2)
print(len(all_weekly_new_sign_ups))


# In[5]:

list_new_ids=[]
for file in all_weekly_new_sign_ups:
    df=pd.read_table(file,dtype=str,sep="|",usecols=['customer_id_hashed','sign_up_date'])
    df=df[df['sign_up_date']>=str(current_quarter_beginning_last_year)]
    list_new_ids=list_new_ids+df['customer_id_hashed'].unique().tolist()
list_new_ids=list(set(list_new_ids))


# In[6]:

audience_files_folder="/home/jian/Projects/Big_Lots/Live_Ramp/Quarterly_Update_"+str_current_quarter.replace("_","")+"/upload_files/"


audience_files_this_quarter=glob.glob(audience_files_folder+"*.csv")
len(audience_files_this_quarter)


audience_files_this_quarter_C=[x for x in audience_files_this_quarter if os.path.basename(x)[0]=="C"]
audience_files_this_quarter_T=[x for x in audience_files_this_quarter if os.path.basename(x)[0]=="T"]
print(len(audience_files_this_quarter_C),len(audience_files_this_quarter_T))

set_Control=set()
for file in audience_files_this_quarter_C:
    df=pd.read_csv(file,dtype=str,usecols=["customer_id_hashed"])
    set_Control=set_Control.union(set(df['customer_id_hashed'].tolist()))
print(len(set_Control))


df_audience=pd.DataFrame()

for file in audience_files_this_quarter_T:
    df=pd.read_csv(file,dtype=str,usecols=['customer_id_hashed','segment'])
    df=df[~df['customer_id_hashed'].isin(set_Control)]
    df_audience=df_audience.append(df)
df_audience=df_audience.drop_duplicates()

print(df_audience.shape," | ",df_audience['customer_id_hashed'].nunique())


# In[ ]:




# In[8]:

df_clean_segment=df_audience[['segment']].drop_duplicates()
df_clean_segment_1_lapsed=df_clean_segment[df_clean_segment['segment'].str.contains("48")]
df_clean_segment_2_in_18=df_clean_segment[~df_clean_segment['segment'].str.contains("48")]

df_clean_segment_1_lapsed['segment_2']="Lapsed_19_48"
df_clean_segment_2_in_18['segment_2']=df_clean_segment_2_in_18['segment'].apply(lambda x: x.split("Score(")[1][0]+"_"+x.split("_2019Q4_RFM")[0][-1])
df_clean_segment=df_clean_segment_1_lapsed.append(df_clean_segment_2_in_18)
df_clean_segment.shape


# In[9]:

print(df_audience.shape)
df_audience_total_count_original=df_audience.groupby("segment")['customer_id_hashed'].nunique().to_frame().reset_index().rename(columns={"customer_id_hashed":"ids_total"})
df_audience=pd.merge(df_audience,df_clean_segment,on="segment",how="left")
print(df_audience.shape)
df_audience_total_count=df_audience.groupby("segment_2")['customer_id_hashed'].nunique().to_frame().reset_index().rename(columns={"customer_id_hashed":"ids_total"})
del df_audience['segment']
df_audience=df_audience.rename(columns={"segment_2":"segment"})

print(df_audience_total_count.shape)
df_audience_total_count


# In[10]:

df_audience_total_count['ids_total'].sum()


# In[11]:

list_POS_this_year_in_quarter=list(recursive_file_gen("/home/jian/BigLots/"))
list_POS_this_year_in_quarter=[x for x in list_POS_this_year_in_quarter if "Daily" in x and x[-4:]==".txt"]
list_POS_this_year_in_quarter=[x for x in list_POS_this_year_in_quarter if "2016" not in x]
list_POS_this_year_in_quarter=[x for x in list_POS_this_year_in_quarter if "2017" not in x]
list_POS_this_year_in_quarter=[x for x in list_POS_this_year_in_quarter if "2018" not in x]
list_POS_this_year_in_quarter=[x for x in list_POS_this_year_in_quarter if "histor" not in x.lower()]
list_POS_this_year_in_quarter.sort()
list_POS_this_year_in_quarter=[x for x in list_POS_this_year_in_quarter if x.split("/MediaStorm_")[1][:10]>=str(current_quarter_beginning)]
# date only >=
print("len(list_POS_this_year_in_quarter): "+str(len(list_POS_this_year_in_quarter)))


# In[12]:

last_year_end_week_in_quarter=last_sturday-datetime.timedelta(days=52*7)

if year_of_quarter=="2019" and (quarter_of_quarter=="3" or quarter_of_quarter=="4"): 
    list_POS_last_year_in_quarter=glob.glob("/home/jian/BigLots/hist_daily_data_itemlevel_decompressed/*.txt")
    list_POS_last_year_in_quarter=[x for x in list_POS_last_year_in_quarter if x.split("/MediaStormDailySalesHistory")[1][:8]>=str(current_quarter_beginning_last_year).replace("-","")]
    list_POS_last_year_in_quarter=[x for x in list_POS_last_year_in_quarter if x.split("/MediaStormDailySalesHistory")[1][:8]<=str(last_year_end_week_in_quarter).replace("-","")]

elif year_of_quarter=="2020" and quarter_of_quarter=="1": 
    list_POS_last_year_in_quarter=list(recursive_file_gen("/home/jian/BigLots/"))
    list_POS_last_year_in_quarter=[x for x in list_POS_last_year_in_quarter if "Daily" in x and x[-4:]==".txt"]
    list_POS_last_year_in_quarter=[x for x in list_POS_last_year_in_quarter if "history" not in x.lower()]
    list_POS_last_year_in_quarter=[x for x in list_POS_last_year_in_quarter if "2019" in x]
    list_POS_last_year_in_quarter=[x for x in list_POS_last_year_in_quarter if "2019-02-09" not in x]

    list_POS_last_year_in_quarter=[x for x in list_POS_last_year_in_quarter if x.split("/MediaStorm_")[1][:10]>=str(current_quarter_beginning_last_year)]
    list_POS_last_year_in_quarter=[x for x in list_POS_last_year_in_quarter if x.split("/MediaStorm_")[1][:10]<=str(last_year_end_week_in_quarter)]
    list_POS_last_year_in_quarter.append("/home/jian/BigLots/hist_daily_data_itemlevel_decompressed/MediaStormDailySalesHistory20190209.txt,1156035811,1551304443000")
else:
    list_POS_last_year_in_quarter=list(recursive_file_gen("/home/jian/BigLots/"))
    list_POS_last_year_in_quarter=[x for x in list_POS_last_year_in_quarter if "Daily" in x and x[-4:]==".txt"]
    list_POS_last_year_in_quarter=[x for x in list_POS_last_year_in_quarter if "2018" not in x]
    list_POS_last_year_in_quarter=[x for x in list_POS_last_year_in_quarter if "2017" not in x]
    list_POS_last_year_in_quarter=[x for x in list_POS_last_year_in_quarter if "2016" not in x]
    list_POS_last_year_in_quarter=[x for x in list_POS_last_year_in_quarter if "history" not in x.lower()]
    list_POS_last_year_in_quarter=[x for x in list_POS_last_year_in_quarter if x.split("/MediaStorm_")[1][:10]>=str(current_quarter_beginning_last_year)]
    list_POS_last_year_in_quarter=[x for x in list_POS_last_year_in_quarter if x.split("/MediaStorm_")[1][:10]<=str(last_year_end_week_in_quarter)]


# In[13]:

list_POS_this_year_in_quarter=list(recursive_file_gen("/home/jian/BigLots/"))
list_POS_this_year_in_quarter=[x for x in list_POS_this_year_in_quarter if "Daily" in x and x[-4:]==".txt"]
list_POS_this_year_in_quarter=[x for x in list_POS_this_year_in_quarter if "2016" not in x]
list_POS_this_year_in_quarter=[x for x in list_POS_this_year_in_quarter if "2017" not in x]
list_POS_this_year_in_quarter=[x for x in list_POS_this_year_in_quarter if "2018" not in x]
list_POS_this_year_in_quarter=[x for x in list_POS_this_year_in_quarter if "histor" not in x.lower()]
list_POS_this_year_in_quarter.sort()
list_POS_this_year_in_quarter=[x for x in list_POS_this_year_in_quarter if x.split("/MediaStorm_")[1][:10]>=str(current_quarter_beginning)]
# date only >=
print("len(list_POS_this_year_in_quarter): "+str(len(list_POS_this_year_in_quarter)))


# In[14]:

if len(list_POS_this_year_in_quarter)!=current_week or len(list_POS_last_year_in_quarter)!=current_week:
    print(str(datetime.datetime.now())+" | Error: POS file lenthg this year and last year different")
    print(str(datetime.datetime.now())+" | current_week "+str(current_week))
    
    print(str(datetime.datetime.now())+" | list_POS_this_year_in_quarter "+str(len(list_POS_this_year_in_quarter)))
    print(str(datetime.datetime.now())+" | list_POS_last_year_in_quarter "+str(len(list_POS_last_year_in_quarter)))
    # break


# In[15]:

print(datetime.datetime.now())


# In[16]:

list_sales_this_year=[]
list_sales_last_year=[]

i_counter=0
for file in list_POS_this_year_in_quarter:
    df=pd.read_table(file,dtype=str,sep="|",usecols=['location_id','transaction_dt','transaction_id','customer_id_hashed',
                                                     'item_transaction_units','item_transaction_amt'])
    df=df[pd.notnull(df['customer_id_hashed'])]
    df['item_transaction_units']=df['item_transaction_units'].astype(int)
    df['item_transaction_amt']=df['item_transaction_amt'].astype(float)
    
    df_sales=df.groupby(['location_id','transaction_dt','customer_id_hashed'])['item_transaction_units','item_transaction_amt'].sum().reset_index()
    df_sales=df_sales.rename(columns={"item_transaction_units":"units","item_transaction_amt":"sales"})
    
    df_trans=df[['location_id','transaction_dt','transaction_id','customer_id_hashed']].drop_duplicates()
    df_trans['trans']=1
    df_trans=df_trans.groupby(['location_id','transaction_dt','customer_id_hashed'])['trans'].sum().to_frame().reset_index()
    
    df=pd.merge(df_sales,df_trans,on=["location_id",'transaction_dt','customer_id_hashed'],how="outer")
    list_sales_this_year.append(df)
    
    
    print(datetime.datetime.now(),i_counter,df.shape)
    i_counter+=1
df_agg_POS_this_year_rewards=pd.concat(list_sales_this_year)
del list_sales_this_year
gc.collect()

#######
i_counter=0
for file in list_POS_last_year_in_quarter:
    df=pd.read_table(file,dtype=str,sep="|",usecols=['location_id','transaction_dt','transaction_id','customer_id_hashed',
                                                     'item_transaction_units','item_transaction_amt'])
    df=df[pd.notnull(df['customer_id_hashed'])]
    df['item_transaction_units']=df['item_transaction_units'].astype(int)
    df['item_transaction_amt']=df['item_transaction_amt'].astype(float)
    
    df_sales=df.groupby(['location_id','transaction_dt','customer_id_hashed'])['item_transaction_units','item_transaction_amt'].sum().reset_index()
    df_sales=df_sales.rename(columns={"item_transaction_units":"units","item_transaction_amt":"sales"})
    
    df_trans=df[['location_id','transaction_dt','transaction_id','customer_id_hashed']].drop_duplicates()
    df_trans['trans']=1
    df_trans=df_trans.groupby(['location_id','transaction_dt','customer_id_hashed'])['trans'].sum().to_frame().reset_index()
    
    df=pd.merge(df_sales,df_trans,on=["location_id",'transaction_dt','customer_id_hashed'],how="outer")
    list_sales_last_year.append(df)
    
    
    print(datetime.datetime.now(),i_counter,df.shape)
    i_counter+=1
df_agg_POS_last_year_rewards=pd.concat(list_sales_last_year)
del list_sales_last_year
gc.collect()


# In[17]:

df_agg_POS_this_year_rewards['Store_Type']=np.where(df_agg_POS_this_year_rewards['location_id']=="6990","Online","InStore")
df_agg_POS_last_year_rewards['Store_Type']=np.where(df_agg_POS_last_year_rewards['location_id']=="6990","Online","InStore")


# In[18]:

def week_end_dt(x):
    y=datetime.datetime.strptime(x,"%Y-%m-%d").date()
    if y.weekday()==6:
        y=y+datetime.timedelta(days=6)
    else:
        y=y+datetime.timedelta(days=5-y.weekday())
    return y

df_week_end_dt=df_agg_POS_this_year_rewards[['transaction_dt']].drop_duplicates()
df_week_end_dt_2=df_agg_POS_last_year_rewards[['transaction_dt']].drop_duplicates()
df_week_end_dt=df_week_end_dt.append(df_week_end_dt_2)
del df_week_end_dt_2
df_week_end_dt['week_end_dt']=df_week_end_dt['transaction_dt'].apply(week_end_dt)


# In[19]:

df_agg_POS_this_year_rewards=pd.merge(df_agg_POS_this_year_rewards,df_week_end_dt,on="transaction_dt",how="left")
df_agg_POS_last_year_rewards=pd.merge(df_agg_POS_last_year_rewards,df_week_end_dt,on="transaction_dt",how="left")


# In[20]:

# Ignore the SOTF because of frequent updates
write_folder="/home/jian/celery/Weekly_Tracker_of_Audience_Performance/weekly_update/output_"+str(datetime.datetime.now().date())+"/"
try:
    os.stat(write_folder)
except:
    os.mkdir(write_folder)


# In[21]:

df_agg_POS_this_year_rewards.to_csv(write_folder+"df_agg_POS_this_year_rewards.csv",index=False)
df_agg_POS_last_year_rewards.to_csv(write_folder+"df_agg_POS_last_year_rewards.csv",index=False)


# In[22]:

df_this_year_rewards_by_id=df_agg_POS_this_year_rewards.groupby(['customer_id_hashed','Store_Type'])['sales','units','trans'].sum().reset_index()
df_last_year_rewards_by_id=df_agg_POS_last_year_rewards.groupby(['customer_id_hashed','Store_Type'])['sales','units','trans'].sum().reset_index()


# In[23]:

df_this_year_rewards_by_id=pd.merge(df_this_year_rewards_by_id,df_audience,on="customer_id_hashed",how="left")
df_last_year_rewards_by_id=pd.merge(df_last_year_rewards_by_id,df_audience,on="customer_id_hashed",how="left")


# In[24]:

df_this_year_rewards_by_id['year']=int(year_of_quarter)
df_last_year_rewards_by_id['year']=int(year_of_quarter)-1


# In[25]:

df_this_year_shopper=df_this_year_rewards_by_id[['customer_id_hashed','year']].drop_duplicates()
df_last_year_shopper=df_last_year_rewards_by_id[['customer_id_hashed','year']].drop_duplicates()

shopper_name_this_year="Shopper_"+str_current_quarter+"_Only"
shopper_name_last_year=shopper_name_this_year.replace(str(year_of_quarter),str(int(year_of_quarter)-1))

df_shopper_type=df_last_year_shopper.append(df_this_year_shopper)
df_shopper_type=df_shopper_type.groupby(['customer_id_hashed'])['year'].sum().to_frame().reset_index()
df_shopper_type['shopper_type']=np.where(df_shopper_type['year']==int(year_of_quarter),shopper_name_this_year,
                                        np.where(df_shopper_type['year']==int(year_of_quarter)-1,shopper_name_last_year,
                                                 "Shopper_Both")
                                        )


# In[26]:

del df_shopper_type['year']
print(df_shopper_type['shopper_type'].unique())
df_shopper_type.head(2)


# In[27]:

df_this_year_rewards_by_id=pd.merge(df_this_year_rewards_by_id,df_shopper_type,on="customer_id_hashed",how="left")
df_last_year_rewards_by_id=pd.merge(df_last_year_rewards_by_id,df_shopper_type,on="customer_id_hashed",how="left")


# In[28]:

df_this_year_rewards_by_id['register_group']=np.where(df_this_year_rewards_by_id['customer_id_hashed'].isin(list_new_ids),"Registered_Since_"+str_current_quarter.replace(year_of_quarter,str(int(year_of_quarter)-1)),
                                           "Registered_Before_"+str_current_quarter.replace(year_of_quarter,str(int(year_of_quarter)-1)))
df_last_year_rewards_by_id['register_group']=np.where(df_last_year_rewards_by_id['customer_id_hashed'].isin(list_new_ids),"Registered_Since_"+str_current_quarter.replace(year_of_quarter,str(int(year_of_quarter)-1)),
                                           "Registered_Before_"+str_current_quarter.replace(year_of_quarter,str(int(year_of_quarter)-1)))


# In[29]:

df_this_year_rewards_by_id.to_csv(write_folder+"df_this_year_shopper.csv",index=False)
df_last_year_rewards_by_id.to_csv(write_folder+"df_last_year_shopper.csv",index=False)


# In[30]:

df_this_year_rewards_by_id.head(2)


# In[31]:

agg_func={"sales":"sum","units":"sum","trans":"sum","customer_id_hashed":"nunique"}
df_this_year_by_group_storetype=df_this_year_rewards_by_id.groupby(['segment','shopper_type','register_group','Store_Type'])['sales','units','trans','customer_id_hashed'].agg(agg_func).reset_index()
df_last_year_by_group_storetype=df_last_year_rewards_by_id.groupby(['segment','shopper_type','register_group','Store_Type'])['sales','units','trans','customer_id_hashed'].agg(agg_func).reset_index()

df_this_year_by_group_NoStoretype=df_this_year_rewards_by_id.groupby(['segment','shopper_type','register_group'])['sales','units','trans','customer_id_hashed'].agg(agg_func).reset_index()
df_last_year_by_group_NoStoretype=df_last_year_rewards_by_id.groupby(['segment','shopper_type','register_group'])['sales','units','trans','customer_id_hashed'].agg(agg_func).reset_index()


# In[32]:

df_this_year_by_group_storetype['year']=int(year_of_quarter)
df_last_year_by_group_storetype['year']=int(year_of_quarter)-1

df_this_year_by_group_NoStoretype['year']=int(year_of_quarter)
df_last_year_by_group_NoStoretype['year']=int(year_of_quarter)-1

df_this_year_by_group_storetype=df_this_year_by_group_storetype.rename(columns={"customer_id_hashed":"shopper_id_count"})
df_last_year_by_group_storetype=df_last_year_by_group_storetype.rename(columns={"customer_id_hashed":"shopper_id_count"})
df_this_year_by_group_NoStoretype=df_this_year_by_group_NoStoretype.rename(columns={"customer_id_hashed":"shopper_id_count"})
df_last_year_by_group_NoStoretype=df_last_year_by_group_NoStoretype.rename(columns={"customer_id_hashed":"shopper_id_count"})


# In[33]:

df_by_gorup_storetype=df_this_year_by_group_storetype.append(df_last_year_by_group_storetype)
df_by_gorup_NoStoretype=df_this_year_by_group_NoStoretype.append(df_last_year_by_group_NoStoretype)


# In[34]:

df_by_gorup_storetype_wide_this_year=df_this_year_by_group_storetype.pivot_table(index=["segment","shopper_type","register_group"],columns="Store_Type",values=["sales","units","trans","shopper_id_count"]).reset_index()
df_by_gorup_storetype_wide_last_year=df_last_year_by_group_storetype.pivot_table(index=["segment","shopper_type","register_group"],columns="Store_Type",values=["sales","units","trans","shopper_id_count"]).reset_index()


# In[35]:

new_col_list=[]
for col in df_by_gorup_storetype_wide_this_year.columns.tolist():
    new_col="_".join(col)
    if new_col[-1]=="_":
        new_col=new_col[:-1]
    new_col_list.append(new_col)
df_by_gorup_storetype_wide_this_year.columns=new_col_list

new_col_list=[]
for col in df_by_gorup_storetype_wide_last_year.columns.tolist():
    new_col="_".join(col)
    if new_col[-1]=="_":
        new_col=new_col[:-1]
    new_col_list.append(new_col)
df_by_gorup_storetype_wide_last_year.columns=new_col_list


# In[36]:

df_by_gorup_storetype_wide_this_year=df_by_gorup_storetype_wide_this_year.fillna(0)
df_by_gorup_storetype_wide_last_year=df_by_gorup_storetype_wide_last_year.fillna(0)


# In[37]:

count_aud_reg=df_audience.copy()

count_aud_reg['register_group']=np.where(count_aud_reg['customer_id_hashed'].isin(list_new_ids),"Registered_Since_"+str_current_quarter.replace(year_of_quarter,str(int(year_of_quarter)-1)),
                                           "Registered_Before_"+str_current_quarter.replace(year_of_quarter,str(int(year_of_quarter)-1)))
count_aud_reg=count_aud_reg.groupby(['segment','register_group'])['customer_id_hashed'].nunique().to_frame().reset_index()


# In[38]:

writer=pd.ExcelWriter(write_folder+"BL_table_view_by_audience_JL_"+str(datetime.datetime.now().date())+".xlsx",engine="xlsxwriter")
df_by_gorup_storetype.to_excel(writer,"df_by_gorup_storetype",index=False)
df_by_gorup_NoStoretype.to_excel(writer,"by_gorup_NoStoretype",index=False)
df_by_gorup_storetype_wide_this_year.to_excel(writer,"by_gorup_storetype_TY_wide",index=False)
df_by_gorup_storetype_wide_last_year.to_excel(writer,"by_gorup_storetype_LY_wide",index=False)
df_audience_total_count_original.to_excel(writer,"audience_original_count",index=False)
df_audience_total_count.to_excel(writer,"audience_PS_HML_count",index=False)
count_aud_reg.to_excel(writer,"audience_Register_count",index=False)
writer.save()

print("Quarterly sales done 1/3: "+str(datetime.datetime.now()))


# # By week

# In[39]:

df_lastyear_by_id_week=df_agg_POS_last_year_rewards.groupby(['customer_id_hashed','Store_Type','week_end_dt'])['sales','units','trans'].sum().reset_index()
df_thisyear_by_id_week=df_agg_POS_this_year_rewards.groupby(['customer_id_hashed','Store_Type','week_end_dt'])['sales','units','trans'].sum().reset_index()


# In[40]:

df_lastyear_by_id_week=pd.merge(df_lastyear_by_id_week,df_audience,on="customer_id_hashed",how="left")
df_thisyear_by_id_week=pd.merge(df_thisyear_by_id_week,df_audience,on="customer_id_hashed",how="left")


# In[41]:

df_lastyear_by_id_week['year']=int(year_of_quarter)-1
df_thisyear_by_id_week['year']=int(year_of_quarter)


# In[42]:

df_lastyear_by_id_week=pd.merge(df_lastyear_by_id_week,df_shopper_type,on="customer_id_hashed",how="left")
df_thisyear_by_id_week=pd.merge(df_thisyear_by_id_week,df_shopper_type,on="customer_id_hashed",how="left")


# In[43]:

df_lastyear_by_id_week['register_group']=np.where(df_lastyear_by_id_week['customer_id_hashed'].isin(list_new_ids),"Registered_Since_"+str_current_quarter.replace(year_of_quarter,str(int(year_of_quarter)-1)),
                                           "Registered_Before_"+str_current_quarter.replace(year_of_quarter,str(int(year_of_quarter)-1)))
df_thisyear_by_id_week['register_group']=np.where(df_thisyear_by_id_week['customer_id_hashed'].isin(list_new_ids),"Registered_Since_"+str_current_quarter.replace(year_of_quarter,str(int(year_of_quarter)-1)),
                                           "Registered_Before_"+str_current_quarter.replace(year_of_quarter,str(int(year_of_quarter)-1)))


# In[44]:

df_lastyear_by_id_week.to_csv(write_folder+"df_lastyear_by_id_week.csv",index=False)
df_thisyear_by_id_week.to_csv(write_folder+"df_thisyear_by_id_week.csv",index=False)


# In[45]:

agg_func={"sales":"sum","units":"sum","trans":"sum","customer_id_hashed":"nunique"}
df_lastyear_by_group_storetype_week=df_lastyear_by_id_week.groupby(['week_end_dt','segment','shopper_type','register_group','Store_Type'])['sales','units','trans','customer_id_hashed'].agg(agg_func).reset_index()
df_thisyear_by_group_storetype_week=df_thisyear_by_id_week.groupby(['week_end_dt','segment','shopper_type','register_group','Store_Type'])['sales','units','trans','customer_id_hashed'].agg(agg_func).reset_index()

df_lastyear_by_group_NoStoretype_week=df_lastyear_by_id_week.groupby(['week_end_dt','segment','shopper_type','register_group'])['sales','units','trans','customer_id_hashed'].agg(agg_func).reset_index()
df_thisyear_by_group_NoStoretype_week=df_thisyear_by_id_week.groupby(['week_end_dt','segment','shopper_type','register_group'])['sales','units','trans','customer_id_hashed'].agg(agg_func).reset_index()


# In[46]:

df_thisyear_by_group_storetype_week['year']=int(year_of_quarter)
df_lastyear_by_group_storetype_week['year']=int(year_of_quarter)-1

df_thisyear_by_group_NoStoretype_week['year']=int(year_of_quarter)
df_lastyear_by_group_NoStoretype_week['year']=int(year_of_quarter)-1

df_thisyear_by_group_storetype_week=df_thisyear_by_group_storetype_week.rename(columns={"customer_id_hashed":"shopper_id_count"})
df_lastyear_by_group_storetype_week=df_lastyear_by_group_storetype_week.rename(columns={"customer_id_hashed":"shopper_id_count"})
df_thisyear_by_group_NoStoretype_week=df_thisyear_by_group_NoStoretype_week.rename(columns={"customer_id_hashed":"shopper_id_count"})
df_lastyear_by_group_NoStoretype_week=df_lastyear_by_group_NoStoretype_week.rename(columns={"customer_id_hashed":"shopper_id_count"})


# In[47]:

df_by_gorup_storetype_week=df_thisyear_by_group_storetype_week.append(df_lastyear_by_group_storetype_week)
df_by_gorup_NoStoretype_week=df_thisyear_by_group_NoStoretype_week.append(df_lastyear_by_group_NoStoretype_week)


# In[48]:

df_by_gorup_storetype_wide_thisyear_week=df_thisyear_by_group_storetype_week.pivot_table(index=["week_end_dt","segment","shopper_type","register_group"],columns="Store_Type",values=["sales","units","trans","shopper_id_count"]).reset_index()
df_by_gorup_storetype_wide_lastyear_week=df_lastyear_by_group_storetype_week.pivot_table(index=["week_end_dt","segment","shopper_type","register_group"],columns="Store_Type",values=["sales","units","trans","shopper_id_count"]).reset_index()


# In[49]:

new_col_list=[]
for col in df_by_gorup_storetype_wide_thisyear_week.columns.tolist():
    new_col="_".join(col)
    if new_col[-1]=="_":
        new_col=new_col[:-1]
    new_col_list.append(new_col)
df_by_gorup_storetype_wide_thisyear_week.columns=new_col_list

new_col_list=[]
for col in df_by_gorup_storetype_wide_lastyear_week.columns.tolist():
    new_col="_".join(col)
    if new_col[-1]=="_":
        new_col=new_col[:-1]
    new_col_list.append(new_col)
df_by_gorup_storetype_wide_lastyear_week.columns=new_col_list


# In[50]:

df_by_gorup_storetype_wide_thisyear_week=df_by_gorup_storetype_wide_thisyear_week.fillna(0)
df_by_gorup_storetype_wide_lastyear_week=df_by_gorup_storetype_wide_lastyear_week.fillna(0)


# In[51]:

writer=pd.ExcelWriter(write_folder+"BL_table_view_by_week_audience_JL_"+str(datetime.datetime.now().date())+".xlsx",engine="xlsxwriter")
df_by_gorup_storetype_week.to_excel(writer,"View_Week_gorup_storetype",index=False)
df_by_gorup_NoStoretype_week.to_excel(writer,"View_Week_gorup_NoStoretype",index=False)
df_by_gorup_storetype_wide_thisyear_week.to_excel(writer,"View_W_gorup_storetype_TY_wide",index=False)
df_by_gorup_storetype_wide_lastyear_week.to_excel(writer,"View_W_gorup_storetype_LY_wide",index=False)
df_audience_total_count_original.to_excel(writer,"audience_original_count",index=False)
df_audience_total_count.to_excel(writer,"audience_PS_HML_count",index=False)
count_aud_reg.to_excel(writer,"audience_Register_count",index=False)
writer.save()

print("Weekly sales done 2/3: "+str(datetime.datetime.now()))


# # Excel Table

# In[128]:

print(df_by_gorup_storetype.shape)
# df_excel_table=df_by_gorup_storetype[df_by_gorup_storetype['register_group'].str.contains("_Before_")]
df_excel_table=df_by_gorup_storetype.copy()
print(df_excel_table.shape)

df_excel_table=df_excel_table.groupby(['segment','shopper_type','register_group','year'])['sales','trans','shopper_id_count'].sum().reset_index()
df_excel_table=df_excel_table.pivot_table(index=['segment','shopper_type','register_group'],columns=['year'],values=['sales','trans','shopper_id_count']).reset_index()

new_col_list=[]
for col in df_excel_table.columns.tolist():
    if col[1]:
        new_col=col[0]+"_"+str(col[1]).replace(str(col[1]),str(col[1])+"Q"+str(quarter_of_quarter))
        new_col_list.append(new_col)
    else:
        new_col_list.append(col[0])
df_excel_table.columns=new_col_list

df_excel_table=df_excel_table.sort_values(['shopper_type','segment'],ascending=[False,True]).fillna(0)
print(df_excel_table.shape)


# In[129]:

sales_col_ty=[x for x in df_excel_table.columns.tolist() if "sales" in x and str(year_of_quarter) in x][0]
sales_col_ly=[x for x in df_excel_table.columns.tolist() if "sales" in x and str(int(year_of_quarter)-1) in x][0]
new_position=df_excel_table.columns.tolist().index(sales_col_ty)
df_excel_table.insert(new_position+1, 'sales_Difference', df_excel_table[sales_col_ty]-df_excel_table[sales_col_ly])
df_excel_table.insert(new_position+1, 'sales_YoY', df_excel_table[sales_col_ty]/df_excel_table[sales_col_ly]-1)

shopper_col_ty=[x for x in df_excel_table.columns.tolist() if "shopper_id_count_" in x and str(year_of_quarter) in x][0]
shopper_col_ly=[x for x in df_excel_table.columns.tolist() if "shopper_id_count_" in x and str(int(year_of_quarter)-1) in x][0]
new_position=df_excel_table.columns.tolist().index(shopper_col_ty)
df_excel_table.insert(new_position+1, 'shoppers_YoY', df_excel_table[shopper_col_ty]/df_excel_table[shopper_col_ly]-1)

trans_col_ty=[x for x in df_excel_table.columns.tolist() if "trans" in x and str(year_of_quarter) in x][0]
trans_col_ly=[x for x in df_excel_table.columns.tolist() if "trans" in x and str(int(year_of_quarter)-1) in x][0]
new_position=df_excel_table.columns.tolist().index(trans_col_ty)
df_excel_table.insert(new_position+1, 'trans_YoY', df_excel_table[trans_col_ty]/df_excel_table[trans_col_ly]-1)

df_excel_table=pd.merge(df_excel_table,count_aud_reg,on=['segment','register_group'],how="left").rename(columns={"customer_id_hashed":"total_id_by_seg_register"})


# In[130]:

str_this_year=str(year_of_quarter)
str_last_year=str(int(year_of_quarter)-1)


# In[131]:

df_excel_table['Conv_Rate_'+str_this_year]=df_excel_table['shopper_id_count_'+str_this_year+"Q"+quarter_of_quarter]/df_excel_table['total_id_by_seg_register']
df_excel_table['Conv_Rate_'+str_last_year]=df_excel_table['shopper_id_count_'+str_last_year+"Q"+quarter_of_quarter]/df_excel_table['total_id_by_seg_register']


# In[132]:

df_excel_table.head(2)


# In[133]:

df_excel_table=df_excel_table.rename(columns={"total_id_by_seg_register":"total_id_by_seg_register_temp"})
df_excel_table.insert(3, 'total_id_by_seg_register', df_excel_table['total_id_by_seg_register_temp'])
del df_excel_table['total_id_by_seg_register_temp']


# In[134]:

df_seg_by_store_type=df_by_gorup_storetype.groupby(['segment','shopper_type','register_group','Store_Type','year'])['sales'].sum().to_frame().reset_index()
df_seg_by_store_type=df_seg_by_store_type.pivot_table(index=['segment','shopper_type','register_group','year'],columns="Store_Type",values="sales").reset_index()
df_seg_by_store_type=df_seg_by_store_type[df_seg_by_store_type['register_group'].str.contains("efore")]

df_this_year=df_seg_by_store_type[df_seg_by_store_type['year']==df_seg_by_store_type['year'].max()].rename(columns={"InStore":"InStore_Sales_"+str(df_seg_by_store_type['year'].max()),"Online":"Online_Sales_"+str(df_seg_by_store_type['year'].max())})
df_last_year=df_seg_by_store_type[df_seg_by_store_type['year']==df_seg_by_store_type['year'].min()].rename(columns={"InStore":"InStore_Sales_"+str(df_seg_by_store_type['year'].min()),"Online":"Online_Sales_"+str(df_seg_by_store_type['year'].min())})
del df_this_year['year']
del df_last_year['year']

df_sales_store_type_wide=pd.merge(df_this_year,df_last_year,on=['segment','shopper_type','register_group'],how="outer")
df_sales_store_type_wide=df_sales_store_type_wide.fillna(0)
df_excel_table=pd.merge(df_excel_table,df_sales_store_type_wide,on=['segment','shopper_type','register_group'],how="outer")


# In[135]:

df_excel_table['AOV_'+str_last_year+"Q"+quarter_of_quarter]=df_excel_table['sales_'+str_last_year+"Q"+quarter_of_quarter]/df_excel_table['trans_'+str_last_year+"Q"+quarter_of_quarter]
df_excel_table['AOV_'+str_this_year+"Q"+quarter_of_quarter]=df_excel_table['sales_'+str_this_year+"Q"+quarter_of_quarter]/df_excel_table['trans_'+str_this_year+"Q"+quarter_of_quarter]
df_excel_table['YoY_AOV']=df_excel_table['AOV_'+str_this_year+"Q"+quarter_of_quarter]/df_excel_table['AOV_'+str_last_year+"Q"+quarter_of_quarter]-1


# In[136]:

df_sales_by_aud_week=df_by_gorup_NoStoretype_week[df_by_gorup_NoStoretype_week['register_group'].str.contains("efore")]
df_sales_by_aud_week=df_sales_by_aud_week[['week_end_dt','segment','shopper_type','register_group','sales']].drop_duplicates()
df_sales_by_aud_week=df_sales_by_aud_week.pivot_table(index=["segment",'shopper_type','register_group'],columns=['week_end_dt'],values=['sales']).reset_index()

new_col_list=[]
for col in df_sales_by_aud_week.columns.tolist():
    if col[1]=="":
        new_col=col[0]
    else:
        new_col=col[0]+"_"+str(col[1])
    new_col_list.append(new_col)

df_sales_by_aud_week.columns=new_col_list
df_sales_by_aud_week=df_sales_by_aud_week.fillna(0)


# In[137]:

sales_by_week_list=[x for x in df_sales_by_aud_week.columns.tolist() if "-" in x]
new_list=[]

sales_week_this_year=[]
for i in range(int((len(sales_by_week_list)+1)/2)):
    col_1=sales_by_week_list[i]
    col_2=sales_by_week_list[i+int((len(sales_by_week_list)+1)/2)]
    new_list.append(col_2)
    new_list.append(col_1)
    sales_week_this_year.append(col_2)
# Reorder below
df_sales_by_aud_week=df_sales_by_aud_week[[x for x in df_sales_by_aud_week.columns.tolist() if x not in new_list]+new_list]


# In[138]:

for sales_week in sales_week_this_year:
    week_str=sales_week.split("_")[1]
    week_str_last_year=str(datetime.datetime.strptime(week_str,"%Y-%m-%d").date()-datetime.timedelta(days=52*7))
    df_sales_by_aud_week.insert(df_sales_by_aud_week.columns.tolist().index(sales_week)+2, 'SalesYoY_'+week_str, df_sales_by_aud_week[sales_week]/df_sales_by_aud_week[sales_week.replace(week_str,week_str_last_year)]-1)
df_excel_table=pd.merge(df_excel_table,df_sales_by_aud_week,on=['segment','shopper_type','register_group'],how="left")
df_excel_table=df_excel_table.sort_values(['shopper_type','segment','register_group'],ascending=[False,True,True])


# In[140]:

cols_in_excel=df_excel_table.columns.tolist()


# In[141]:

dimension_1=df_excel_table['segment'].unique().tolist()
dimension_2=df_excel_table['shopper_type'].unique().tolist()
dimension_3=df_excel_table['register_group'].unique().tolist()

df_dimension_1=pd.DataFrame({"segment":dimension_1}).reset_index()
df_dimension_1['index']=1

df_dimension_2=pd.DataFrame({"shopper_type":dimension_2}).reset_index()
df_dimension_2['index']=1

df_dimension_3=pd.DataFrame({"register_group":dimension_3}).reset_index()
df_dimension_3['index']=1

df_dimension=pd.merge(df_dimension_1,df_dimension_2,on="index")
df_dimension=pd.merge(df_dimension,df_dimension_3,on="index")
df_excel_table=pd.merge(df_dimension,df_excel_table,on=['segment','shopper_type','register_group'],how="left")
df_excel_table=df_excel_table[cols_in_excel]
df_excel_table_0=df_excel_table[df_excel_table['shopper_type'].str.contains("_Both")]
df_excel_table_1=df_excel_table[~df_excel_table['shopper_type'].str.contains("_Both")]
df_excel_table_1=df_excel_table_1.sort_values(["segment","register_group","shopper_type"],ascending=[True,True,False])

df_excel_table_1=df_excel_table_1.reset_index()
del df_excel_table_1['index']


# In[142]:

for col in df_excel_table_1.columns.tolist():
    if "yoy" in col.lower():
        df_excel_table_1[col]=np.nan
        if "-" in col:
            col_this_year=df_excel_table_1.columns.tolist()[df_excel_table_1.columns.tolist().index(col)-2]
            col_last_year=df_excel_table_1.columns.tolist()[df_excel_table_1.columns.tolist().index(col)-1]
            print(col_this_year,col_last_year,col)
        else:
            col_last_year=df_excel_table_1.columns.tolist()[df_excel_table_1.columns.tolist().index(col)-2]
            col_this_year=df_excel_table_1.columns.tolist()[df_excel_table_1.columns.tolist().index(col)-1]
            print(col_this_year,col_last_year,col)
            

            for i in range(len(df_excel_table_1)):
                if i%2==0:
                    df_excel_table_1.loc[i,col]=df_excel_table_1.loc[i,col_this_year]/df_excel_table_1.loc[i+1,col_last_year]-1
df_excel_table=df_excel_table_0.append(df_excel_table_1)
df_excel_table=df_excel_table[pd.notnull(df_excel_table['total_id_by_seg_register'])]


# In[143]:

old_cols=df_excel_table.columns.tolist()

old_cols_0=[x for x in old_cols if "sales" not in x.lower() or "-" not in x]
old_cols_1=[x for x in old_cols if "sales" in x.lower() and "-" in x]

new_cols_0=[x.replace(str_this_year,"ThisYear").replace(str_last_year,"LastYear") for x in old_cols_0]
new_cols_0=[x.replace("Q"+quarter_of_quarter,"_InQuarter") for x in new_cols_0]

new_cols=new_cols_0+old_cols_1

new_cols_1_unique_weeks_thisyear=[x.split("_")[1] for x in new_cols if "YoY" in x and "_" in x and "-" in x]
new_cols_1_unique_weeks_thisyear.sort()

df_new_cols_1_unique_weeks_thisyear=pd.DataFrame({"week_end_date":new_cols_1_unique_weeks_thisyear},index=[x for x in range(len(new_cols_1_unique_weeks_thisyear))])
df_new_cols_1_unique_weeks_thisyear=df_new_cols_1_unique_weeks_thisyear.reset_index()
df_new_cols_1_unique_weeks_thisyear['index']=df_new_cols_1_unique_weeks_thisyear['index'].apply(lambda x: x+1)
df_new_cols_1_unique_weeks_thisyear['index']=df_new_cols_1_unique_weeks_thisyear['index'].apply(lambda x: "W"+str(x).zfill(2)+"_TY")

df_new_cols_1_unique_weeks_lastyear=df_new_cols_1_unique_weeks_thisyear.copy()
df_new_cols_1_unique_weeks_lastyear['week_end_date']=df_new_cols_1_unique_weeks_lastyear['week_end_date'].apply(lambda x: datetime.datetime.strptime(x,"%Y-%m-%d").date()-datetime.timedelta(days=52*7))
df_new_cols_1_unique_weeks_lastyear['week_end_date']=df_new_cols_1_unique_weeks_lastyear['week_end_date'].astype(str)
df_new_cols_1_unique_weeks_lastyear['index']=df_new_cols_1_unique_weeks_lastyear['index'].apply(lambda x: x.replace("_TY","_LY"))

dict_replace_this_year=df_new_cols_1_unique_weeks_thisyear.set_index("week_end_date").to_dict()['index']
dict_replace_last_year=df_new_cols_1_unique_weeks_lastyear.set_index("week_end_date").to_dict()['index']

for week_str,week_num in dict_replace_this_year.items():
    new_cols=[x.replace(week_str,week_num) for x in new_cols]
for week_str,week_num in dict_replace_last_year.items():
    new_cols=[x.replace(week_str,week_num) for x in new_cols]


# In[144]:

df_excel_table['segment'].unique()


# In[145]:

df_excel_table.head(2)


# In[146]:

df_excel_table.columns=new_cols


df_excel_table.insert(1, 'Audience Group', np.nan)
df_excel_table['Audience Group']=df_excel_table['segment'].apply(lambda x: x.replace("P","Primary").replace("S","Secondary").replace("H_","High_").replace("M_","Medium_").replace("L_","Low_"))
df_excel_table.insert(1, 'Quarter', str_this_year+"Q"+quarter_of_quarter)

for col in df_excel_table.columns.tolist():
    if col[:10]=="SalesYoY_W":
        df_excel_table=df_excel_table.rename(columns={col:col.replace("_TY","")})


# In[147]:

writer=pd.ExcelWriter(write_folder+"BL_weekly_audience_tracker_"+str(last_sturday)+"_"+str_current_quarter+"_week"+str(current_week)+"_JL_"+str(datetime.datetime.now().date())+".xlsx",engine="xlsxwriter")
df_excel_table.to_excel(writer,"weekly_tracker",index=False)
writer.save()

print("Excel Dashboard done 3/3: "+str(datetime.datetime.now()))


print("Worker Done: "+str(datetime.datetime.now()))


# In[148]:

writer=pd.ExcelWriter("/home/simeng/outputs_"+str(last_sturday)+"/BL_weekly_audience_tracker_"+str(last_sturday)+".xlsx",engine="xlsxwriter")
df_excel_table.to_excel(writer,"weekly_tracker",index=False)
writer.save()

print("Saved to Simeng: "+str(datetime.datetime.now()))


# In[149]:

df_excel_table


# In[ ]:




# In[ ]:



