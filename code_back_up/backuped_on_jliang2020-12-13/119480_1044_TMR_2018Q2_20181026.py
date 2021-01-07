
# coding: utf-8

# In[1]:

import pandas as pd
import json
import os
import datetime
import numpy as np
import glob

# Email to be allocated from EE to DMA, not done yet


# In[2]:

today_str=str(datetime.datetime.now().date())
writer_folder="/home/jian/Projects/Big_Lots/TMR/To_Spencer/output/"+today_str+"/"
try:
    os.stat(writer_folder)
except:
    os.mkdir(writer_folder)


# In[ ]:




# In[3]:

National_TV_2017=pd.read_csv("/home/jian/Projects/Big_Lots/TMR/To_Spencer/finaltvlogs_0306.csv",dtype=str)

National_TV_2017['impression']=National_TV_2017['Act Impression'].astype(float)
National_TV_2017['cost']=National_TV_2017['Net Cost'].astype(float)
National_TV_2017['week_start_date']=National_TV_2017['Week BL'].apply(lambda x: datetime.datetime.strptime(x,"%m/%d/%y").date())
National_TV_2017=National_TV_2017[National_TV_2017['week_start_date']<datetime.date(2017,12,30)]
National_TV_2017=National_TV_2017[National_TV_2017['week_start_date']>=datetime.date(2016,10,2)]


# In[4]:

National_TV_2017=National_TV_2017[['week_start_date','cleaned DMA','Media Type','Network','impression','cost']]
National_TV_2017=National_TV_2017.groupby(['week_start_date','cleaned DMA','Media Type','Network'])['impression','cost'].sum().reset_index()
National_TV_2017=National_TV_2017.rename(columns={"cleaned DMA":"cleaned dma","Media Type":"submedia","Network":"placement"})
National_TV_2017['submedia']=National_TV_2017['submedia'].replace(['National Cable','DirecTV'],"National")
National_TV_2017['submedia']=National_TV_2017['submedia'].replace(['Spot Cable', 'Spot Broadcast', 'FOOTPRINT-SINCLAIR'],"Local")
National_TV_2017=National_TV_2017[National_TV_2017['submedia']=="National"]


# In[5]:

National_TV_2017['impression'].sum()


# In[6]:

data_Joann=pd.read_table("/home/jian/Projects/Big_Lots/TMR/To_Spencer/Up_to_2018Q2/BL ALL TMR 0926 updated.dat",sep="\t",dtype=str)
data_Joann['week date']=data_Joann['week date'].apply(lambda x: datetime.datetime.strptime(x,"%m/%d/%Y").date())


# In[7]:

data_Joann['impression']=data_Joann['impression'].astype(float)
data_Joann['click']=data_Joann['click'].astype(float)
data_Joann['cost']=data_Joann['cost'].astype(float)


# In[8]:

# Replace Q2 TV from Connor because of actualized spend

new_Q2_TV=pd.read_csv("/home/jian/Projects/Big_Lots/TMR/To_Spencer/Up_to_2018Q2/BigLots_Q2_TMR_TV_CC_20181016.csv",dtype=str)
new_Q2_TV=new_Q2_TV[data_Joann.columns.tolist()]
new_Q2_TV['week date']=new_Q2_TV['week date'].apply(lambda x: datetime.datetime.strptime(x,"%m/%d/%Y").date())
new_Q2_TV['impression']=new_Q2_TV['impression'].astype(float)
new_Q2_TV['click']=new_Q2_TV['click'].astype(float)
new_Q2_TV['cost']=new_Q2_TV['cost'].astype(float)

data_Joann_Q2_TV=data_Joann[(data_Joann['media']=="TV") & (data_Joann['week date']>=datetime.date(2018,5,6)) & (data_Joann['week date']<=datetime.date(2018,8,4))]
data_Joann_others=data_Joann[(data_Joann['media']!="TV") | (data_Joann['week date']<datetime.date(2018,5,6)) | (data_Joann['week date']>datetime.date(2018,8,4))]

data_Joann=data_Joann_others.append(new_Q2_TV)


# In[9]:

'''
date_range_func={"week date":['max','min'],"impression":"sum","click":"sum","cost":"sum"}
data_Joann.groupby(['media','submedia'])['week date','impression','click','cost'].agg(date_range_func).reset_index()
'''


# In[10]:

data_Joann=data_Joann[data_Joann['week date']<=datetime.date(2018,7,29)] # week start date
data_Joann=data_Joann[data_Joann['week date']>=datetime.date(2016,10,1)]
data_Joann['week date']=data_Joann['week date'].astype(str)


# In[11]:

data_Joann.head(2)


# In[15]:

email_all=data_Joann[data_Joann['media']=="Email"]
non_email_all=data_Joann[data_Joann['media']!="Email"]
email_all['cost']=email_all['impression']*0.000455784518529597
data_Joann=email_all.append(non_email_all)


# In[ ]:




# In[ ]:




# In[16]:

data_media_national=data_Joann.groupby(['week date','media'])[['impression','click','cost']].sum().reset_index()

data_sub_media_national=data_Joann.groupby(['week date','media','submedia'])[['impression','click','cost']].sum().reset_index()
data_sub_media_national['submedia']=data_sub_media_national['media']+"_"+data_sub_media_national['submedia']
del data_sub_media_national['media']


# In[17]:

data_sub_media_national['week_start_date']=data_sub_media_national['week date']
data_sub_media_national['week_start_date']=data_sub_media_national['week_start_date'].apply(lambda x: datetime.datetime.strptime(x,"%Y-%m-%d").date())
data_sub_media_national_2018=data_sub_media_national[data_sub_media_national['week_start_date']>=datetime.date(2017,12,31)]
data_sub_media_national_2017=data_sub_media_national[data_sub_media_national['week_start_date']<datetime.date(2017,12,31)]
data_sub_media_national_2017_Non_NationalTV=data_sub_media_national_2017[data_sub_media_national_2017['submedia']!="TV_National"]
data_sub_media_national_2017_NationalTV=data_sub_media_national_2017[data_sub_media_national_2017['submedia']=="TV_National"]


# In[18]:

National_TV_2017['week date']=np.nan
National_TV_2017['click']=0
data_sub_media_national_2017_NationalTV=National_TV_2017[['week date','submedia','impression','click','cost','week_start_date']]
data_sub_media_national_2017_NationalTV['submedia']="TV_National"

data_sub_media_national_2017_NationalTV['week date']=data_sub_media_national_2017_NationalTV['week_start_date'].astype(str)
data_sub_media_national_2017_NationalTV=data_sub_media_national_2017_NationalTV.groupby(['week date','submedia','week_start_date'])['impression','click','cost'].sum().reset_index()


# In[19]:

data_sub_media_national=data_sub_media_national_2017_Non_NationalTV.append(data_sub_media_national_2017_NationalTV).append(data_sub_media_national_2018)
data_sub_media_national=data_sub_media_national.reset_index()
del data_sub_media_national['index']


# In[20]:

data_sub_media_national_2017_NationalTV.head(2)


# In[21]:

def pivot_level_media(level,df_input):
    df_impr=df_input[['week date',level,'impression']]
    df_impr=df_impr.pivot_table(index='week date',columns=level,values='impression').reset_index()
    for col in df_impr.columns.tolist()[1:]:
        df_impr=df_impr.rename(columns={col:col+"_impr"})
        
    df_click=df_input[['week date',level,'click']]
    df_click=df_click.pivot_table(index='week date',columns=level,values='click').reset_index()
    for col in df_click.columns.tolist()[1:]:
        df_click=df_click.rename(columns={col:col+"_click"})
        
    df_cost=df_input[['week date',level,'cost']]
    df_cost=df_cost.pivot_table(index='week date',columns=level,values='cost').reset_index()
    for col in df_cost.columns.tolist()[1:]:
        df_cost=df_cost.rename(columns={col:col+"_cost"})
        
    result=pd.merge(df_impr,df_click,on="week date",how="outer")
    result=pd.merge(result,df_cost,on="week date",how="outer")
    result=result.fillna(0)
    return result


# In[22]:

data_media_national_wide=pivot_level_media('media',data_media_national)
data_submedia_national_wide=pivot_level_media('submedia',data_sub_media_national)


# In[23]:

data_media_dma=data_Joann.groupby(['week date','media','cleaned dma'])[['impression','click','cost']].sum().reset_index()
data_media_dma=data_media_dma[data_media_dma['cleaned dma']!="National"]
data_media_dma=data_media_dma[data_media_dma['cleaned dma']!="xx"]
data_sub_media_dma=data_Joann.groupby(['week date','media','submedia','cleaned dma'])[['impression','click','cost']].sum().reset_index()
data_sub_media_dma['submedia']=data_sub_media_dma['media']+"_"+data_sub_media_dma['submedia']
del data_sub_media_dma['media']
data_sub_media_dma=data_sub_media_dma[data_sub_media_dma['cleaned dma']!="National"]
data_sub_media_dma=data_sub_media_dma[data_sub_media_dma['cleaned dma']!="xx"]

data_media_dma['week date']=data_media_dma['week date']+"|"+data_media_dma['cleaned dma']
del data_media_dma['cleaned dma']

data_sub_media_dma['week date']=data_sub_media_dma['week date']+"|"+data_sub_media_dma['cleaned dma']
del data_sub_media_dma['cleaned dma']


# In[24]:

data_media_dma_wide=pivot_level_media('media',data_media_dma)
data_media_dma_wide['cleaned dma']=data_media_dma_wide['week date'].apply(lambda x: x.split("|")[1])
data_media_dma_wide['week date']=data_media_dma_wide['week date'].apply(lambda x: x.split("|")[0])

data_submedia_dma_wide=pivot_level_media('submedia',data_sub_media_dma)
data_submedia_dma_wide['cleaned dma']=data_submedia_dma_wide['week date'].apply(lambda x: x.split("|")[1])
data_submedia_dma_wide['week date']=data_submedia_dma_wide['week date'].apply(lambda x: x.split("|")[0])


# In[ ]:




# In[ ]:

data_media_national_wide['week date']=data_media_national_wide['week date'].apply(lambda x:datetime.datetime.strptime(x,"%Y-%m-%d").date())
data_submedia_national_wide['week date']=data_submedia_national_wide['week date'].apply(lambda x:datetime.datetime.strptime(x,"%Y-%m-%d").date())

data_media_dma_wide['week date']=data_media_dma_wide['week date'].apply(lambda x:datetime.datetime.strptime(x,"%Y-%m-%d").date())
data_submedia_dma_wide['week date']=data_submedia_dma_wide['week date'].apply(lambda x:datetime.datetime.strptime(x,"%Y-%m-%d").date())


# # Sales & Transaction

# In[ ]:

folder_file_pattern=glob.glob("/home/jian/Projects/Big_Lots/Sales_All/Sales_Data/*.txt")
sales_all=pd.DataFrame()

for file in folder_file_pattern:
    df=pd.read_csv(file,sep="|",dtype=str,na_values="?")

    if "class_code_id" in df.columns:
        try:
            df['week_end_dt']=df['week_end_dt'].apply(lambda x: datetime.datetime.strptime(x,"%Y-%m-%d").date())
            
        except:
            print(file,"Date Error")

    
    else:
        print(file,"Error: class_code_id not in columns")
        
    sales_all=sales_all.append(df)
    
sales_all['class_gross_sales_amt']=sales_all['class_gross_sales_amt'].astype(float)
sales_all['class_gross_sales_amt']=sales_all['class_gross_sales_amt'].fillna(0.0)
sales_all['subclass_gross_sales_amt']=sales_all['subclass_gross_sales_amt'].astype(float)
sales_all['subclass_gross_sales_amt']=np.where(pd.isnull(sales_all['subclass_gross_sales_amt']),sales_all['class_gross_sales_amt'],sales_all['subclass_gross_sales_amt'])

trans_all=sales_all.copy()
sales_all=sales_all[(sales_all['week_end_dt']<=datetime.date(2018,8,4)) & (sales_all['week_end_dt']>=datetime.date(2016,10,8))]    
sales_all=sales_all.drop_duplicates()
ecommerce_sales=sales_all[sales_all['location_id']=="6990"]
sales_all=sales_all[sales_all['location_id']!="6990"]
sales_all=sales_all[sales_all['location_id']!="145"]


# In[ ]:

trans_all=trans_all[['location_id','week_end_dt','gross_transaction_cnt']].drop_duplicates()
trans_all['gross_transaction_cnt']=trans_all['gross_transaction_cnt'].astype(int)
trans_all['week_end_dt']=trans_all['week_end_dt'].astype(str)
trans_all_0429=trans_all[trans_all['week_end_dt']=="2017-04-29"]
trans_all_0429['week_end_dt']="2017-05-06"
trans_all_0429=trans_all_0429.rename(columns={"gross_transaction_cnt":"0429"})

trans_all_0513=trans_all[trans_all['week_end_dt']=="2017-05-13"]
trans_all_0513['week_end_dt']="2017-05-06"
trans_all_0513=trans_all_0513.rename(columns={"gross_transaction_cnt":"0513"})

trans_all_0506=pd.merge(trans_all_0429,trans_all_0513,on=['location_id','week_end_dt'],how="outer")
trans_all_0506=trans_all_0506.fillna(0)
trans_all_0506['gross_transaction_cnt']=(trans_all_0506['0429']+trans_all_0506['0513'])/2
trans_all_0506=trans_all_0506[['location_id','week_end_dt','gross_transaction_cnt']]

trans_all_exc=trans_all[trans_all['week_end_dt']!="2017-05-06"]
trans_data=trans_all_exc.append(trans_all_0506)
trans_data=trans_data.sort_values(['location_id','week_end_dt'])
# trans_data['week_end_dt']=trans_data['week_end_dt'].apply(lambda x: datetime.datetime.strptime(x,"%Y-%m-%d").date())

ecommerce_trans=trans_data[trans_data['location_id']=="6990"]
trans_data=trans_data[trans_data['location_id']!="6990"]
trans_data=trans_data[trans_data['location_id']!="145"]


# In[ ]:

store_DMA=pd.read_excel("/home/jian/Projects/Big_Lots/Other_Input/all_store_DMA_20180726.xlsx",dtype=str)
store_DMA=store_DMA[['location_id','cleaned_dma']].rename(columns={"cleaned_dma":"cleaned dma"})
# dma_clean=pd.read_excel("/home/jian/Projects/Big_Lots/Other_Input/DMA cleaning.xlsx",dtype=str)


# In[ ]:

sales_all_by_store=sales_all.groupby(['location_id','week_end_dt'])['subclass_gross_sales_amt'].sum().to_frame().reset_index()
sales_all_by_store=pd.merge(sales_all_by_store,store_DMA,on="location_id",how="left")
trans_data['week_end_dt']=trans_data['week_end_dt'].apply(lambda x: datetime.datetime.strptime(x,"%Y-%m-%d").date())

transaction_sales_data=pd.merge(sales_all_by_store,trans_data,on=['location_id','week_end_dt'],how="left")


# In[ ]:

transaction_sales_data['subclass_gross_sales_amt'].apply(lambda x: type(x)).unique()


# In[ ]:

transaction_sales_data_dma=transaction_sales_data.groupby(['cleaned dma','week_end_dt'])[['subclass_gross_sales_amt','gross_transaction_cnt']].sum().reset_index()

transaction_sales_data_dma=transaction_sales_data_dma[(transaction_sales_data_dma['week_end_dt']>=datetime.date(2016,10,8)) &                                                     (transaction_sales_data_dma['week_end_dt']<=datetime.date(2018,8,4))]
transaction_sales_data_dma=transaction_sales_data_dma.rename(columns={"subclass_gross_sales_amt":"sales",'gross_transaction_cnt':"trans"})
transaction_sales_data_dma['week date']=transaction_sales_data_dma['week_end_dt'].apply(lambda x: x-datetime.timedelta(days=6))

del transaction_sales_data_dma['week_end_dt']
transaction_sales_data_national=transaction_sales_data_dma.groupby(['week date'])['sales','trans'].sum().reset_index()


# In[ ]:

store_counts_dma=sales_all_by_store[sales_all_by_store['subclass_gross_sales_amt']>0]
store_counts_dma=store_counts_dma.groupby(['cleaned dma','week_end_dt'])['location_id'].count().to_frame().reset_index()
store_counts_dma=store_counts_dma.rename(columns={"location_id":"store_count"})
store_counts_dma['week date']=store_counts_dma['week_end_dt'].apply(lambda x: x-datetime.timedelta(days=6))


store_counts_national=store_counts_dma.groupby(['week_end_dt'])['store_count'].sum().to_frame().reset_index()

store_counts_dma['week date']=store_counts_dma['week_end_dt'].apply(lambda x: x-datetime.timedelta(days=6))
store_counts_national['week date']=store_counts_national['week_end_dt'].apply(lambda x: x-datetime.timedelta(days=6))

del store_counts_dma['week_end_dt']
del store_counts_national['week_end_dt']


# In[ ]:

MMM_national_media=pd.merge(data_media_national_wide,store_counts_national,on="week date",how="left")
MMM_national_media=pd.merge(MMM_national_media,transaction_sales_data_national,on="week date",how="left")

MMM_dma_media=pd.merge(data_media_dma_wide,store_counts_dma,on=["week date","cleaned dma"],how="left")
MMM_dma_media=pd.merge(MMM_dma_media,transaction_sales_data_dma,on=["week date","cleaned dma"],how="left")

MMM_national_sub_media=pd.merge(data_submedia_national_wide,store_counts_national,on="week date",how="left")
MMM_national_sub_media=pd.merge(MMM_national_sub_media,transaction_sales_data_national,on="week date",how="left")

MMM_dma_sub_media=pd.merge(data_submedia_dma_wide,store_counts_dma,on=["week date","cleaned dma"],how="left")
MMM_dma_sub_media=pd.merge(MMM_dma_sub_media,transaction_sales_data_dma,on=["week date","cleaned dma"],how="left")


# In[ ]:

MMM_national_media=MMM_national_media.sort_values(['week date'])
MMM_dma_media=MMM_dma_media.sort_values(['week date','cleaned dma'])
MMM_national_sub_media=MMM_national_sub_media.sort_values(['week date'])
MMM_dma_sub_media=MMM_dma_sub_media.sort_values(['week date','cleaned dma'])

def order_columns(df):
    iv_list=[col for col in df.columns.tolist() if "_" in col]
    dv_list=[col for col in df.columns.tolist() if "_" not in col]
    df=df[dv_list+iv_list]
    return df
MMM_national_media=order_columns(MMM_national_media)
MMM_dma_media=order_columns(MMM_dma_media)
MMM_dma_media=MMM_dma_media[~pd.isnull(MMM_dma_media['sales'])]
MMM_national_sub_media=order_columns(MMM_national_sub_media)
MMM_dma_sub_media=order_columns(MMM_dma_sub_media)
MMM_dma_sub_media=MMM_dma_sub_media[~pd.isnull(MMM_dma_sub_media['sales'])]

'''
MMM_national_media.to_csv(writer_folder+"BL_MMM_national_media_JL_"+today_str+".csv",index=False)
MMM_dma_media.to_csv(writer_folder+"BL_MMM_dma_media_JL_"+today_str+".csv",index=False)
MMM_national_sub_media.to_csv(writer_folder+"BL_MMM_national_sub_media_JL_"+today_str+".csv",index=False)
MMM_dma_sub_media.to_csv(writer_folder+"BL_MMM_dma_sub_media_JL_"+today_str+".csv",index=False)
'''


# In[ ]:

ecommerce_trans_by_week=ecommerce_trans[['week_end_dt','gross_transaction_cnt']].rename(columns={"gross_transaction_cnt":"e_transaction"})
ecommerce_trans_by_week['week_end_dt']=ecommerce_trans_by_week['week_end_dt'].apply(lambda x: datetime.datetime.strptime(x,"%Y-%m-%d").date())
ecommerce_trans_by_week['week date']=ecommerce_trans_by_week['week_end_dt'].apply(lambda x: x-datetime.timedelta(days=6))
ecommerce_trans_by_week=ecommerce_trans_by_week[['week date','e_transaction']]



# In[ ]:

ecommerce_sales_by_week=ecommerce_sales.groupby(['week_end_dt'])['subclass_gross_sales_amt'].sum().to_frame().reset_index()
ecommerce_sales_by_week=ecommerce_sales_by_week.rename(columns={"subclass_gross_sales_amt":"e_sales"})
ecommerce_sales_by_week['week date']=ecommerce_sales_by_week['week_end_dt'].apply(lambda x: x-datetime.timedelta(days=6))
ecommerce_sales_by_week=ecommerce_sales_by_week[['week date','e_sales']]
ecommerce_sales_by_week.head(2)


# In[ ]:

ecommerce_trans_by_week.tail(2)


# In[ ]:

MMM_national_media=pd.merge(MMM_national_media,ecommerce_sales_by_week,on="week date",how="left")
MMM_national_media=pd.merge(MMM_national_media,ecommerce_trans_by_week,on="week date",how="left")

MMM_national_sub_media=pd.merge(MMM_national_sub_media,ecommerce_sales_by_week,on="week date",how="left")
MMM_national_sub_media=pd.merge(MMM_national_sub_media,ecommerce_trans_by_week,on="week date",how="left")


# In[ ]:

Binary_List_1=[datetime.date(2016,12,18),datetime.date(2017,12,17)]

Binary_List_2_16=[datetime.date(2016,11,20)+datetime.timedelta(days=x*7) for x in range(5)]
Binary_List_2_17=[datetime.date(2017,11,19)+datetime.timedelta(days=x*7) for x in range(5)]
Binary_List_2=Binary_List_2_16+Binary_List_2_17

MMM_national_media['Holiday_1_week_only_Ind']=np.where(np.isin(MMM_national_media['week date'],Binary_List_1),1,0)
MMM_national_media['Holiday_5_weeks_Indicator']=np.where(np.isin(MMM_national_media['week date'],Binary_List_2),1,0)

MMM_dma_media['Holiday_1_week_only_Ind']=np.where(np.isin(MMM_dma_media['week date'],Binary_List_1),1,0)
MMM_dma_media['Holiday_5_weeks_Indicator']=np.where(np.isin(MMM_dma_media['week date'],Binary_List_2),1,0)

MMM_national_sub_media['Holiday_1_week_only_Ind']=np.where(np.isin(MMM_national_sub_media['week date'],Binary_List_1),1,0)
MMM_national_sub_media['Holiday_5_weeks_Indicator']=np.where(np.isin(MMM_national_sub_media['week date'],Binary_List_2),1,0)

MMM_dma_sub_media['Holiday_1_week_only_Ind']=np.where(np.isin(MMM_dma_sub_media['week date'],Binary_List_1),1,0)
MMM_dma_sub_media['Holiday_5_weeks_Indicator']=np.where(np.isin(MMM_dma_sub_media['week date'],Binary_List_2),1,0)


# In[ ]:

MMM_dma_sub_media.head(2)


# In[ ]:

# Add the sales 20% dummy variables
Rewards_Promotion_list=[datetime.date(2016,10,1),datetime.date(2016,10,2),
                  datetime.date(2017,1,21),datetime.date(2017,1,22),
                  datetime.date(2017,4,1),datetime.date(2017,4,2),
                  datetime.date(2017,7,8),datetime.date(2017,7,9),
                  datetime.date(2017,9,30),datetime.date(2017,10,1),
                  datetime.date(2018,1,20),datetime.date(2018,1,21),
                  datetime.date(2018,4,7),datetime.date(2018,4,8),
                  datetime.date(2018,7,7),datetime.date(2018,7,8)]
df_Rewards_Promotion=pd.DataFrame({"Date":Rewards_Promotion_list},index=range(len(Rewards_Promotion_list)))
df_Rewards_Promotion['weekday']=df_Rewards_Promotion['Date'].apply(lambda x: x.weekday())
df_Rewards_Promotion['week date']=np.where(df_Rewards_Promotion['weekday']==6,df_Rewards_Promotion['Date'],df_Rewards_Promotion['Date']-datetime.timedelta(days=6))

del df_Rewards_Promotion['Date']

df_Rewards_Promotion_Sunday=df_Rewards_Promotion[df_Rewards_Promotion['weekday']==6]
df_Rewards_Promotion_Sunday['Sunday_rewards_ind']=1
del df_Rewards_Promotion_Sunday['weekday']
df_Rewards_Promotion_Saturday=df_Rewards_Promotion[df_Rewards_Promotion['weekday']==5]
df_Rewards_Promotion_Saturday['Saturday_rewards_ind']=1
del df_Rewards_Promotion_Saturday['weekday']


# In[ ]:




# In[ ]:

MMM_national_media=pd.merge(MMM_national_media,df_Rewards_Promotion_Saturday,on="week date",how="left")
MMM_national_media=pd.merge(MMM_national_media,df_Rewards_Promotion_Sunday,on="week date",how="left")
MMM_national_media=MMM_national_media.fillna(0)

MMM_dma_media=pd.merge(MMM_dma_media,df_Rewards_Promotion_Saturday,on="week date",how="left")
MMM_dma_media=pd.merge(MMM_dma_media,df_Rewards_Promotion_Sunday,on="week date",how="left")
MMM_dma_media=MMM_dma_media.fillna(0)

MMM_national_sub_media=pd.merge(MMM_national_sub_media,df_Rewards_Promotion_Saturday,on="week date",how="left")
MMM_national_sub_media=pd.merge(MMM_national_sub_media,df_Rewards_Promotion_Sunday,on="week date",how="left")
MMM_national_sub_media=MMM_national_sub_media.fillna(0)

MMM_dma_sub_media=pd.merge(MMM_dma_sub_media,df_Rewards_Promotion_Saturday,on="week date",how="left")
MMM_dma_sub_media=pd.merge(MMM_dma_sub_media,df_Rewards_Promotion_Sunday,on="week date",how="left")
MMM_dma_sub_media=MMM_dma_sub_media.fillna(0)


# In[ ]:




# In[ ]:

MMM_national_media.to_csv(writer_folder+"BL_MMM_national_media_JL_"+today_str+".csv",index=False)
MMM_dma_media.to_csv(writer_folder+"BL_MMM_dma_media_JL_"+today_str+".csv",index=False)
MMM_national_sub_media.to_csv(writer_folder+"BL_MMM_national_sub_media_JL_"+today_str+".csv",index=False)
MMM_dma_sub_media.to_csv(writer_folder+"BL_MMM_dma_sub_media_JL_"+today_str+".csv",index=False)


# In[ ]:

data_Joann.to_csv(writer_folder+"BL_MMM_long_JL_"+today_str+".csv",index=False)


# In[ ]:



