
# coding: utf-8

# In[41]:

import pandas as pd
import datetime
import numpy as np
import glob
import json
import os


# In[42]:

today_str=str(datetime.datetime.now().date())
writer_folder="/home/jian/Projects/Big_Lots/TMR/To_Spencer/output/"+today_str+"/"
try:
    os.stat(writer_folder)
except:
    os.mkdir(writer_folder)


# In[43]:

National_TV_2017=pd.read_csv("/home/jian/Projects/Big_Lots/TMR/To_Spencer/finaltvlogs_0306.csv",dtype=str)

National_TV_2017['impression']=National_TV_2017['Act Impression'].astype(float)
National_TV_2017['cost']=National_TV_2017['Net Cost'].astype(float)
National_TV_2017['week_start_date']=National_TV_2017['Week BL'].apply(lambda x: datetime.datetime.strptime(x,"%m/%d/%y").date())
National_TV_2017=National_TV_2017[National_TV_2017['week_start_date']<datetime.date(2017,12,30)]
National_TV_2017=National_TV_2017[National_TV_2017['week_start_date']>=datetime.date(2016,10,2)]


# In[44]:

National_TV_2017=National_TV_2017[['week_start_date','cleaned DMA','Media Type','Network','impression','cost']]
National_TV_2017=National_TV_2017.groupby(['week_start_date','cleaned DMA','Media Type','Network'])['impression','cost'].sum().reset_index()
National_TV_2017=National_TV_2017.rename(columns={"cleaned DMA":"cleaned dma","Media Type":"submedia","Network":"placement"})
National_TV_2017['submedia']=National_TV_2017['submedia'].replace(['National Cable','DirecTV'],"National")
National_TV_2017['submedia']=National_TV_2017['submedia'].replace(['Spot Cable', 'Spot Broadcast', 'FOOTPRINT-SINCLAIR'],"Local")
National_TV_2017=National_TV_2017[National_TV_2017['submedia']=="National"]


# In[45]:

National_TV_2017['impression'].sum()


# In[46]:

data=pd.read_table("/home/jian/Projects/Big_Lots/TMR/To_Spencer/BL ALL TMR 0619 updated.dat",sep="|",dtype=str)
data=data[~pd.isnull(data['cleaned dma'])]
data['cleaned dma']=data['cleaned dma'].apply(lambda x: x.replace('Zanesville, OH\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff퓔ￔ퓔ￔ\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff','Zanesville, OH'))
data['impression']=data['impression'].replace(".","0")
data['click']=data['click'].replace(".","0")
data['cost']=data['cost'].replace(".","0")


data['impression']=data['impression'].astype(float)
data['click']=data['click'].astype(float)
data['cost']=data['cost'].astype(float)
data['date']=data['week date'].apply(lambda x: datetime.datetime.strptime(x,"%m/%d/%Y").date())


# In[47]:

data=data[data['date']<=datetime.date(2018,5,1)] # start time
data=data[data['date']>=datetime.date(2016,10,1)]

# test=data[data['date']<=datetime.date(2017,12,30)]
# test=test[test['date']>=datetime.date(2016,10,2)]
del data['date']


# In[48]:

data_media_national=data.groupby(['week date','media'])[['impression','click','cost']].sum().reset_index()

data_sub_media_national=data.groupby(['week date','media','submedia'])[['impression','click','cost']].sum().reset_index()
data_sub_media_national['submedia']=data_sub_media_national['media']+"_"+data_sub_media_national['submedia']
del data_sub_media_national['media']


# In[49]:

data_sub_media_national['week_start_date']=data_sub_media_national['week date'].apply(lambda x: datetime.datetime.strptime(x,"%m/%d/%Y").date())
data_sub_media_national_2018=data_sub_media_national[data_sub_media_national['week_start_date']>=datetime.date(2017,12,31)]
data_sub_media_national_2017=data_sub_media_national[data_sub_media_national['week_start_date']<datetime.date(2017,12,31)]
data_sub_media_national_2017_Non_NationalTV=data_sub_media_national_2017[data_sub_media_national_2017['submedia']!="TV_National"]
data_sub_media_national_2017_NationalTV=data_sub_media_national_2017[data_sub_media_national_2017['submedia']=="TV_National"]


# In[50]:

National_TV_2017['week date']=np.nan
National_TV_2017['click']=0
data_sub_media_national_2017_NationalTV=National_TV_2017[['week date','submedia','impression','click','cost','week_start_date']]
data_sub_media_national_2017_NationalTV['submedia']="TV_National"
data_sub_media_national_2017_NationalTV['week date']=data_sub_media_national_2017_NationalTV['week_start_date'].apply(lambda x: str(x)[5:7]+"/"+str(x)[8:10]+"/"+str(x)[0:4])
data_sub_media_national_2017_NationalTV=data_sub_media_national_2017_NationalTV.groupby(['week date','submedia','week_start_date'])['impression','click','cost'].sum().reset_index()




# In[ ]:




# In[51]:

data_sub_media_national=data_sub_media_national_2017_Non_NationalTV.append(data_sub_media_national_2017_NationalTV).append(data_sub_media_national_2018)
data_sub_media_national=data_sub_media_national.reset_index()
del data_sub_media_national['index']


# In[52]:

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


# In[53]:

data_media_national_wide=pivot_level_media('media',data_media_national)
data_submedia_national_wide=pivot_level_media('submedia',data_sub_media_national)


# In[13]:

data_media_dma=data.groupby(['week date','media','cleaned dma'])[['impression','click','cost']].sum().reset_index()
data_media_dma=data_media_dma[data_media_dma['cleaned dma']!="National"]
data_media_dma=data_media_dma[data_media_dma['cleaned dma']!="xx"]
data_sub_media_dma=data.groupby(['week date','media','submedia','cleaned dma'])[['impression','click','cost']].sum().reset_index()
data_sub_media_dma['submedia']=data_sub_media_dma['media']+"_"+data_sub_media_dma['submedia']
del data_sub_media_dma['media']
data_sub_media_dma=data_sub_media_dma[data_sub_media_dma['cleaned dma']!="National"]
data_sub_media_dma=data_sub_media_dma[data_sub_media_dma['cleaned dma']!="xx"]

data_media_dma['week date']=data_media_dma['week date']+"|"+data_media_dma['cleaned dma']
del data_media_dma['cleaned dma']

data_sub_media_dma['week date']=data_sub_media_dma['week date']+"|"+data_sub_media_dma['cleaned dma']
del data_sub_media_dma['cleaned dma']


# In[14]:

data_media_dma_wide=pivot_level_media('media',data_media_dma)
data_media_dma_wide['cleaned dma']=data_media_dma_wide['week date'].apply(lambda x: x.split("|")[1])
data_media_dma_wide['week date']=data_media_dma_wide['week date'].apply(lambda x: x.split("|")[0])

data_submedia_dma_wide=pivot_level_media('submedia',data_sub_media_dma)
data_submedia_dma_wide['cleaned dma']=data_submedia_dma_wide['week date'].apply(lambda x: x.split("|")[1])
data_submedia_dma_wide['week date']=data_submedia_dma_wide['week date'].apply(lambda x: x.split("|")[0])


# In[15]:

data_media_national_wide['week date']=data_media_national_wide['week date'].apply(lambda x:datetime.datetime.strptime(x,"%m/%d/%Y").date())
data_submedia_national_wide['week date']=data_submedia_national_wide['week date'].apply(lambda x:datetime.datetime.strptime(x,"%m/%d/%Y").date())

data_media_dma_wide['week date']=data_media_dma_wide['week date'].apply(lambda x:datetime.datetime.strptime(x,"%m/%d/%Y").date())
data_submedia_dma_wide['week date']=data_submedia_dma_wide['week date'].apply(lambda x:datetime.datetime.strptime(x,"%m/%d/%Y").date())


# # Sales & Transaction

# In[16]:

folder_file_pattern=glob.glob("/home/jian/Projects/Big_Lots/Sales_All/Sales_Data/*.txt")
sales_all=pd.DataFrame()
trans_all=pd.DataFrame()

for file in folder_file_pattern:
    df=pd.read_csv(file,sep="|",dtype=str,na_values="?")

    trans_all=trans_all.append(df)
    if "class_code_id" in df.columns:


        df['class_gross_sales_amt']=df['class_gross_sales_amt'].astype(float)
        df['class_gross_sales_amt']=df['class_gross_sales_amt'].fillna(0.0)
        df=df[['location_id','week_end_dt','class_code_id','class_gross_sales_amt']].drop_duplicates()
        df.reset_index(inplace=True)
        try:
            df['week_end_dt']=df['week_end_dt'].apply(lambda x: datetime.datetime.strptime(x,"%Y-%m-%d").date())
            
        except:
            print(file,"Date Error")

    
    else:
        print(file,"Error: class_code_id not in columns")
        
    sales_all=sales_all.append(df)
    
sales_all=sales_all.drop_duplicates()
ecommerce_sales=sales_all[sales_all['location_id']=="6990"]
sales_all=sales_all[sales_all['location_id']!="6990"]
sales_all=sales_all[sales_all['location_id']!="145"]


# In[17]:

trans_all=trans_all[['location_id','week_end_dt','gross_transaction_cnt']].drop_duplicates()
trans_all['gross_transaction_cnt']=trans_all['gross_transaction_cnt'].astype(int)

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
trans_data['week_end_dt']=trans_data['week_end_dt'].apply(lambda x: datetime.datetime.strptime(x,"%Y-%m-%d").date())

ecommerce_trans=trans_data[trans_data['location_id']=="6990"]
trans_data=trans_data[trans_data['location_id']!="6990"]
trans_data=trans_data[trans_data['location_id']!="145"]


# In[18]:

store_DMA=pd.read_excel("/home/jian/Projects/Big_Lots/Other_Input/all_store_DMA_20180620.xlsx",dtype=str)
store_DMA=store_DMA[['location_id','cleaned_dma']].rename(columns={"cleaned_dma":"cleaned dma"})
# dma_clean=pd.read_excel("/home/jian/Projects/Big_Lots/Other_Input/DMA cleaning.xlsx",dtype=str)


# In[19]:

sales_all_by_store=sales_all.groupby(['location_id','week_end_dt'])['class_gross_sales_amt'].sum().to_frame().reset_index()
sales_all_by_store=pd.merge(sales_all_by_store,store_DMA,on="location_id",how="left")
transaction_sales_data=pd.merge(sales_all_by_store,trans_data,on=['location_id','week_end_dt'],how="left")

transaction_sales_data_dma=transaction_sales_data.groupby(['cleaned dma','week_end_dt'])[['class_gross_sales_amt','gross_transaction_cnt']].sum().reset_index()

transaction_sales_data_dma=transaction_sales_data_dma[(transaction_sales_data_dma['week_end_dt']>=datetime.date(2016,10,8)) &                                                     (transaction_sales_data_dma['week_end_dt']<=datetime.date(2018,5,6))]
transaction_sales_data_dma=transaction_sales_data_dma.rename(columns={"class_gross_sales_amt":"sales",'gross_transaction_cnt':"trans"})
transaction_sales_data_dma['week date']=transaction_sales_data_dma['week_end_dt'].apply(lambda x: x-datetime.timedelta(days=6))

del transaction_sales_data_dma['week_end_dt']
transaction_sales_data_national=transaction_sales_data_dma.groupby(['week date'])['sales','trans'].sum().reset_index()



# In[20]:

store_counts_dma=sales_all_by_store[sales_all_by_store['class_gross_sales_amt']>0]
store_counts_dma=store_counts_dma.groupby(['cleaned dma','week_end_dt'])['location_id'].count().to_frame().reset_index()
store_counts_dma=store_counts_dma.rename(columns={"location_id":"store_count"})
store_counts_dma['week date']=store_counts_dma['week_end_dt'].apply(lambda x: x-datetime.timedelta(days=6))


store_counts_national=store_counts_dma.groupby(['week_end_dt'])['store_count'].sum().to_frame().reset_index()

store_counts_dma['week date']=store_counts_dma['week_end_dt'].apply(lambda x: x-datetime.timedelta(days=6))
store_counts_national['week date']=store_counts_national['week_end_dt'].apply(lambda x: x-datetime.timedelta(days=6))

del store_counts_dma['week_end_dt']
del store_counts_national['week_end_dt']


# In[21]:

MMM_national_media=pd.merge(data_media_national_wide,store_counts_national,on="week date",how="left")
MMM_national_media=pd.merge(MMM_national_media,transaction_sales_data_national,on="week date",how="left")

MMM_dma_media=pd.merge(data_media_dma_wide,store_counts_dma,on=["week date","cleaned dma"],how="left")
MMM_dma_media=pd.merge(MMM_dma_media,transaction_sales_data_dma,on=["week date","cleaned dma"],how="left")

MMM_national_sub_media=pd.merge(data_submedia_national_wide,store_counts_national,on="week date",how="left")
MMM_national_sub_media=pd.merge(MMM_national_sub_media,transaction_sales_data_national,on="week date",how="left")

MMM_dma_sub_media=pd.merge(data_submedia_dma_wide,store_counts_dma,on=["week date","cleaned dma"],how="left")
MMM_dma_sub_media=pd.merge(MMM_dma_sub_media,transaction_sales_data_dma,on=["week date","cleaned dma"],how="left")


# In[22]:

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




# In[23]:

MMM_national_media['week date'].apply(lambda x: type(x)).unique()


# In[24]:

ecommerce_trans_by_week=ecommerce_trans[['week_end_dt','gross_transaction_cnt']].rename(columns={"gross_transaction_cnt":"e_transaction"})
ecommerce_trans_by_week['week date']=ecommerce_trans_by_week['week_end_dt'].apply(lambda x: x-datetime.timedelta(days=6))
ecommerce_trans_by_week=ecommerce_trans_by_week[['week date','e_transaction']]



# In[25]:

ecommerce_sales_by_week=ecommerce_sales.groupby(['week_end_dt'])['class_gross_sales_amt'].sum().to_frame().reset_index()
ecommerce_sales_by_week=ecommerce_sales_by_week.rename(columns={"class_gross_sales_amt":"e_sales"})
ecommerce_sales_by_week['week date']=ecommerce_sales_by_week['week_end_dt'].apply(lambda x: x-datetime.timedelta(days=6))
ecommerce_sales_by_week=ecommerce_sales_by_week[['week date','e_sales']]
ecommerce_sales_by_week.head(2)


# In[26]:

MMM_national_media=pd.merge(MMM_national_media,ecommerce_sales_by_week,on="week date",how="left")
MMM_national_media=pd.merge(MMM_national_media,ecommerce_trans_by_week,on="week date",how="left")

MMM_national_sub_media=pd.merge(MMM_national_sub_media,ecommerce_sales_by_week,on="week date",how="left")
MMM_national_sub_media=pd.merge(MMM_national_sub_media,ecommerce_trans_by_week,on="week date",how="left")


# # Weather

# In[27]:

folder="/home/jian/Projects/Big_Lots/Weather/Json_data/daily/"
json_list_all=glob.glob(folder+"*.json")

df_files=pd.DataFrame({"file":json_list_all},index=range(len(json_list_all)))
df_files['Date']=df_files['file'].apply(lambda x: datetime.datetime.strptime(x[len(x)-15:len(x)-5],"%Y-%m-%d").date())
df_files=df_files[(df_files['Date']>=datetime.date(2017,12,31)) & (df_files['Date']<=datetime.date(2018,5,5))]


# In[28]:

store_zip=pd.read_excel("/home/jian/Projects/Big_Lots/Other_Input/all_store_DMA_20180620.xlsx",dtype=str)
store_zip=store_zip[['location_id','zip_cd']].drop_duplicates()
store_zip=store_zip[store_zip['zip_cd']!='nan']

trans_all_for_weather=trans_all[trans_all['gross_transaction_cnt']>0]
trans_all_for_weather['week_end_dt']=trans_all_for_weather['week_end_dt'].apply(lambda x: datetime.datetime.strptime(x,"%Y-%m-%d").date())
trans_all_for_weather=trans_all_for_weather[(trans_all_for_weather['week_end_dt']>=datetime.date(2018,1,1)) &                                           (trans_all_for_weather['week_end_dt']<=datetime.date(2018,5,6))]
store_zip=store_zip[store_zip['location_id'].isin(trans_all_for_weather['location_id'])]


# In[ ]:

all_weather_groups=[]

df_missing_weather=pd.DataFrame()
for file in df_files['file']:
    response=json.load(open(file,"r"))
    for zip_cd in store_zip['zip_cd'].unique().tolist():
        try:        
            data_zip_weather=response[zip_cd]['weather']
            for i in range(len(data_zip_weather)):            
                weather_group=data_zip_weather[i]['main']                
                all_weather_groups=list(set(all_weather_groups+[weather_group]))
                
        except:
            df_missing_weather=df_missing_weather.append(pd.DataFrame({"Date":file[len(file)-15:len(file)-5],"zips":zip_cd},index=[0]))
            # print("zip data not available: " + zip_cd+" | "+file[len(file)-15:len(file)-5])


# In[ ]:

k_count=0
df_output_group=pd.DataFrame()
for file in df_files['file']:
    date=datetime.datetime.strptime(file[len(file)-15:len(file)-5],"%Y-%m-%d").date()
    response=json.load(open(file,"r"))
        
    for zip_cd in store_zip['zip_cd'].unique().tolist():

        df_group=pd.DataFrame(columns=["zip_cd","Date","Collected"]+all_weather_groups,index=[0])

        df_group['zip_cd']=zip_cd
        df_group['Date']=date
        
        if zip_cd in list(response.keys()):
            df_group["Collected"]="Recorded"

            for w_group in all_weather_groups:
                for j in range(len(response[zip_cd]['weather'])):
                    if response[zip_cd]['weather'][j]['main']==w_group:
                        df_group[w_group]=1
        else:
            df_group["Collected"]="Not_recorded"

        df_output_group=df_output_group.append(df_group)
    k_count=k_count+1
    if k_count % 10==3:
        print(k_count, datetime.datetime.now(),"finished of the date: ",date)


# In[ ]:

group_weight=pd.read_excel("/home/jian/Projects/Big_Lots/Weather/Q1_Weather_Counts/Q1_inclusion_store_all_weather_type_ranked.xlsx",sheetname="all_weather_group_list")
group_weight['Severity']=group_weight['Severity'].astype(int)
group_weight=group_weight[['all_type_group','Severity']]


# In[ ]:

all_store_zip=sales_all[sales_all['class_gross_sales_amt']>0][['location_id','week_end_dt']].drop_duplicates()
all_store_zip=all_store_zip[(all_store_zip['week_end_dt']>=datetime.date(2018,2,3)) & (all_store_zip['week_end_dt']<=datetime.date(2018,5,5))]
all_store_zip=pd.merge(all_store_zip,store_zip,on="location_id",how="left")
all_store_zip=all_store_zip[['location_id','zip_cd']].drop_duplicates()


# In[ ]:

store_DMA=pd.read_excel("/home/jian/Projects/Big_Lots/Other_Input/all_store_DMA_20180620.xlsx",dtype=str)
store_DMA=store_DMA[['location_id','cleaned_dma']]
store_DMA=store_DMA.rename(columns={"cleaned_dma":"cleaned dma"})
all_store_zip_dma=pd.merge(all_store_zip,store_DMA,on='location_id',how="left")


# In[ ]:

df_output_group_store=pd.merge(all_store_zip,df_output_group,on="zip_cd",how="left")


# In[ ]:

group_weight_dict=group_weight[['all_type_group', 'Severity']].set_index('all_type_group').T.to_dict()
for col in list(group_weight_dict.keys()):
    df_output_group_store[col]=df_output_group_store[col]*group_weight_dict[col]['Severity']
df_output_group_store['Severity']=df_output_group_store[list(group_weight_dict.keys())].max(axis=1)


# In[ ]:

traffic_folder="/home/jian/Projects/Big_Lots/Weather/Q1_Weather_Counts/Daily Traffic/"
traffic_files=glob.glob(traffic_folder+"*.txt")

traffic_Q1=pd.DataFrame()

for file in traffic_files:
    df=pd.read_csv(file,sep="|",dtype=str)
    df['week_end_dt']=df['week_end_dt'].apply(lambda x: datetime.datetime.strptime(x,"%Y-%m-%d").date())
    df['weekly_traffic']=0
    col_date_list=[]
    for i in range(1,8):
        day_num=str(i)
        col=[x for x in df.columns.tolist() if day_num in x][0]
        df[col]=df[col].astype(float)
        df['weekly_traffic']=df['weekly_traffic']+df[col]
        df_app=df[["location_id","week_end_dt"]+[col]]
        df_app['weekday']=col
        df_app.columns=["location_id","week_end_dt","Traffic","weekday"]
        df_app['Date']=df_app['week_end_dt'].unique()[0]-datetime.timedelta(days=(7-i))
        traffic_Q1=traffic_Q1.append(df_app)
traffic_Q1=traffic_Q1[traffic_Q1['Traffic']>0]
df_date_weekday=traffic_Q1[['Date','weekday']].drop_duplicates()
traffic_Q1=traffic_Q1[traffic_Q1['location_id'].isin(store_zip['location_id'])]


# In[ ]:

df_weight_weekday_store=traffic_Q1.groupby(['location_id','weekday'])['Traffic'].mean().to_frame().reset_index()
df_weight_store=df_weight_weekday_store.groupby(['location_id'])['Traffic'].sum().to_frame().reset_index()
df_weight_store=df_weight_store.rename(columns={"Traffic":"weekly_traffic"})
df_weight_weekday_store=pd.merge(df_weight_weekday_store,df_weight_store,on="location_id",how="left")
df_weight_weekday_store['weight']=df_weight_weekday_store['Traffic']/df_weight_weekday_store['weekly_traffic']
df_weight_weekday_store['weekday_py']=np.where(df_weight_weekday_store['weekday']=="traffic_day_1",6,
                                               df_weight_weekday_store['weekday'].apply(lambda x: int(x[len(x)-1:])-2)                                              
                                              )
overall_weekday=traffic_Q1.groupby(['weekday'])['Traffic'].mean().to_frame().reset_index()
overall_weekday['weight']=overall_weekday['Traffic'].apply(lambda x: x/overall_weekday['Traffic'].sum())


overall_weekday['weekday_py']=np.where(overall_weekday['weekday']=="traffic_day_1",6,
                                               overall_weekday['weekday'].apply(lambda x: int(x[len(x)-1:])-2)                                              
                                              )



# In[ ]:

def weather_desc_by_day_table(df_input,desc_list):
    df_snow_desc_long=pd.DataFrame()
    for col in desc_list:
        df=df_input[['location_id','zip_cd','Date','Collected','Severity']+[col]]
        df=df.rename(columns={col:"nan_index"})
        df['desc']=col
        df_snow_desc_long=df_snow_desc_long.append(df)
    df_snow_desc_long_Not_Record=df_snow_desc_long[df_snow_desc_long['Collected']=="Not_recorded"][['location_id','zip_cd','Date']].drop_duplicates()
    df_snow_desc_long_no_nan=df_snow_desc_long[~pd.isnull(df_snow_desc_long['nan_index'])]

    df_snow_desc_long_Not_Record['key']=df_snow_desc_long_Not_Record['location_id']+"|"+df_snow_desc_long_Not_Record['Date'].apply(lambda x: str(x-datetime.timedelta(days=1)))
    df_snow_desc_long_Not_Record_Key=df_snow_desc_long_no_nan.copy()
    df_snow_desc_long_Not_Record_Key['key']=df_snow_desc_long_Not_Record_Key['location_id']+"|"+df_snow_desc_long_Not_Record_Key['Date'].apply(lambda x: str(x))
    df_snow_desc_long_Not_Record_Fill=df_snow_desc_long_Not_Record_Key[df_snow_desc_long_Not_Record_Key['key'].isin(df_snow_desc_long_Not_Record['key'])]
    del df_snow_desc_long_Not_Record_Fill['key']
    df_snow_desc_long_Not_Record_Fill['Date']=df_snow_desc_long_Not_Record_Fill['Date'].apply(lambda x: x+datetime.timedelta(days=1))

    df_snow_desc_long=df_snow_desc_long_no_nan.append(df_snow_desc_long_Not_Record_Fill)
    df_snow_desc_long['weekday_py']=df_snow_desc_long['Date'].apply(lambda x: x.weekday())
    
    df_snow_desc_long_in=pd.merge(df_snow_desc_long,df_weight_weekday_store[['location_id','weekday_py','weight']],
                                  on=['location_id','weekday_py'],how="left")
    df_snow_desc_long_out=df_snow_desc_long_in[pd.isnull(df_snow_desc_long_in['weight'])]
    # 4012 only
    df_snow_desc_long_in=df_snow_desc_long_in[~pd.isnull(df_snow_desc_long_in['weight'])]
    del df_snow_desc_long_out['weight']
    df_snow_desc_long_out=pd.merge(df_snow_desc_long_out,overall_weekday[['weekday_py','weight']],
                                  on=['weekday_py'],how="left")
    
    result=df_snow_desc_long_in.append(df_snow_desc_long_out)
    result['weighted_severity']=result['Severity']*result['weight']
    del result['nan_index']
    del result['weekday_py']
    return result

desc_list_group=all_weather_groups
df_weather_group_long=weather_desc_by_day_table(df_output_group_store,desc_list_group)


# In[ ]:

weather_group_rank=pd.read_excel("/home/jian/Projects/Big_Lots/Weather/Q1_Weather_Counts/weather_desc_drop_dup_rank.xlsx",sheetname="weather_group_rank")
df_weather_group_long_dedup=pd.merge(df_weather_group_long,weather_group_rank,on="desc",how='left')
df_weather_group_long_dedup=df_weather_group_long_dedup.sort_values('rank',ascending=False).drop_duplicates(['location_id','Date'])
df_weather_group_long_dedup['weight']=df_weather_group_long_dedup['weight'].astype(float)
df_weather_group_long_dedup['weighted_severity']=df_weather_group_long_dedup['weighted_severity'].astype(float)
df_date_week_end=traffic_Q1[['week_end_dt','Date']].drop_duplicates()
weather_week_severity=pd.merge(df_weather_group_long_dedup,df_date_week_end,on="Date",how='left')
weather_week_severity=weather_week_severity[weather_week_severity['Collected']=="Recorded"]


# In[ ]:

weekday_count=weather_week_severity.groupby(['week_end_dt','location_id'])['Date'].count().to_frame().reset_index()

weekly_weather=weather_week_severity.groupby(['location_id','week_end_dt'])['weighted_severity'].sum().to_frame().reset_index()
weekly_weather=pd.merge(weekly_weather,weekday_count,on=['location_id','week_end_dt'],how="left")
weekly_weather['weekly_weigher_severity']=weekly_weather['weighted_severity']/weekly_weather['Date']*7
weekly_weather=pd.merge(weekly_weather,all_store_zip_dma,on='location_id',how="left")
weekly_weather_DMA=weekly_weather.groupby(['week_end_dt','cleaned dma'])['weekly_weigher_severity'].mean().to_frame().reset_index()
weekly_weather_National=weekly_weather.groupby(['week_end_dt'])['weekly_weigher_severity'].mean().to_frame().reset_index()

weekly_weather_National['week date']=weekly_weather_National['week_end_dt'].apply(lambda x: x-datetime.timedelta(days=6))
weekly_weather_DMA['week date']=weekly_weather_DMA['week_end_dt'].apply(lambda x: x-datetime.timedelta(days=6))
del weekly_weather_National['week_end_dt']
del weekly_weather_DMA['week_end_dt']



# In[ ]:

MMM_national_media=pd.merge(MMM_national_media,weekly_weather_National,on=['week date'],how="left")
MMM_dma_media=pd.merge(MMM_dma_media,weekly_weather_DMA,on=['week date','cleaned dma'],how="left")
MMM_national_sub_media=pd.merge(MMM_national_sub_media,weekly_weather_National,on=['week date'],how="left")
MMM_dma_sub_media=pd.merge(MMM_dma_sub_media,weekly_weather_DMA,on=['week date','cleaned dma'],how="left")
'''
MMM_national_media.to_csv(writer_folder+"BL_MMM_national_media_JL_"+today_str+".csv",index=False)
MMM_dma_media.to_csv(writer_folder+"BL_MMM_dma_media_JL_"+today_str+".csv",index=False)
MMM_national_sub_media.to_csv(writer_folder+"BL_MMM_national_sub_media_JL_"+today_str+".csv",index=False)
MMM_dma_sub_media.to_csv(writer_folder+"BL_MMM_dma_sub_media_JL_"+today_str+".csv",index=False)
'''


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

MMM_national_media.to_csv(writer_folder+"BL_MMM_national_media_JL_"+today_str+".csv",index=False)
MMM_dma_media.to_csv(writer_folder+"BL_MMM_dma_media_JL_"+today_str+".csv",index=False)
MMM_national_sub_media.to_csv(writer_folder+"BL_MMM_national_sub_media_JL_"+today_str+".csv",index=False)
MMM_dma_sub_media.to_csv(writer_folder+"BL_MMM_dma_sub_media_JL_"+today_str+".csv",index=False)


# In[ ]:




# In[ ]:



