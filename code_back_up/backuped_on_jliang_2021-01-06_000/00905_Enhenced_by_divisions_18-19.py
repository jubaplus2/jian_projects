
# coding: utf-8

# In[1]:

import pandas as pd
import os
import numpy as np
import datetime
import glob
print(os.getcwd())
print(datetime.datetime.now())
import gc
def recursive_file_gen(root_folder):
    for root, dirs, files in os.walk(root_folder):
        for file in files:
            yield os.path.join(root, file)
            
# os.listdir(os.getcwd())


# In[2]:

df_id_defination=pd.read_csv("./BL_Loyalty_Health_2018_2019_all_ids_JL_2019-11-12.csv",
                             dtype=str,usecols=['customer_id_hashed','Active_Status','HML_Group','zip_type'])
print("df_id_defination['Active_Status'].unique()",df_id_defination['Active_Status'].unique())
print("df_id_defination['HML_Group'].unique()",df_id_defination['HML_Group'].unique())
print("df_id_defination['zip_type'].unique()",df_id_defination['zip_type'].unique())

print(df_id_defination.shape,df_id_defination['customer_id_hashed'].nunique())
df_id_defination=df_id_defination.drop_duplicates()
print(df_id_defination.shape,df_id_defination['customer_id_hashed'].nunique())


# In[3]:

df_id_defination['HML_Group']=df_id_defination['HML_Group'].fillna("nan")
df_id_defination['zip_type']=df_id_defination['zip_type'].fillna("T")


# In[4]:

df_total_id_count=df_id_defination.groupby(['Active_Status','HML_Group','zip_type'])['customer_id_hashed'].count().to_frame().reset_index()


# # Sales in 1 year by division

# In[8]:

# Hard coded below: 2018-10-01 to 2019-09-30

historical_item_files=glob.glob("/home/jian/BigLots/hist_daily_data_itemlevel_decompressed/*.txt")
historical_item_files=[x for x in historical_item_files if x.split("/MediaStormDailySalesHistory")[1][:8]>="201810"]
historical_item_files.sort()
historical_item_files


# In[9]:

daily_item_files_2019=list(recursive_file_gen("/home/jian/BigLots/2019_by_weeks/"))
daily_item_files_2019=[x for x in daily_item_files_2019 if x[-4:]==".txt" and "daily" in x.lower()]


daily_item_files_2019=[x for x in daily_item_files_2019 if x.split("eks/MediaStorm_")[1][:10]>="2019-02-16"]
daily_item_files_2019=[x for x in daily_item_files_2019 if x.split("eks/MediaStorm_")[1][:10]<="2019-10-07"]
daily_item_files_2019


# In[10]:

list_POS_item_in_1_year=historical_item_files+daily_item_files_2019
len(list_POS_item_in_1_year)


# In[11]:

mapping_division_class=pd.read_table("/home/jian/BigLots/static_files/ProductTaxonomy/MediaStormProductTaxonomy20191101-134011-956.txt",
                                    dtype=str,sep="|",usecols=['division_id','class_code_id']).drop_duplicates()
print(mapping_division_class.shape)
mapping_division_name=pd.read_table("/home/jian/BigLots/static_files/MediaStorm Data Extract - Division Names.txt",sep="|",dtype=str)
mapping_division_class=pd.merge(mapping_division_class,mapping_division_name,on="division_id",how="left")
print(mapping_division_class.shape)


# In[ ]:

# Grouped by in the loop
df_sales_in_1_year_by_division=pd.DataFrame()
i_counter=0
for file in list_POS_item_in_1_year:
    try:
        df=pd.read_table(file,dtype=str,sep="|",usecols=['customer_id_hashed','transaction_dt','transaction_id','location_id','item_transaction_amt','class_code_id'])
        df=df.rename(columns={"item_transaction_amt":"sales"})
    except:
        df=pd.read_table(file,dtype=str,sep="|",usecols=['customer_id_hashed','transaction_dt','transaction_id','location_id','subclass_transaction_amt','class_code_id'])
        df=df.rename(columns={"subclass_transaction_amt":"sales"})
        
    df=df[pd.notnull(df['customer_id_hashed'])]
    ###
    df=df[df['transaction_dt']>="2018-10-01"]
    df=df[df['transaction_dt']<="2019-09-30"]
    ###
    df=pd.merge(df,mapping_division_class,on="class_code_id")
    df['sales']=df['sales'].astype(float)
    df_sales=df.groupby(['customer_id_hashed','transaction_dt','division_id','division_desc'])['sales'].sum().to_frame().reset_index()
    
    df_trans=df[['customer_id_hashed','transaction_dt','transaction_id','location_id','division_id','division_desc']].drop_duplicates()
    df_trans['trans']=1
    df_trans=df_trans.groupby(['customer_id_hashed','transaction_dt','division_id','division_desc'])['trans'].sum().to_frame().reset_index()
    df=pd.merge(df_sales,df_trans,on=["customer_id_hashed",'transaction_dt','division_id','division_desc'],how="outer")
    print(datetime.datetime.now(),file)
    print(i_counter,df['transaction_dt'].min(),df['transaction_dt'].max())
    
    i_counter+=1
    
    df=df.groupby(["customer_id_hashed","division_id","division_desc"])['sales','trans'].sum().reset_index()
    
    df_sales_in_1_year_by_division=df_sales_in_1_year_by_division.append(df)


# In[ ]:

print(df_sales_in_1_year_by_division.shape)


# In[ ]:

gc.collect()
# So far, by each week
df_sales_in_1_year_by_division=df_sales_in_1_year_by_division.rename(columns={"sales":"sales_in_1_year"})
df_sales_in_1_year_by_division=df_sales_in_1_year_by_division.rename(columns={"trans":"trans_in_1_year"})
print(datetime.datetime.now(),df_sales_in_1_year_by_division.shape)
df_sales_in_1_year_by_division.head(2)


# In[ ]:

df_sales_in_1_year_by_division[['customer_id_hashed','division_id']].drop_duplicates().shape


# In[ ]:

# Rolling up by year
df_sales_in_1_year_by_division=df_sales_in_1_year_by_division.groupby(['customer_id_hashed','division_id','division_desc'])['sales_in_1_year','trans_in_1_year'].sum().reset_index()
print(df_sales_in_1_year_by_division.shape)


# In[ ]:

df_id_defination=pd.merge(df_id_defination,df_sales_in_1_year_by_division,on="customer_id_hashed",how="left")
df_id_defination.shape


# In[ ]:

df_id_defination['shoppers_in_1_year']=np.where(pd.isnull(df_id_defination['sales_in_1_year']),0,1)


# In[ ]:

print(df_id_defination.shape)
df_id_defination.head(2)


# In[ ]:

# df_total_id_count # defined before merging with sales


# In[ ]:

df_output=df_id_defination.groupby(["Active_Status",'HML_Group','zip_type','division_id','division_desc'])['shoppers_in_1_year','sales_in_1_year','trans_in_1_year'].sum().reset_index()
df_output.shape


# In[ ]:

df_output.to_csv("./BL_2018Audience_Performance_in_summary_19_by_division_JL_"+str(datetime.datetime.now().date())+".csv",index=False)


# In[ ]:



