
# coding: utf-8

# In[1]:

import pandas as pd
import numpy as np
import datetime
import os
import logging
import gc
from multiprocessing import Pool
from functools import partial

price_threshold=10


# In[2]:

# last_saturday=datetime.datetime.now().date()-datetime.timedelta(days=(datetime.datetime.now().date().weekday()+2))
# print(last_saturday)
last_saturday=datetime.date(2019,2,2) # To be changed to the running Tuesday
logging.basicConfig(filename='V2_Multiprocessing_'+str(last_saturday)+'.log', level=logging.INFO)


# In[3]:

output_folder="/home/jian/celery/DBasket/output/"

output_folder=output_folder+str(last_saturday)+"/"

try:
    os.stat(output_folder)
except:
    os.mkdir(output_folder)


# In[4]:

def recursive_file_gen(my_root_dir):
    for root, dirs, files in os.walk(my_root_dir):
        for file in files:
            yield os.path.join(root, file)
            
most_recent_daily_data=list(recursive_file_gen("/home/jian/BigLots/"))
most_recent_daily_data=[x for x in most_recent_daily_data if ("MediaStormDailySales" in x) and (str(last_saturday) in x)]

if len(most_recent_daily_data)==1:
    most_recent_daily_data=most_recent_daily_data[0]
else:
    most_recent_daily_data=np.nan
    logging.info("Last Weekly Daily Data Error", str(datetime.datetime.now()))


# In[5]:

most_recent_daily_data


# In[6]:

data=pd.read_table(most_recent_daily_data,dtype=str,sep="|")
print("len_sub_class_id:",data['subclass_id'].apply(lambda x: len(x)).unique())
print("len_class_code_id:",data['class_code_id'].apply(lambda x: len(x)).unique())
data['subclass_id']=data['subclass_id'].apply(lambda x: x.zfill(3))
data['product_comb']=data['class_code_id']+"-"+data['subclass_id']

data['subclass_transaction_amt']=data['subclass_transaction_amt'].astype(float)
data['subclass_transaction_units']=data['subclass_transaction_units'].astype(int)
data=data[(data['subclass_transaction_amt']>0) & (data['subclass_transaction_units']>0)]

data['price']=data['subclass_transaction_amt']/data['subclass_transaction_units']


# In[7]:

taxonomy=pd.read_csv("/home/jian/BigLots/static_files/ProductTaxonomy/MediaStormProductTaxonomy20190201-133832-059.txt",dtype=str,sep="|")
taxonomy['subclass_id']=taxonomy['subclass_id'].apply(lambda x: x.zfill(3))
division_id_id_name=pd.read_table("/home/jian/BigLots/static_files/MediaStorm Data Extract - Division Names.txt",dtype=str,sep="|")
department_id_name=pd.read_table("/home/jian/BigLots/static_files/MediaStorm Data Extract - Department Names.txt",dtype=str,sep="|")
class_id_name=pd.read_table("/home/jian/BigLots/static_files/MediaStorm Data Extract - Class Names.txt",dtype=str,sep="|",encoding ='ISO-8859-1')
# 

data_item_avg_price=data[['product_comb','price']].groupby(['product_comb'])['price'].mean().to_frame().reset_index()
data_item_avg_price=data_item_avg_price.rename(columns={"price":"avg_price"})

data_item_avg_price['class_code_id']=data_item_avg_price['product_comb'].apply(lambda x: x.split("-")[0])
data_item_avg_price['subclass_id']=data_item_avg_price['product_comb'].apply(lambda x: x.split("-")[1])

data_item_avg_price=pd.merge(data_item_avg_price,taxonomy,on=['class_code_id','subclass_id'],how="left")


data_item_avg_price=pd.merge(data_item_avg_price,division_id_id_name,on="division_id",how="left")
data_item_avg_price=pd.merge(data_item_avg_price,department_id_name,on="department_id",how="left")
data_item_avg_price=pd.merge(data_item_avg_price,class_id_name,on="class_code_id",how="left")
data_item_avg_price=data_item_avg_price[['product_comb','avg_price','division_id','division_desc','department_id','department_desc',
                                         'class_code_id','class_code_desc','subclass_id','subclass_desc']]

data_item_avg_price.to_csv(output_folder+"/Price_"+str(last_saturday)+".csv",index=False)


# In[8]:

# $10 of all items as in the email on 2019-01-14

product_comb_under_10_set=set(data_item_avg_price[data_item_avg_price['avg_price']<price_threshold]['product_comb'].unique().tolist())
product_comb_10_and_above_list=data_item_avg_price[data_item_avg_price['avg_price']>=10]['product_comb'].unique().tolist()
product_comb_10_and_above_df=data_item_avg_price.sort_values('avg_price',ascending=False)
product_comb_10_and_above_df=product_comb_10_and_above_df[product_comb_10_and_above_df['avg_price']>=10].reset_index()
del product_comb_10_and_above_df['index']

print(data.shape)
data=data[~data['product_comb'].isin(product_comb_under_10_set)]
data_under_10=data[data['product_comb'].isin(product_comb_under_10_set)]
data=data.reset_index()
del data['index']
print(data.shape)
dict_item_avg_price=data_item_avg_price.set_index(['product_comb'])['avg_price'].to_dict()


# In[9]:

del data['class_code_id']
del data['subclass_id']
data_NonRewards=data[pd.isnull(data['customer_id_hashed'])]
data_Rewards=data[~pd.isnull(data['customer_id_hashed'])]

print("Rewards - Row_RawData:",data_Rewards.shape)
print("Rewards - Unique_id:", len(data_Rewards['customer_id_hashed'].unique()))

print("Non_Rewards - Row_RawData:",data_NonRewards.shape)
print("Non_Rewards - Unique_id:", len(data_NonRewards['customer_id_hashed'].unique()))
# data=data[(data['subclass_transaction_amt']>0) & (data['subclass_transaction_units']>0)] #Already filtered at the beginning

gc.collect()


# In[12]:

def count_unique(x):
    return len(set(x))


# # Get the count of actual transactions

# In[13]:

Rewards_transactions_list=data_Rewards.groupby(['location_id','transaction_dt','transaction_id','customer_id_hashed'])['product_comb'].apply(list).to_frame().reset_index().rename(columns={"product_comb":"basket_list"})
Rewards_transactions_units_sales=data_Rewards.groupby(['location_id','transaction_dt','transaction_id','customer_id_hashed'])['subclass_transaction_units','subclass_transaction_amt'].sum().reset_index().rename(columns={"subclass_transaction_units":"total_item_units","subclass_transaction_amt":"total_item_revenue"})
Rewards_transactions=pd.merge(Rewards_transactions_list,Rewards_transactions_units_sales,on=['location_id','transaction_dt','transaction_id','customer_id_hashed'],how="left")
Rewards_transactions['basket_str']=Rewards_transactions['basket_list'].apply(lambda x: sorted(x)).astype(str)
Rewards_transactions['transactin_id_given']=[x for x in range(1,len(Rewards_transactions)+1)]
Rewards_transactions['types']=Rewards_transactions['basket_list'].apply(lambda x: len(x))


# In[14]:

Rewards_Trans_by_ID=Rewards_transactions.groupby(['customer_id_hashed'])['transactin_id_given'].count().to_frame().reset_index().rename(columns={"transactin_id_given":"trans_count"})

Rewards_IDCounts_by_Trans=Rewards_Trans_by_ID.groupby(['trans_count'])['customer_id_hashed'].count().to_frame().reset_index()
df_Rewards_IDCounts_by_Trans=Rewards_IDCounts_by_Trans.copy()
df_Rewards_IDCounts_by_Trans['trans_count']=np.where(df_Rewards_IDCounts_by_Trans['trans_count']>=3,"3+",df_Rewards_IDCounts_by_Trans['trans_count'])
df_Rewards_IDCounts_by_Trans['trans_count']=df_Rewards_IDCounts_by_Trans['trans_count'].replace(1,"1").replace(2,"2")
df_Rewards_IDCounts_by_Trans=df_Rewards_IDCounts_by_Trans.groupby(['trans_count'])['customer_id_hashed'].sum().to_frame().reset_index().rename(columns={"customer_id_hashed":"ID_Counts"})
df_Rewards_IDCounts_by_Trans['Label']="Rewards_ID"
df_Rewards_IDCounts_by_Trans=df_Rewards_IDCounts_by_Trans[['Label','trans_count','ID_Counts']]


# In[15]:

df_Non_Rewards_Trans_Count=data_NonRewards[['location_id','transaction_dt','transaction_id']].drop_duplicates()

df_output_1_count_by_trans_of_ids_price_10Plus=df_Rewards_IDCounts_by_Trans.append(pd.DataFrame({'Label':"Non_Rewards_Trans",'trans_count':"1+",'ID_Counts':len(df_Non_Rewards_Trans_Count)},index=[3]))
df_output_1_count_by_trans_of_ids_price_10Plus=df_output_1_count_by_trans_of_ids_price_10Plus[['Label','trans_count','ID_Counts']]
df_output_1_count_by_trans_of_ids_price_10Plus


# In[16]:

Rewards_data_transactions_list=data_Rewards.groupby(['location_id','transaction_dt','transaction_id','customer_id_hashed'])['product_comb'].apply(list).to_frame().reset_index().rename(columns={"product_comb":"basket_list"})
Rewards_data_transactions_units_sales=data_Rewards.groupby(['location_id','transaction_dt','transaction_id','customer_id_hashed'])['subclass_transaction_units','subclass_transaction_amt'].sum().reset_index().rename(columns={"subclass_transaction_units":"total_item_units","subclass_transaction_amt":"total_item_revenue"})

Rewards_data_transactions=pd.merge(Rewards_data_transactions_list,Rewards_data_transactions_units_sales,on=['location_id','transaction_dt','transaction_id','customer_id_hashed'],how="left")
Rewards_data_transactions['basket_str']=Rewards_data_transactions['basket_list'].apply(lambda x: sorted(x)).astype(str)
Rewards_data_transactions['transactin_id_given']=[x for x in range(1,len(Rewards_data_transactions)+1)]
Rewards_data_transactions['types']=Rewards_data_transactions['basket_list'].apply(lambda x: len(x))

# To save


Rewards_data_transactions=pd.merge(data_Rewards,Rewards_data_transactions,on=["location_id","transaction_dt","transaction_id","customer_id_hashed"],how="left")
apply_func={"subclass_transaction_units":"sum","transactin_id_given":"count","subclass_transaction_amt":"sum"}

single_prod_df=Rewards_data_transactions.groupby(['product_comb'])['subclass_transaction_units','transactin_id_given','subclass_transaction_amt'].agg(apply_func).reset_index().rename(columns={"subclass_transaction_units":"Total_Units","transactin_id_given":"Total_Trans","subclass_transaction_amt":"revenue"})
total_unit=single_prod_df['Total_Units'].sum()
total_trans=len(Rewards_data_transactions)

single_prod_df['prob_unit']=single_prod_df['Total_Units']/total_unit
single_prod_df['prob_tran']=single_prod_df['Total_Trans']/total_trans

dict_single_prod_unit=single_prod_df.set_index(['product_comb'])['prob_unit'].to_dict()
dict_single_prod_tran=single_prod_df.set_index(['product_comb'])['prob_tran'].to_dict()


# In[17]:

Rewards_Trans_by_ID=Rewards_Trans_by_ID.rename(columns={"trans_count":"trans_count_by_id"})
Rewards_data_transactions=pd.merge(Rewards_data_transactions,Rewards_Trans_by_ID,on="customer_id_hashed",how="left")
Rewards_data_transactions['trans_count_by_id']=np.where(Rewards_data_transactions['trans_count_by_id']>=3,"3+",Rewards_data_transactions['trans_count_by_id'])
Rewards_data_transactions['trans_count_by_id']=Rewards_data_transactions['trans_count_by_id'].replace(1,"1").replace(2,"2")


# In[19]:

df_output_2_1_count_by_trans_of_ids_price_10Plus=Rewards_data_transactions.groupby(['trans_count_by_id','types'])['transactin_id_given'].apply(count_unique).reset_index().rename(columns={"transactin_id_given":"Transaction_Count"})
df_output_2_1_count_by_trans_of_ids_price_10Plus_actual=df_output_2_1_count_by_trans_of_ids_price_10Plus.copy()
df_output_2_1_count_by_trans_of_ids_price_10Plus['types']=np.where(df_output_2_1_count_by_trans_of_ids_price_10Plus['types']>=6,"6+",df_output_2_1_count_by_trans_of_ids_price_10Plus['types'])

df_output_2_1_count_by_trans_of_ids_price_10Plus=df_output_2_1_count_by_trans_of_ids_price_10Plus.groupby(['trans_count_by_id','types'])['Transaction_Count'].sum().reset_index()
df_output_2_1_count_by_trans_of_ids_price_10Plus=df_output_2_1_count_by_trans_of_ids_price_10Plus.pivot_table(index="types",columns="trans_count_by_id",values="Transaction_Count").reset_index().rename(columns={"types":"item_types"})

df_output_2_1_count_by_trans_of_ids_price_10Plus=df_output_2_1_count_by_trans_of_ids_price_10Plus.sort_values("item_types")
df_output_2_1_count_by_trans_of_ids_price_10Plus['Label']="Rewards"
df_output_2_1_count_by_trans_of_ids_price_10Plus


# In[20]:

df_output_2_2_count_by_trans_of_ids_price_10Plus=data_NonRewards.groupby(['location_id','transaction_dt','transaction_id'])['product_comb'].apply(list).reset_index()
df_output_2_2_count_by_trans_of_ids_price_10Plus['item_types']=df_output_2_2_count_by_trans_of_ids_price_10Plus['product_comb'].apply(len)
df_output_2_2_count_by_trans_of_ids_price_10Plus_actual=df_output_2_2_count_by_trans_of_ids_price_10Plus.groupby(['item_types'])['transaction_id'].count().to_frame().reset_index()
df_output_2_2_count_by_trans_of_ids_price_10Plus=df_output_2_2_count_by_trans_of_ids_price_10Plus_actual.copy()
df_output_2_2_count_by_trans_of_ids_price_10Plus['item_types']=np.where(df_output_2_2_count_by_trans_of_ids_price_10Plus['item_types']>=6,"6+",df_output_2_2_count_by_trans_of_ids_price_10Plus['item_types'])
df_output_2_2_count_by_trans_of_ids_price_10Plus=df_output_2_2_count_by_trans_of_ids_price_10Plus.groupby(['item_types'])['transaction_id'].sum().to_frame().reset_index().rename(columns={"transaction_id":"Transaction_Count"})
df_output_2_2_count_by_trans_of_ids_price_10Plus['Label']="Non_Rewards"
df_output_2_2_count_by_trans_of_ids_price_10Plus


# In[21]:

data['customer_id_hashed']=data['customer_id_hashed'].fillna("nan")
df_output_3_count_by_trans_of_ids_price_10Plus=data.groupby(['location_id','transaction_dt','transaction_id','customer_id_hashed'])['product_comb'].apply(list).reset_index()
df_output_3_count_by_trans_of_ids_price_10Plus['item_types']=df_output_3_count_by_trans_of_ids_price_10Plus['product_comb'].apply(len)
df_output_3_count_by_trans_of_ids_price_10Plus_actual=df_output_3_count_by_trans_of_ids_price_10Plus.groupby(['item_types'])['transaction_id'].count().to_frame().reset_index()
df_output_3_count_by_trans_of_ids_price_10Plus=df_output_3_count_by_trans_of_ids_price_10Plus_actual.copy()
df_output_3_count_by_trans_of_ids_price_10Plus['item_types']=np.where(df_output_3_count_by_trans_of_ids_price_10Plus['item_types']>=6,"6+",df_output_3_count_by_trans_of_ids_price_10Plus['item_types'])
df_output_3_count_by_trans_of_ids_price_10Plus=df_output_3_count_by_trans_of_ids_price_10Plus.groupby(['item_types'])['transaction_id'].sum().to_frame().reset_index().rename(columns={"transaction_id":"Transaction_Count"})
df_output_3_count_by_trans_of_ids_price_10Plus['Label']="Rewards_and_NonRewards"
df_output_3_count_by_trans_of_ids_price_10Plus


# In[22]:

writer=pd.ExcelWriter(output_folder+"BL_Transaction_Summary_JL_"+str(datetime.datetime.now().date())+".xlsx",engine="xlsxwriter")
df_output_1_count_by_trans_of_ids_price_10Plus.to_excel(writer,"summary_1_transactions_ids")
df_output_2_1_count_by_trans_of_ids_price_10Plus.to_excel(writer,"summary_2_1_Rewards_trans_items")
df_output_2_2_count_by_trans_of_ids_price_10Plus.to_excel(writer,"summary_2_2_NonRew_trans_items")
df_output_3_count_by_trans_of_ids_price_10Plus.to_excel(writer,"summary_3_all_transactions")
writer.save()

del data
gc.collect()


# # Calcuating BAI

# In[23]:

unique_id_df=Rewards_data_transactions.groupby(['product_comb'])['customer_id_hashed'].apply(count_unique).to_frame().reset_index().rename(columns={"customer_id_hashed":"unique_ids"})
single_prod_df=pd.merge(single_prod_df,unique_id_df,on="product_comb")


# In[24]:

data_basket=Rewards_data_transactions.groupby(['basket_str'])['total_item_units','total_item_revenue','transactin_id_given'].agg(
            {"total_item_units":"sum","total_item_revenue":"sum","transactin_id_given":"count"}).reset_index().rename(columns={"transactin_id_given":"trans_count"})
data_basket['basket_list']=data_basket['basket_str'].apply(eval)
data_basket['item_types']=data_basket['basket_list'].apply(len)
data_basket=data_basket.sort_values(['item_types','basket_str'])

data_basket=data_basket.reset_index()
del data_basket['index']

unique_id_by_basket=Rewards_data_transactions.groupby(['basket_str'])['customer_id_hashed'].apply(lambda x: len(set(x))).to_frame().reset_index().rename(columns={'customer_id_hashed':"unique_ids"})
data_basket=pd.merge(data_basket,unique_id_by_basket,on="basket_str",how="left")


# In[25]:

# data_basket.to_csv("/home/jian/Projects/Big_Lots/Analysis/2018_Q4/Product_Basket/data_for_freq_dist_JL_"+str(datetime.datetime.now().date())+".csv",index=False)


# In[26]:

from itertools import combinations
def findsubsets(total_set,item_counts):
    return list(set(combinations(total_set, item_counts)))

for i in range(2,6):
    locals()['set_'+str(i)+"_comb"]=[]
    output_1_basket_str_list_i=sorted(data_basket[data_basket['item_types']==i]['basket_str'].unique().tolist())
    output_2_basket_str_list_i_plus=[]
    basket_str_list_i_plus=data_basket[data_basket['item_types']>i]['basket_str'].unique().tolist()
    
    
    for set_str in basket_str_list_i_plus:
        set_list=eval(set_str)
        output_2_basket_str_list_i_plus=list(set(output_2_basket_str_list_i_plus+[str(list(x)) for x in findsubsets(set_list,i)]))
        
    locals()['set_'+str(i)+"_comb"]=sorted(list(set(output_1_basket_str_list_i+output_2_basket_str_list_i_plus)))
    print(i, datetime.datetime.now())
    print(len(locals()['set_'+str(i)+"_comb"]))
    


# In[27]:

'''
basket_transaction_2_plus=data_basket[data_basket['item_types']>=2][['basket_str','trans_count']]
basket_transaction_3_plus=data_basket[data_basket['item_types']>=3][['basket_str','trans_count']]
basket_transaction_4_plus=data_basket[data_basket['item_types']>=4][['basket_str','trans_count']]
basket_transaction_5_plus=data_basket[data_basket['item_types']>=5][['basket_str','trans_count']]
'''


# In[28]:

list_set_all=set_2_comb+set_3_comb+set_4_comb+set_5_comb
total_len=len(list_set_all)
total_len


# In[30]:

processors=25

interval=int(np.floor(total_len/processors))
# list_set_all_subset_0=list_set_all_subset_[:interval]
# 0 to 9, 10 in total
all_list_of_input=[]
for i in range(processors-1): 
    #1 to 9
    locals()['list_set_all_subset_'+str(i)]=list_set_all[interval*i:interval*(i+1)]
    all_list_of_input=all_list_of_input+[locals()['list_set_all_subset_'+str(i)]]
locals()['list_set_all_subset_'+str(processors-1)]=list_set_all[interval*(processors-1):]
all_list_of_input=all_list_of_input+[locals()['list_set_all_subset_'+str(processors-1)]]


# In[32]:

def getting_BAI_items(list_set_subset_i):
    i_counter=0
    dict_basket_support_trans={}
    dict_basket_support_items={}
    dict_basket_BAI_trans={}
    dict_basket_BAI_items={}
    dict_basket_unique_ids={}
    dict_basket_revenue={} #revenue only for the selected subset of items
    for basket_n in list_set_subset_i:
        basket_n_list=eval(basket_n)
        len_items=len(basket_n_list)
        
        df=Rewards_data_transactions[Rewards_data_transactions['product_comb'].isin(basket_n_list)][['basket_str','transactin_id_given','subclass_transaction_units','customer_id_hashed','subclass_transaction_amt']]
        
        trans_denominator=1
        items_denominator=1
        
        for k in range(len_items):
            globals()['basket_item_'+str(k)]=basket_n_list[k]
            df=df[df['basket_str'].apply(lambda x: globals()['basket_item_'+str(k)] in x)]
            trans_denominator=trans_denominator*dict_single_prod_tran[globals()['basket_item_'+str(k)]]
            items_denominator=items_denominator*dict_single_prod_unit[globals()['basket_item_'+str(k)]]

        trans_basket=len(df['transactin_id_given'].unique())
        items_basket=df['subclass_transaction_units'].sum()
        unique_ids_basket=len(df['customer_id_hashed'].unique())
        revenue_bakset=df['subclass_transaction_amt'].sum()
        
        dict_basket_support_trans.update({basket_n:trans_basket})
        dict_basket_support_items.update({basket_n:items_basket})
        dict_basket_unique_ids.update({basket_n:unique_ids_basket})
        dict_basket_revenue.update({basket_n:revenue_bakset})

        BAI_basket_trans=(trans_basket/total_trans)/trans_denominator*100
        BAI_basket_items=(items_basket/total_unit)/items_denominator*100
        
        dict_basket_BAI_trans.update({basket_n:BAI_basket_trans})
        dict_basket_BAI_items.update({basket_n:BAI_basket_items})
        
        i_counter+=1
        if i_counter%1000==10:
            logging.info(str(datetime.datetime.now())+"|"+str(i_counter))
    results_json={}
    results_json.update({"dict_basket_support_trans":dict_basket_support_trans})
    results_json.update({"dict_basket_support_items":dict_basket_support_items})
    results_json.update({"dict_basket_BAI_trans":dict_basket_BAI_trans})
    results_json.update({"dict_basket_BAI_items":dict_basket_BAI_items})
    results_json.update({"dict_basket_unique_ids":dict_basket_unique_ids})
    results_json.update({"dict_basket_revenue":dict_basket_revenue})
    
    return results_json


# In[33]:

from multiprocessing import Pool

result_dict_basket_support_trans={}
result_dict_basket_support_items={}
result_dict_basket_BAI_trans={}
result_dict_basket_BAI_items={}
result_dict_basket_unique_ids={}
result_dict_basket_revenue={}

if __name__ == '__main__':
    p = Pool(processors)
    result=p.map(getting_BAI_items, all_list_of_input)
    for res in result:
        if res is not None:
            result_dict_basket_support_trans.update(res["dict_basket_support_trans"])
            result_dict_basket_support_items.update(res["dict_basket_support_items"])
            result_dict_basket_BAI_trans.update(res["dict_basket_BAI_trans"])
            result_dict_basket_BAI_items.update(res["dict_basket_BAI_items"])
            result_dict_basket_unique_ids.update(res['dict_basket_unique_ids'])
            result_dict_basket_revenue.update(res['dict_basket_revenue'])
    p.close()
    p.join()
    


# In[35]:

output_1=data_basket[data_basket['item_types']==1]
output_2=data_basket[data_basket['item_types'].isin([2,3,4,5])]
output_3=data_basket[data_basket['item_types']>5]

output_1['BAI_trans']=100
output_1['BAI_items']=100

output_2['BAI_trans']=output_2['basket_str'].apply(lambda x: result_dict_basket_BAI_trans[x])
output_2['BAI_items']=output_2['basket_str'].apply(lambda x: result_dict_basket_BAI_items[x])

output_basket=output_1.append(output_2).append(output_3) # To add those only in multiple item trans
#E.g. [a,b,c,d] [a,c] doesn't exsit


# In[37]:

single_prod_df['BAI_Trans']=100
single_prod_df['BAI_Items']=100


# In[38]:

df1=pd.DataFrame(result_dict_basket_support_trans,index=['Total_Trans']).T.reset_index().rename(columns={"index":"basket_str"})
df2=pd.DataFrame(result_dict_basket_support_items,index=['Total_Units']).T.reset_index().rename(columns={"index":"basket_str"})
df3=pd.DataFrame(result_dict_basket_BAI_trans,index=['BAI_Trans']).T.reset_index().rename(columns={"index":"basket_str"})
df4=pd.DataFrame(result_dict_basket_BAI_items,index=['BAI_Items']).T.reset_index().rename(columns={"index":"basket_str"})
df5=pd.DataFrame(result_dict_basket_unique_ids,index=['unique_ids']).T.reset_index().rename(columns={"index":"basket_str"})
df6=pd.DataFrame(result_dict_basket_revenue,index=['revenue']).T.reset_index().rename(columns={"index":"basket_str"})

output_all_2345_available=pd.merge(df1,df2,on='basket_str')
output_all_2345_available=pd.merge(df3,output_all_2345_available,on='basket_str')
output_all_2345_available=pd.merge(df4,output_all_2345_available,on='basket_str')
output_all_2345_available=pd.merge(df5,output_all_2345_available,on='basket_str')
output_all_2345_available=pd.merge(df6,output_all_2345_available,on='basket_str')


# In[40]:

single_prod_df['basket_str']="['"+single_prod_df['product_comb']+"']"
del single_prod_df['product_comb']

output_all_12345_available=single_prod_df.append(output_all_2345_available)
output_all_12345_available['basket_list']=output_all_12345_available['basket_str'].apply(eval)
output_all_12345_available['item_types']=output_all_12345_available['basket_list'].apply(len)
output_all_12345_available=output_all_12345_available.sort_values('item_types',ascending=True)

# All posibble from the shopped large basket 1-5
output_3=data_basket[data_basket['item_types']>5]
output_3=output_3.rename(columns={"trans_count":"Total_Trans","total_item_units":"Total_Units","total_item_revenue":"revenue"})

output_all_12345_available=output_all_12345_available.append(output_3) #Appended >5


# # Step 2

# In[49]:

# Apply the BAI of items to the baskets

len(result_dict_basket_BAI_items)
data_item_avg_price_dict=data_item_avg_price.set_index(["product_comb"]).to_dict()['avg_price']
len(data_item_avg_price_dict)


# In[51]:

'''
def brewak_basket_to_top_5(input_x):
    input_x=eval(input_x)
    len_input_x=len(input_x)
    df=pd.DataFrame({"subcalss_item":input_x},index=range(len(len_input_x)))
    df['price']=df['subcalss_item'].apply(lambda x: data_item_avg_price_dict[x])
    df=df.sort_values("price",ascending=False).head(5)
    
    chapion_subclass=df['subcalss_item'].tolist()[0]
    df['class_code_id']=df['subcalss_item'].apply(lambda x: x.split({"-"}[0]))
    chapion_subclass_id=df['class_code_id'].tolist(0)
    
    complementary_subclass_df=df[df['class_code_id']==chapion_class_id]
    
    if len(complementary_subclass_df)>0:
        complementary_subclass_df_1=complementary_subclass_df.head(3)
        complementary_subclass_df_2=
'''


# In[26]:

'''
writer=pd.ExcelWriter(output_folder+"/BL_DBasket_Version2_JL_'+str(datetime.datetime.now().date())+".xlsx",engine="xlsxwriter")
# output=output[['basket_str','basket_list','BAI_trans','BAI_units','item_types','total_item_revenue','total_item_units','trans_count','unique_ids','price_list']]
output_all_12345_available=output_all_12345_available[['basket_list','BAI_Trans','BAI_Items','item_types','revenue','Total_Units','Total_Trans','unique_ids']]
output_all_12345_available=output_all_12345_available.sort_values(['item_types','BAI_Trans'],ascending=[True,False])
output_all_12345_available.to_excel(writer,"BAI_including_subsets",index=False)
data_basket.to_excel(writer,"basket_shopped_together",index=False)
writer.save()
'''
logging.info("Done: "+str(datetime.datetime.now()))


# In[27]:

output_all_12345_available.to_csv(output_folder+"/BL_DBasket_Version2_BAI_output_JL_"+str(datetime.datetime.now().date())+".csv",index=False)
data_basket.to_csv(output_folder+"/BL_DBasket_Version2_actual_whole_baskets_output_JL_"+str(datetime.datetime.now().date())+".csv",index=False)



# In[37]:

output_all_12345_available[output_all_12345_available['item_types']==1].to_csv(output_folder+"/BL_DBasket_Version2_BAI_output_Item_1_JL_"+str(datetime.datetime.now().date())+".csv",index=False)
output_all_12345_available[output_all_12345_available['item_types']==2].to_csv(output_folder+"/BL_DBasket_Version2_BAI_output_Item_2_JL_"+str(datetime.datetime.now().date())+".csv",index=False)
output_all_12345_available[output_all_12345_available['item_types']==3].to_csv(output_folder+"/BL_DBasket_Version2_BAI_output_Item_3_JL_"+str(datetime.datetime.now().date())+".csv",index=False)
output_all_12345_available[output_all_12345_available['item_types']==4].to_csv(output_folder+"/BL_DBasket_Version2_BAI_output_Item_4_JL_"+str(datetime.datetime.now().date())+".csv",index=False)
output_all_12345_available[output_all_12345_available['item_types']==5].to_csv(output_folder+"/BL_DBasket_Version2_BAI_output_Item_5_JL_"+str(datetime.datetime.now().date())+".csv",index=False)
output_all_12345_available[output_all_12345_available['item_types']>5].to_csv(output_folder+"/BL_DBasket_Version2_BAI_output_Item_6Plus_JL_"+str(datetime.datetime.now().date())+".csv",index=False)



