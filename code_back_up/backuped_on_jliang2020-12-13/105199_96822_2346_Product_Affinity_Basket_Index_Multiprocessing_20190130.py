
# coding: utf-8

# In[1]:

import pandas as pd
import numpy as np
import datetime
import os
import logging

from multiprocessing import Pool
from functools import partial

price_threshold=10
logging.basicConfig(filename='V2_Multiprocessing.log', level=logging.INFO)


# In[2]:

last_saturday=datetime.date(2019,1,12) # To be changed to the running Tuesday
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


# In[3]:

most_recent_daily_data


# In[4]:

data=pd.read_table(most_recent_daily_data,dtype=str,sep="|")
print("len_sub_class_id:",data['subclass_id'].apply(lambda x: len(x)).unique())
print("len_class_code_id:",data['class_code_id'].apply(lambda x: len(x)).unique())
data['subclass_id']=data['subclass_id'].apply(lambda x: x.zfill(3))
data['product_comb']=data['class_code_id']+"-"+data['subclass_id']
del data['class_code_id']
del data['subclass_id']
data=data[~pd.isnull(data['customer_id_hashed'])]
data['subclass_transaction_amt']=data['subclass_transaction_amt'].astype(float)
data['subclass_transaction_units']=data['subclass_transaction_units'].astype(int)
data['price']=data['subclass_transaction_amt']/data['subclass_transaction_units']
print("Row_RawData:",data.shape)
print("Unique_id:", len(data['customer_id_hashed'].unique()))
data=data[(data['subclass_transaction_amt']>0) & (data['subclass_transaction_units']>0)]


# In[5]:

len(data['product_comb'].unique())


# In[6]:

taxonomy=pd.read_csv("/home/jian/BigLots/static_files/ProductTaxonomy/MediaStormProductTaxonomy20190101-135843-066.txt",dtype=str,sep="|")
taxonomy['subclass_id']=taxonomy['subclass_id'].apply(lambda x: x.zfill(3))
division_id_id_name=pd.read_table("/home/jian/BigLots/static_files/MediaStorm Data Extract - Division Names.txt",dtype=str,sep="|")
department_id_name=pd.read_table("/home/jian/BigLots/static_files/MediaStorm Data Extract - Department Names.txt",dtype=str,sep="|")
class_id_name=pd.read_table("/home/jian/BigLots/static_files/MediaStorm Data Extract - Class Names.txt",dtype=str,sep="|",encoding ='ISO-8859-1')


# In[7]:

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

data_item_avg_price.to_csv("/home/jian/Projects/Big_Lots/Analysis/2018_Q4/Product_Basket/Price_"+str(last_saturday)+".csv",index=False)


# In[8]:

# $10 of all items as in the email on 2019-01-14

product_comb_under_10_set=set(data_item_avg_price[data_item_avg_price['avg_price']<price_threshold]['product_comb'].unique().tolist())

print(data.shape)
data=data[~data['product_comb'].isin(product_comb_under_10_set)]
data_under_10=data[data['product_comb'].isin(product_comb_under_10_set)]
data=data.reset_index()
del data['index']
print(data.shape)
dict_item_avg_price=data_item_avg_price.set_index(['product_comb'])['avg_price'].to_dict()


# In[9]:

product_comb_10_and_above_list=data_item_avg_price[data_item_avg_price['avg_price']>=10]['product_comb'].unique().tolist()

data_transactions_list=data.groupby(['location_id','transaction_dt','transaction_id','customer_id_hashed'])['product_comb'].apply(list).to_frame().reset_index().rename(columns={"product_comb":"basket_list"})
data_transactions_units_sales=data.groupby(['location_id','transaction_dt','transaction_id','customer_id_hashed'])['subclass_transaction_units','subclass_transaction_amt'].sum().reset_index().rename(columns={"subclass_transaction_units":"total_item_units","subclass_transaction_amt":"total_item_revenue"})

data_transactions=pd.merge(data_transactions_list,data_transactions_units_sales,on=['location_id','transaction_dt','transaction_id','customer_id_hashed'],how="left")
data_transactions['basket_str']=data_transactions['basket_list'].apply(lambda x: sorted(x)).astype(str)
data_transactions['transactin_id_given']=[x for x in range(1,len(data_transactions)+1)]
data_transactions['types']=data_transactions['basket_list'].apply(lambda x: len(x))

# To save


data=pd.merge(data,data_transactions,on=["location_id","transaction_dt","transaction_id","customer_id_hashed"],how="left")
apply_func={"subclass_transaction_units":"sum","transactin_id_given":"count","subclass_transaction_amt":"sum"}

single_prod_df=data.groupby(['product_comb'])['subclass_transaction_units','transactin_id_given','subclass_transaction_amt'].agg(apply_func).reset_index().rename(columns={"subclass_transaction_units":"Total_Units","transactin_id_given":"Total_Trans","subclass_transaction_amt":"revenue"})
total_unit=single_prod_df['Total_Units'].sum()
total_trans=len(data_transactions)

single_prod_df['prob_unit']=single_prod_df['Total_Units']/total_unit
single_prod_df['prob_tran']=single_prod_df['Total_Trans']/total_trans

dict_single_prod_unit=single_prod_df.set_index(['product_comb'])['prob_unit'].to_dict()
dict_single_prod_tran=single_prod_df.set_index(['product_comb'])['prob_tran'].to_dict()


# In[10]:

def count_unique(x):
    return len(set(x))

unique_id_df=data.groupby(['product_comb'])['customer_id_hashed'].apply(count_unique).to_frame().reset_index().rename(columns={"customer_id_hashed":"unique_ids"})
single_prod_df=pd.merge(single_prod_df,unique_id_df,on="product_comb")


# In[ ]:




# In[11]:

data_basket=data_transactions.groupby(['basket_str'])['total_item_units','total_item_revenue','transactin_id_given'].agg(
            {"total_item_units":"sum","total_item_revenue":"sum","transactin_id_given":"count"}).reset_index().rename(columns={"transactin_id_given":"trans_count"})
data_basket['basket_list']=data_basket['basket_str'].apply(eval)
data_basket['item_types']=data_basket['basket_list'].apply(len)
data_basket=data_basket.sort_values(['item_types','basket_str'])

data_basket=data_basket.reset_index()
del data_basket['index']

unique_id_by_basket=data_transactions.groupby(['basket_str'])['customer_id_hashed'].apply(lambda x: len(set(x))).to_frame().reset_index().rename(columns={'customer_id_hashed':"unique_ids"})
data_basket=pd.merge(data_basket,unique_id_by_basket,on="basket_str",how="left")


# In[12]:

# data_basket.to_csv("/home/jian/Projects/Big_Lots/Analysis/2018_Q4/Product_Basket/data_for_freq_dist_JL_"+str(datetime.datetime.now().date())+".csv",index=False)


# In[13]:

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
    


# In[14]:

'''
basket_transaction_2_plus=data_basket[data_basket['item_types']>=2][['basket_str','trans_count']]
basket_transaction_3_plus=data_basket[data_basket['item_types']>=3][['basket_str','trans_count']]
basket_transaction_4_plus=data_basket[data_basket['item_types']>=4][['basket_str','trans_count']]
basket_transaction_5_plus=data_basket[data_basket['item_types']>=5][['basket_str','trans_count']]
'''


# In[15]:

list_set_all=set_2_comb+set_3_comb+set_4_comb+set_5_comb
total_len=len(list_set_all)
total_len


# In[16]:

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


# In[62]:

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
        
        df=data[data['product_comb'].isin(basket_n_list)][['basket_str','transactin_id_given','subclass_transaction_units','customer_id_hashed','subclass_transaction_amt']]
        
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
            
        BAI_basket_trans=(trans_basket/total_trans)/trans_denominator
        BAI_basket_items=(items_basket/total_unit)/items_denominator
        
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


# In[18]:

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
    


# In[53]:

all_list_of_input[0][0]


# In[19]:

output_1=data_basket[data_basket['item_types']==1]
output_2=data_basket[data_basket['item_types'].isin([2,3,4,5])]
output_3=data_basket[data_basket['item_types']>5]

output_1['BAI_trans']=1
output_1['BAI_items']=1

output_2['BAI_trans']=output_2['basket_str'].apply(lambda x: result_dict_basket_BAI_trans[x])
output_2['BAI_items']=output_2['basket_str'].apply(lambda x: result_dict_basket_BAI_items[x])

output_basket=output_1.append(output_2).append(output_3) # To add those only in multiple item trans
#E.g. [a,b,c,d] [a,c] doesn't exsit


# In[20]:

single_prod_df.head(2)


# In[21]:

single_prod_df['BAI_Trans']=1
single_prod_df['BAI_Items']=1


# In[22]:

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


# In[23]:

single_prod_df.head(2)


# In[24]:

single_prod_df['basket_str']="['"+single_prod_df['product_comb']+"']"
del single_prod_df['product_comb']

output_all_12345_available=single_prod_df.append(output_all_2345_available)
output_all_12345_available['basket_list']=output_all_12345_available['basket_str'].apply(eval)
output_all_12345_available['item_types']=output_all_12345_available['basket_list'].apply(len)
output_all_12345_available=output_all_12345_available.sort_values('item_types',ascending=True)

# All posibble from the shopped large basket 1-5
output_3=data_basket[data_basket['item_types']>5]
output_3=output_3.rename(columns={"trans_count":"Total_Trans","total_item_units":"Total_Units","total_item_revenue":"revenue"})

output_all_12345_available=output_all_12345_available.append(output_3)


# In[25]:

writer=pd.ExcelWriter('/home/jian/Projects/Big_Lots/Analysis/2018_Q4/Product_Basket/BL_DBasket_Version2_JL_'+str(datetime.datetime.now().date())+".xlsx",engine="xlsxwriter")
# output=output[['basket_str','basket_list','BAI_trans','BAI_units','item_types','total_item_revenue','total_item_units','trans_count','unique_ids','price_list']]
output_all_12345_available=output_all_12345_available[['basket_list','BAI_Trans','BAI_Items','item_types','revenue','Total_Units','Total_Trans','unique_ids']]
output_all_12345_available=output_all_12345_available.sort_values(['item_types','BAI_Trans'],ascending=[True,False])
output_all_12345_available.to_excel(writer,"BAI_including_subsets",index=False)
data_basket.to_excel(writer,"basket_shopped_together",index=False)
writer.save()

logging.info("Done: "+str(datetime.datetime.now()))


# In[34]:

output_all_12345_available[output_all_12345_available['item_types']==2].head(20)

