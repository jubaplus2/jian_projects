
# coding: utf-8

# In[1]:

import pandas as pd
import numpy as np
import datetime
import os
import logging
price_threshold=10

logging.basicConfig(filename='V2.log', level=logging.INFO)


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
apply_func={"subclass_transaction_units":"sum","transactin_id_given":"count"}

single_prod_df=data.groupby(['product_comb'])['subclass_transaction_units','transactin_id_given'].agg(apply_func).reset_index().rename(columns={"subclass_transaction_units":"Total_Units","transactin_id_given":"Total_Trans"})
total_unit=single_prod_df['Total_Units'].sum()
total_trans=len(data_transactions)

single_prod_df['prob_unit']=single_prod_df['Total_Units']/total_unit
single_prod_df['prob_tran']=single_prod_df['Total_Trans']/total_trans

dict_single_prod_unit=single_prod_df.set_index(['product_comb'])['prob_unit'].to_dict()
dict_single_prod_tran=single_prod_df.set_index(['product_comb'])['prob_tran'].to_dict()


# In[10]:

data_basket=data_transactions.groupby(['basket_str'])['total_item_units','total_item_revenue','transactin_id_given'].agg(
            {"total_item_units":"sum","total_item_revenue":"sum","transactin_id_given":"count"}).reset_index().rename(columns={"transactin_id_given":"trans_count"})
data_basket['basket_list']=data_basket['basket_str'].apply(eval)
data_basket['item_types']=data_basket['basket_list'].apply(len)
data_basket=data_basket.sort_values(['item_types','basket_str'])

data_basket=data_basket.reset_index()
del data_basket['index']

unique_id_by_basket=data_transactions.groupby(['basket_str'])['customer_id_hashed'].apply(lambda x: len(set(x))).to_frame().reset_index().rename(columns={'customer_id_hashed':"unique_ids"})
data_basket=pd.merge(data_basket,unique_id_by_basket,on="basket_str",how="left")


# In[11]:

single_prod_df['prob_tran'].sum()


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

basket_transaction_2_plus=data_basket[data_basket['item_types']>=2][['basket_str','trans_count']]
basket_transaction_3_plus=data_basket[data_basket['item_types']>=3][['basket_str','trans_count']]
basket_transaction_4_plus=data_basket[data_basket['item_types']>=4][['basket_str','trans_count']]
basket_transaction_5_plus=data_basket[data_basket['item_types']>=5][['basket_str','trans_count']]


# In[15]:

data_basket['total_item_units'].sum()


# In[16]:

'''
Fast to run because of only trans measurement

# Starting with transction BAI only first
# Approach 1 for the speed test: from the basket level data
all_trans_list=data_transactions['basket_str'].tolist()
dict_basket_2_support={}
dict_basket_2_prob={}
i_counter=0
for basket_2 in set_2_comb:
    basket_item_1=eval(basket_2)[0]
    basket_item_2=eval(basket_2)[1]
    
    df=basket_transaction_2_plus[basket_transaction_2_plus['basket_str'].apply(lambda x: (basket_item_1 in x) and (basket_item_2 in x))]
    
    trans_basket_2=df['trans_count'].sum()
    dict_basket_2_support.update({basket_2:trans_basket_2})
    
    prob_basket_2=(trans_basket_2/total_trans)/(dict_single_prod_tran[basket_item_1]*dict_single_prod_tran[basket_item_2])
    dict_basket_2_prob.update({basket_2:prob_basket_2})
    i_counter+=1
    if i_counter%1000==20:
        print(datetime.datetime.now(),i_counter)
'''    


# In[21]:

# Starting with transction BAI only first
# Approach 2 for the speed test: from the detailed raw data level (> $10 only)
dict_basket_2_support_trans={}
dict_basket_2_BAI_trans={}

dict_basket_2_support_items={}
dict_basket_2_BAI_items={}

i_counter=0
for basket_2 in set_2_comb:
    basket_item_1=eval(basket_2)[0]
    basket_item_2=eval(basket_2)[1]
    
    df=data[(data['product_comb']==basket_item_1) | (data['product_comb']==basket_item_2)]
    
    trans_basket_2=len(df['transactin_id_given'].unique())
    items_basket_2=df['subclass_transaction_units'].sum()
    
    dict_basket_2_support_trans.update({basket_2:trans_basket_2})
    dict_basket_2_support_items.update({basket_2:items_basket_2})
    
    
    BAI_basket_2_trans=(trans_basket_2/total_trans)/(dict_single_prod_tran[basket_item_1]*dict_single_prod_tran[basket_item_2])
    BAI_basket_2_items=(items_basket_2/total_unit)/(dict_single_prod_unit[basket_item_1]*dict_single_prod_unit[basket_item_2])
    
    dict_basket_2_BAI_trans.update({basket_2:BAI_basket_2_trans})
    dict_basket_2_BAI_items.update({basket_2:BAI_basket_2_items})
    
    
    i_counter+=1
    if i_counter%10000==20:
        logging.info(str(datetime.datetime.now())+"|"+str(i_counter))


# In[24]:

# 3

dict_basket_3_support_trans={}
dict_basket_3_BAI_trans={}

dict_basket_3_support_items={}
dict_basket_3_BAI_items={}

i_counter=0
for basket_3 in set_3_comb:
    
    
    basket_list=eval(basket_3)
    basket_item_1=basket_list[0]
    basket_item_2=basket_list[1]
    basket_item_3=basket_list[2]
    
    df=data[data['product_comb'].isin(basket_list)]
    ####
    trans_basket_3=len(df['transactin_id_given'].unique())
    items_basket_3=df['subclass_transaction_units'].sum()
    
    dict_basket_3_support_trans.update({basket_3:trans_basket_3})
    dict_basket_3_support_items.update({basket_3:items_basket_3})
    ####
    
    BAI_basket_3_trans=(trans_basket_3/total_trans)/(dict_single_prod_tran[basket_item_1]*dict_single_prod_tran[basket_item_2]*dict_single_prod_tran[basket_item_3])
    BAI_basket_3_items=(items_basket_3/total_unit)/(dict_single_prod_unit[basket_item_1]*dict_single_prod_unit[basket_item_2]*dict_single_prod_unit[basket_item_3])
    
    dict_basket_3_BAI_trans.update({basket_3:BAI_basket_3_trans})
    dict_basket_3_BAI_items.update({basket_3:BAI_basket_3_items})
    
    
    i_counter+=1
    if i_counter%10000==20:
        logging.info(str(datetime.datetime.now())+"|"+str(i_counter))


# In[26]:

# 4

dict_basket_4_support_trans={}
dict_basket_4_BAI_trans={}

dict_basket_4_support_items={}
dict_basket_4_BAI_items={}

i_counter=0
for basket_4 in set_4_comb:
    
    
    basket_list=eval(basket_4)
    basket_item_1=basket_list[0]
    basket_item_2=basket_list[1]
    basket_item_3=basket_list[2]
    basket_item_4=basket_list[3]
    
    df=data[data['product_comb'].isin(basket_list)]
    ####
    trans_basket_4=len(df['transactin_id_given'].unique())
    items_basket_4=df['subclass_transaction_units'].sum()
    
    dict_basket_4_support_trans.update({basket_4:trans_basket_4})
    dict_basket_4_support_items.update({basket_4:items_basket_4})
    ####
    
    BAI_basket_4_trans=(trans_basket_4/total_trans)/(dict_single_prod_tran[basket_item_1]*dict_single_prod_tran[basket_item_2]*dict_single_prod_tran[basket_item_3]*dict_single_prod_tran[basket_item_4])
    BAI_basket_4_items=(items_basket_4/total_unit)/(dict_single_prod_unit[basket_item_1]*dict_single_prod_unit[basket_item_2]*dict_single_prod_unit[basket_item_3]*dict_single_prod_unit[basket_item_4])
    
    dict_basket_4_BAI_trans.update({basket_4:BAI_basket_4_trans})
    dict_basket_4_BAI_items.update({basket_4:BAI_basket_4_items})
    
    
    i_counter+=1
    if i_counter%10000==20:
        logging.info(str(datetime.datetime.now())+"|"+str(i_counter))
        


# In[27]:

# 5

dict_basket_5_support_trans={}
dict_basket_5_BAI_trans={}

dict_basket_5_support_items={}
dict_basket_5_BAI_items={}

i_counter=0
for basket_5 in set_5_comb:
    
    
    basket_list=eval(basket_5)
    basket_item_1=basket_list[0]
    basket_item_2=basket_list[1]
    basket_item_3=basket_list[2]
    basket_item_4=basket_list[3]
    basket_item_5=basket_list[4]
    df=data[data['product_comb'].isin(basket_list)]
    ####
    trans_basket_5=len(df['transactin_id_given'].unique())
    items_basket_5=df['subclass_transaction_units'].sum()
    
    dict_basket_5_support_trans.update({basket_5:trans_basket_5})
    dict_basket_5_support_items.update({basket_5:items_basket_5})
    ####
    
    BAI_basket_5_trans=(trans_basket_5/total_trans)/(dict_single_prod_tran[basket_item_1]*dict_single_prod_tran[basket_item_2]*dict_single_prod_tran[basket_item_3]*dict_single_prod_tran[basket_item_4]*dict_single_prod_tran[basket_item_5])
    BAI_basket_5_items=(items_basket_5/total_unit)/(dict_single_prod_unit[basket_item_1]*dict_single_prod_unit[basket_item_2]*dict_single_prod_unit[basket_item_3]*dict_single_prod_unit[basket_item_4]*dict_single_prod_unit[basket_item_5])
    
    dict_basket_5_BAI_trans.update({basket_5:BAI_basket_5_trans})
    dict_basket_5_BAI_items.update({basket_5:BAI_basket_5_items})
    
    
    i_counter+=1
    if i_counter%10000==20:
        logging.info(str(datetime.datetime.now())+"|"+str(i_counter))
       


# In[29]:

BAI_2345_trans=dict_basket_2_BAI_trans
BAI_2345_items=dict_basket_2_BAI_items

BAI_2345_trans.update(dict_basket_3_BAI_trans)
BAI_2345_trans.update(dict_basket_4_BAI_trans)
BAI_2345_trans.update(dict_basket_5_BAI_trans)

BAI_2345_items.update(dict_basket_3_BAI_items)
BAI_2345_items.update(dict_basket_4_BAI_items)
BAI_2345_items.update(dict_basket_5_BAI_items)



# In[31]:

output_1=data_basket[data_basket['item_types']==1]
output_2=data_basket[data_basket['item_types'].isin([2,3,4,5])]
output_3=data_basket[data_basket['item_types']>5]

output_1['BAI_trans']=1
output_1['BAI_items']=1

output_2['BAI_trans']=output_2['basket_str'].apply(lambda x: BAI_2345_trans[x])
output_2['BAI_items']=output_2['basket_str'].apply(lambda x: BAI_2345_item[x])

output_basket=output_1.append(output_2).append(output_3) # To add those only in multiple item trans
#E.g. [a,b,c,d] [a,c] doesn't exsit


# In[46]:

df1=pd.DataFrame(BAI_2345_trans,index=['BAI_Trans']).T.reset_index().rename(columns={"index":"basket_str"})
df2=pd.DataFrame(BAI_2345_items,index=['BAI_Items']).T.reset_index().rename(columns={"index":"basket_str"})
output_all_available=pd.merge(df1,df2,on='basket_str')


# In[47]:

writer=pd.ExcelWriter('/home/jian/Projects/Big_Lots/Analysis/2018_Q4/Product_Basket/BL_DBasket_Version2_JL_'+str(datetime.datetime.now().date())+".xlsx",engine="xlsxwriter")
output=output[['basket_str','basket_list','BAI_trans','BAI_units','item_types','total_item_revenue','total_item_units','trans_count','unique_ids','price_list']]
output_all_available.to_excel(writer,"BAI_2_to_5",index=False)
output_basket.to_excel(writer,"basket_only",index=False)
writer.save()

logging.info("Done: "+str(datetime.datetime.now()))


# In[ ]:




# In[ ]:



