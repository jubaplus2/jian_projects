
# coding: utf-8

# In[1]:

'''
Ideas:

1. XX & YY seperately deal with, with different weight of probabilities
2. use the last transactions, and each item in the last basket with equal weight
3. Remove the already purchased items

'''


# In[2]:

import pandas as pd
import numpy as np
import datetime
import gc
import os
from multiprocessing import Pool
import logging
import json
os.getcwd()
logging.basicConfig(filename='Multiprocessing.log', level=logging.INFO)


# In[3]:

os.listdir('/home/jian/Projects/Big_Lots/Analysis/2019_Q2/Planner_Requests/temp/data/')


# In[4]:

'''
df_R1_item_attr=pd.read_csv('C:\\Users\\jlian\\OneDrive\\Desktop\\Tianchi_201908_Antai\\data\\Antai_AE_round1_item_attr_20190626.zip',
                           dtype=str,compression="zip",sep=",")
print(df_R1_item_attr.shape)
df_R1_item_attr.tail(3)
'''


# In[5]:

'''
df_R1_submit_example=pd.read_csv('C:\\Users\\jlian\\OneDrive\\Desktop\\Tianchi_201908_Antai\\data\\Antai_AE_round1_submit_20190715.csv',
                           dtype=str,sep=",")
print(df_R1_submit_example.shape)
df_R1_submit_example.head(3)
'''


# In[6]:

df_R1_train=pd.read_csv('/home/jian/Projects/Big_Lots/Analysis/2019_Q2/Planner_Requests/temp/data/Antai_AE_round1_train_20190626.zip',
                           dtype=str,sep=",")
print(df_R1_train.shape)
print(df_R1_train['buyer_admin_id'].nunique())

print(df_R1_train['create_order_time'].min())
print(df_R1_train['create_order_time'].max())
df_R1_train.head(3)
df_R1_train['irank']=df_R1_train['irank'].astype(int)
df_R1_train=df_R1_train.sort_values(["buyer_admin_id","irank"])


# In[7]:

count_by_id=df_R1_train.groupby('buyer_admin_id')['irank'].count().to_frame().reset_index().sort_values("irank",ascending=False)
count_by_id_exclude=count_by_id[count_by_id['irank']>=2000]
list_id_excluded=count_by_id_exclude['buyer_admin_id'].unique().tolist()
print(len(list_id_excluded))
df_R1_train=df_R1_train[~df_R1_train['buyer_admin_id'].isin(list_id_excluded)]
print(df_R1_train.shape)

train_item_all=df_R1_train['item_id'].unique().tolist()


# In[9]:

df_id_countries=df_R1_train.groupby(['buyer_admin_id'])['buyer_country_id'].nunique().to_frame().reset_index().sort_values("buyer_country_id")
df_id_xx_and_yy=df_id_countries[df_id_countries['buyer_country_id']==2]
df_id_1_country=df_id_countries[df_id_countries['buyer_country_id']==1]

id_list_1_country=df_id_1_country['buyer_admin_id'].tolist()
id_list_2_countries=df_id_xx_and_yy['buyer_admin_id'].tolist()

print(len(id_list_1_country))
print(len(id_list_2_countries))


# In[11]:

df_R1_train_YY=df_R1_train[((df_R1_train['buyer_country_id']=="yy") & (df_R1_train['buyer_admin_id'].isin(id_list_1_country)))]
id_list_YY=df_R1_train_YY['buyer_admin_id'].tolist()
## XX & YY combined in to XX only
df_R1_train_XX=df_R1_train[~df_R1_train['buyer_admin_id'].isin(id_list_YY)]

print(df_R1_train_YY.shape)
print(df_R1_train_XX.shape)


# In[12]:

df_R1_test=pd.read_csv('./data/Antai_AE_round1_test_20190626.csv',
                           dtype=str,sep=",")
df_R1_test['irank']=df_R1_test['irank'].astype(int)
df_R1_test_yy_only=df_R1_test[~df_R1_test['buyer_admin_id'].isin(df_R1_train_XX['buyer_admin_id'].tolist())]

df_R1_train_YY=df_R1_train_YY.append(df_R1_test_yy_only)


# In[13]:

df_R1_train_YY=df_R1_train_YY.sort_values(['buyer_admin_id','create_order_time'],ascending=[True,True])
# del df_R1_train_YY['irank']


# In[14]:

print(datetime.datetime.now())
df_temp=df_R1_train_YY[['buyer_admin_id','create_order_time']].drop_duplicates()
new_rank=[]
for buyer_id,df_group in df_temp.groupby("buyer_admin_id"):
    new_rank=new_rank+[x+1 for x in range(len(df_group))]
        
df_temp['new_rank']=new_rank
df_R1_train_YY=pd.merge(df_R1_train_YY,df_temp,on=['buyer_admin_id','create_order_time'])
print(datetime.datetime.now())


# In[15]:

df_YY_id_purchased_count=df_R1_train_YY.groupby("item_id")['buyer_admin_id'].nunique().to_frame().reset_index().sort_values("buyer_admin_id",ascending=False)
item_list_hold_YY=df_YY_id_purchased_count[df_YY_id_purchased_count['buyer_admin_id']>=28]['item_id'].tolist()


# In[16]:

# new rank: the higher, the later
# the same for same time 
df_R1_train_YY_iterration=df_R1_train_YY[['buyer_admin_id','item_id','new_rank']]
df_R1_train_YY_iterration['count']=1
df_R1_train_YY_iterration=df_R1_train_YY_iterration.groupby(['buyer_admin_id','item_id','new_rank'])['count'].sum().to_frame().reset_index()


# In[17]:

print("len(item_list_hold_YY)",len(item_list_hold_YY))


# In[18]:

len(item_list_hold_YY)


# In[20]:

processors=25

len_per_subset=int(np.ceil(len(item_list_hold_YY)/processors))
list_of_list_25_subset=[]
for i in range(25):
    lower_boundry=i*len_per_subset
    upper_boundry=(i+1)*len_per_subset
    list_of_list_25_subset=list_of_list_25_subset+[item_list_hold_YY[lower_boundry:upper_boundry]]
len(list_of_list_25_subset)


# In[24]:

def get_dict_of_list_prod_prob_YY(list_input):
    dict_output={}
    for item in list_input:

        df_subset_of_item=df_R1_train_YY_iterration.loc[df_R1_train_YY_iterration['item_id']==item,['buyer_admin_id','new_rank']].drop_duplicates()
        len_to_exclude=len(df_subset_of_item)
        
        df_subset_of_item_2=df_subset_of_item.copy()
        df_subset_of_item_2['new_rank']=df_subset_of_item_2['new_rank'].apply(lambda x: x+1)
        df_subset_of_item=df_subset_of_item.append(df_subset_of_item_2)

        df_subset_of_item=pd.merge(df_subset_of_item,df_R1_train_YY_iterration,on=['buyer_admin_id','new_rank'],how="left")
        df_subset_of_item=df_subset_of_item[pd.notnull(df_subset_of_item['item_id'])]

        total_others=len(df_subset_of_item)-len_to_exclude
        if total_others>0:
            df_subset_of_item=df_subset_of_item.groupby("item_id")['count'].sum().to_frame().reset_index()
            df_subset_of_item['count']=np.where(df_subset_of_item['item_id']==item,df_subset_of_item['count']-len_to_exclude,df_subset_of_item['count'])
            df_subset_of_item['prob']=df_subset_of_item['count']/total_others
            df_subset_of_item=df_subset_of_item.sort_values("prob",ascending=False).head(30)
            dict_1_item=df_subset_of_item.set_index("item_id").to_dict()['prob']
        else:
            dict_1_item={}
        dict_output.update({item:dict_1_item})
    return dict_output


# In[29]:

logging.info("Start of YY dict creation: "+str(datetime.datetime.now()))
from multiprocessing import Pool
p = Pool(processors)
result=p.map(get_dict_of_list_prod_prob_YY, list_of_list_25_subset)
overall_output={}
for res in result:
    overall_output.update(res)

p.close()
p.join()

logging.info("Done of YY dict creation: "+str(datetime.datetime.now()))


# In[30]:

del result
gc.collect()

json.dump(overall_output, open('./dict_YY_prodcut.json', 'w'))
logging.info("Done of YY dict writing: "+str(datetime.datetime.now()))


# In[31]:

###########


# In[ ]:




# In[41]:

print(datetime.datetime.now())
df_R1_train_XX=df_R1_train_XX.sort_values(['buyer_admin_id','create_order_time'],ascending=[True,True])

df_temp=df_R1_train_XX[['buyer_admin_id','create_order_time']].drop_duplicates()
new_rank=[]
for buyer_id,df_group in df_temp.groupby("buyer_admin_id"):
    new_rank.extend([x+1 for x in range(len(df_group))])
        
df_temp['new_rank']=new_rank
print(datetime.datetime.now())

df_R1_train_XX=pd.merge(df_R1_train_XX,df_temp,on=['buyer_admin_id','create_order_time'])
print(datetime.datetime.now())

logging.info("Done of XX df sorting rerank: "+str(datetime.datetime.now()))


# In[42]:

df_XX_id_purchased_count=df_R1_train_XX.groupby("item_id")['buyer_admin_id'].nunique().to_frame().reset_index().sort_values("buyer_admin_id",ascending=False)
item_list_hold_XX=df_XX_id_purchased_count[df_XX_id_purchased_count['buyer_admin_id']>=28]['item_id'].tolist()
print("len(item_list_hold_XX)",len(item_list_hold_XX))


# In[43]:

len_per_subset=int(np.ceil(len(item_list_hold_XX)/processors))
list_of_list_25_subset=[]
for i in range(25):
    lower_boundry=i*len_per_subset
    upper_boundry=(i+1)*len_per_subset
    list_of_list_25_subset=list_of_list_25_subset+[item_list_hold_XX[lower_boundry:upper_boundry]]
len(list_of_list_25_subset)


# In[44]:

df_R1_train_XX_iterration=df_R1_train_XX[['buyer_admin_id','item_id','new_rank']]
df_R1_train_XX_iterration['count']=1
df_R1_train_XX_iterration=df_R1_train_XX_iterration.groupby(['buyer_admin_id','item_id','new_rank'])['count'].sum().to_frame().reset_index()



# In[45]:

def get_dict_of_list_prod_prob_XX(list_input):
    dict_output={}
    for item in list_input:

        df_subset_of_item=df_R1_train_XX_iterration.loc[df_R1_train_XX_iterration['item_id']==item,['buyer_admin_id','new_rank']].drop_duplicates()
        len_to_exclude=len(df_subset_of_item)
        
        df_subset_of_item_2=df_subset_of_item.copy()
        df_subset_of_item_2['new_rank']=df_subset_of_item_2['new_rank'].apply(lambda x: x+1)
        df_subset_of_item=df_subset_of_item.append(df_subset_of_item_2)

        df_subset_of_item=pd.merge(df_subset_of_item,df_R1_train_XX_iterration,on=['buyer_admin_id','new_rank'],how="left")
        df_subset_of_item=df_subset_of_item[pd.notnull(df_subset_of_item['item_id'])]

        total_others=len(df_subset_of_item)-len_to_exclude
        if total_others>0:
            df_subset_of_item=df_subset_of_item.groupby("item_id")['count'].sum().to_frame().reset_index()
            df_subset_of_item['count']=np.where(df_subset_of_item['item_id']==item,df_subset_of_item['count']-len_to_exclude,df_subset_of_item['count'])
            df_subset_of_item['prob']=df_subset_of_item['count']/total_others
            df_subset_of_item=df_subset_of_item.sort_values("prob",ascending=False).head(30)
            dict_1_item=df_subset_of_item.set_index("item_id").to_dict()['prob']
        else:
            dict_1_item={}
        dict_output.update({item:dict_1_item})
    return dict_output


# In[ ]:

logging.info("Start of XX dict creating: "+str(datetime.datetime.now()))

p = Pool(processors)
result=p.map(get_dict_of_list_prod_prob_XX, list_of_list_25_subset)
overall_output={}
for res in result:
    overall_output.update(res)

p.close()
p.join()

logging.info("Done of XX dict creation: "+str(datetime.datetime.now()))


# In[ ]:

json.dump(overall_output, open('./dict_XX_prodcut.json', 'w'))
logging.info("Done of XX dict writing: "+str(datetime.datetime.now()))


# In[ ]:



