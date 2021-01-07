
# coding: utf-8

# In[2]:

import pandas as pd


# In[13]:

select_cols=['customer_id_hashed','transaction_date','transaction_time','transaction_id','location_id','total_transaction_amt','total_transaction_units']
rewards_sales=pd.read_csv("/home/jian/Projects/Big_Lots/Loyal_members/loyalty_sales_data/From_Sp/combinedtransactions_0811.csv",
                            dtype=str,usecols=select_cols)
rewards_sales=rewards_sales.drop_duplicates()


# In[ ]:

rewards_sales_2=pd.read_table("/home/jian/Projects/Big_Lots/Loyal_members/loyalty_sales_data/MediaStormSalesBiWeekly20180828-131144-584.txt",
                             dtype=str,sep="|",usecols=select_cols)
rewards_sales_2=rewards_sales_2.drop_duplicates()
rewards_sales_2=rewards_sales_2.rename(columns={"transaction_dt":"transaction_date"})
rewards_sales=rewards_sales.append(rewards_sales_2)
del rewards_sales_2

rewards_sales_3=pd.read_table("/home/jian/Projects/Big_Lots/Loyal_members/loyalty_sales_data/MediaStormSalesBiWeekly20180911-133008-590.txt",
                             dtype=str,sep="|",usecols=select_cols)
rewards_sales_3=rewards_sales_3.drop_duplicates()
rewards_sales_3=rewards_sales_3.rename(columns={"transaction_dt":"transaction_date"})
rewards_sales_3['transaction_date']=rewards_sales_3['transaction_date'].apply(lambda x: datetime.datetime.strptime(x,"%Y-%m-%d").date())
rewards_sales=rewards_sales.append(rewards_sales_3)
del rewards_sales_3

rewards_sales_4=pd.read_table("/home/jian/Projects/Big_Lots/Loyal_members/loyalty_sales_data/MediaStormSalesBiWeekly20180925-132958-543.txt",
                             dtype=str,sep="|",usecols=select_cols)
rewards_sales_4=rewards_sales_4.drop_duplicates()
rewards_sales_4=rewards_sales_4.rename(columns={"transaction_dt":"transaction_date"})
rewards_sales_4['transaction_date']=rewards_sales_4['transaction_date'].apply(lambda x: datetime.datetime.strptime(x,"%Y-%m-%d").date())
rewards_sales=rewards_sales.append(rewards_sales_4)
del rewards_sales_4

rewards_sales_5=pd.read_table("/home/jian/Projects/Big_Lots/Loyal_members/loyalty_sales_data/MediaStormSalesBiWeekly20181009-132856-945.txt",
                             dtype=str,sep="|",usecols=select_cols)
rewards_sales_5=rewards_sales_5.drop_duplicates()
rewards_sales_5=rewards_sales_5.rename(columns={"transaction_dt":"transaction_date"})
rewards_sales_5['transaction_date']=rewards_sales_5['transaction_date'].apply(lambda x: datetime.datetime.strptime(x,"%Y-%m-%d").date())
rewards_sales=rewards_sales.append(rewards_sales_5)
del rewards_sales_5


# In[ ]:

rewards_sales['month']=rewards_sales['transaction_date'].apply(lambda x: x[:7])


# In[ ]:

for month,group in rewards_sales.groupby(['month']):
    del group['month']
    group.to_csv("/home/jian/Projects/Big_Lots/Loyal_members/loyalty_sales_data/Rewards_sales_by_month/rewards_sales_"+month+".csv",index=False)
    
    
    

