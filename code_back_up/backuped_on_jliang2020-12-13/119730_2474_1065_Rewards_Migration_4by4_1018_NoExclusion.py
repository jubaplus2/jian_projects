
# coding: utf-8

# In[2]:

import pandas as pd
import os
import numpy as np
import datetime
import gc


# In[ ]:

rewards_sales=pd.read_csv("/home/jian/Projects/Big_Lots/Loyal_members/loyalty_sales_data/From_Sp/combinedtransactions_0811.csv",
                            dtype=str,usecols=['customer_id_hashed','transaction_date','transaction_id','location_id','total_transaction_amt'])
rewards_sales['transaction_date']=rewards_sales['transaction_date'].apply(lambda x: datetime.datetime.strptime(x,"%Y-%m-%d").date())
rewards_sales=rewards_sales.drop_duplicates()
rewards_sales=rewards_sales[rewards_sales['location_id']!="6990"]


# In[ ]:




# In[ ]:

rewards_sales_2=pd.read_table("/home/jian/Projects/Big_Lots/Loyal_members/loyalty_sales_data/MediaStormSalesBiWeekly20180828-131144-584.txt",
                             dtype=str,sep="|",usecols=['customer_id_hashed','transaction_dt','transaction_id','location_id','total_transaction_amt'])
rewards_sales_2=rewards_sales_2.drop_duplicates()
rewards_sales_2=rewards_sales_2.rename(columns={"transaction_dt":"transaction_date"})
rewards_sales_2=rewards_sales_2[rewards_sales_2['location_id']!="6990"]
rewards_sales_2['transaction_date']=rewards_sales_2['transaction_date'].apply(lambda x: datetime.datetime.strptime(x,"%Y-%m-%d").date())
rewards_sales=rewards_sales.append(rewards_sales_2)
del rewards_sales_2

rewards_sales_3=pd.read_table("/home/jian/Projects/Big_Lots/Loyal_members/loyalty_sales_data/MediaStormSalesBiWeekly20180911-133008-590.txt",
                             dtype=str,sep="|",usecols=['customer_id_hashed','transaction_dt','transaction_id','location_id','total_transaction_amt'])
rewards_sales_3=rewards_sales_3.drop_duplicates()
rewards_sales_3=rewards_sales_3.rename(columns={"transaction_dt":"transaction_date"})
rewards_sales_3=rewards_sales_3[rewards_sales_3['location_id']!="6990"]
rewards_sales_3['transaction_date']=rewards_sales_3['transaction_date'].apply(lambda x: datetime.datetime.strptime(x,"%Y-%m-%d").date())
rewards_sales=rewards_sales.append(rewards_sales_3)
del rewards_sales_3

rewards_sales_4=pd.read_table("/home/jian/Projects/Big_Lots/Loyal_members/loyalty_sales_data/MediaStormSalesBiWeekly20180925-132958-543.txt",
                             dtype=str,sep="|",usecols=['customer_id_hashed','transaction_dt','transaction_id','location_id','total_transaction_amt'])
rewards_sales_4=rewards_sales_4.drop_duplicates()
rewards_sales_4=rewards_sales_4.rename(columns={"transaction_dt":"transaction_date"})
rewards_sales_4=rewards_sales_4[rewards_sales_4['location_id']!="6990"]
rewards_sales_4['transaction_date']=rewards_sales_4['transaction_date'].apply(lambda x: datetime.datetime.strptime(x,"%Y-%m-%d").date())
rewards_sales=rewards_sales.append(rewards_sales_4)
del rewards_sales_4

rewards_sales_5=pd.read_table("/home/jian/Projects/Big_Lots/Loyal_members/loyalty_sales_data/MediaStormSalesBiWeekly20181009-132856-945.txt",
                             dtype=str,sep="|",usecols=['customer_id_hashed','transaction_dt','transaction_id','location_id','total_transaction_amt'])
rewards_sales_5=rewards_sales_5.drop_duplicates()
rewards_sales_5=rewards_sales_5.rename(columns={"transaction_dt":"transaction_date"})
rewards_sales_5=rewards_sales_5[rewards_sales_5['location_id']!="6990"]
rewards_sales_5['transaction_date']=rewards_sales_5['transaction_date'].apply(lambda x: datetime.datetime.strptime(x,"%Y-%m-%d").date())
rewards_sales=rewards_sales.append(rewards_sales_5)
del rewards_sales_5


# In[ ]:

rewards_sales=rewards_sales.drop_duplicates()
rewards_sales['total_transaction_amt']=rewards_sales['total_transaction_amt'].astype(float)
# exclude_transaction=rewards_sales[rewards_sales['total_transaction_amt']<=0]
rewards_sales=rewards_sales[rewards_sales['total_transaction_amt']>=-400000]
rewards_sales=rewards_sales.reset_index()
del rewards_sales['index']
gc.collect()


# In[ ]:

len(rewards_sales['transaction_date'].unique())


# In[ ]:

recent_12_month_id_count=len(rewards_sales[(rewards_sales['total_transaction_amt']>0) & (rewards_sales['transaction_date']>(datetime.date(2018,9,30)-datetime.timedelta(days=52*7)))]['customer_id_hashed'].unique())
recent_18_month_id_count=len(rewards_sales[(rewards_sales['total_transaction_amt']>0) & (rewards_sales['transaction_date']>(datetime.date(2018,9,30)-datetime.timedelta(days=78*7)))]['customer_id_hashed'].unique())
recent_24_month_id_count=len(rewards_sales[(rewards_sales['total_transaction_amt']>0) & (rewards_sales['transaction_date']>(datetime.date(2018,9,30)-datetime.timedelta(days=104*7)))]['customer_id_hashed'].unique())

print("recent_12_month_id_count: "+str(recent_12_month_id_count))
print("recent_18_month_id_count: "+str(recent_18_month_id_count))
print("recent_24_month_id_count: "+str(recent_24_month_id_count))


# In[5]:

df_count_ids=pd.DataFrame({"Time_Range":['Recent_52_Weeks_to_20180930','Recent_78_Weeks_to_20180930','Recent_104_Weeks_to_20180930'],"Rewards_unique_id_count":[recent_12_month_id_count,recent_18_month_id_count,recent_24_month_id_count]},index=[0,1,2])


# In[ ]:

rewards_sales=rewards_sales[(rewards_sales['transaction_date']>=datetime.date(2018,4,1)) & (rewards_sales['transaction_date']<=datetime.date(2018,9,30))]
rewards_sales=rewards_sales.reset_index()
del rewards_sales['index']


# # 2018_July_1_Split

# In[ ]:

rewards_sales_2018_Apr_June=rewards_sales[(rewards_sales['transaction_date']>=datetime.date(2018,4,1)) & (rewards_sales['transaction_date']<=datetime.date(2018,6,30))]
rewards_sales_2018_July_Sep=rewards_sales[(rewards_sales['transaction_date']>=datetime.date(2018,7,1)) & (rewards_sales['transaction_date']<=datetime.date(2018,9,30))]


# In[ ]:

# 91 days vs 92 days


# In[ ]:

# >0 only

rewards_sales_2018_Apr_June_Positive=rewards_sales_2018_Apr_June[rewards_sales_2018_Apr_June['total_transaction_amt']>0]
rewards_sales_2018_July_Sep_Positive=rewards_sales_2018_July_Sep[rewards_sales_2018_July_Sep['total_transaction_amt']>0]


# In[ ]:

Positive_Apr_June_transaction_by_id=rewards_sales_2018_Apr_June_Positive.groupby(['customer_id_hashed'])['transaction_id'].count().to_frame().reset_index()
'''
Pctl_99_Apr_June=np.percentile(Positive_Apr_June_transaction_by_id['transaction_id'],99)
# Cut of at 10
exclude_Positive_Apr_June_transaction_by_id=Positive_Apr_June_transaction_by_id[Positive_Apr_June_transaction_by_id['transaction_id']>10]
Positive_Apr_June_transaction_by_id=Positive_Apr_June_transaction_by_id[Positive_Apr_June_transaction_by_id['transaction_id']<=10]

Pctl_33_Apr_June=np.percentile(Positive_Apr_June_transaction_by_id['transaction_id'],33) #1
Pctl_66_Apr_June=np.percentile(Positive_Apr_June_transaction_by_id['transaction_id'],66) #2
'''

# Changed to no Exclusion


# In[ ]:

Positive_July_Sep_transaction_by_id=rewards_sales_2018_July_Sep_Positive.groupby(['customer_id_hashed'])['transaction_id'].count().to_frame().reset_index()
'''
Pctl_99_July_Sep=np.percentile(Positive_July_Sep_transaction_by_id['transaction_id'],99)
# Cut of at 11
exclude_Positive_July_Sep_transaction_by_id=Positive_July_Sep_transaction_by_id[Positive_July_Sep_transaction_by_id['transaction_id']>11]
Positive_July_Sep_transaction_by_id=Positive_July_Sep_transaction_by_id[Positive_July_Sep_transaction_by_id['transaction_id']<=11]

Pctl_33_July_Sep=np.percentile(Positive_July_Sep_transaction_by_id['transaction_id'],33) #1
Pctl_66_July_Sep=np.percentile(Positive_July_Sep_transaction_by_id['transaction_id'],66) #2

print("Transacted once from April 1 to June 30",Positive_Apr_June_transaction_by_id[Positive_Apr_June_transaction_by_id['transaction_id']==1].shape[0])
print("Transacted twice from April 1 to June 30",Positive_Apr_June_transaction_by_id[Positive_Apr_June_transaction_by_id['transaction_id']==2].shape[0])
print("Transacted more than 2 from April 1 to June 30",Positive_Apr_June_transaction_by_id[Positive_Apr_June_transaction_by_id['transaction_id']>2].shape[0])
'''
# Changed to No Exclusion


# In[ ]:

Positive_Apr_June_transaction_by_id['id_Group_Apr_June']=np.where(Positive_Apr_June_transaction_by_id['transaction_id']==1,"L1",
                                                                  np.where(Positive_Apr_June_transaction_by_id['transaction_id']==2,"M1","H1")
                                                                 )
Positive_July_Sep_transaction_by_id['id_Group_July_Sep']=np.where(Positive_July_Sep_transaction_by_id['transaction_id']==1,"L2",
                                                                  np.where(Positive_July_Sep_transaction_by_id['transaction_id']==2,"M2","H2")
                                                                 )


# In[ ]:

Positive_Apr_June_transaction_by_id=Positive_Apr_June_transaction_by_id.rename(columns={"transaction_id":"transaction_Apr_June"})
Positive_July_Sep_transaction_by_id=Positive_July_Sep_transaction_by_id.rename(columns={"transaction_id":"transaction_July_Sep"})


Positive_both_transaction_by_id=pd.merge(Positive_Apr_June_transaction_by_id,Positive_July_Sep_transaction_by_id,on="customer_id_hashed",how="outer")



# In[ ]:

Positive_both_transaction_by_id['transaction_Apr_June']=Positive_both_transaction_by_id['transaction_Apr_June'].fillna(0.0)
Positive_both_transaction_by_id['transaction_July_Sep']=Positive_both_transaction_by_id['transaction_July_Sep'].fillna(0.0)

Positive_both_transaction_by_id['id_Group_Apr_June']=Positive_both_transaction_by_id['id_Group_Apr_June'].fillna("N1")
Positive_both_transaction_by_id['id_Group_July_Sep']=Positive_both_transaction_by_id['id_Group_July_Sep'].fillna("N2")


# In[ ]:

Positive_both_transaction_by_id.shape


# In[ ]:

transaction_matrix=Positive_both_transaction_by_id.copy()
transaction_matrix['label']=transaction_matrix['id_Group_Apr_June']+"|"+transaction_matrix['id_Group_July_Sep']
transaction_matrix=transaction_matrix.groupby(['label'])['customer_id_hashed'].count().to_frame().reset_index()
transaction_matrix=transaction_matrix.rename(columns={"customer_id_hashed":"id_count"})
transaction_matrix['id_Group_Apr_June']=transaction_matrix['label'].apply(lambda x: x.split("|")[0])
transaction_matrix['id_Group_July_Sep']=transaction_matrix['label'].apply(lambda x: x.split("|")[1])
transaction_matrix=transaction_matrix.pivot(index="id_Group_Apr_June",columns="id_Group_July_Sep",values="id_count")
transaction_matrix=transaction_matrix.reset_index().fillna(0.0)


# In[ ]:

transaction_matrix=transaction_matrix[transaction_matrix['id_Group_Apr_June']=="N1"].append(transaction_matrix[transaction_matrix['id_Group_Apr_June']=="L1"]).append(transaction_matrix[transaction_matrix['id_Group_Apr_June']=="M1"]).append(transaction_matrix[transaction_matrix['id_Group_Apr_June']=="H1"])
transaction_matrix=transaction_matrix[['id_Group_Apr_June','N2',"L2","M2","H2"]]


# In[ ]:

user_group=Positive_both_transaction_by_id[['customer_id_hashed','id_Group_Apr_June','id_Group_July_Sep']]
user_group['lable']=user_group['id_Group_Apr_June']+"|"+user_group['id_Group_July_Sep']
del user_group['id_Group_Apr_June']
del user_group['id_Group_July_Sep']



# # Saels

# In[ ]:

sales_by_id_Apr_June=rewards_sales_2018_Apr_June.groupby(['customer_id_hashed'])['total_transaction_amt'].sum().to_frame().reset_index()
sales_by_id_July_Sep=rewards_sales_2018_July_Sep.groupby(['customer_id_hashed'])['total_transaction_amt'].sum().to_frame().reset_index()


# In[ ]:

sales_by_id_Apr_June=pd.merge(sales_by_id_Apr_June,user_group,on="customer_id_hashed",how="right")
sales_by_id_Apr_June=sales_by_id_Apr_June.fillna(0)
sales_by_id_Apr_June=sales_by_id_Apr_June.rename(columns={"total_transaction_amt":"sales_Apr_June"})
sales_by_group_Apr_June=sales_by_id_Apr_June.groupby(['lable'])['sales_Apr_June'].sum().to_frame().reset_index()
sales_by_group_Apr_June['id_Group_Apr_June']=sales_by_group_Apr_June['lable'].apply(lambda x: x.split("|")[0])
sales_by_group_Apr_June['id_Group_July_Sep']=sales_by_group_Apr_June['lable'].apply(lambda x: x.split("|")[1])
del sales_by_group_Apr_June['lable']
sales_by_group_Apr_June_matrix=sales_by_group_Apr_June.pivot(index='id_Group_Apr_June',columns='id_Group_July_Sep',values="sales_Apr_June")
sales_by_group_Apr_June_matrix=sales_by_group_Apr_June_matrix.reset_index()
sales_by_group_Apr_June_matrix=sales_by_group_Apr_June_matrix[sales_by_group_Apr_June_matrix['id_Group_Apr_June']=="N1"].append(sales_by_group_Apr_June_matrix[sales_by_group_Apr_June_matrix['id_Group_Apr_June']=="L1"]).append(sales_by_group_Apr_June_matrix[sales_by_group_Apr_June_matrix['id_Group_Apr_June']=="M1"]).append(sales_by_group_Apr_June_matrix[sales_by_group_Apr_June_matrix['id_Group_Apr_June']=="H1"])
sales_by_group_Apr_June_matrix=sales_by_group_Apr_June_matrix[['id_Group_Apr_June','N2','L2','M2','H2']]
sales_by_group_Apr_June_matrix=sales_by_group_Apr_June_matrix.fillna(0)
sales_by_group_Apr_June_matrix=sales_by_group_Apr_June_matrix.rename(columns={"id_Group_Apr_June":"Sales_April_June"})

sales_by_id_July_Sep=pd.merge(sales_by_id_July_Sep,user_group,on="customer_id_hashed",how="right")
sales_by_id_July_Sep=sales_by_id_July_Sep.fillna(0)
sales_by_id_July_Sep=sales_by_id_July_Sep.rename(columns={"total_transaction_amt":"sales_July_Sep"})
sales_by_group_July_Sep=sales_by_id_July_Sep.groupby(['lable'])['sales_July_Sep'].sum().to_frame().reset_index()
sales_by_group_July_Sep['id_Group_Apr_June']=sales_by_group_July_Sep['lable'].apply(lambda x: x.split("|")[0])
sales_by_group_July_Sep['id_Group_July_Sep']=sales_by_group_July_Sep['lable'].apply(lambda x: x.split("|")[1])
del sales_by_group_July_Sep['lable']
sales_by_group_July_Sep_matrix=sales_by_group_July_Sep.pivot(index='id_Group_Apr_June',columns='id_Group_July_Sep',values="sales_July_Sep")
sales_by_group_July_Sep_matrix=sales_by_group_July_Sep_matrix.reset_index()
sales_by_group_July_Sep_matrix=sales_by_group_July_Sep_matrix[sales_by_group_July_Sep_matrix['id_Group_Apr_June']=="N1"].append(sales_by_group_July_Sep_matrix[sales_by_group_July_Sep_matrix['id_Group_Apr_June']=="L1"]).append(sales_by_group_July_Sep_matrix[sales_by_group_July_Sep_matrix['id_Group_Apr_June']=="M1"]).append(sales_by_group_July_Sep_matrix[sales_by_group_July_Sep_matrix['id_Group_Apr_June']=="H1"])
sales_by_group_July_Sep_matrix=sales_by_group_July_Sep_matrix[['id_Group_Apr_June','N2','L2','M2','H2']]
sales_by_group_July_Sep_matrix=sales_by_group_July_Sep_matrix.fillna(0)
sales_by_group_July_Sep_matrix=sales_by_group_July_Sep_matrix.rename(columns={"id_Group_Apr_June":"Sales_July_Sep"})


# # Transaction

# In[ ]:

trans_by_id_Apr_June=rewards_sales_2018_Apr_June_Positive.groupby(['customer_id_hashed'])['total_transaction_amt'].count().to_frame().reset_index()
trans_by_id_July_Sep=rewards_sales_2018_July_Sep_Positive.groupby(['customer_id_hashed'])['total_transaction_amt'].count().to_frame().reset_index()


# In[ ]:

trans_by_id_Apr_June=pd.merge(trans_by_id_Apr_June,user_group,on="customer_id_hashed",how="right")
trans_by_id_Apr_June=trans_by_id_Apr_June.fillna(0)
trans_by_id_Apr_June=trans_by_id_Apr_June.rename(columns={"total_transaction_amt":"trans_Apr_June"})
trans_by_group_Apr_June=trans_by_id_Apr_June.groupby(['lable'])['trans_Apr_June'].sum().to_frame().reset_index()
trans_by_group_Apr_June['id_Group_Apr_June']=trans_by_group_Apr_June['lable'].apply(lambda x: x.split("|")[0])
trans_by_group_Apr_June['id_Group_July_Sep']=trans_by_group_Apr_June['lable'].apply(lambda x: x.split("|")[1])
del trans_by_group_Apr_June['lable']
trans_by_group_Apr_June_matrix=trans_by_group_Apr_June.pivot(index='id_Group_Apr_June',columns='id_Group_July_Sep',values="trans_Apr_June")
trans_by_group_Apr_June_matrix=trans_by_group_Apr_June_matrix.reset_index()
trans_by_group_Apr_June_matrix=trans_by_group_Apr_June_matrix[trans_by_group_Apr_June_matrix['id_Group_Apr_June']=="N1"].append(trans_by_group_Apr_June_matrix[trans_by_group_Apr_June_matrix['id_Group_Apr_June']=="L1"]).append(trans_by_group_Apr_June_matrix[trans_by_group_Apr_June_matrix['id_Group_Apr_June']=="M1"]).append(trans_by_group_Apr_June_matrix[trans_by_group_Apr_June_matrix['id_Group_Apr_June']=="H1"])
trans_by_group_Apr_June_matrix=trans_by_group_Apr_June_matrix[['id_Group_Apr_June','N2','L2','M2','H2']]
trans_by_group_Apr_June_matrix=trans_by_group_Apr_June_matrix.fillna(0)
trans_by_group_Apr_June_matrix=trans_by_group_Apr_June_matrix.rename(columns={"id_Group_Apr_June":"trans_April_June"})

trans_by_id_July_Sep=pd.merge(trans_by_id_July_Sep,user_group,on="customer_id_hashed",how="right")
trans_by_id_July_Sep=trans_by_id_July_Sep.fillna(0)
trans_by_id_July_Sep=trans_by_id_July_Sep.rename(columns={"total_transaction_amt":"trans_July_Sep"})
trans_by_group_July_Sep=trans_by_id_July_Sep.groupby(['lable'])['trans_July_Sep'].sum().to_frame().reset_index()
trans_by_group_July_Sep['id_Group_Apr_June']=trans_by_group_July_Sep['lable'].apply(lambda x: x.split("|")[0])
trans_by_group_July_Sep['id_Group_July_Sep']=trans_by_group_July_Sep['lable'].apply(lambda x: x.split("|")[1])
del trans_by_group_July_Sep['lable']
trans_by_group_July_Sep_matrix=trans_by_group_July_Sep.pivot(index='id_Group_Apr_June',columns='id_Group_July_Sep',values="trans_July_Sep")
trans_by_group_July_Sep_matrix=trans_by_group_July_Sep_matrix.reset_index()
trans_by_group_July_Sep_matrix=trans_by_group_July_Sep_matrix[trans_by_group_July_Sep_matrix['id_Group_Apr_June']=="N1"].append(trans_by_group_July_Sep_matrix[trans_by_group_July_Sep_matrix['id_Group_Apr_June']=="L1"]).append(trans_by_group_July_Sep_matrix[trans_by_group_July_Sep_matrix['id_Group_Apr_June']=="M1"]).append(trans_by_group_July_Sep_matrix[trans_by_group_July_Sep_matrix['id_Group_Apr_June']=="H1"])
trans_by_group_July_Sep_matrix=trans_by_group_July_Sep_matrix[['id_Group_Apr_June','N2','L2','M2','H2']]
trans_by_group_July_Sep_matrix=trans_by_group_July_Sep_matrix.fillna(0)
trans_by_group_July_Sep_matrix=trans_by_group_July_Sep_matrix.rename(columns={"id_Group_Apr_June":"trans_July_Sep"})


# In[ ]:

writer=pd.ExcelWriter("/home/jian/Projects/Big_Lots/Loyal_members/Segment_Movement_analysis/output/Rewards_Migration_4by4_JL_"+str(datetime.datetime.now().date())+".xlsx",engine="xlsxwriter")


# In[ ]:

transaction_matrix.to_excel(writer,"id_group",index=False)
df_count_ids.to_excel(writer,"recent_x_months_id",index=False)
sales_by_group_Apr_June_matrix.to_excel(writer,"Sales_Apr_June_Period1",index=False)
sales_by_group_July_Sep_matrix.to_excel(writer,"Sales_July_Sep_Period2",index=False)
trans_by_group_Apr_June_matrix.to_excel(writer,"Trans_Apr_June_Period1",index=False)
trans_by_group_July_Sep_matrix.to_excel(writer,"Trans_July_Sep_Period2",index=False)
writer.save()


# In[ ]:




# In[ ]:




# In[54]:

transaction_matrix.to_csv("/home/jian/Projects/Big_Lots/Loyal_members/Segment_Movement_analysis/output/Rewards_Migration_4by4_1011.csv",index=False)


# In[ ]:



