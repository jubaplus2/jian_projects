
# coding: utf-8

# In[1]:

import pandas as pd
import datetime
import os
import gc
import glob
import numpy as np


# In[2]:

start_week=datetime.date(2018,6,16)
week_list=[]
file_list=[]
daily_data=pd.DataFrame()
for i in range(8):
    week=start_week+datetime.timedelta(days=7*i)
    week_list=week_list+[str(week)]
    folder="/home/jian/BigLots/2018 by weeks/MediaStorm_"+str(week)+"/*.txt"
    file=[x for x in glob.glob(folder) if "Daily" in x][0]
    file_list=file_list+[file]
    df=pd.read_table(file,sep="|",dtype=str)
    df=df[df['location_id']!="6990"]
    daily_data=daily_data.append(df)
    print(week,datetime.datetime.now())
    
# can change to agg first and then app to make it faster
daily_data['transaction_dt']=daily_data['transaction_dt'].apply(lambda x: datetime.datetime.strptime(x,"%Y-%m-%d").date())
daily_data['Member_Type']=np.where(pd.isnull(daily_data['customer_id_hashed']),"Non_Rewards","Rewards")
daily_data['class_sub_class']=daily_data['class_code_id']+"|"+daily_data['subclass_id']
daily_data['subclass_transaction_amt']=daily_data['subclass_transaction_amt'].astype(float)
daily_data['subclass_transaction_units']=daily_data['subclass_transaction_units'].astype(int)


# In[ ]:




# In[3]:

product_taxonomy=pd.read_table("/home/jian/BigLots/static_files/ProductTaxonomy/MediaStormProductTaxonomy20181001-135417-040.txt",dtype=str,sep="|")
product_taxonomy['class_sub_class']=product_taxonomy['class_code_id']+"|"+product_taxonomy['subclass_id']
product_taxonomy=product_taxonomy[['class_sub_class','subclass_desc']].drop_duplicates()


# In[4]:

df_all_stores_sales_by_day=daily_data.groupby(['location_id','Member_Type','transaction_dt','class_sub_class'])['subclass_transaction_amt'].sum().to_frame().reset_index()
df_all_stores_sales_by_day['weekday']=df_all_stores_sales_by_day['transaction_dt'].apply(lambda x: x.weekday())
df_all_stores_sales_by_day['group']=np.where(df_all_stores_sales_by_day['weekday']==6,"Sunday",
                                             np.where(df_all_stores_sales_by_day['weekday']==5,"Saturday",
                                                      "Mon_Fri")
                                            )


df_all_stores_units_by_day=daily_data.groupby(['location_id','Member_Type','transaction_dt','class_sub_class'])['subclass_transaction_units'].sum().to_frame().reset_index()
df_all_stores_units_by_day['weekday']=df_all_stores_units_by_day['transaction_dt'].apply(lambda x: x.weekday())
df_all_stores_units_by_day['group']=np.where(df_all_stores_units_by_day['weekday']==6,"Sunday",
                                             np.where(df_all_stores_units_by_day['weekday']==5,"Saturday",
                                                      "Mon_Fri")
                                            )


# In[5]:

1+2


# In[ ]:

# Pass the following due to momery limitation


# In[6]:

df_all_stores_sales_by_day=pd.merge(df_all_stores_sales_by_day,product_taxonomy,on="class_sub_class",how="left")
df_all_stores_units_by_day=pd.merge(df_all_stores_units_by_day,product_taxonomy,on="class_sub_class",how="left")


# In[7]:

output_1_Saturday_Rewards_sales=df_all_stores_sales_by_day[(df_all_stores_sales_by_day['Member_Type']=="Rewards") & (df_all_stores_sales_by_day['group']=="Saturday")]
output_1_Saturday_Rewards_sales=output_1_Saturday_Rewards_sales.groupby(['subclass_desc','class_sub_class','group'])['subclass_transaction_amt'].sum().to_frame().reset_index().sort_values(['subclass_transaction_amt'],ascending=False).reset_index()
del output_1_Saturday_Rewards_sales['index']

output_2_Sunday_Rewards_sales=df_all_stores_sales_by_day[(df_all_stores_sales_by_day['Member_Type']=="Rewards") & (df_all_stores_sales_by_day['group']=="Sunday")]
output_2_Sunday_Rewards_sales=output_2_Sunday_Rewards_sales.groupby(['subclass_desc','class_sub_class','group'])['subclass_transaction_amt'].sum().to_frame().reset_index().sort_values(['subclass_transaction_amt'],ascending=False).reset_index()
del output_2_Sunday_Rewards_sales['index']

output_3_Mon_Fri_Rewards_sales=df_all_stores_sales_by_day[(df_all_stores_sales_by_day['Member_Type']=="Rewards") & (df_all_stores_sales_by_day['group']=="Mon_Fri")]
output_3_Mon_Fri_Rewards_sales=output_3_Mon_Fri_Rewards_sales.groupby(['subclass_desc','class_sub_class','group'])['subclass_transaction_amt'].sum().to_frame().reset_index().sort_values(['subclass_transaction_amt'],ascending=False).reset_index()
del output_3_Mon_Fri_Rewards_sales['index']

output_4_Saturday_NonRewards_sales=df_all_stores_sales_by_day[(df_all_stores_sales_by_day['Member_Type']=="Non_Rewards") & (df_all_stores_sales_by_day['group']=="Saturday")]
output_4_Saturday_NonRewards_sales=output_4_Saturday_NonRewards_sales.groupby(['subclass_desc','class_sub_class','group'])['subclass_transaction_amt'].sum().to_frame().reset_index().sort_values(['subclass_transaction_amt'],ascending=False).reset_index()
del output_4_Saturday_NonRewards_sales['index']

output_5_Sunday_NonRewards_sales=df_all_stores_sales_by_day[(df_all_stores_sales_by_day['Member_Type']=="Non_Rewards") & (df_all_stores_sales_by_day['group']=="Sunday")]
output_5_Sunday_NonRewards_sales=output_5_Sunday_NonRewards_sales.groupby(['subclass_desc','class_sub_class','group'])['subclass_transaction_amt'].sum().to_frame().reset_index().sort_values(['subclass_transaction_amt'],ascending=False).reset_index()
del output_5_Sunday_NonRewards_sales['index']

output_6_Mon_Fri_NonRewards_sales=df_all_stores_sales_by_day[(df_all_stores_sales_by_day['Member_Type']=="Non_Rewards") & (df_all_stores_sales_by_day['group']=="Mon_Fri")]
output_6_Mon_Fri_NonRewards_sales=output_6_Mon_Fri_NonRewards_sales.groupby(['subclass_desc','class_sub_class','group'])['subclass_transaction_amt'].sum().to_frame().reset_index().sort_values(['subclass_transaction_amt'],ascending=False).reset_index()
del output_6_Mon_Fri_NonRewards_sales['index']


# In[8]:

df_all_stores_units_by_day.head(2)


# In[9]:

output_1_Saturday_Rewards_units=df_all_stores_units_by_day[(df_all_stores_units_by_day['Member_Type']=="Rewards") & (df_all_stores_units_by_day['group']=="Saturday")]
output_1_Saturday_Rewards_units=output_1_Saturday_Rewards_units.groupby(['subclass_desc','class_sub_class','group'])['subclass_transaction_units'].sum().to_frame().reset_index().sort_values(['subclass_transaction_units'],ascending=False).reset_index()
del output_1_Saturday_Rewards_units['index']

output_2_Sunday_Rewards_units=df_all_stores_units_by_day[(df_all_stores_units_by_day['Member_Type']=="Rewards") & (df_all_stores_units_by_day['group']=="Sunday")]
output_2_Sunday_Rewards_units=output_2_Sunday_Rewards_units.groupby(['subclass_desc','class_sub_class','group'])['subclass_transaction_units'].sum().to_frame().reset_index().sort_values(['subclass_transaction_units'],ascending=False).reset_index()
del output_2_Sunday_Rewards_units['index']

output_3_Mon_Fri_Rewards_units=df_all_stores_units_by_day[(df_all_stores_units_by_day['Member_Type']=="Rewards") & (df_all_stores_units_by_day['group']=="Mon_Fri")]
output_3_Mon_Fri_Rewards_units=output_3_Mon_Fri_Rewards_units.groupby(['subclass_desc','class_sub_class','group'])['subclass_transaction_units'].sum().to_frame().reset_index().sort_values(['subclass_transaction_units'],ascending=False).reset_index()
del output_3_Mon_Fri_Rewards_units['index']

output_4_Saturday_NonRewards_units=df_all_stores_units_by_day[(df_all_stores_units_by_day['Member_Type']=="Non_Rewards") & (df_all_stores_units_by_day['group']=="Saturday")]
output_4_Saturday_NonRewards_units=output_4_Saturday_NonRewards_units.groupby(['subclass_desc','class_sub_class','group'])['subclass_transaction_units'].sum().to_frame().reset_index().sort_values(['subclass_transaction_units'],ascending=False).reset_index()
del output_4_Saturday_NonRewards_units['index']

output_5_Sunday_NonRewards_units=df_all_stores_units_by_day[(df_all_stores_units_by_day['Member_Type']=="Non_Rewards") & (df_all_stores_units_by_day['group']=="Sunday")]
output_5_Sunday_NonRewards_units=output_5_Sunday_NonRewards_units.groupby(['subclass_desc','class_sub_class','group'])['subclass_transaction_units'].sum().to_frame().reset_index().sort_values(['subclass_transaction_units'],ascending=False).reset_index()
del output_5_Sunday_NonRewards_units['index']

output_6_Mon_Fri_NonRewards_units=df_all_stores_units_by_day[(df_all_stores_units_by_day['Member_Type']=="Non_Rewards") & (df_all_stores_units_by_day['group']=="Mon_Fri")]
output_6_Mon_Fri_NonRewards_units=output_6_Mon_Fri_NonRewards_units.groupby(['subclass_desc','class_sub_class','group'])['subclass_transaction_units'].sum().to_frame().reset_index().sort_values(['subclass_transaction_units'],ascending=False).reset_index()
del output_6_Mon_Fri_NonRewards_units['index']


# In[10]:

writer=pd.ExcelWriter("/home/jian/Projects/Big_Lots/Analysis/2018_Q3/Top_50_Products/BL_Products_Sales_by_rewards_mem_weekday_JL_"+str(datetime.datetime.now().date())+".xlsx", engine="xlsxwriter")
output_1_Saturday_Rewards_sales.to_excel(writer,'output_1_Saturday_R_sales',index=False)
output_2_Sunday_Rewards_sales.to_excel(writer,'output_2_Sunday_R_sales',index=False)
output_3_Mon_Fri_Rewards_sales.to_excel(writer,'output_3_Mon_Fri_R_sales',index=False)
output_4_Saturday_NonRewards_sales.to_excel(writer,'output_4_Saturday_NonR_sales',index=False)
output_5_Sunday_NonRewards_sales.to_excel(writer,'output_5_Sunday_NonR_sales',index=False)
output_6_Mon_Fri_NonRewards_sales.to_excel(writer,'output_6_Mon_Fri_NonR_sales',index=False)
writer.save()

del output_1_Saturday_Rewards_sales
del output_2_Sunday_Rewards_sales
del output_3_Mon_Fri_Rewards_sales
del output_4_Saturday_NonRewards_sales
del output_5_Sunday_NonRewards_sales
del output_6_Mon_Fri_NonRewards_sales


# In[11]:

writer=pd.ExcelWriter("/home/jian/Projects/Big_Lots/Analysis/2018_Q3/Top_50_Products/BL_Products_Units_by_rewards_mem_weekday_JL_"+str(datetime.datetime.now().date())+".xlsx", engine="xlsxwriter")
output_1_Saturday_Rewards_units.to_excel(writer,'output_1_Saturday_R_units',index=False)
output_2_Sunday_Rewards_units.to_excel(writer,'output_2_Sunday_R_units',index=False)
output_3_Mon_Fri_Rewards_units.to_excel(writer,'output_3_Mon_Fri_R_units',index=False)
output_4_Saturday_NonRewards_units.to_excel(writer,'output_4_Saturday_NonR_units',index=False)
output_5_Sunday_NonRewards_units.to_excel(writer,'output_5_Sunday_NonR_units',index=False)
output_6_Mon_Fri_NonRewards_units.to_excel(writer,'output_6_Mon_Fri_NonR_units',index=False)
writer.save()

del output_1_Saturday_Rewards_units
del output_2_Sunday_Rewards_units
del output_3_Mon_Fri_Rewards_units
del output_4_Saturday_NonRewards_units
del output_5_Sunday_NonRewards_units
del output_6_Mon_Fri_NonRewards_units


# In[12]:

del df_all_stores_units_by_day
del df_all_stores_sales_by_day

gc.collect()


# In[7]:

# Keep rewards only to save memary 
daily_data=daily_data[~pd.isnull(daily_data['customer_id_hashed'])]
daily_data=daily_data.reset_index()
del daily_data['index']


# # rewards by deciles

# In[8]:

df_deciles_SP=pd.read_csv("/home/jian/Projects/Big_Lots/Loyal_members/Segment_Movement_analysis/Data_From_Sp/df_crm_finalscore_0922_data.csv",
                          dtype=str,usecols=['customer_id_hashed','frmindex','zipcodegroup'])
df_deciles_SP=df_deciles_SP.drop_duplicates()


# In[9]:

daily_data=pd.merge(daily_data,df_deciles_SP,on="customer_id_hashed",how="left")


# In[10]:

daily_data_rewards_decile=daily_data[~pd.isnull(daily_data['frmindex'])]
del daily_data


# In[11]:

def count_unit(x):
    y=len(set(x))
    return y
daily_data_rewards_decile_count_mem=daily_data_rewards_decile.groupby(['frmindex'])['customer_id_hashed'].apply(count_unit).to_frame().reset_index()
daily_data_rewards_decile_count_mem=daily_data_rewards_decile_count_mem.rename(columns={"customer_id_hashed":"members_count"})

daily_data_rewards_PST_count_mem=daily_data_rewards_decile.groupby(['zipcodegroup'])['customer_id_hashed'].apply(count_unit).to_frame().reset_index()
daily_data_rewards_PST_count_mem=daily_data_rewards_PST_count_mem.rename(columns={"customer_id_hashed":"members_count"})


# In[ ]:

daily_data_rewards_decile_by_day_pro_units=daily_data_rewards_decile.groupby(['location_id','frmindex','transaction_dt','class_sub_class'])['subclass_transaction_units'].sum().to_frame().reset_index()
daily_data_rewards_decile_by_day_pro_sales=daily_data_rewards_decile.groupby(['location_id','frmindex','transaction_dt','class_sub_class'])['subclass_transaction_amt'].sum().to_frame().reset_index()

daily_data_rewards_decile_by_day_pro_units=pd.merge(daily_data_rewards_decile_by_day_pro_units,product_taxonomy,on="class_sub_class",how="left")
daily_data_rewards_decile_by_day_pro_sales=pd.merge(daily_data_rewards_decile_by_day_pro_sales,product_taxonomy,on="class_sub_class",how="left")


# In[ ]:

daily_data_rewards_decile_by_day_pro_units['weekday']=daily_data_rewards_decile_by_day_pro_units['transaction_dt'].apply(lambda x: x.weekday())
daily_data_rewards_decile_by_day_pro_units['group']=np.where(daily_data_rewards_decile_by_day_pro_units['weekday']==6,"Sunday",
                                             np.where(daily_data_rewards_decile_by_day_pro_units['weekday']==5,"Saturday",
                                                      "Mon_Fri")
                                            )
daily_data_rewards_decile_by_day_pro_sales['weekday']=daily_data_rewards_decile_by_day_pro_sales['transaction_dt'].apply(lambda x: x.weekday())
daily_data_rewards_decile_by_day_pro_sales['group']=np.where(daily_data_rewards_decile_by_day_pro_sales['weekday']==6,"Sunday",
                                             np.where(daily_data_rewards_decile_by_day_pro_sales['weekday']==5,"Saturday",
                                                      "Mon_Fri")
                                            )

gc.collect()


# In[ ]:




# In[ ]:

def get_df_by_decile_units(df_input,weekday_group):
    df_output=df_input[df_input['group']==weekday_group]
    df_output=df_output.groupby(['subclass_desc','class_sub_class','frmindex'])['subclass_transaction_units'].sum().to_frame().reset_index().sort_values(['frmindex','subclass_transaction_units'],ascending=[True,False]).reset_index()
    del df_output['index']
    output=pd.DataFrame()
    for decile,group in df_output.groupby(['frmindex']):
        group=group.reset_index()
        del group['index']
        group['order_in_group']=[str(x+1) for x in range(len(group))]    
        output=output.append(group)
    output=output.reset_index()
    del output['index']
    output['group_rank']=output['frmindex']+"|"+output['order_in_group']        
    return output


# In[ ]:

def get_df_by_decile_sales(df_input,weekday_group):
    df_output=df_input[df_input['group']==weekday_group]
    df_output=df_output.groupby(['subclass_desc','class_sub_class','frmindex'])['subclass_transaction_amt'].sum().to_frame().reset_index().sort_values(['frmindex','subclass_transaction_amt'],ascending=[True,False]).reset_index()
    del df_output['index']
    output=pd.DataFrame()
    for decile,group in df_output.groupby(['frmindex']):
        group=group.reset_index()
        del group['index']
        group['order_in_group']=[str(x+1) for x in range(len(group))]    
        output=output.append(group)
    output=output.reset_index()
    del output['index']
    output['group_rank']=output['frmindex']+"|"+output['order_in_group']
    
    return output


# In[ ]:

def get_df_by_PST_units(df_input,weekday_group):
    df_output=df_input[df_input['group']==weekday_group]
    df_output=df_output.groupby(['subclass_desc','class_sub_class','zipcodegroup'])['subclass_transaction_units'].sum().to_frame().reset_index().sort_values(['zipcodegroup','subclass_transaction_units'],ascending=[True,False]).reset_index()
    del df_output['index']
    output=pd.DataFrame()
    for decile,group in df_output.groupby(['zipcodegroup']):
        group=group.reset_index()
        del group['index']
        group['order_in_group']=[str(x+1) for x in range(len(group))]    
        output=output.append(group)
    output=output.reset_index()
    del output['index']
    output['group_rank']=output['zipcodegroup']+"|"+output['order_in_group'] 
    return output


# In[ ]:

def get_df_by_PST_sales(df_input,weekday_group):
    df_output=df_input[df_input['group']==weekday_group]
    df_output=df_output.groupby(['subclass_desc','class_sub_class','zipcodegroup'])['subclass_transaction_amt'].sum().to_frame().reset_index().sort_values(['zipcodegroup','subclass_transaction_amt'],ascending=[True,False]).reset_index()
    del df_output['index']
    output=pd.DataFrame()
    for decile,group in df_output.groupby(['zipcodegroup']):
        group=group.reset_index()
        del group['index']
        group['order_in_group']=[str(x+1) for x in range(len(group))]    
        output=output.append(group)
    output=output.reset_index()
    del output['index']
    output['group_rank']=output['zipcodegroup']+"|"+output['order_in_group']
    return output


# In[ ]:

writer=pd.ExcelWriter("/home/jian/Projects/Big_Lots/Analysis/2018_Q3/Top_50_Products/BL_Products_by_Deciles_weekday_JL_"+str(datetime.datetime.now().date())+".xlsx", engine="xlsxwriter")

daily_data_rewards_decile_count_mem.to_excel(writer,"member_count_by_decile",index=False)

output_1_Saturday_Rewards_Decile_units=get_df_by_decile_units(daily_data_rewards_decile_by_day_pro_units,"Saturday")
output_1_Saturday_Rewards_Decile_units.to_excel(writer,"Saturday_Units",index=False)
del output_1_Saturday_Rewards_Decile_units

output_2_Sunday_Rewards_Decile_units=get_df_by_decile_units(daily_data_rewards_decile_by_day_pro_units,"Sunday")
output_2_Sunday_Rewards_Decile_units.to_excel(writer,"Sunday_Units",index=False)
del output_2_Sunday_Rewards_Decile_units

output_3_Mon_Fri_Rewards_Decile_units=get_df_by_decile_units(daily_data_rewards_decile_by_day_pro_units,"Mon_Fri")
output_3_Mon_Fri_Rewards_Decile_units.to_excel(writer,"Mon_Fri_Units",index=False)
del output_3_Mon_Fri_Rewards_Decile_units

output_4_Saturday_Rewards_Decile_sales=get_df_by_decile_sales(daily_data_rewards_decile_by_day_pro_sales,"Saturday")
output_4_Saturday_Rewards_Decile_sales.to_excel(writer,"Saturday_sales",index=False)
del output_4_Saturday_Rewards_Decile_sales

output_5_Sunday_Rewards_Decile_sales=get_df_by_decile_sales(daily_data_rewards_decile_by_day_pro_sales,"Sunday")
output_5_Sunday_Rewards_Decile_sales.to_excel(writer,"Sunday_sales",index=False)
del output_5_Sunday_Rewards_Decile_sales

output_6_Mon_Fri_Rewards_Decile_sales=get_df_by_decile_sales(daily_data_rewards_decile_by_day_pro_sales,"Mon_Fri")
output_6_Mon_Fri_Rewards_Decile_sales.to_excel(writer,"Mon_Fri_sales",index=False)
del output_6_Mon_Fri_Rewards_Decile_sales

writer.save()


# In[ ]:

daily_data_rewards_PST_by_day_pro_units=daily_data_rewards_decile.groupby(['location_id','zipcodegroup','transaction_dt','class_sub_class'])['subclass_transaction_units'].sum().to_frame().reset_index()
daily_data_rewards_PST_by_day_pro_sales=daily_data_rewards_decile.groupby(['location_id','zipcodegroup','transaction_dt','class_sub_class'])['subclass_transaction_amt'].sum().to_frame().reset_index()

daily_data_rewards_PST_by_day_pro_units=pd.merge(daily_data_rewards_PST_by_day_pro_units,product_taxonomy,on="class_sub_class",how="left")
daily_data_rewards_PST_by_day_pro_sales=pd.merge(daily_data_rewards_PST_by_day_pro_sales,product_taxonomy,on="class_sub_class",how="left")


daily_data_rewards_PST_by_day_pro_units['weekday']=daily_data_rewards_PST_by_day_pro_units['transaction_dt'].apply(lambda x: x.weekday())
daily_data_rewards_PST_by_day_pro_units['group']=np.where(daily_data_rewards_PST_by_day_pro_units['weekday']==6,"Sunday",
                                             np.where(daily_data_rewards_PST_by_day_pro_units['weekday']==5,"Saturday",
                                                      "Mon_Fri")
                                            )
daily_data_rewards_PST_by_day_pro_sales['weekday']=daily_data_rewards_PST_by_day_pro_sales['transaction_dt'].apply(lambda x: x.weekday())
daily_data_rewards_PST_by_day_pro_sales['group']=np.where(daily_data_rewards_PST_by_day_pro_sales['weekday']==6,"Sunday",
                                             np.where(daily_data_rewards_PST_by_day_pro_sales['weekday']==5,"Saturday",
                                                      "Mon_Fri")
                                            )

gc.collect()


# In[ ]:

writer=pd.ExcelWriter("/home/jian/Projects/Big_Lots/Analysis/2018_Q3/Top_50_Products/BL_Products_by_Zip_Group_weekday_JL_"+str(datetime.datetime.now().date())+".xlsx", engine="xlsxwriter")
daily_data_rewards_PST_count_mem.to_excel(writer,"member_count_by_PST",index=False)

output_1_Saturday_Rewards_PST_units=get_df_by_PST_units(daily_data_rewards_PST_by_day_pro_units,"Saturday")
output_1_Saturday_Rewards_PST_units.to_excel(writer,"Saturday_Units",index=False)
del output_1_Saturday_Rewards_PST_units

output_2_Sunday_Rewards_PST_units=get_df_by_PST_units(daily_data_rewards_PST_by_day_pro_units,"Sunday")
output_2_Sunday_Rewards_PST_units.to_excel(writer,"Sunday_Units",index=False)
del output_2_Sunday_Rewards_PST_units

output_3_Mon_Fri_Rewards_PST_units=get_df_by_PST_units(daily_data_rewards_PST_by_day_pro_units,"Mon_Fri")
output_3_Mon_Fri_Rewards_PST_units.to_excel(writer,"Mon_Fri_Units",index=False)
del output_3_Mon_Fri_Rewards_PST_units

output_4_Saturday_Rewards_PST_sales=get_df_by_PST_sales(daily_data_rewards_PST_by_day_pro_sales,"Saturday")
output_4_Saturday_Rewards_PST_sales.to_excel(writer,"Saturday_sales",index=False)
del output_4_Saturday_Rewards_PST_sales

output_5_Sunday_Rewards_PST_sales=get_df_by_PST_sales(daily_data_rewards_PST_by_day_pro_sales,"Sunday")
output_5_Sunday_Rewards_PST_sales.to_excel(writer,"Sunday_sales",index=False)
del output_5_Sunday_Rewards_PST_sales

output_6_Mon_Fri_Rewards_PST_sales=get_df_by_PST_sales(daily_data_rewards_PST_by_day_pro_sales,"Mon_Fri")
output_6_Mon_Fri_Rewards_PST_sales.to_excel(writer,"Mon_Fri_sales",index=False)
del output_6_Mon_Fri_Rewards_PST_sales

writer.save()


# In[ ]:




# In[ ]:




# In[ ]:

'''
writer=pd.ExcelWriter("/home/jian/Projects/Big_Lots/Analysis/2018_Q3/Top_50_Products/BL_Products_by_Deciles_weekday_JL_"+str(datetime.datetime.now().date())+".xlsx", engine="xlsxwriter")
daily_data_rewards_decile_count_mem.to_excel(writer,"member_count_by_decile",index=False)

output_1_Saturday_Rewards_Decile_units=daily_data_rewards_decile_by_day_pro_units[daily_data_rewards_decile_by_day_pro_units['group']=="Saturday"]
output_1_Saturday_Rewards_Decile_units=output_1_Saturday_Rewards_Decile_units.groupby(['subclass_desc','class_sub_class','frmindex'])['subclass_transaction_units'].sum().to_frame().reset_index().sort_values(['frmindex','subclass_transaction_units'],ascending=[True,False]).reset_index()
del output_1_Saturday_Rewards_Decile_units['index']
output_1_Saturday_Rewards_Decile_units.to_excel(writer,"Saturday_Units",index=False)
del output_1_Saturday_Rewards_Decile_units

output_2_Sunday_Rewards_Decile_units=daily_data_rewards_decile_by_day_pro_units[daily_data_rewards_decile_by_day_pro_units['group']=="Sunday"]
output_2_Sunday_Rewards_Decile_units=output_2_Sunday_Rewards_Decile_units.groupby(['subclass_desc','class_sub_class','frmindex'])['subclass_transaction_units'].sum().to_frame().reset_index().sort_values(['frmindex','subclass_transaction_units'],ascending=[True,False]).reset_index()
del output_2_Sunday_Rewards_Decile_units['index']
output_2_Sunday_Rewards_Decile_units.to_excel(writer,"Sunday_Units",index=False)
del output_2_Sunday_Rewards_Decile_units

output_3_Mon_Fri_Rewards_Decile_units=daily_data_rewards_decile_by_day_pro_units[daily_data_rewards_decile_by_day_pro_units['group']=="Mon_Fri"]
output_3_Mon_Fri_Rewards_Decile_units=output_3_Mon_Fri_Rewards_Decile_units.groupby(['subclass_desc','class_sub_class','frmindex'])['subclass_transaction_units'].sum().to_frame().reset_index().sort_values(['frmindex','subclass_transaction_units'],ascending=[True,False]).reset_index()
del output_3_Mon_Fri_Rewards_Decile_units['index']
output_3_Mon_Fri_Rewards_Decile_units.to_excel(writer,"Weekday_Units",index=False)
del output_3_Mon_Fri_Rewards_Decile_units

output_4_Saturday_Rewards_Decile_sales=daily_data_rewards_decile_by_day_pro_sales[daily_data_rewards_decile_by_day_pro_sales['group']=="Saturday"]
output_4_Saturday_Rewards_Decile_sales=output_4_Saturday_Rewards_Decile_sales.groupby(['subclass_desc','class_sub_class','frmindex'])['subclass_transaction_amt'].sum().to_frame().reset_index().sort_values(['frmindex','subclass_transaction_units'],ascending=[True,False]).reset_index()
del output_4_Saturday_Rewards_Decile_sales['index']
output_4_Saturday_Rewards_Decile_sales.to_excel(writer,"Saturday_Sales",index=False)
del output_4_Saturday_Rewards_Decile_sales

output_5_Sunday_Rewards_Decile_sales=daily_data_rewards_decile_by_day_pro_sales[daily_data_rewards_decile_by_day_pro_sales['group']=="Sunday"]
output_5_Sunday_Rewards_Decile_sales=output_5_Sunday_Rewards_Decile_sales.groupby(['subclass_desc','class_sub_class','frmindex'])['subclass_transaction_amt'].sum().to_frame().reset_index().sort_values(['frmindex','subclass_transaction_units'],ascending=[True,False]).reset_index()
del output_5_Sunday_Rewards_Decile_sales['index']
output_5_Sunday_Rewards_Decile_sales.to_excel(writer,"Sunday_Sales",index=False)
del output_5_Sunday_Rewards_Decile_sales

output_6_Mon_Fri_Rewards_Decile_sales=daily_data_rewards_decile_by_day_pro_sales[daily_data_rewards_decile_by_day_pro_sales['group']=="Mon_Fri"]
output_6_Mon_Fri_Rewards_Decile_sales=output_6_Mon_Fri_Rewards_Decile_sales.groupby(['subclass_desc','class_sub_class','frmindex'])['subclass_transaction_amt'].sum().to_frame().reset_index().sort_values(['frmindex','subclass_transaction_units'],ascending=[True,False]).reset_index()
del output_6_Mon_Fri_Rewards_Decile_sales['index']
output_6_Mon_Fri_Rewards_Decile_sales.to_excel(writer,"Weekday_Sales",index=False)
del output_6_Mon_Fri_Rewards_Decile_sales
writer.save()
'''


# In[ ]:

'''
daily_data_rewards_PST_by_day_pro_units=daily_data_rewards_decile.groupby(['location_id','zipcodegroup','transaction_dt','class_sub_class'])['subclass_transaction_units'].sum().to_frame().reset_index()
daily_data_rewards_decile_count_mem=daily_data_rewards_decile.groupby(['location_id','zipcodegroup','transaction_dt','class_sub_class'])['subclass_transaction_amt'].sum().to_frame().reset_index()

daily_data_rewards_PST_by_day_pro_units=pd.merge(daily_data_rewards_PST_by_day_pro_units,product_taxonomy,on="class_sub_class",how="left")
daily_data_rewards_PST_by_day_pro_sales=pd.merge(daily_data_rewards_PST_by_day_pro_sales,product_taxonomy,on="class_sub_class",how="left")


daily_data_rewards_PST_by_day_pro_units['weekday']=daily_data_rewards_PST_by_day_pro_units['transaction_dt'].apply(lambda x: x.weekday())
daily_data_rewards_PST_by_day_pro_units['group']=np.where(daily_data_rewards_PST_by_day_pro_units['weekday']==6,"Sunday",
                                             np.where(daily_data_rewards_PST_by_day_pro_units['weekday']==5,"Saturday",
                                                      "Mon_Fri")
                                            )
daily_data_rewards_PST_by_day_pro_sales['weekday']=daily_data_rewards_PST_by_day_pro_sales['transaction_dt'].apply(lambda x: x.weekday())
daily_data_rewards_PST_by_day_pro_sales['group']=np.where(daily_data_rewards_PST_by_day_pro_sales['weekday']==6,"Sunday",
                                             np.where(daily_data_rewards_PST_by_day_pro_sales['weekday']==5,"Saturday",
                                                      "Mon_Fri")
                                            )

gc.collect()
'''


# In[ ]:

'''
writer=pd.ExcelWriter("/home/jian/Projects/Big_Lots/Analysis/2018_Q3/Top_50_Products/BL_Products_by_Zip_Group_weekday_JL_"+str(datetime.datetime.now().date())+".xlsx", engine="xlsxwriter")
daily_data_rewards_PST_count_mem.to_excel(writer,"member_count_by_PST",index=False)

output_1_Saturday_Rewards_PST_units=daily_data_rewards_PST_by_day_pro_units[daily_data_rewards_PST_by_day_pro_units['group']=="Saturday"]
output_1_Saturday_Rewards_PST_units=output_1_Saturday_Rewards_PST_units.groupby(['subclass_desc','class_sub_class','zipcodegroup'])['subclass_transaction_units'].sum().to_frame().reset_index().sort_values(['zipcodegroup','subclass_transaction_units'],ascending=[True,False]).reset_index()
del output_1_Saturday_Rewards_PST_units['index']
output_1_Saturday_Rewards_PST_units.to_excel(writer,"Saturday_Units",index=False)
del output_1_Saturday_Rewards_PST_units

output_2_Sunday_Rewards_PST_units=daily_data_rewards_PST_by_day_pro_units[daily_data_rewards_PST_by_day_pro_units['group']=="Sunday"]
output_2_Sunday_Rewards_PST_units=output_2_Sunday_Rewards_PST_units.groupby(['subclass_desc','class_sub_class','zipcodegroup'])['subclass_transaction_units'].sum().to_frame().reset_index().sort_values(['zipcodegroup','subclass_transaction_units'],ascending=[True,False]).reset_index()
del output_2_Sunday_Rewards_PST_units['index']
output_2_Sunday_Rewards_PST_units.to_excel(writer,"Sunday_Units",index=False)
del output_2_Sunday_Rewards_PST_units

output_3_Mon_Fri_Rewards_PST_units=daily_data_rewards_PST_by_day_pro_units[daily_data_rewards_PST_by_day_pro_units['group']=="Mon_Fri"]
output_3_Mon_Fri_Rewards_PST_units=output_3_Mon_Fri_Rewards_PST_units.groupby(['subclass_desc','class_sub_class','zipcodegroup'])['subclass_transaction_units'].sum().to_frame().reset_index().sort_values(['zipcodegroup','subclass_transaction_units'],ascending=[True,False]).reset_index()
del output_3_Mon_Fri_Rewards_PST_units['index']
output_3_Mon_Fri_Rewards_PST_units.to_excel(writer,"Weekday_Units",index=False)
del output_3_Mon_Fri_Rewards_PST_units

output_4_Saturday_Rewards_PST_sales=daily_data_rewards_PST_by_day_pro_sales[daily_data_rewards_PST_by_day_pro_sales['group']=="Saturday"]
output_4_Saturday_Rewards_PST_sales=output_4_Saturday_Rewards_PST_sales.groupby(['subclass_desc','class_sub_class','zipcodegroup'])['subclass_transaction_amt'].sum().to_frame().reset_index().sort_values(['zipcodegroup','subclass_transaction_units'],ascending=[True,False]).reset_index()
del output_4_Saturday_Rewards_PST_sales['index']
output_4_Saturday_Rewards_PST_sales.to_excel(writer,"Saturday_Sales",index=False)
del output_4_Saturday_Rewards_PST_sales

output_5_Sunday_Rewards_PST_sales=daily_data_rewards_PST_by_day_pro_sales[daily_data_rewards_PST_by_day_pro_sales['group']=="Sunday"]
output_5_Sunday_Rewards_PST_sales=output_5_Sunday_Rewards_PST_sales.groupby(['subclass_desc','class_sub_class','zipcodegroup'])['subclass_transaction_amt'].sum().to_frame().reset_index().sort_values(['zipcodegroup','subclass_transaction_units'],ascending=[True,False]).reset_index()
del output_5_Sunday_Rewards_PST_sales['index']
output_5_Sunday_Rewards_PST_sales.to_excel(writer,"Sunday_Sales",index=False)
del output_5_Sunday_Rewards_PST_sales

output_6_Mon_Fri_Rewards_PST_sales=daily_data_rewards_PST_by_day_pro_sales[daily_data_rewards_PST_by_day_pro_sales['group']=="Mon_Fri"]
output_6_Mon_Fri_Rewards_PST_sales=output_6_Mon_Fri_Rewards_PST_sales.groupby(['subclass_desc','class_sub_class','zipcodegroup'])['subclass_transaction_amt'].sum().to_frame().reset_index().sort_values(['zipcodegroup','subclass_transaction_units'],ascending=[True,False]).reset_index()
del output_6_Mon_Fri_Rewards_PST_sales['index']
output_6_Mon_Fri_Rewards_PST_sales.to_excel(writer,"Weekday_Sales",index=False)
del output_6_Mon_Fri_Rewards_PST_sales
writer.save()
'''


# In[ ]:



