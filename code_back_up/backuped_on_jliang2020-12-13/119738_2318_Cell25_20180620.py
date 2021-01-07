
# coding: utf-8

# In[1]:

import pandas as pd
import gc
import numpy as np
import datetime
import os
import json
import haversine


# In[2]:

today_str=str(datetime.datetime.now().date())

folder="/home/jian/Projects/Big_Lots/Newspaper/Analysis/"+today_str+"/"
try:
    os.stat(folder)
except:
    os.mkdir(folder)


# # Prepare data

# In[3]:

zip_circ_analysis=pd.read_csv("/home/jian/Projects/Big_Lots/Newspaper/Analysis/zip_level_information.csv")
zip_circ_analysis=zip_circ_analysis[(zip_circ_analysis['zip_cd']!=0) & (zip_circ_analysis['zip_cd']!=99999)]
zip_circ_analysis=zip_circ_analysis[zip_circ_analysis['Event_Count']!=1]
zip_circ_analysis=zip_circ_analysis[zip_circ_analysis['total_circ']!=0]
del zip_circ_analysis['loyalty_label']
del zip_circ_analysis['circ_label']
zip_circ_analysis=zip_circ_analysis.reset_index()
del zip_circ_analysis['index']

zip_circ_analysis['zip_cd']=zip_circ_analysis['zip_cd'].apply(lambda x: str(x).zfill(5))
zip_circ_analysis['F_25to54']=zip_circ_analysis['F_25to54'].fillna(0)

zip_circ_analysis['loyalty_mem_penetration_F25_54']=zip_circ_analysis['loyalty_mem_count']/zip_circ_analysis['F_25to54']



# In[4]:

zip_circ_analysis['loyalty_label']=np.where(zip_circ_analysis['loyalty_mem_count']>np.percentile(zip_circ_analysis['loyalty_mem_count'], 80),"Loyalty_5",
                                                     np.where(zip_circ_analysis['loyalty_mem_count']>np.percentile(zip_circ_analysis['loyalty_mem_count'], 60),"Loyalty_4",
                                                              np.where(zip_circ_analysis['loyalty_mem_count']>np.percentile(zip_circ_analysis['loyalty_mem_count'], 40),"Loyalty_3",
                                                                       np.where(zip_circ_analysis['loyalty_mem_count']>np.percentile(zip_circ_analysis['loyalty_mem_count'], 20),"Loyalty_2",
                                                                                "Loyalty_1")
                                                                      )
                                                             )
                                                     )
zip_circ_analysis['circ_label']=np.where(zip_circ_analysis['total_circ']>np.percentile(zip_circ_analysis['total_circ'], 80),"Total_Circ_5",
                                                     np.where(zip_circ_analysis['total_circ']>np.percentile(zip_circ_analysis['total_circ'], 60),"Total_Circ_4",
                                                              np.where(zip_circ_analysis['total_circ']>np.percentile(zip_circ_analysis['total_circ'], 40),"Total_Circ_3",
                                                                       np.where(zip_circ_analysis['total_circ']>np.percentile(zip_circ_analysis['total_circ'], 20),"Total_Circ_2",
                                                                                "Total_Circ_1")
                                                                      )
                                                             )
                                                     )





# In[5]:

zip_circ_analysis_for_LP_label=zip_circ_analysis[zip_circ_analysis['F_25to54']!=0][['zip_cd','loyalty_mem_penetration_F25_54']]
zip_circ_analysis_for_LP_label['loyalty_pen_to_F25_54_label']=np.where(zip_circ_analysis_for_LP_label['loyalty_mem_penetration_F25_54']>np.percentile(zip_circ_analysis_for_LP_label['loyalty_mem_penetration_F25_54'], 80),"loyalty_penetration_5",
                                                                 np.where(zip_circ_analysis_for_LP_label['loyalty_mem_penetration_F25_54']>np.percentile(zip_circ_analysis_for_LP_label['loyalty_mem_penetration_F25_54'], 60),"loyalty_penetration_4",
                                                                          np.where(zip_circ_analysis_for_LP_label['loyalty_mem_penetration_F25_54']>np.percentile(zip_circ_analysis_for_LP_label['loyalty_mem_penetration_F25_54'], 40),"loyalty_penetration_3",
                                                                                   np.where(zip_circ_analysis_for_LP_label['loyalty_mem_penetration_F25_54']>np.percentile(zip_circ_analysis_for_LP_label['loyalty_mem_penetration_F25_54'], 20),"loyalty_penetration_2",
                                                                                            "loyalty_penetration_1")
                                                                                  )
                                                                         )
                                                                 )
del zip_circ_analysis_for_LP_label['loyalty_mem_penetration_F25_54']

zip_circ_analysis=pd.merge(zip_circ_analysis,zip_circ_analysis_for_LP_label,on="zip_cd",how="left")


# In[ ]:




# In[6]:

zip_city=pd.read_csv("/home/jian/Docs/Geo_mapping/free-zipcode-database.csv",dtype=str)
zip_city_data=zip_city[['Zipcode','City','State']].drop_duplicates().reset_index()
del zip_city_data['index']
zip_city_data=zip_city_data.rename(columns={"Zipcode":"zip_cd"})
zip_city_data['zip_cd']=zip_city_data['zip_cd'].apply(lambda x: x.zfill(5))
zip_city=zip_city_data.groupby("zip_cd")["City"].apply(set).to_frame().reset_index()


# In[7]:

zip_DMA=pd.read_excel("/home/jian/Docs/Geo_mapping/Zips by DMA by County16-17 nielsen.xlsx",skiprows=1,dtype=str)
zip_DMA=zip_DMA.iloc[:,[0,2,6]]
zip_DMA=zip_DMA.rename(columns={"CODE":"zip_cd","NAME":"DMA","ABV":"ST"})
zip_DMA_DMA=zip_DMA.groupby("zip_cd")["DMA"].apply(set).to_frame().reset_index()
zip_DMA_ST_Nielsen=zip_DMA.groupby("zip_cd")["ST"].apply(set).to_frame().reset_index()


# In[8]:

zip_circ_analysis=pd.merge(zip_circ_analysis,zip_city,on="zip_cd",how="left")
zip_circ_analysis=pd.merge(zip_circ_analysis,zip_DMA_ST_Nielsen,on="zip_cd",how="left")
zip_circ_analysis=pd.merge(zip_circ_analysis,zip_DMA_DMA,on="zip_cd",how="left")


# In[9]:

zip_center=json.load(open("/home/jian/Docs/Geo_mapping/center_of_rentrak_zip.json","r"))
# From the Domic
store_zip=pd.read_excel("/home/jian/Projects/Big_Lots/Other_Input/Store_list 5-4-18_From Dom.xlsx",sheetname="Store List",dtype=str)
store_zip=store_zip.iloc[0:store_zip.index[store_zip['Store']=='nan'][0],:]

store_zip=store_zip[["Store","Zip","latitude_meas","longitude_meas"]].rename(columns={"Zip":"zip_cd"})
store_zip['latitude_meas']=store_zip['latitude_meas'].astype(float)
store_zip['longitude_meas']=store_zip['longitude_meas'].astype(float)
store_zip['zip_cd']=store_zip['zip_cd'].apply(lambda x: x.split("-")[0].zfill(5))


# In[10]:

zip_circ_analysis['loyalty_mem_penetration_F25_54'].apply(lambda x:type(x)).unique()


# In[11]:

np.nanpercentile(zip_circ_analysis['loyalty_mem_penetration_F25_54'], 80)


# # Closest Stores

# In[12]:

count=0 #1000 bin
df_closest_store=pd.DataFrame()
not_fount_zip_list=[]
for zip_circ in zip_circ_analysis['zip_cd'].unique().tolist():
    shortest_dist=99999
    for i in range(len(store_zip)):
        try:
            dist=haversine.haversine((store_zip['latitude_meas'][i],store_zip['longitude_meas'][i]),zip_center[zip_circ],miles=True)
            if shortest_dist>=dist:
                shortest_dist=dist
                closest_store=store_zip['Store'][i]
        except:
            not_fount_zip_list=not_fount_zip_list+[zip_circ]
            
    df=pd.DataFrame({"zip_cd":zip_circ,"closest_store":closest_store,"closest_dist":shortest_dist},index=[count])
    # zip_cd: zip in circ  
    count=count+1
    if count%1000==1:
        print(count,datetime.datetime.now())
    
    df_closest_store=df_closest_store.append(df)    


# In[13]:

zip_circ_analysis=pd.merge(zip_circ_analysis,df_closest_store,on="zip_cd",how="left")


# In[14]:

zip_circ_analysis['F_25to54']=zip_circ_analysis['F_25to54'].fillna(0)
zip_circ_analysis['loyalty_mem_count']=zip_circ_analysis['loyalty_mem_count'].fillna(0)


# In[15]:

zip_circ_analysis['prospect_count']=zip_circ_analysis['F_25to54']-zip_circ_analysis['loyalty_mem_count']
zip_circ_analysis['prospect_penetration_F25_54']=zip_circ_analysis['prospect_count']/zip_circ_analysis['F_25to54']



zip_circ_analysis["Cost per loyalty_mem"]=zip_circ_analysis['cost']/zip_circ_analysis['loyalty_mem_count']
zip_circ_analysis["Circ per event_to_Loyalty mem"]=zip_circ_analysis['Circ per Event']/zip_circ_analysis['loyalty_mem_count']



# In[16]:

zip_circ_analysis_for_PP_label=zip_circ_analysis[zip_circ_analysis['F_25to54']!=0][['zip_cd','prospect_penetration_F25_54']]
zip_circ_analysis_for_PP_label['prospects_pen_to_F25_54_label']=np.where(zip_circ_analysis_for_PP_label['prospect_penetration_F25_54']>np.percentile(zip_circ_analysis_for_PP_label['prospect_penetration_F25_54'], 80),"prospects_penetration_5",
                                                                 np.where(zip_circ_analysis_for_PP_label['prospect_penetration_F25_54']>np.percentile(zip_circ_analysis_for_PP_label['prospect_penetration_F25_54'], 60),"prospects_penetration_4",
                                                                          np.where(zip_circ_analysis_for_PP_label['prospect_penetration_F25_54']>np.percentile(zip_circ_analysis_for_PP_label['prospect_penetration_F25_54'], 40),"prospects_penetration_3",
                                                                                   np.where(zip_circ_analysis_for_PP_label['prospect_penetration_F25_54']>np.percentile(zip_circ_analysis_for_PP_label['prospect_penetration_F25_54'], 20),"prospects_penetration_2",
                                                                                            "prospects_penetration_1")
                                                                                  )
                                                                         )
                                                                 )
del zip_circ_analysis_for_PP_label['prospect_penetration_F25_54']

zip_circ_analysis=pd.merge(zip_circ_analysis,zip_circ_analysis_for_PP_label,on="zip_cd",how="left")


# In[17]:

zip_circ_analysis['location_id']=np.where(zip_circ_analysis['location_id']==0,np.nan,zip_circ_analysis['location_id'])


# In[18]:

zip_circ_analysis['Store_1']=np.nan
zip_circ_analysis['Store_2']=np.nan
zip_circ_analysis['location_id_list']=zip_circ_analysis['location_id'].apply(lambda x: str(x).replace("[","").replace("]","").replace(" ","").split(","))
zip_circ_analysis['store_count']=zip_circ_analysis['location_id_list'].apply(lambda x:len(x))

zip_circ_analysis_0=zip_circ_analysis[zip_circ_analysis['store_count']==0]
zip_circ_analysis_1=zip_circ_analysis[zip_circ_analysis['store_count']==1]
zip_circ_analysis_2=zip_circ_analysis[zip_circ_analysis['store_count']==2]
zip_circ_analysis_1['Store_1']=zip_circ_analysis_1['location_id_list'].apply(lambda x: x[0])

zip_circ_analysis_2['Store_1']=zip_circ_analysis_2['location_id_list'].apply(lambda x: x[0])
zip_circ_analysis_2['Store_2']=zip_circ_analysis_2['location_id_list'].apply(lambda x: x[1])

                                  

zip_circ_analysis=zip_circ_analysis_0.append(zip_circ_analysis_1).append(zip_circ_analysis_2)
zip_circ_analysis=zip_circ_analysis.sort_values("zip_cd")



# In[19]:

del zip_circ_analysis['store_count']
del zip_circ_analysis['location_id_list']   
del zip_circ_analysis['Split_of_weekly_circ']


# In[20]:

date_columns=[x for x in zip_circ_analysis.columns.tolist() if "-" in x]

iv_columns=["zip_cd","City","ST","DMA","location_id","Store_1","Store_2","closest_dist","closest_store",
            "selected_TA","trade_area_code","revenue_flag","HH15","total_pop","F_25to54","loyalty_mem_count","loyalty_mem_penetration_F25_54",
           "prospect_count","prospect_penetration_F25_54",
            "total_circ","Event_Count","Circ per Event","cost","Circ Penetration of F25_54",
           "Cost per loyalty_mem","Circ per event_to_Loyalty mem","2017_compariable_sales","2018_compariable_sales","YoY","store_list","loyalty_sales_by_zip",
            "loyalty_transactions_by_zip","loyalty_label","circ_label","loyalty_pen_to_F25_54_label",'prospects_pen_to_F25_54_label']

zip_circ_analysis=zip_circ_analysis[iv_columns+date_columns]



# In[21]:

zip_circ_analysis['Store_1']=zip_circ_analysis['Store_1'].astype(str)
zip_circ_analysis['Store_1']=np.where(zip_circ_analysis['Store_1']=="nan","",zip_circ_analysis['Store_1'])
zip_circ_analysis=zip_circ_analysis.rename(columns={"Cost per loyalty_mem":"Total cost per loyalty_mem"})


# In[22]:

zip_circ_analysis.head(2)


# In[65]:

loyalty_sales_df.head(2)


# In[66]:

#test
loyalty_sales_df[loyalty_sales_df['customer_zip_code']=="02195"]


# In[27]:

loyalty_sales_df=pd.read_csv("/home/jian/Projects/Big_Lots/Loyal_members/sales_of_loyalty_member/2018-06-20/sales_by_location_id_agg_2018-06-20.csv",dtype=str)
loyalty_sales_df['customer_zip_code']=loyalty_sales_df['customer_zip_code'].apply(lambda x: x.zfill(5))
loyalty_sales_df['non_loyalty_sales_zip']=loyalty_sales_df['non_loyalty_sales_zip'].astype(float)
loyalty_sales_df['loyal_sales_zip']=loyalty_sales_df['loyal_sales_zip'].astype(float)
loyalty_sales_df['total_sales_zip']=loyalty_sales_df['loyal_sales_zip']+loyalty_sales_df['non_loyalty_sales_zip']

non_loyalty_sales_by_zip=loyalty_sales_df.groupby(['customer_zip_code'])['non_loyalty_sales_zip','loyal_sales_zip','total_sales_zip'].sum().reset_index()
non_loyalty_sales_by_zip=non_loyalty_sales_by_zip.rename(columns={"customer_zip_code":"zip_cd",'non_loyalty_sales_zip':'non_loyalty_sales_46',
                                                                 'loyal_sales_zip':'loyal_sales_46','total_sales_zip':'total_sales_46'})

zip_circ_analysis=pd.merge(zip_circ_analysis,non_loyalty_sales_by_zip,on='zip_cd',how="left")


# In[28]:

date_columns=[x for x in zip_circ_analysis.columns.tolist() if "-" in x]

iv_columns=["zip_cd","City","ST","DMA","location_id","Store_1","Store_2","closest_dist","closest_store",
            "selected_TA","trade_area_code","revenue_flag",
            "HH15","total_pop","F_25to54","loyalty_mem_count","loyalty_mem_penetration_F25_54",
            "prospect_count","prospect_penetration_F25_54","total_circ","Event_Count","Circ per Event","cost","Circ Penetration of F25_54",
           "Total cost per loyalty_mem","Circ per event_to_Loyalty mem","2017_compariable_sales","2018_compariable_sales","YoY","store_list","loyalty_sales_by_zip",
            "loyalty_transactions_by_zip",'non_loyalty_sales_46','loyal_sales_46','total_sales_46',
            "loyalty_label","circ_label","loyalty_pen_to_F25_54_label","prospects_pen_to_F25_54_label"]

zip_circ_analysis=zip_circ_analysis[iv_columns+date_columns]

writer=pd.ExcelWriter(folder+"zip_level_data_revised_"+today_str+".xlsx",engine='xlsxwriter')
zip_circ_analysis.to_excel(writer,'zip_circ_analysis',index=False)
non_loyalty_sales_by_zip.to_excel(writer,'46_weeks_sales_by_zip',index=False)
writer.save()

zip_circ_analysis.to_csv(folder+"zip_level_data_revised_"+today_str+".csv",index=False)

zip_circ_analysis_orginal=zip_circ_analysis.copy()


# # Matrix Old - 1

# In[29]:

# Old use the matrix axis of totla Loyalty members and total Circ
old_matrix_excel_writer=pd.ExcelWriter(folder+"matrix_of_total_Loyalty_and_Circ_"+today_str+".xlsx",engine="xlsxwriter")
def old_matrix_of_sum(df,col):
    df_pivot=df[['circ_label','loyalty_label',col]].groupby(['loyalty_label','circ_label'])[col].sum().to_frame().reset_index()
    result=df_pivot.pivot(index="loyalty_label",columns="circ_label",values=col).reset_index()
    return result


# In[30]:

matrix_of_zip_list=zip_circ_analysis[['zip_cd','loyalty_label','circ_label']].groupby(['loyalty_label','circ_label'])['zip_cd'].apply(list).to_frame().reset_index()
matrix_of_zip_list.columns=matrix_of_zip_list.columns.tolist()[0:2]+["zip_cd_list"]
matrix_of_zip_list=matrix_of_zip_list.pivot(index="loyalty_label",columns="circ_label",values="zip_cd_list").reset_index()
matrix_of_zip_list.to_excel(old_matrix_excel_writer,"zip_list",index=False)

matrix_of_totol_zip_count=matrix_of_zip_list.copy()
for col in matrix_of_totol_zip_count.columns.tolist()[1:]:
    matrix_of_totol_zip_count[col]=[len(x) for x in matrix_of_totol_zip_count[col]]
matrix_of_totol_zip_count.to_excel(old_matrix_excel_writer,"zip_count",index=False)


# In[31]:

matrix_of_total_pop=old_matrix_of_sum(zip_circ_analysis,"total_pop")
matrix_of_total_female25_54=old_matrix_of_sum(zip_circ_analysis,"F_25to54")
matrix_of_total_households=old_matrix_of_sum(zip_circ_analysis,"HH15")
matrix_of_total_loyalty_mem=old_matrix_of_sum(zip_circ_analysis,"loyalty_mem_count")
matrix_of_circ_per_event=old_matrix_of_sum(zip_circ_analysis,"Circ per Event")
matrix_of_lotyalty_sales=old_matrix_of_sum(zip_circ_analysis,"loyalty_sales_by_zip")
matrix_of_lotyalty_trans=old_matrix_of_sum(zip_circ_analysis,"loyalty_transactions_by_zip")
matrix_of_lotyalty_cost=old_matrix_of_sum(zip_circ_analysis,"cost")
matrix_of_Diff_Female25_54_to_loyalty_mem=old_matrix_of_sum(zip_circ_analysis,"prospect_count")


# In[32]:

matrix_of_total_pop.to_excel(old_matrix_excel_writer,"total_population",index=False)
matrix_of_total_female25_54.to_excel(old_matrix_excel_writer,"female_25_54",index=False)
matrix_of_total_households.to_excel(old_matrix_excel_writer,"total_households",index=False)
matrix_of_total_loyalty_mem.to_excel(old_matrix_excel_writer,"loyalty_members",index=False)
matrix_of_circ_per_event.to_excel(old_matrix_excel_writer,"circ_per_event",index=False)
matrix_of_lotyalty_sales.to_excel(old_matrix_excel_writer,"loyalty_sales",index=False)
matrix_of_lotyalty_trans.to_excel(old_matrix_excel_writer,"loyalty_transaction",index=False)
matrix_of_lotyalty_cost.to_excel(old_matrix_excel_writer,"circ_cost",index=False)
matrix_of_Diff_Female25_54_to_loyalty_mem.to_excel(old_matrix_excel_writer,"prospect_count",index=False)


# In[33]:

matrix_of_store_sales_2017=old_matrix_of_sum(zip_circ_analysis,"2017_compariable_sales")
matrix_of_store_sales_2018=old_matrix_of_sum(zip_circ_analysis,"2018_compariable_sales")
matrix_of_store_sales_2017.to_excel(old_matrix_excel_writer,"store_sales_2017",index=False)
matrix_of_store_sales_2018.to_excel(old_matrix_excel_writer,"store_sales_2018",index=False)


matrix_of_store_sales_YoY=pd.DataFrame({"Total_Circ_1":[np.nan]*5,"Total_Circ_2":[np.nan]*5,"Total_Circ_3":[np.nan]*5,
                                       "Total_Circ_4":[np.nan]*5,"Total_Circ_5":[np.nan]*5},index=matrix_of_store_sales_2018['loyalty_label']).reset_index()

for i in range(5):
    for col in matrix_of_store_sales_2018.columns.tolist()[1:]:
        matrix_of_store_sales_YoY[col][i]=(matrix_of_store_sales_2018[col][i]-matrix_of_store_sales_2017[col][i])/matrix_of_store_sales_2017[col][i]
matrix_of_store_sales_YoY.to_excel(old_matrix_excel_writer,"store_sales_YoY",index=False)


# In[34]:

# Ratio of Circ per Event/Loyalty Members
matrix_of_Ratio=pd.DataFrame({"Total_Circ_1":[np.nan]*5,"Total_Circ_2":[np.nan]*5,"Total_Circ_3":[np.nan]*5,
                                       "Total_Circ_4":[np.nan]*5,"Total_Circ_5":[np.nan]*5},index=matrix_of_store_sales_2018['loyalty_label']).reset_index()

for i in range(5):
    for col in matrix_of_total_loyalty_mem.columns.tolist()[1:]:
        matrix_of_Ratio[col][i]=matrix_of_circ_per_event[col][i]/matrix_of_total_loyalty_mem[col][i]
matrix_of_Ratio.to_excel(old_matrix_excel_writer,"AvgRatio_CircPerEv_to_Loyalty",index=False)


# In[35]:

old_matrix_excel_writer.save()


# In[36]:

Cell25_old=zip_circ_analysis[(zip_circ_analysis['loyalty_label']== "Loyalty_5") & (zip_circ_analysis['circ_label']== "Total_Circ_5")]
Cell25_old.to_csv(folder+"Cell_25 for matrix with highest loyalty members and total circulations"+today_str+".csv",index=False)


# # Matrix New - 2

# In[37]:

# Old use the matrix axis of loyalty_pen_to_F25_54_label and total Circ
# remove 154 rows with no Female 25 to 54 value
zip_circ_analysis_removed=zip_circ_analysis[zip_circ_analysis['F_25to54']==0]
zip_circ_analysis=zip_circ_analysis[zip_circ_analysis['F_25to54']!=0]



# In[38]:

new_matrix_excel_writer=pd.ExcelWriter(folder+"matrix_of_loyalty_Penetration_and_total_Circ_"+today_str+".xlsx",engine="xlsxwriter")

zip_circ_analysis_removed.to_excel(new_matrix_excel_writer,"removed",index=False)
def new_matrix_of_sum(df,col):
    df_pivot=df[['circ_label','loyalty_pen_to_F25_54_label',col]].groupby(['loyalty_pen_to_F25_54_label','circ_label'])[col].sum().to_frame().reset_index()
    result=df_pivot.pivot(index="loyalty_pen_to_F25_54_label",columns="circ_label",values=col).reset_index()
    return result


# In[39]:

matrix_of_zip_list=zip_circ_analysis[['zip_cd','loyalty_pen_to_F25_54_label','circ_label']].groupby(['loyalty_pen_to_F25_54_label','circ_label'])['zip_cd'].apply(list).to_frame().reset_index()
matrix_of_zip_list.columns=matrix_of_zip_list.columns.tolist()[0:2]+["zip_cd_list"]
matrix_of_zip_list=matrix_of_zip_list.pivot(index="loyalty_pen_to_F25_54_label",columns="circ_label",values="zip_cd_list").reset_index()
matrix_of_zip_list.to_excel(new_matrix_excel_writer,"zip_list",index=False)

matrix_of_totol_zip_count=matrix_of_zip_list.copy()
for col in matrix_of_totol_zip_count.columns.tolist()[1:]:
    matrix_of_totol_zip_count[col]=[len(x) for x in matrix_of_totol_zip_count[col]]
matrix_of_totol_zip_count.to_excel(new_matrix_excel_writer,"zip_count",index=False)


# In[40]:

matrix_of_total_pop=new_matrix_of_sum(zip_circ_analysis,"total_pop")
matrix_of_total_female25_54=new_matrix_of_sum(zip_circ_analysis,"F_25to54")
matrix_of_total_households=new_matrix_of_sum(zip_circ_analysis,"HH15")
matrix_of_total_loyalty_mem=new_matrix_of_sum(zip_circ_analysis,"loyalty_mem_count")
matrix_of_circ_per_event=new_matrix_of_sum(zip_circ_analysis,"Circ per Event")
matrix_of_lotyalty_sales=new_matrix_of_sum(zip_circ_analysis,"loyalty_sales_by_zip")
matrix_of_lotyalty_trans=new_matrix_of_sum(zip_circ_analysis,"loyalty_transactions_by_zip")
matrix_of_lotyalty_cost=new_matrix_of_sum(zip_circ_analysis,"cost")
matrix_of_Diff_Female25_54_to_loyalty_mem=new_matrix_of_sum(zip_circ_analysis,"prospect_count")


# In[41]:

matrix_of_total_pop.to_excel(new_matrix_excel_writer,"total_population",index=False)
matrix_of_total_female25_54.to_excel(new_matrix_excel_writer,"female_25_54",index=False)
matrix_of_total_households.to_excel(new_matrix_excel_writer,"total_households",index=False)
matrix_of_total_loyalty_mem.to_excel(new_matrix_excel_writer,"loyalty_members",index=False)
matrix_of_circ_per_event.to_excel(new_matrix_excel_writer,"circ_per_event",index=False)
matrix_of_lotyalty_sales.to_excel(new_matrix_excel_writer,"loyalty_sales",index=False)
matrix_of_lotyalty_trans.to_excel(new_matrix_excel_writer,"loyalty_transaction",index=False)
matrix_of_lotyalty_cost.to_excel(new_matrix_excel_writer,"circ_cost",index=False)
matrix_of_Diff_Female25_54_to_loyalty_mem.to_excel(old_matrix_excel_writer,"prospect_count",index=False)


# In[42]:

matrix_of_store_sales_2017=new_matrix_of_sum(zip_circ_analysis,"2017_compariable_sales")
matrix_of_store_sales_2018=new_matrix_of_sum(zip_circ_analysis,"2018_compariable_sales")
matrix_of_store_sales_2017.to_excel(new_matrix_excel_writer,"store_sales_2017",index=False)
matrix_of_store_sales_2018.to_excel(new_matrix_excel_writer,"store_sales_2018",index=False)


matrix_of_store_sales_YoY=pd.DataFrame({"Total_Circ_1":[np.nan]*5,"Total_Circ_2":[np.nan]*5,"Total_Circ_3":[np.nan]*5,
                                       "Total_Circ_4":[np.nan]*5,"Total_Circ_5":[np.nan]*5},index=matrix_of_store_sales_2018['loyalty_pen_to_F25_54_label']).reset_index()

for i in range(5):
    for col in matrix_of_store_sales_2018.columns.tolist()[1:]:
        matrix_of_store_sales_YoY[col][i]=(matrix_of_store_sales_2018[col][i]-matrix_of_store_sales_2017[col][i])/matrix_of_store_sales_2017[col][i]
matrix_of_store_sales_YoY.to_excel(new_matrix_excel_writer,"store_sales_YoY",index=False)


# In[43]:

matrix_of_circ_per_event.head(2)


# In[44]:

matrix_of_total_loyalty_mem.head(2)


# In[45]:

# Ratio of Circ per Event/Loyalty Members
matrix_of_Ratio=pd.DataFrame({"Total_Circ_1":[np.nan]*5,"Total_Circ_2":[np.nan]*5,"Total_Circ_3":[np.nan]*5,
                                       "Total_Circ_4":[np.nan]*5,"Total_Circ_5":[np.nan]*5},index=matrix_of_store_sales_2018['loyalty_pen_to_F25_54_label']).reset_index()

for i in range(5):
    for col in matrix_of_total_loyalty_mem.columns.tolist()[1:]:
        matrix_of_Ratio[col][i]=matrix_of_circ_per_event[col][i]/matrix_of_total_loyalty_mem[col][i]
matrix_of_Ratio.to_excel(new_matrix_excel_writer,"AvgRatio_CircPerEv_to_Loyalty",index=False)


# In[46]:

new_matrix_excel_writer.save()


# In[47]:

Cell25_new=zip_circ_analysis[(zip_circ_analysis['loyalty_pen_to_F25_54_label']== "loyalty_penetration_5") & (zip_circ_analysis['circ_label']== "Total_Circ_5")]
Cell25_new.to_csv(folder+"Cell_25 for matrix with highest loyalty penetration and total circulations"+today_str+".csv",index=False)


# In[ ]:




# # Matrix New - 3

# In[48]:

# Old use the matrix axis of loyalty_pen_to_F25_54_label and total Circ
# remove 154 rows with no Female 25 to 54 value
zip_circ_analysis_removed=zip_circ_analysis_orginal[zip_circ_analysis_orginal['F_25to54']==0]
zip_circ_analysis=zip_circ_analysis_orginal[zip_circ_analysis_orginal['F_25to54']!=0]



# In[49]:

new_matrix_excel_writer=pd.ExcelWriter(folder+"matrix_of_prospects_Penetration_and_total_Circ_"+today_str+".xlsx",engine="xlsxwriter")

zip_circ_analysis_removed.to_excel(new_matrix_excel_writer,"removed",index=False)
def new_matrix_of_sum(df,col):
    df_pivot=df[['circ_label','prospects_pen_to_F25_54_label',col]].groupby(['prospects_pen_to_F25_54_label','circ_label'])[col].sum().to_frame().reset_index()
    result=df_pivot.pivot(index="prospects_pen_to_F25_54_label",columns="circ_label",values=col).reset_index()
    return result


# In[50]:

matrix_of_zip_list=zip_circ_analysis[['zip_cd','prospects_pen_to_F25_54_label','circ_label']].groupby(['prospects_pen_to_F25_54_label','circ_label'])['zip_cd'].apply(list).to_frame().reset_index()
matrix_of_zip_list.columns=matrix_of_zip_list.columns.tolist()[0:2]+["zip_cd_list"]
matrix_of_zip_list=matrix_of_zip_list.pivot(index="prospects_pen_to_F25_54_label",columns="circ_label",values="zip_cd_list").reset_index()
matrix_of_zip_list.to_excel(new_matrix_excel_writer,"zip_list",index=False)

matrix_of_totol_zip_count=matrix_of_zip_list.copy()
for col in matrix_of_totol_zip_count.columns.tolist()[1:]:
    matrix_of_totol_zip_count[col]=[len(x) for x in matrix_of_totol_zip_count[col]]
matrix_of_totol_zip_count.to_excel(new_matrix_excel_writer,"zip_count",index=False)


# In[51]:

matrix_of_total_pop=new_matrix_of_sum(zip_circ_analysis,"total_pop")
matrix_of_total_female25_54=new_matrix_of_sum(zip_circ_analysis,"F_25to54")
matrix_of_total_households=new_matrix_of_sum(zip_circ_analysis,"HH15")
matrix_of_total_loyalty_mem=new_matrix_of_sum(zip_circ_analysis,"loyalty_mem_count")
matrix_of_circ_per_event=new_matrix_of_sum(zip_circ_analysis,"Circ per Event")
matrix_of_lotyalty_sales=new_matrix_of_sum(zip_circ_analysis,"loyalty_sales_by_zip")
matrix_of_lotyalty_trans=new_matrix_of_sum(zip_circ_analysis,"loyalty_transactions_by_zip")
matrix_of_lotyalty_cost=new_matrix_of_sum(zip_circ_analysis,"cost")
matrix_of_Diff_Female25_54_to_loyalty_mem=new_matrix_of_sum(zip_circ_analysis,"prospect_count")


# In[52]:

matrix_of_total_pop.to_excel(new_matrix_excel_writer,"total_population",index=False)
matrix_of_total_female25_54.to_excel(new_matrix_excel_writer,"female_25_54",index=False)
matrix_of_total_households.to_excel(new_matrix_excel_writer,"total_households",index=False)
matrix_of_total_loyalty_mem.to_excel(new_matrix_excel_writer,"loyalty_members",index=False)
matrix_of_circ_per_event.to_excel(new_matrix_excel_writer,"circ_per_event",index=False)
matrix_of_lotyalty_sales.to_excel(new_matrix_excel_writer,"loyalty_sales",index=False)
matrix_of_lotyalty_trans.to_excel(new_matrix_excel_writer,"loyalty_transaction",index=False)
matrix_of_lotyalty_cost.to_excel(new_matrix_excel_writer,"circ_cost",index=False)
matrix_of_Diff_Female25_54_to_loyalty_mem.to_excel(old_matrix_excel_writer,"prospect_count",index=False)


# In[53]:

matrix_of_store_sales_2017=new_matrix_of_sum(zip_circ_analysis,"2017_compariable_sales")
matrix_of_store_sales_2018=new_matrix_of_sum(zip_circ_analysis,"2018_compariable_sales")
matrix_of_store_sales_2017.to_excel(new_matrix_excel_writer,"store_sales_2017",index=False)
matrix_of_store_sales_2018.to_excel(new_matrix_excel_writer,"store_sales_2018",index=False)


matrix_of_store_sales_YoY=pd.DataFrame({"Total_Circ_1":[np.nan]*5,"Total_Circ_2":[np.nan]*5,"Total_Circ_3":[np.nan]*5,
                                       "Total_Circ_4":[np.nan]*5,"Total_Circ_5":[np.nan]*5},index=matrix_of_store_sales_2018['prospects_pen_to_F25_54_label']).reset_index()

for i in range(5):
    for col in matrix_of_store_sales_2018.columns.tolist()[1:]:
        matrix_of_store_sales_YoY[col][i]=(matrix_of_store_sales_2018[col][i]-matrix_of_store_sales_2017[col][i])/matrix_of_store_sales_2017[col][i]
matrix_of_store_sales_YoY.to_excel(new_matrix_excel_writer,"store_sales_YoY",index=False)


# In[54]:

# Ratio of Circ per Event/Loyalty Members
matrix_of_Ratio=pd.DataFrame({"Total_Circ_1":[np.nan]*5,"Total_Circ_2":[np.nan]*5,"Total_Circ_3":[np.nan]*5,
                                       "Total_Circ_4":[np.nan]*5,"Total_Circ_5":[np.nan]*5},index=matrix_of_store_sales_2018['prospects_pen_to_F25_54_label']).reset_index()

for i in range(5):
    for col in matrix_of_total_loyalty_mem.columns.tolist()[1:]:
        matrix_of_Ratio[col][i]=matrix_of_circ_per_event[col][i]/matrix_of_total_loyalty_mem[col][i]
matrix_of_Ratio.to_excel(new_matrix_excel_writer,"AvgRatio_CircPerEv_to_Loyalty",index=False)


# In[55]:

new_matrix_excel_writer.save()


# In[56]:

Cell25_new=zip_circ_analysis[(zip_circ_analysis['loyalty_pen_to_F25_54_label']== "prospects_penetration_5") & (zip_circ_analysis['circ_label']== "Total_Circ_5")]
Cell25_new.to_csv(folder+"Cell_25 for matrix with highest prospects penetration and total circulations"+today_str+".csv",index=False)


# In[ ]:




# # By Zone as variables in cloumn

# In[57]:

In_TA=zip_circ_analysis_orginal[~pd.isnull(zip_circ_analysis_orginal['trade_area_code'])]
Out_TA=zip_circ_analysis_orginal[pd.isnull(zip_circ_analysis_orginal['trade_area_code'])]


# In[58]:

Df_by_zip_by_Zone=In_TA.append(Out_TA)


# In[60]:

Out_TA.columns


# In[ ]:



