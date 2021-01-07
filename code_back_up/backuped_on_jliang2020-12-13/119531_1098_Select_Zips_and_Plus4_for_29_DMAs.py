
# coding: utf-8

# In[1]:

import pandas as pd
import zipcodes
import datetime


# In[ ]:




# In[2]:

EASI_1A=pd.read_csv("/home/jian/Docs/EASI/2018-07-19/ZIP4_18_DATA_A2_CSV/ZIP4_18_DATA_A2.CSV",
                    skiprows=1,usecols=['ZIP_CODE','ZIP4','HH18','MEDHHINC','PCTHH200P'],dtype=str)


# In[3]:

EASI_1A['HH18']=EASI_1A['HH18'].astype(float)
EASI_1A['MEDHHINC']=EASI_1A['MEDHHINC'].astype(float)
EASI_1A['PCTHH200P']=EASI_1A['PCTHH200P'].astype(float)
EASI_1A=EASI_1A.rename(columns={"MEDHHINC":"Median_HH_Inc"})


# In[4]:

DMA_Zips=pd.read_excel("/home/jian/Docs/Geo_mapping/Zips by DMA by County16-17 nielsen.xlsx",dtype=str,skiprows=1)
DMA_Zips=DMA_Zips.iloc[:,[0,2,6]]
DMA_Zips.columns=['ZIP_CODE','DMA','state']


# In[5]:

DMA_29=pd.read_csv("/home/jian/Projects/Saatva/High_Income_Zips_for_29_DMAs/Seleceted_DMA_Top25_TV25.csv",dtype=str)
DMA_29=DMA_29['dma'].unique().tolist()


# In[6]:

DMA_Zips_29=DMA_Zips[DMA_Zips['DMA'].isin(DMA_29)]


# In[7]:

DMA_Zips_29=DMA_Zips_29.drop_duplicates()


# In[8]:

DMA_Zips_29=pd.merge(DMA_Zips_29,EASI_1A,on="ZIP_CODE",how="left")


# In[9]:

DMA_Zips_29.head(3)


# In[10]:

len(DMA_Zips_29['ZIP_CODE'].unique())


# # On Median Income Zip+4

# In[11]:

# Meidan Income > $ 100K 
DMA_Zips_29_High=DMA_Zips_29[DMA_Zips_29['Median_HH_Inc']>=100000]


# In[12]:

unique_zip_city=pd.DataFrame()
for zip_cd in DMA_Zips_29_High['ZIP_CODE'].unique().tolist():
    if len(zipcodes.matching(zip_cd))>=1:
        city=zipcodes.matching(zip_cd)[0]['city']
        df=pd.DataFrame({"ZIP_CODE":zip_cd,"city":city},index=[zip_cd])
        unique_zip_city=unique_zip_city.append(df)
    else:
        print(zip_cd)


# In[13]:

unique_zip_city=unique_zip_city.append(pd.DataFrame({"ZIP_CODE":['86005','97003'],"city":["Kachina Village","Hillsboro"]},index=['86005','97003']))


# In[14]:

DMA_Zips_29_High=pd.merge(DMA_Zips_29_High,unique_zip_city,on="ZIP_CODE",how="left")


# In[15]:

DMA_Zips_29_High=DMA_Zips_29_High[['ZIP_CODE','ZIP4','DMA','city','state','HH18','Median_HH_Inc','PCTHH200P']]


# In[16]:

DMA_Zips_29_High['HH_200K_Plus']=DMA_Zips_29_High['HH18']/100*DMA_Zips_29_High['PCTHH200P']


# In[17]:

writer=pd.ExcelWriter("/home/jian/Projects/Saatva/High_Income_Zips_for_29_DMAs/Saatva_Zip Plus4 Median HH Inc above 100K for 29 DMAs_JL_20180823.xlsx",engine="xlsxwriter")


# In[18]:

for dma in DMA_29:
    df=DMA_Zips_29_High[DMA_Zips_29_High['DMA']==dma]
    df=df.reset_index()
    del df['index']
    if df.shape[0]>1000000:
        df=df.sort_values('Avg_HH_Inc',ascending=False)
        df=df.reset_index()
        del df['index']
        df_0=df.iloc[0:50000,]
        df_1=df.iloc[50000:,]
        df_0.to_excel(writer,dma+"_1",index=False)
        df_1.to_excel(writer,dma+"_2",index=False)
    else:
        df.to_excel(writer,dma,index=False)
    print(dma+" with rows of: "+str(df.shape[0]))


# In[19]:

writer.save()


# # On Median Income Zip5

# In[12]:

zip5_HH=DMA_Zips_29.groupby(['ZIP_CODE','DMA','state'])['HH18'].sum().to_frame().reset_index()


# In[13]:

i=0
zip5_HH_Median_Inc=pd.DataFrame()

for zip_cd,group in DMA_Zips_29.groupby(['ZIP_CODE']):
    total_HH=group['HH18'].sum()
    df=group[['ZIP_CODE','HH18','Median_HH_Inc','PCTHH200P']]
    df['HH_weight']=df['HH18']/total_HH
    df['Weighted_Median_HH_Inc']=df['Median_HH_Inc']*df['HH_weight']
    df['Weighted_PCTHH200P']=df['PCTHH200P']*df['HH_weight']
    
    agg_Pctg_HH_200P=df['Weighted_PCTHH200P'].sum()
    agg_median=df['Weighted_Median_HH_Inc'].sum()
    
    df_app=pd.DataFrame({"ZIP_CODE":zip_cd,"Meidan_HH_Inc_agg":agg_median,"Pctg_HH_200P_agg":agg_Pctg_HH_200P},index=[zip_cd])
    
    zip5_HH_Median_Inc=zip5_HH_Median_Inc.append(df_app)
    
    i=i+1
    if i % 1000==20:
        print(i,datetime.datetime.now())

    


# In[14]:

# Zip 5 level HH Inc > $ 100K
zip5_HH_Median_Inc_High=zip5_HH_Median_Inc[zip5_HH_Median_Inc['Meidan_HH_Inc_agg']>100000]
zip5_HH_Median_Inc_High=pd.merge(zip5_HH_Median_Inc_High,zip5_HH,on="ZIP_CODE",how="left")
zip5_HH_Median_Inc_High['HH_200K_Plus']=zip5_HH_Median_Inc_High['HH18']*zip5_HH_Median_Inc_High['Pctg_HH_200P_agg']/100


# In[15]:

unique_zip_city=pd.DataFrame()
for zip_cd in zip5_HH_Median_Inc_High['ZIP_CODE'].unique().tolist():
    if len(zipcodes.matching(zip_cd))>=1:
        city=zipcodes.matching(zip_cd)[0]['city']
        df=pd.DataFrame({"ZIP_CODE":zip_cd,"city":city},index=[zip_cd])
        unique_zip_city=unique_zip_city.append(df)
    else:
        print(zip_cd)
# No need to fill through google


# In[17]:

zip5_HH_Median_Inc_High=pd.merge(zip5_HH_Median_Inc_High,unique_zip_city,on="ZIP_CODE",how="left")


# In[18]:

zip5_HH_Median_Inc_High.head(2)


# In[22]:

zip5_HH_Median_Inc_High=zip5_HH_Median_Inc_High[['ZIP_CODE','DMA','city','state','HH18','Meidan_HH_Inc_agg','Pctg_HH_200P_agg','HH_200K_Plus']]


# In[23]:

writer=pd.ExcelWriter("/home/jian/Projects/Saatva/High_Income_Zips_for_29_DMAs/Saatva_Zip5 Median HH Inc above 100K for 29 DMAs_JL_20180823.xlsx",engine="xlsxwriter")
for dma in DMA_29:
    df=zip5_HH_Median_Inc_High[zip5_HH_Median_Inc_High['DMA']==dma]
    df.to_excel(writer,dma,index=False)
writer.save()


# In[ ]:



