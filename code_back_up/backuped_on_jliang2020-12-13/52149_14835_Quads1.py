# -*- coding: utf-8 -*-
"""
Created on Mon Jun  6 15:02:11 2016

@author: Jubaplus
"""
import pandas as pd
import numpy as np

###download this file from server
HHIntab=pd.read_csv("/Users/jubaplus1/Desktop/Nielsen/type250717.csv",dtype={'Week':'object','Weeknumber':'int'})
HHIntab['ViewDate2']=pd.to_datetime(HHIntab['Week'])
HHIntab.groupby('Weeknumber').count()
HHIntab['Weeknumber']=HHIntab['Weeknumber']-7

HHIntab['ViewDate']=np.where(HHIntab['Weeknumber']==-1,(HHIntab['ViewDate2']+pd.DateOffset(-1)),
np.where(HHIntab['Weeknumber']==-2,(HHIntab['ViewDate2']+pd.DateOffset(-2)),
np.where(HHIntab['Weeknumber']==-3,(HHIntab['ViewDate2']+pd.DateOffset(-3)),
np.where(HHIntab['Weeknumber']==-4,(HHIntab['ViewDate2']+pd.DateOffset(-4)),
np.where(HHIntab['Weeknumber']==-5,(HHIntab['ViewDate2']+pd.DateOffset(-5)),
np.where(HHIntab['Weeknumber']==-6,(HHIntab['ViewDate2']+pd.DateOffset(-6)),
(HHIntab['ViewDate2'])))))))
HHIntab=HHIntab[['HHID', 'HHIntab','ViewDate']]
HHIntab=HHIntab[HHIntab['HHIntab']>0]
HHIntab=HHIntab.drop_duplicates(['HHID', 'ViewDate'])

##update date to last three month
HHIntab2=HHIntab[(HHIntab['ViewDate']>='2016-04-17')&(HHIntab['ViewDate']<='2016-07-17')]

HHIntab2=HHIntab2.groupby('HHID').mean()
HHIntab2.reset_index(inplace=True)


##quads
Startdate=20160417
Enddate=20160717

names36=[
'/Users/Jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_04_17_CBLNM_R0/N16042V0.ECN',
'/Users/Jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_04_24_CBLNM_R0/N16043V0.ECN',
'/Users/Jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_05_01_CBLNM_R0/N16044V0.ECN',
'/Users/Jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_05_08_CBLNM_R0/N16051V0.ECN',
'/Users/Jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_05_15_CBLNM_R0/N16052V0.ECN',
'/Users/Jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_05_22_CBLNM_R0/N16053V0.ECN',
'/Users/Jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_05_29_CBLNM_R0/N16054V0.ECN',
'/Users/Jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_06_05_CBLNM_R0/N16055V0.ECN',
'/Users/Jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_06_12_CBLNM_R0/N16061V0.ECN',
'/Users/Jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_06_19_CBLNM_R0/N16062V0.ECN',
'/Users/Jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_06_26_CBLNM_R0/N16063V0.ECN',
'/Users/jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_07_03_CBLNM_R0/N16064V0.ECN',
'/Users/jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_07_10_CBLNM_R0/N16071V0.ECN',
'/Users/jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_07_17_CBLNM_R1/N16072V1.ECN']
names30=[
'/Users/Jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_04_17_CBLNM_R0/N16042D0.ECN',
'/Users/Jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_04_24_CBLNM_R0/N16043D0.ECN',
'/Users/Jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_05_01_CBLNM_R0/N16044D0.ECN',
'/Users/Jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_05_08_CBLNM_R0/N16051D0.ECN',
'/Users/Jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_05_15_CBLNM_R0/N16052D0.ECN',
'/Users/Jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_05_22_CBLNM_R0/N16053D0.ECN',
'/Users/Jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_05_29_CBLNM_R0/N16054D0.ECN',
'/Users/Jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_06_05_CBLNM_R0/N16055D0.ECN',
'/Users/Jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_06_12_CBLNM_R0/N16061D0.ECN',
'/Users/Jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_06_19_CBLNM_R0/N16062D0.ECN',
'/Users/Jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_06_26_CBLNM_R0/N16063D0.ECN',
'/Users/jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_07_03_CBLNM_R0/N16064D0.ECN',
'/Users/jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_07_10_CBLNM_R0/N16071D0.ECN',
'/Users/jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_07_17_CBLNM_R1/N16072D1.ECN']

df=pd.DataFrame()
type36=pd.DataFrame()
for i in names36:
    df=pd.read_csv(i,sep='\t')
    df.columns=['all']
    df['type']=df['all'].str[0:2]
    df=df[df['type']=='36']
    df['HHID']=df['all'].str[29:36]
    df['CableID']=df['all'].str[3:9]
    df=df[df['CableID']=='009157']
    df['PersonID']=df['all'].str[36:38]
    df=df[df['PersonID']!='00']
    df['Programcode']=df['all'].str[9:16]
    df['TelecastN']=df['all'].str[16:23]
    type36=type36.append(df,ignore_index=True)
 
   
df=pd.DataFrame()
type30=pd.DataFrame()
for i in names30:
    df=pd.read_csv(i,sep='\t')
    df.columns=['all']
    df['type']=df['all'].str[0:2]
    df=df[df['type']=='30']
    df['ProgramDate']=df['all'].str[39:47]
    df['CableID']=df['all'].str[8:14]
    df=df[df['CableID']=='009157']
    type30=type30.append(df,ignore_index=True)
  
type30['Programcode']=type30['all'].str[14:21]
type30['TelecastN']=type30['all'].str[21:28]
type30['StartTime']=type30['all'].str[88:92].astype(int)
type30['EndTime']=type30['all'].str[94:98].astype(int)
type30['Date2']=type30['ProgramDate'].astype('int')
type30=type30[(type30['Date2']>=Startdate)&(type30['Date2']<=Enddate)]

###hash below line if you want all time
#type30=type30[(((type30['StartTime']>2000)&(type30['StartTime']<2300))|((type30['EndTime']>2000)&(type30['EndTime']<2300)))]
#type30['Weekday']=pd.to_datetime(type30['ProgramDate']).dt.dayofweek
#type30=type30[type30.Weekday.isin([3,4])]


STARZviewers=pd.merge(type36,type30,on=['CableID','Programcode','TelecastN'])

STARZviewers=STARZviewers[['HHID']]
STARZviewers['Quads']=1
STARZviewers=STARZviewers.drop_duplicates()
STARZviewers['HHID']=STARZviewers['HHID'].astype(int)

df=pd.merge(STARZviewers,HHIntab2,on='HHID')

df.to_csv("/Users/jubaplus1/Desktop/Starz - American Gods/STARZ 3month.csv")












