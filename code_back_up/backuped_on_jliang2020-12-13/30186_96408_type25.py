# -*- coding: utf-8 -*-
"""
Created on Wed May 25 16:00:04 2016

@author: Jubaplus
"""
import pandas as pd

names25=['/home/account1/marymary/NPM_ARD_W_2015_07_05_EHH_R0/N150640.EHH',
'/home/account1/marymary/NPM_ARD_W_2015_07_12_EHH_R0/N150710.EHH',
'/home/account1/marymary/NPM_ARD_W_2015_07_19_EHH_R0/N150720.EHH',
'/home/account1/marymary/NPM_ARD_W_2015_07_26_EHH_R0/N150730.EHH',
'/home/account1/marymary/NPM_ARD_W_2015_08_02_EHH_R0/N150740.EHH',
'/home/account1/marymary/NPM_ARD_W_2015_08_09_EHH_R0/N150810.EHH',
'/home/account1/marymary/NPM_ARD_W_2015_08_16_EHH_R0/N150820.EHH',
'/home/account1/marymary/NPM_ARD_W_2015_08_23_EHH_R0/N150830.EHH',
'/home/account1/marymary/NPM_ARD_W_2015_08_30_EHH_R0/N150840.EHH',
'/home/account1/marymary/NPM_ARD_W_2015_09_06_EHH_R1/N150851.EHH',
'/home/account1/marymary/NPM_ARD_W_2015_09_13_EHH_R0/N150910.EHH',
'/home/account1/marymary/NPM_ARD_W_2015_09_20_EHH_R0/N150920.EHH',
'/home/account1/marymary/NPM_ARD_W_2015_09_27_EHH_R0/N150930.EHH',
'/home/account1/marymary/NPM_ARD_W_2015_10_04_EHH_R0/N150940.EHH',
'/home/account1/marymary/NPM_ARD_W_2015_10_11_EHH_R0/N151010.EHH',
'/home/account1/marymary/NPM_ARD_W_2015_10_18_EHH_R0/N151020.EHH',
'/home/account1/marymary/NPM_ARD_W_2015_10_25_EHH_R0/N151030.EHH',
'/home/account1/marymary/NPM_ARD_W_2015_11_01_EHH_R0/N151040.EHH',
'/disk2/Projects/NielsenData/NPM_ARD_W_2015_11_08_EHH_R0/N151110.EHH',
'/disk2/Projects/NielsenData/NPM_ARD_W_2015_11_15_EHH_R0/N151120.EHH',
'/disk2/Projects/NielsenData/NPM_ARD_W_2015_11_22_EHH_R0/N151130.EHH',
'/disk2/Projects/NielsenData/NPM_ARD_W_2015_11_29_EHH_R0/N151140.EHH',
'/disk2/Projects/NielsenData/NPM_ARD_W_2015_12_06_EHH_R0/N151150.EHH',
'/disk2/Projects/NielsenData/NPM_ARD_W_2015_12_13_EHH_R0/N151210.EHH',
'/disk2/Projects/NielsenData/NPM_ARD_W_2015_12_20_EHH_R0/N151220.EHH',
'/disk2/Projects/NielsenData/NPM_ARD_W_2015_12_27_EHH_R0/N151230.EHH',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_01_03_EHH_R0/N161240.EHH',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_01_10_EHH_R0/N160110.EHH',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_01_17_EHH_R0/N160120.EHH',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_01_24_EHH_R0/N160130.EHH',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_01_31_EHH_R0/N160140.EHH',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_02_07_EHH_R0/N160150.EHH',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_02_14_EHH_R0/N160210.EHH',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_02_21_EHH_R0/N160220.EHH',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_02_28_EHH_R0/N160230.EHH',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_03_06_EHH_R0/N160240.EHH',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_03_13_EHH_R0/N160310.EHH',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_03_20_EHH_R0/N160320.EHH',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_03_27_EHH_R0/N160330.EHH',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_04_03_EHH_R0/N160340.EHH',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_04_10_EHH_R0/N160410.EHH',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_04_17_EHH_R0/N160420.EHH',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_04_24_EHH_R0/N160430.EHH',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_05_01_EHH_R0/N160440.EHH',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_05_08_EHH_R0/N160510.EHH',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_05_15_EHH_R0/N160520.EHH',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_05_22_EHH_R0/N160530.EHH',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_05_29_EHH_R0/N160540.EHH',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_06_05_EHH_R0/N160550.EHH',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_06_12_EHH_R0/N160610.EHH']
week=['20150705',
'20150712',
'20150719',
'20150726',
'20150802',
'20150809',
'20150816',
'20150823',
'20150830',
'20150906',
'20150913',
'20150920',
'20150927',
'20151004',
'20151011',
'20151018',
'20151025',
'20151101',
'20151108',
'20151115',
'20151122',
'20151129',
'20151206',
'20151213',
'20151220',
'20151227',
'20160103',
'20160110',
'20160117',
'20160124',
'20160131',
'20160207',
'20160214',
'20160221',
'20160228',
'20160306',
'20160313',
'20160320',
'20160327',
'20160403',
'20160410',
'20160417',
'20160424',
'20160501',
'20160508',
'20160515',
'20160522',
'20160529',
'20160605',
'20160612']

df=pd.DataFrame()
df2=pd.DataFrame()
type25=pd.DataFrame()
type26=pd.DataFrame()
for i,j in zip(names25,week):
    df=pd.read_csv(i,sep='\t')
    df.columns=['all']
    df['type']=df['all'].str[0:2]
    df2=df[df['type']=='26']
    df=df[df['type']=='25']
    df['HHID']=df['all'].str[13:20]
    df['Week']=df['all'].str[5:13]
    df=df[df['Week']==j]
    df['Mon']=df['all'].str[20:26]
    df['Tue']=df['all'].str[26:32]
    df['Wed']=df['all'].str[32:38]
    df['Thu']=df['all'].str[38:44]
    df['Fri']=df['all'].str[44:50]
    df['Sat']=df['all'].str[50:56]
    df['Sun']=df['all'].str[56:62]
    df=df[['HHID', 'Week', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat','Sun']]
    df=df.drop_duplicates()   
    type25=type25.append(df,ignore_index=True)
    df2['HHID']=df2['all'].str[13:20]
    df2['PersonID']=df2['all'].str[20:22]
    df2['Week']=df2['all'].str[5:13]
    df2=df2[df2['Week']==j]
    df2['Mon']=df2['all'].str[22:28]
    df2['Tue']=df2['all'].str[28:34]
    df2['Wed']=df2['all'].str[34:40]
    df2['Thu']=df2['all'].str[40:46]
    df2['Fri']=df2['all'].str[46:52]
    df2['Sat']=df2['all'].str[52:58]
    df2['Sun']=df2['all'].str[58:64]
    df2=df2[['HHID','PersonID','Week', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat','Sun']]
    df2=df2.drop_duplicates()   
    type26=type26.append(df2,ignore_index=True)
    
df=pd.DataFrame()
type25v2=pd.DataFrame()
week=['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat','Sun']
number=[1,2,3,4,5,6,7]
for i,j in zip(week,number):
    df=type25[['HHID', 'Week',i]]
    df.columns=['HHID', 'Week','HHIntab']
    df['Weeknumber']=j
    df=df[df['HHIntab']!='000000']
    type25v2=type25v2.append(df,ignore_index=True)

type25v2.to_csv('/disk2/Projects/SpencerTest/type25all.csv',index=False,dtype={'HHID':'object'})


df=pd.DataFrame()
type26v2=pd.DataFrame()
week=['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat','Sun']
number=[1,2,3,4,5,6,7]
for i,j in zip(week,number):
    df=type26[['HHID', 'Week','PersonID',i]]
    df.columns=['HHID', 'Week','PersonID','PersonIntab']
    df['Weeknumber']=j
    df=df[df['PersonIntab']!='000000']
    type26v2=type26v2.append(df,ignore_index=True)

type26v2.to_csv('/disk2/Projects/SpencerTest/type26all.csv',index=False,dtype={'HHID':'object','PersonID':'object'})
