# -*- coding: utf-8 -*-
"""
Created on Wed May 25 16:00:04 2016

@author: Jubaplus
"""
import pandas as pd

names2024=[
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_08_21_EHH_R0/N160820.EHH',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_08_14_EHH_R0/N160810.EHH',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_08_07_EHH_R0/N160750.EHH',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_07_31_EHH_R0/N160740.EHH',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_07_24_EHH_R0/N160730.EHH',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_07_17_EHH_R0/N160720.EHH',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_07_10_EHH_R0/N160710.EHH',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_07_03_EHH_R0/N160640.EHH',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_06_26_EHH_R0/N160630.EHH',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_06_19_EHH_R0/N160620.EHH',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_06_12_EHH_R0/N160610.EHH',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_06_05_EHH_R0/N160550.EHH',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_05_29_EHH_R0/N160540.EHH',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_05_22_EHH_R0/N160530.EHH',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_05_15_EHH_R0/N160520.EHH',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_05_08_EHH_R0/N160510.EHH',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_05_01_EHH_R0/N160440.EHH',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_04_24_EHH_R0/N160430.EHH',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_04_17_EHH_R0/N160420.EHH',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_04_10_EHH_R0/N160410.EHH',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_04_03_EHH_R0/N160340.EHH',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_03_27_EHH_R0/N160330.EHH',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_03_20_EHH_R0/N160320.EHH',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_03_13_EHH_R0/N160310.EHH',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_03_06_EHH_R0/N160240.EHH',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_02_28_EHH_R0/N160230.EHH',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_02_21_EHH_R0/N160220.EHH']

names14=[
'/disk2/Projects/NielsenData/NPM_MRD_W_2016_08_21_HHC_R0/W160820.HHC',
'/disk2/Projects/NielsenData/NPM_MRD_W_2016_08_14_HHC_R0/W160810.HHC',
'/disk2/Projects/NielsenData/NPM_MRD_W_2016_08_07_HHC_R0/W160750.HHC',
'/disk2/Projects/NielsenData/NPM_MRD_W_2016_07_31_HHC_R0/W160740.HHC',
'/disk2/Projects/NielsenData/NPM_MRD_W_2016_07_24_HHC_R0/W160730.HHC',
'/disk2/Projects/NielsenData/NPM_MRD_W_2016_07_17_HHC_R0/W160720.HHC',
'/disk2/Projects/NielsenData/NPM_MRD_W_2016_07_10_HHC_R0/W160710.HHC',
'/disk2/Projects/NielsenData/NPM_MRD_W_2016_07_03_HHC_R0/W160640.HHC',
'/disk2/Projects/NielsenData/NPM_MRD_W_2016_06_26_HHC_R0/W160630.HHC',
'/disk2/Projects/NielsenData/NPM_MRD_W_2016_06_19_HHC_R0/W160620.HHC',
'/disk2/Projects/NielsenData/NPM_MRD_W_2016_06_12_HHC_R0/W160610.HHC',
'/disk2/Projects/NielsenData/NPM_MRD_W_2016_06_05_HHC_R0/W160550.HHC',
'/disk2/Projects/NielsenData/NPM_MRD_W_2016_05_29_HHC_R0/W160540.HHC',
'/disk2/Projects/NielsenData/NPM_MRD_W_2016_05_22_HHC_R0/W160530.HHC',
'/disk2/Projects/NielsenData/NPM_MRD_W_2016_05_15_HHC_R0/W160520.HHC',
'/disk2/Projects/NielsenData/NPM_MRD_W_2016_05_08_HHC_R0/W160510.HHC',
'/disk2/Projects/NielsenData/NPM_MRD_W_2016_05_01_HHC_R0/W160440.HHC',
'/disk2/Projects/NielsenData/NPM_MRD_W_2016_04_24_HHC_R0/W160430.HHC',
'/disk2/Projects/NielsenData/NPM_MRD_W_2016_04_17_HHC_R0/W160420.HHC',
'/disk2/Projects/NielsenData/NPM_MRD_W_2016_04_10_HHC_R0/W160410.HHC',
'/disk2/Projects/NielsenData/NPM_MRD_W_2016_04_03_HHC_R0/W160340.HHC',
'/disk2/Projects/NielsenData/NPM_MRD_W_2016_03_27_HHC_R0/W160330.HHC',
'/disk2/Projects/NielsenData/NPM_MRD_W_2016_03_20_HHC_R0/W160320.HHC',
'/disk2/Projects/NielsenData/NPM_MRD_W_2016_03_13_HHC_R0/W160310.HHC',
'/disk2/Projects/NielsenData/NPM_MRD_W_2016_03_06_HHC_R0/W160240.HHC',
'/disk2/Projects/NielsenData/NPM_MRD_W_2016_02_28_HHC_R0/W160230.HHC',
'/disk2/Projects/NielsenData/NPM_MRD_W_2016_02_21_HHC_R0/W160220.HHC']


names65=[
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_08_21_MVD_R0/N160820.MVD',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_08_14_MVD_R0/N160810.MVD',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_08_07_MVD_R0/N160750.MVD',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_07_31_MVD_R0/N160740.MVD',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_07_24_MVD_R0/N160730.MVD',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_07_17_MVD_R0/N160720.MVD',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_07_10_MVD_R0/N160710.MVD',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_07_03_MVD_R0/N160640.MVD',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_06_26_MVD_R0/N160630.MVD',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_06_19_MVD_R0/N160620.MVD',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_06_12_MVD_R0/N160610.MVD',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_06_05_MVD_R0/N160550.MVD',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_05_29_MVD_R0/N160540.MVD',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_05_22_MVD_R0/N160530.MVD',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_05_15_MVD_R0/N160520.MVD',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_05_08_MVD_R0/N160510.MVD',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_05_01_MVD_R0/N160440.MVD',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_04_24_MVD_R1/N160431.MVD',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_04_17_MVD_R0/N160420.MVD',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_04_10_MVD_R0/N160410.MVD',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_04_03_MVD_R0/N160340.MVD',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_03_27_MVD_R0/N160330.MVD',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_03_20_MVD_R0/N160320.MVD',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_03_13_MVD_R0/N160310.MVD',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_03_06_MVD_R0/N160240.MVD',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_02_28_MVD_R0/N160230.MVD',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_02_21_MVD_R0/N160220.MVD']

df=pd.DataFrame()
df2=pd.DataFrame()
type20=pd.DataFrame()
type24=pd.DataFrame()
for i in names2024:
    df=pd.read_csv(i,sep='\t')
    df.columns=['all']
    df['type']=df['all'].str[0:2]
    df=df[df['type']=='20']
    df=df.drop_duplicates()   
    df['HHID']=df['all'].str[13:20]
    type20=type20.append(df,ignore_index=True)
    
type20=type20.drop_duplicates('HHID') 

df=pd.DataFrame()
type14=pd.DataFrame()
for i in names14:
    df=pd.read_csv(i,sep='\t')
    df.columns=['all']
    df['type']=df['all'].str[0:2]
    df=df[df['type']=='14']
    df['HHID']=df['all'].str[13:20]
    df['LPMFlag']=df['all'].str[64:65]
    df['LPMCode']=df['all'].str[65:68]
    df=df.drop_duplicates()   
    type14=type14.append(df,ignore_index=True)
    
type14=type14[['HHID','LPMFlag','LPMCode']]
type14=type14.drop_duplicates('HHID') 
alldemo=pd.merge(type20,type14,on='HHID')

df=pd.DataFrame()
type65=pd.DataFrame()
for i in names65:
    df=pd.read_csv(i,sep='\t')
    df.columns=['all']
    df['type']=df['all'].str[0:2]
    df=df[df['type']=='65']
    df['HHID']=df['all'].str[13:20]
    df['ATT']=df['all'].str[37:38]
    df['Brighthouse']=df['all'].str[38:39]
    df['Cablevision']=df['all'].str[39:40]
    df['Charter']=df['all'].str[40:41]
    df['Comcast']=df['all'].str[41:42]
    df['Cox']=df['all'].str[42:43]
    df['Time Warner']=df['all'].str[43:44]
    df['Verizon']=df['all'].str[44:45]
    df['DirecTV']=df['all'].str[45:46]
    df['Dish Network']=df['all'].str[46:47]
    df=df.drop_duplicates()   
    type65=type65.append(df,ignore_index=True)
    
type65=type65.drop_duplicates('HHID') 

df=pd.DataFrame()
type65v2=pd.DataFrame()
MVPD=['ATT', 'Brighthouse', 'Cablevision', 'Charter', 'Comcast', 'Cox','Time Warner','Verizon','DirecTV','Dish Network']
for i in MVPD:
    df=type65[['HHID', i]]
    df.columns=['HHID', 'MVPD']
    df=df[df['MVPD']=='Y']
    df['MVPD']=i
    type65v2=type65v2.append(df,ignore_index=True)
    
HHIDs=alldemo[['HHID']]
type65v2=type65v2.drop_duplicates('HHID')
type65v2=pd.merge(HHIDs,type65v2,on='HHID',how='outer')
type65v2=type65v2.fillna('Other')
type65v2=type65v2.drop_duplicates('HHID')
alldemo=pd.merge(alldemo,type65v2,on='HHID')
alldemo=alldemo.drop_duplicates(['HHID'])

alldemo.to_csv('/disk2/PostAnalysis/demographichhid0221-0821.csv',index=False,dtype={'HHID':'object'})

del type65
del type65v2
del df
del type20
del type14
del names2024
del names14
del names65
del df2
del alldemo
del HHIDs
import gc
gc.collect()
