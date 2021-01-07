# -*- coding: utf-8 -*-
"""
Created on Mon May 23 10:30:00 2016

@author: Jubaplus
"""

import pandas as pd
import numpy as np
import gc

HHIDs=pd.read_csv("/Users/Jubaplus/Desktop/showaffinitysund(1).csv",dtype={'HHID':'object'})
HHIDs['HHID'] = HHIDs['HHID'].apply(lambda x: x.zfill(7))

names36=[
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2015_06_14_CBLNM_R0/N15061V0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2015_06_21_CBLNM_R0/N15062V0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2015_06_28_CBLNM_R0/N15063V0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2015_07_05_CBLNM_R0/N15064V0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2015_07_12_CBLNM_R0/N15071V0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2015_07_19_CBLNM_R0/N15072V0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2015_07_26_CBLNM_R0/N15073V0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2015_08_02_CBLNM_R0/N15074V0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2015_08_09_CBLNM_R0/N15081V0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2015_08_16_CBLNM_R0/N15082V0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2015_08_23_CBLNM_R0/N15083V0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2015_08_30_CBLNM_R0/N15084V0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2015_09_06_CBLNM_R0/N15085V0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2015_09_13_CBLNM_R0/N15091V0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2015_09_20_CBLNM_R0/N15092V0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2015_09_27_CBLNM_R0/N15093V0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2015_10_04_CBLNM_R0/N15094V0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2015_10_11_CBLNM_R0/N15101V0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2015_10_18_CBLNM_R0/N15102V0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2015_10_25_CBLNM_R0/N15103V0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2015_11_01_CBLNM_R0/N15104V0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2015_11_08_CBLNM_R0/N15111V0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2015_11_15_CBLNM_R0/N15112V0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2015_11_22_CBLNM_R0/N15113V0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2015_11_29_CBLNM_R0/N15114V0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2015_12_06_CBLNM_R0/N15115V0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2015_12_13_CBLNM_R0/N15121V0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2015_12_20_CBLNM_R0/N15122V0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2015_12_27_CBLNM_R0/N15123V0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2016_01_03_CBLNM_R0/N16124V0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2016_01_10_CBLNM_R1/N16011V1.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2016_01_17_CBLNM_R0/N16012V0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2016_01_24_CBLNM_R0/N16013V0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2016_01_31_CBLNM_R0/N16014V0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2016_02_07_CBLNM_R0/N16015V0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2016_02_14_CBLNM_R0/N16021V0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2016_02_21_CBLNM_R0/N16022V0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2016_02_28_CBLNM_R0/N16023V0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2016_03_06_CBLNM_R0/N16024V0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2016_03_13_CBLNM_R0/N16031V0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2016_03_20_CBLNM_R0/N16032V0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2016_03_27_CBLNM_R0/N16033V0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2016_04_03_CBLNM_R0/N16034V0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2016_04_10_CBLNM_R0/N16041V0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2016_04_17_CBLNM_R0/N16042V0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2016_04_24_CBLNM_R0/N16043V0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2016_05_01_CBLNM_R0/N16044V0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2016_05_08_CBLNM_R0/N16051V0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2016_05_15_CBLNM_R0/N16052V0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2016_05_22_CBLNM_R0/N16053V0.ECN']
names30=['/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2015_06_14_CBLNM_R0/N15061D0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2015_06_21_CBLNM_R0/N15062D0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2015_06_28_CBLNM_R0/N15063D0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2015_07_05_CBLNM_R0/N15064D0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2015_07_12_CBLNM_R0/N15071D0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2015_07_19_CBLNM_R0/N15072D0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2015_07_26_CBLNM_R0/N15073D0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2015_08_02_CBLNM_R0/N15074D0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2015_08_09_CBLNM_R0/N15081D0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2015_08_16_CBLNM_R0/N15082D0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2015_08_23_CBLNM_R0/N15083D0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2015_08_30_CBLNM_R0/N15084D0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2015_09_06_CBLNM_R0/N15085D0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2015_09_13_CBLNM_R0/N15091D0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2015_09_20_CBLNM_R0/N15092D0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2015_09_27_CBLNM_R0/N15093D0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2015_10_04_CBLNM_R0/N15094D0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2015_10_11_CBLNM_R0/N15101D0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2015_10_18_CBLNM_R0/N15102D0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2015_10_25_CBLNM_R0/N15103D0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2015_11_01_CBLNM_R0/N15104D0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2015_11_08_CBLNM_R0/N15111D0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2015_11_15_CBLNM_R0/N15112D0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2015_11_22_CBLNM_R0/N15113D0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2015_11_29_CBLNM_R0/N15114D0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2015_12_06_CBLNM_R0/N15115D0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2015_12_13_CBLNM_R0/N15121D0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2015_12_20_CBLNM_R0/N15122D0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2015_12_27_CBLNM_R0/N15123D0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2016_01_03_CBLNM_R0/N16124D0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2016_01_10_CBLNM_R1/N16011D1.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2016_01_17_CBLNM_R0/N16012D0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2016_01_24_CBLNM_R0/N16013D0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2016_01_31_CBLNM_R0/N16014D0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2016_02_07_CBLNM_R0/N16015D0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2016_02_14_CBLNM_R0/N16021D0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2016_02_21_CBLNM_R0/N16022D0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2016_02_28_CBLNM_R0/N16023D0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2016_03_06_CBLNM_R0/N16024D0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2016_03_13_CBLNM_R0/N16031D0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2016_03_20_CBLNM_R0/N16032D0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2016_03_27_CBLNM_R0/N16033D0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2016_04_03_CBLNM_R0/N16034D0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2016_04_10_CBLNM_R0/N16041D0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2016_04_17_CBLNM_R0/N16042D0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2016_04_24_CBLNM_R0/N16043D0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2016_05_01_CBLNM_R0/N16044D0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2016_05_08_CBLNM_R0/N16051D0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2016_05_15_CBLNM_R0/N16052D0.ECN',
'/Users/Jubaplus/Desktop/Nielsen/NPM_ARD_W_2016_05_22_CBLNM_R0/N16053D0.ECN']

df=pd.DataFrame()
type36=pd.DataFrame()
for i in names36:
    df=pd.read_csv(i,sep='\t')
    df.columns=['all']
    df['type']=df['all'].str[0:2]
    df=df[df['type']=='36']
    df['CableID']=df['all'].str[3:9]
    df=df[df.CableID.isin(['006593','008250','006111','007602','007516'])]
    df['HHID']=df['all'].str[29:36]
    df['Date']=df['all'].str[68:76]
    df['PersonID']=df['all'].str[36:38]
    df=df[df['PersonID']!='00']
    df=pd.merge(df,HHIDs,on=['HHID'])
    type36=type36.append(df,ignore_index=True)
 
type36['Programcode']=type36['all'].str[9:16]
type36['TelecastN']=type36['all'].str[16:23]
type36['ViewStartTime']=type36['all'].str[38:44]
type36['ViewEndTime']=type36['all'].str[44:50]


df=pd.DataFrame()
type30=pd.DataFrame()
for i in names30:
    df=pd.read_csv(i,sep='\t')
    df.columns=['all']
    df['type']=df['all'].str[0:2]
    df=df[df['type']=='30']
    df['Date']=df['all'].str[39:47]
    df['CableID']=df['all'].str[8:14]
    df=df[df.CableID.isin(['006593','008250','006111','007602','007516'])]
    type30=type30.append(df,ignore_index=True)
  

type30['CableName']=type30['all'].str[3:8]
type30['Programcode']=type30['all'].str[14:21]
type30['ProgramName']=type30['all'].str[55:80]
type30['TelecastN']=type30['all'].str[21:28]
type30['StartTime']=type30['all'].str[88:90]
type30['ProgramType']=type30['all'].str[122:126]
type30['EpisodeName']=type30['all'].str[130:170]
type30['EpisodeName']=type30.EpisodeName.str.strip()
type30=type30[type30['Programcode']!='0364702']

type30.to_csv("/Users/Jubaplus/Desktop/allprograms.csv",index=False)

allfile=pd.merge(type36,type30,on=['CableID','Programcode','TelecastN'])
allfile=allfile[['CableID', 'HHID', 'Date_x', 'Programcode',
       'TelecastN', 'ViewStartTime', 'ViewEndTime', 'PersonID',
       'Date_y', 'CableName', 'ProgramName', 'StartTime','ProgramType','EpisodeName']]
allfile.columns=['CableID', 'HHID', 'ViewDate', 'Programcode',
       'TelecastN', 'ViewStartTime', 'ViewEndTime', 'PersonID',
       'ProgramDate', 'CableName', 'ProgramName', 'StartTime','ProgramType','EpisodeName']
allfile=allfile.drop_duplicates()

allfile['ViewStartTime']=pd.to_numeric(allfile['ViewStartTime'])  
allfile['ViewEndTime']=pd.to_numeric(allfile['ViewEndTime']) 
allfile['min']=allfile['ViewEndTime']-allfile['ViewStartTime']    
allfile['ProgramDateWD']=pd.to_datetime(allfile['ProgramDate'])
allfile['ProgramDateWD']=allfile['ProgramDateWD'].dt.dayofweek

days = {0:'1Mon',1:'2Tue',2:'3Wed',3:'4Thu',4:'5Fri',5:'6Sat',6:'7Sun'}

allfile['ProgramDateWD']= allfile['ProgramDateWD'].apply(lambda x: days[x])
allfile=allfile.drop_duplicates()
allfile['freq']=1
allfile['StartTime']=allfile['CableName']+'_'+allfile['StartTime']
allfile['ProgramDateWD']=allfile['CableName']+'_'+allfile['ProgramDateWD']
allfile['programnew']=np.where((allfile['ProgramType']=='FF  ')&(allfile['EpisodeName']!='N/A'),
allfile['CableName']+'_'+allfile['EpisodeName'],allfile['CableName']+'_'+allfile['ProgramName'])

allfile.to_csv('/Users/Jubaplus/Desktop/alltransaction0527.csv',index=False)

del df
del type30
del type36
gc.collect()

all2=allfile[['CableID', 'HHID', 'Programcode',
       'TelecastN', 
       'ProgramDate', 'CableName', 'programnew', 'StartTime','ProgramDateWD','freq','ProgramType']]
all2=all2.drop_duplicates()

hourfreq=all2[['StartTime','HHID','freq']]
hourfreq=pd.pivot_table(hourfreq, values=['freq'], index=['HHID'], columns=['StartTime'], aggfunc=np.sum)
hourfreq=hourfreq.fillna(0)
hourfreq.to_csv('/Users/Jubaplus/Desktop/hourfreq.csv')

hourmin=allfile[['StartTime','HHID','min']]
hourmin=pd.pivot_table(hourmin, values=['min'], index=['HHID'], columns=['StartTime'], aggfunc=np.sum)
hourmin=hourmin.fillna(0)
hourmin.to_csv('/Users/Jubaplus/Desktop/hourmin.csv')

dayfreq=all2[['ProgramDateWD','HHID','freq']]
dayfreq=pd.pivot_table(dayfreq, values=['freq'], index=['HHID'], columns=['ProgramDateWD'], aggfunc=np.sum)
dayfreq=dayfreq.fillna(0)
dayfreq.to_csv('/Users/Jubaplus/Desktop/dayfreq.csv')

daymin=allfile[['ProgramDateWD','HHID','min']]
daymin=pd.pivot_table(daymin, values=['min'], index=['HHID'], columns=['ProgramDateWD'], aggfunc=np.sum)
daymin=daymin.fillna(0)
daymin.to_csv('/Users/Jubaplus/Desktop/daymin.csv')

programfreq=all2[all2['ProgramType']!='FF  ']
programfreq=programfreq[['programnew','HHID','freq']]
programfreq=pd.pivot_table(programfreq, values=['freq'], index=['HHID'], columns=['programnew'], aggfunc=np.sum)
programfreq=programfreq.fillna(0)
programfreq.to_csv('/Users/Jubaplus/Desktop/programnonfffreq.csv')

programmin=allfile[allfile['ProgramType']!='FF  ']
programmin=programmin[['programnew','HHID','min']]
programmin=pd.pivot_table(programmin, values=['min'], index=['HHID'], columns=['programnew'], aggfunc=np.sum)
programmin=programmin.fillna(0)
programmin.to_csv('/Users/Jubaplus/Desktop/programnonffmin.csv')

programfreq=all2[all2['ProgramType']=='FF  ']
programfreq=programfreq[['programnew','HHID','freq']]
programfreq=pd.pivot_table(programfreq, values=['freq'], index=['HHID'], columns=['programnew'], aggfunc=np.sum)
programfreq=programfreq.fillna(0)
programfreq.to_csv('/Users/Jubaplus/Desktop/episodefffreq.csv')

programmin=allfile[allfile['ProgramType']=='FF  ']
programmin=programmin[['programnew','HHID','min']]
programmin=pd.pivot_table(programmin, values=['min'], index=['HHID'], columns=['programnew'], aggfunc=np.sum)
programmin=programmin.fillna(0)
programmin.to_csv('/Users/Jubaplus/Desktop/episodeffmin.csv')




