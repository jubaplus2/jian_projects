# -*- coding: utf-8 -*-
"""
Created on Mon May 23 10:30:00 2016

@author: Jubaplus
"""

import pandas as pd
import numpy as np

df=pd.read_csv("/Users/jubaplus1/Desktop/Starz - Ash vs Evil Dead/IT-HH-ViewershipDayPartsNew.csv",sep='\t',dtype={'Household Number':'object'})
HHIDs=df[['Household Number']]
HHIDs=HHIDs.drop_duplicates()
HHIDs.columns=['HHID']
HHIDs['HHID'] = HHIDs['HHID'].apply(lambda x: x.zfill(7))
print(HHIDs)

###put IT Shows here
itlist=[
'845515']

names36=[
'/Users/jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_01_17_BPGM_R0/N16012V0.EPG',
'/Users/jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_01_24_BPGM_R0/N16013V0.EPG',
'/Users/jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_01_31_BPGM_R0/N16014V0.EPG',
'/Users/jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_02_07_BPGM_R0/N16015V0.EPG',
'/Users/jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_02_14_BPGM_R0/N16021V0.EPG',
'/Users/jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_02_21_BPGM_R0/N16022V0.EPG',
'/Users/jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_02_28_BPGM_R0/N16023V0.EPG',
'/Users/jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_03_06_BPGM_R0/N16024V0.EPG',
'/Users/jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_03_13_BPGM_R0/N16031V0.EPG',
'/Users/jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_03_20_BPGM_R0/N16032V0.EPG',
'/Users/jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_03_27_BPGM_R0/N16033V0.EPG',
'/Users/jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_04_03_BPGM_R0/N16034V0.EPG',
'/Users/jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_04_10_BPGM_R0/N16041V0.EPG',
'/Users/jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_04_17_BPGM_R0/N16042V0.EPG',
'/Users/jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_04_24_BPGM_R0/N16043V0.EPG',
'/Users/jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_05_01_BPGM_R0/N16044V0.EPG',
'/Users/jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_05_08_BPGM_R0/N16051V0.EPG',
'/Users/jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_05_15_BPGM_R0/N16052V0.EPG',
'/Users/jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_05_22_BPGM_R0/N16053V0.EPG',
'/Users/jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_05_29_BPGM_R0/N16054V0.EPG',
'/Users/jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_06_05_BPGM_R0/N16055V0.EPG',
'/Users/jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_06_12_BPGM_R0/N16061V0.EPG',
'/Users/jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_06_19_BPGM_R0/N16062V0.EPG',
'/Users/jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_06_26_BPGM_R0/N16063V0.EPG',
'/Users/jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_07_03_BPGM_R0/N16064V0.EPG',
'/Users/jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_07_10_BPGM_R0/N16071V0.EPG',
'/Users/jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_07_17_BPGM_R1/N16072V1.EPG'
]

names30=[
'/Users/jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_01_17_BPGM_R0/N16012D0.EPG',
'/Users/jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_01_24_BPGM_R0/N16013D0.EPG',
'/Users/jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_01_31_BPGM_R0/N16014D0.EPG',
'/Users/jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_02_07_BPGM_R0/N16015D0.EPG',
'/Users/jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_02_14_BPGM_R0/N16021D0.EPG',
'/Users/jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_02_21_BPGM_R0/N16022D0.EPG',
'/Users/jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_02_28_BPGM_R0/N16023D0.EPG',
'/Users/jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_03_06_BPGM_R0/N16024D0.EPG',
'/Users/jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_03_13_BPGM_R0/N16031D0.EPG',
'/Users/jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_03_20_BPGM_R0/N16032D0.EPG',
'/Users/jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_03_27_BPGM_R0/N16033D0.EPG',
'/Users/jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_04_03_BPGM_R0/N16034D0.EPG',
'/Users/jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_04_10_BPGM_R0/N16041D0.EPG',
'/Users/jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_04_17_BPGM_R0/N16042D0.EPG',
'/Users/jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_04_24_BPGM_R0/N16043D0.EPG',
'/Users/jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_05_01_BPGM_R0/N16044D0.EPG',
'/Users/jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_05_08_BPGM_R0/N16051D0.EPG',
'/Users/jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_05_15_BPGM_R0/N16052D0.EPG',
'/Users/jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_05_22_BPGM_R0/N16053D0.EPG',
'/Users/jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_05_29_BPGM_R0/N16054D0.EPG',
'/Users/jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_06_05_BPGM_R0/N16055D0.EPG',
'/Users/jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_06_12_BPGM_R0/N16061D0.EPG',
'/Users/jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_06_19_BPGM_R0/N16062D0.EPG',
'/Users/jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_06_26_BPGM_R0/N16063D0.EPG',
'/Users/jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_07_03_BPGM_R0/N16064D0.EPG',
'/Users/jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_07_10_BPGM_R0/N16071D0.EPG',
'/Users/jubaplus1/Desktop/Nielsen/NPM_ARD_W_2016_07_17_BPGM_R1/N16072D1.EPG'
]

df=pd.DataFrame()
type36=pd.DataFrame()
for i in names36:
    df=pd.read_csv(i,sep='\t')
    df.columns=['all']
    df['type']=df['all'].str[0:2]
    df=df[df['type']=='36']
    df['CableID']=df['all'].str[3:9]
    df['HHID']=df['all'].str[29:36]
    df['PersonID']=df['all'].str[36:38]
    df=df[df['PersonID']!='00']
    df['ViewDate']=df['all'].str[68:76]
    df=pd.merge(df,HHIDs,on=['HHID'])
    df['ShiftedViewCode']=df['all'].str[76:77]
    df['ShiftedViewCode']=np.where(df['ShiftedViewCode']=="1",1,0)

#########live+3#############
   # df['Minimum Play Delay Minutes']=df['all'].str[109:114].astype('int')
   # df=df[df['Minimum Play Delay Minutes']<=4500]    
    
    df['Programcode']=df['all'].str[9:16]
    df['TelecastN']=df['all'].str[16:23]
    df['count']=1
    df['ViewStartTime']=df['all'].str[38:44].astype('int')
    df['ViewEndTime']=df['all'].str[44:50].astype('int')
    df['min']=df['ViewEndTime']-df['ViewStartTime']    
    df=df[['HHID','CableID','Programcode','TelecastN','ShiftedViewCode','count','min']]
    df=df.groupby(['HHID','CableID','Programcode','TelecastN']).sum()
    df.reset_index(inplace=True) 
    type36=type36.append(df,ignore_index=True)
 
type36=type36.groupby(['HHID','CableID','Programcode','TelecastN']).sum()
type36.reset_index(inplace=True) 
type36['ShiftedViewCode']=type36['ShiftedViewCode']/type36['count']
type36['ShiftedViewCode']=np.where(type36['ShiftedViewCode']>=0.5,1,0)

df=pd.DataFrame()
type30=pd.DataFrame()
for i in names30:
    df=pd.read_csv(i,sep='\t')
    df.columns=['all']
    df['type']=df['all'].str[0:2]
    df=df[df['type']=='30']
    df['ProgramDate']=df['all'].str[39:47]
    type30=type30.append(df,ignore_index=True)
  
type30['CableID']=type30['all'].str[8:14]
type30['CableName']=type30['all'].str[3:8]
type30['Programcode']=type30['all'].str[14:21]
type30['ProgramName']=type30['all'].str[55:80]
type30['TelecastN']=type30['all'].str[21:28]
type30['StartTime']=type30['all'].str[88:90]

type30=type30[['ProgramDate', 'CableID', 'CableName', 'Programcode',
       'ProgramName', 'TelecastN', 'StartTime']]
type30=type30.drop_duplicates()

allfile=pd.merge(type36,type30,on=['CableID','Programcode','TelecastN'])

del df
del type30
del type36
import gc
gc.collect()
  
allfile['ProgramDateWD']=pd.to_datetime(allfile['ProgramDate'])
allfile['ProgramDateWD']=allfile['ProgramDateWD'].dt.dayofweek
days = {0:'1Mon',1:'2Tue',2:'3Wed',3:'4Thu',4:'5Fri',5:'6Sat',6:'7Sun'}

allfile['ProgramDateWD']= allfile['ProgramDateWD'].apply(lambda x: days[x])
allfile=allfile.drop_duplicates()

allfile.to_csv('/Users/jubaplus1/Desktop/Starz - Ash vs Evil Dead/alltransaction.csv',index=False)

for i in itlist:
    allfile=allfile[allfile['Programcode']!=i]

allfile['freq']=1
allfile['programnew']=allfile['CableName']+'_'+allfile['ProgramName']

hourfreq=allfile[['StartTime','HHID','freq']]
hourfreq=pd.pivot_table(hourfreq, values=['freq'], index=['HHID'], columns=['StartTime'], aggfunc=np.sum)
hourfreq=hourfreq.fillna(0)
hourfreq.to_csv('/Users/jubaplus1/Desktop/Starz - Ash vs Evil Dead/hourfreq.csv')

hourmin=allfile[['StartTime','HHID','min']]
hourmin=pd.pivot_table(hourmin, values=['min'], index=['HHID'], columns=['StartTime'], aggfunc=np.sum)
hourmin=hourmin.fillna(0)
hourmin.to_csv('/Users/jubaplus1/Desktop/Starz - Ash vs Evil Dead/hourmin.csv')

dayfreq=allfile[['ProgramDateWD','HHID','freq']]
dayfreq=pd.pivot_table(dayfreq, values=['freq'], index=['HHID'], columns=['ProgramDateWD'], aggfunc=np.sum)
dayfreq=dayfreq.fillna(0)
dayfreq.to_csv('/Users/jubaplus1/Desktop/Starz - Ash vs Evil Dead/dayfreq.csv')

daymin=allfile[['ProgramDateWD','HHID','min']]
daymin=pd.pivot_table(daymin, values=['min'], index=['HHID'], columns=['ProgramDateWD'], aggfunc=np.sum)
daymin=daymin.fillna(0)
daymin.to_csv('/Users/jubaplus1/Desktop/Starz - Ash vs Evil Dead/daymin.csv')

networkfreq=allfile[['CableName','HHID','freq']]
networkfreq=pd.pivot_table(networkfreq, values=['freq'], index=['HHID'], columns=['CableName'], aggfunc=np.sum)
networkfreq=networkfreq.fillna(0)
networkfreq.to_csv('/Users/jubaplus1/Desktop/Starz - Ash vs Evil Dead/networkfreq.csv')

networkmin=allfile[['CableName','HHID','min']]
networkmin=pd.pivot_table(networkmin, values=['min'], index=['HHID'], columns=['CableName'], aggfunc=np.sum)
networkmin=networkmin.fillna(0)
networkmin.to_csv('/Users/jubaplus1/Desktop/Starz - Ash vs Evil Dead/networkmin.csv')

programfreq=allfile[['programnew','HHID','freq']]
programfreq=pd.pivot_table(programfreq, values=['freq'], index=['HHID'], columns=['programnew'], aggfunc=np.sum)
programfreq=programfreq.fillna(0)
programfreq.to_csv('/Users/jubaplus1/Desktop/Starz - Ash vs Evil Dead/programfreq.csv')

programmin=allfile[['programnew','HHID','min']]
programmin=pd.pivot_table(programmin, values=['min'], index=['HHID'], columns=['programnew'], aggfunc=np.sum)
programmin=programmin.fillna(0)
programmin.to_csv('/Users/jubaplus1/Desktop/Starz - Ash vs Evil Dead/programmin.csv')


shiftview2=allfile[['HHID','ShiftedViewCode']]
shiftview2=shiftview2.groupby(['HHID']).mean()
shiftview2.reset_index(inplace=True)  
shiftview2['ShiftedViewCode']=np.where(shiftview2['ShiftedViewCode']==1,1,
np.where(shiftview2['ShiftedViewCode']==0,2,3))
shiftview2.columns=['HHID','ShiftedViewindicator']

totalfre2=allfile[['HHID','ShiftedViewCode','TelecastN']]
totalfre2=totalfre2.groupby(['HHID','ShiftedViewCode']).count()
totalfre2.reset_index(inplace=True)  
totalfre2p1=totalfre2[totalfre2['ShiftedViewCode']==1]
totalfre2p1=totalfre2p1[['HHID', 'TelecastN']]
totalfre2p1.columns=['HHID', 'Totalfreq_Live']
totalfre2p2=totalfre2[totalfre2['ShiftedViewCode']!=1]
totalfre2p2=totalfre2p2[['HHID', 'TelecastN']]
totalfre2p2.columns=['HHID', 'Totalfreq_Shifted']
totalfre2=pd.merge(totalfre2p1,totalfre2p2,on='HHID',how='outer')
totalfre2=totalfre2.fillna(0)

totalfre2=pd.merge(totalfre2,shiftview2,on='HHID',how='outer')

totalfre2.to_csv('/Users/jubaplus1/Desktop/Starz - Ash vs Evil Dead/shiftedviewing.csv')



#allfile=pd.read_csv('/Users/Jubaplus/Desktop/RectifyCableOT/alltransaction.csv',dtype={'HHID':'object'})

Cluster=pd.read_csv('/Users/jubaplus1/Desktop/Starz - Ash vs Evil Dead/ash cluster.csv',dtype={'Household Number':'object'})
Cluster.columns=['HHID','Clusters']
Cluster['HHID'] = Cluster['HHID'].apply(lambda x: x.zfill(7))
#print(Cluster)
Cluster['Clusters']=np.where(Cluster['Clusters']==1,'Balance',np.where(Cluster['Clusters']==5,'Occasional','Core'))
print(Cluster.groupby('Clusters').count())

shiftnetwork=allfile[['HHID','ShiftedViewCode','TelecastN','CableName']]
shiftnetwork=shiftnetwork.groupby(['HHID','ShiftedViewCode','CableName']).count()
shiftnetwork.reset_index(inplace=True)  
shiftnetworkp1=shiftnetwork[shiftnetwork['ShiftedViewCode']==1]
shiftnetworkp1=shiftnetworkp1[['HHID','CableName', 'TelecastN']]
shiftnetworkp1.columns=['HHID', 'CableName','Totalfreq_Live']
shiftnetwork2=shiftnetwork[shiftnetwork['ShiftedViewCode']!=1]
shiftnetwork2=shiftnetwork2[['HHID', 'CableName','TelecastN']]
shiftnetwork2.columns=['HHID', 'CableName','Totalfreq_Shifted']
shiftnetwork=pd.merge(shiftnetworkp1,shiftnetwork2,on=['HHID','CableName'],how='outer')
shiftnetwork=shiftnetwork.fillna(0)
shiftnetwork=pd.merge(shiftnetwork,Cluster,on=['HHID'])
shiftnetwork=shiftnetwork[['CableName', 'Totalfreq_Live', 'Totalfreq_Shifted', 'Clusters']]
shiftnetwork=shiftnetwork.groupby(['Clusters','CableName']).sum()
shiftnetwork.reset_index(inplace=True)  
shiftnetwork=pd.pivot_table(shiftnetwork, values=[ 'Totalfreq_Live', 'Totalfreq_Shifted'], index=['CableName'], columns=['Clusters'], aggfunc=np.sum)
shiftnetwork.to_csv('/Users/jubaplus1/Desktop/Starz - Ash vs Evil Dead/shiftedviewingnetwork.csv')



del (shiftnetwork,shiftnetworkp1,shiftnetwork2)
import gc
gc.collect()

shiftnetwork=allfile[['HHID','ShiftedViewCode','TelecastN','programnew']]
shiftnetwork=shiftnetwork.groupby(['HHID','ShiftedViewCode','programnew']).count()
shiftnetwork.reset_index(inplace=True)  
shiftnetworkp1=shiftnetwork[shiftnetwork['ShiftedViewCode']==1]
shiftnetworkp1=shiftnetworkp1[['HHID','programnew', 'TelecastN']]
shiftnetworkp1.columns=['HHID', 'programnew','Totalfreq_Live']
shiftnetwork2=shiftnetwork[shiftnetwork['ShiftedViewCode']!=1]
shiftnetwork2=shiftnetwork2[['HHID', 'programnew','TelecastN']]
shiftnetwork2.columns=['HHID', 'programnew','Totalfreq_Shifted']
shiftnetwork=pd.merge(shiftnetworkp1,shiftnetwork2,on=['HHID','programnew'],how='outer')
shiftnetwork=shiftnetwork.fillna(0)
shiftnetwork=pd.merge(shiftnetwork,Cluster,on=['HHID'])
shiftnetwork=shiftnetwork[['programnew', 'Totalfreq_Live', 'Totalfreq_Shifted', 'Clusters']]
shiftnetwork=shiftnetwork.groupby(['Clusters','programnew']).sum()
shiftnetwork.reset_index(inplace=True)  
shiftnetwork=pd.pivot_table(shiftnetwork, values=[ 'Totalfreq_Live', 'Totalfreq_Shifted'], index=['programnew'], columns=['Clusters'], aggfunc=np.sum)
shiftnetwork.to_csv('/Users/jubaplus1/Desktop/Starz - Ash vs Evil Dead/shiftedviewingprogram.csv')

del (shiftnetwork,shiftnetworkp1,shiftnetwork2)
import gc
gc.collect()

shiftnetwork=allfile[['HHID','programnew']]
shiftnetwork=shiftnetwork.drop_duplicates('HHID')
shiftnetwork=pd.merge(shiftnetwork,Cluster,on=['HHID'])
shiftnetwork.groupby('Clusters').count()

  


