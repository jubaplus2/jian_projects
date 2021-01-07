# -*- coding: utf-8 -*-
"""
Created on Mon May 23 10:30:00 2016

@author: Jubaplus
"""

#####update the direcotry--all output files will be saved in this directory
foldername='/Users/jubaplus1/Desktop/Freeform/Shadowhunters S2/SH S1 DATA/SH S1 Core vs FF L3M OT/FF only'

###### make sure the directory file is up to date
directorypath='/Users/jubaplus1/Desktop/Code/New Code/NielsenDirectory.csv'
daypartspath='/Users/jubaplus1/Desktop/Freeform/Shadowhunters S2/SH S1 DATA/SH S1 Core vs FF L3M OT/SH S1 Core vs FF L3M.csv'
clusterpath='/Users/jubaplus1/Desktop/Freeform/Shadowhunters S2/SH S1 DATA/SH S1 Core vs FF L3M OT/SH S1 Core vs FF L3M cluster.csv'
####put startdate and enddate for the project here:

Startdate=20160221
Enddate=20160821

##### For Quads update Cable ID (WeTV: '007516',SUND: '007602',STARZ: '009157', WGNA: '006788', CMT:'006647', MTV:'006198',FREEFORM: '0062O1')
Cable_ID=['006201']

###put IT Shows here
# 7 DIGITS
itlist=['0397803']
balance_cluster=[4]
occasional_cluster=[3]

####change value to F if BroadcastOT
cableotindicator='T'

#####change value to T if we switch to live+3
livestreamindicator='F'



#####code starts here, DO NOT make any changes
import os
os.chdir(foldername)

import datetime
a=datetime.datetime.strptime(str(Startdate), '%Y%m%d').date()
b=6-a.weekday()
startdate=a+datetime.timedelta(days=b)
c=datetime.datetime.strptime(str(Enddate), '%Y%m%d').date()
d=6-c.weekday()
enddate=c+datetime.timedelta(days=d)
del(a,b,c,d)



import pandas as pd
import numpy as np

df=pd.read_csv(daypartspath)
#,dtype={'Household Number':'object'})
HHIDs=df[['HHID_PersonID']]
HHIDs=HHIDs.drop_duplicates()
HHIDs.columns=['HHID_PersonID']

Cluster=pd.read_csv(clusterpath)
#,dtype={'Household Number':'object'})
Cluster.columns=['HHID_PersonID','Clusters']
#Cluster['Clusters']=np.where(Cluster.Clusters.isin(balance_cluster),'Balance',np.where(Cluster.Clusters.isin(occasional_cluster),'Occasional','Core'))
print(Cluster.groupby('Clusters').count())


directory=pd.read_csv(directorypath)
directory['Week']=pd.to_datetime(directory['Week'])

if cableotindicator=='T':
    names36=directory[(directory['Week']<=enddate)&(directory['Week']>=startdate)&(directory['Table']=='C36')]
    names30=directory[(directory['Week']<=enddate)&(directory['Week']>=startdate)&(directory['Table']=='C30')]
else:
    names36=directory[(directory['Week']<=enddate)&(directory['Week']>=startdate)&(directory['Table']=='B36')]
    names30=directory[(directory['Week']<=enddate)&(directory['Week']>=startdate)&(directory['Table']=='B30')]

names30=names30['FilePath']
names36=names36['FilePath']



df=pd.DataFrame()
type36=pd.DataFrame()
for i in names36:
    df=pd.read_csv(i,sep='\t')
    df.columns=['all']
    df['type']=df['all'].str[0:2]
    df=df[df['type']=='36']
    df['CableID']=df['all'].str[3:9]
    df=df[df.CableID.isin(Cable_ID)]
    df['HHID']=df['all'].str[29:36]
    df['PersonID']=df['all'].str[36:38]
    df=df[df['PersonID']!='00']
    df['ViewDate']=df['all'].str[68:76]
    df['HHID_PersonID']=df['HHID']+'_'+df['PersonID']
    df=pd.merge(df,HHIDs,on=['HHID_PersonID'])
    df['ShiftedViewCode']=df['all'].str[76:77]
    df['ShiftedViewCode']=np.where(df['ShiftedViewCode']=="1",1,0)
    if livestreamindicator=='T':
        df['Minimum Play Delay Minutes']=df['all'].str[109:114].astype('int')
        df=df[df['Minimum Play Delay Minutes']<=4500]
    df['Programcode']=df['all'].str[9:16]
    df['TelecastN']=df['all'].str[16:23]
    df['count']=1
    df['ViewStartTime']=df['all'].str[38:44].astype('int')
    df['ViewEndTime']=df['all'].str[44:50].astype('int')
    df['min']=df['ViewEndTime']-df['ViewStartTime']    
    df=df[['HHID_PersonID','CableID','Programcode','TelecastN','ShiftedViewCode','count','min']]
    df=df.groupby(['HHID_PersonID','CableID','Programcode','TelecastN']).sum()
    df.reset_index(inplace=True) 
    type36=type36.append(df,ignore_index=True)
 
type36=type36.groupby(['HHID_PersonID','CableID','Programcode','TelecastN']).sum()
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
    df['CableID']=df['all'].str[8:14]
    df=df[df.CableID.isin(Cable_ID)]
    type30=type30.append(df,ignore_index=True)
  
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

allfile.to_csv('alltransaction.csv',index=False)

for i in itlist:
    allfile=allfile[allfile['Programcode']!=i]

import pandas as pd
foldername='/Users/jubaplus1/Desktop/Freeform/Shadowhunters S2/SH S1 DATA/SH S1 Lapsed (Occ) vs. SH S1 Core OT/FF only'
import os
os.chdir(foldername)

#allfile=pd.read_csv('/Users/jubaplus1/Desktop/Freeform/Shadowhunters S2/SH S1 DATA/Freeform Only OT Core vs Balance/alltransaction.csv')
programlist= allfile[['ProgramName']]
programlist= programlist.drop_duplicates()
programlist.to_csv('programlist.csv',index=False)
#add new col ProgramName_New
programlist=pd.read_csv('programlist.csv')
allfile=pd.merge(allfile, programlist,on='ProgramName')

allfile['freq']=1
allfile['programnew']=allfile['CableName']+'_'+allfile['ProgramName']

hourfreq=allfile[['StartTime','HHID_PersonID','freq']]
hourfreq=pd.pivot_table(hourfreq, values='freq', index=['HHID_PersonID'], columns=['StartTime'], aggfunc=np.sum)
hourfreq=hourfreq.fillna(0)
hourfreq.reset_index(inplace=True)  
hourfreq=pd.merge(hourfreq,Cluster,on=['HHID_PersonID'])
#hourfreq=hourfreq[hourfreq['Clusters']=='Core']
hourfreq.to_csv('/Users/jubaplus1/Desktop/Freeform/Shadowhunters S2/SH S1 DATA/SH S1 Lapsed (Occ) vs. SH S1 Core OT/FF only/hourfreq.csv',index=False)

hourmin=allfile[['StartTime','HHID_PersonID','min']]
hourmin=pd.pivot_table(hourmin, values='min', index=['HHID_PersonID'], columns=['StartTime'], aggfunc=np.sum)
hourmin=hourmin.fillna(0)
hourmin.reset_index(inplace=True)  
hourmin=pd.merge(hourmin,Cluster,on=['HHID_PersonID'])
#hourmin=hourmin[hourmin['Clusters']=='Core']
hourmin.to_csv('/Users/jubaplus1/Desktop/Freeform/Shadowhunters S2/SH S1 DATA/SH S1 Lapsed (Occ) vs. SH S1 Core OT/FF only/hourmin.csv',index=False)

dayfreq=allfile[['ProgramDateWD','HHID_PersonID','freq']]
dayfreq=pd.pivot_table(dayfreq, values='freq', index=['HHID_PersonID'], columns=['ProgramDateWD'], aggfunc=np.sum)
dayfreq=dayfreq.fillna(0)
dayfreq.reset_index(inplace=True)  
dayfreq=pd.merge(dayfreq,Cluster,on=['HHID_PersonID'])
#dayfreq=dayfreq[dayfreq['Clusters']=='Core']
dayfreq.to_csv('/Users/jubaplus1/Desktop/Freeform/Shadowhunters S2/SH S1 DATA/SH S1 Lapsed (Occ) vs. SH S1 Core OT/FF only/dayfreq.csv',index=False)

daymin=allfile[['ProgramDateWD','HHID_PersonID','min']]
daymin=pd.pivot_table(daymin, values='min', index=['HHID_PersonID'], columns=['ProgramDateWD'], aggfunc=np.sum)
daymin=daymin.fillna(0)
daymin.reset_index(inplace=True)  
daymin=pd.merge(daymin,Cluster,on=['HHID_PersonID'])
#daymin=daymin[daymin['Clusters']=='Core']
daymin.to_csv('/Users/jubaplus1/Desktop/Freeform/Shadowhunters S2/SH S1 DATA/SH S1 Lapsed (Occ) vs. SH S1 Core OT/FF only/daymin.csv',index=False)

networkfreq=allfile[['CableName','HHID_PersonID','freq']]
networkfreq=pd.pivot_table(networkfreq, values='freq', index=['HHID_PersonID'], columns=['CableName'], aggfunc=np.sum)
networkfreq=networkfreq.fillna(0)
networkfreq.reset_index(inplace=True)  
networkfreq=pd.merge(networkfreq,Cluster,on=['HHID_PersonID'])
#networkfreq=networkfreq[networkfreq['Clusters']!='Occasional']
networkfreq.to_csv('/Users/jubaplus1/Desktop/Freeform/Shadowhunters S2/SH S1 DATA/SH S1 Lapsed (Occ) vs. SH S1 Core OT/FF only/networkfreq.csv',index=False)

networkmin=allfile[['CableName','HHID_PersonID','min']]
networkmin=pd.pivot_table(networkmin, values='min', index=['HHID_PersonID'], columns=['CableName'], aggfunc=np.sum)
networkmin=networkmin.fillna(0)
networkmin.reset_index(inplace=True)  
networkmin=pd.merge(networkmin,Cluster,on=['HHID_PersonID'])
#networkmin=networkmin[networkmin['Clusters']!='Occasional']
networkmin.to_csv('/Users/jubaplus1/Desktop/Freeform/Shadowhunters S2/SH S1 DATA/SH S1 Lapsed (Occ) vs. SH S1 Core OT/FF only/networkmin.csv',index=False)

programfreq=allfile[['programnew','HHID_PersonID','freq']]
programfreq=pd.pivot_table(programfreq, values='freq', index=['HHID_PersonID'], columns=['programnew'], aggfunc=np.sum)
programfreq=programfreq.fillna(0)
programfreq.to_csv('programfreqallclusters.csv')
programfreq.reset_index(inplace=True) 
programfreq=pd.merge(programfreq,Cluster,on=['HHID_PersonID'])
#programfreq=programfreq[programfreq['Clusters']!='Occasional']
programfreq.to_csv('/Users/jubaplus1/Desktop/Freeform/Shadowhunters S2/SH S1 DATA/SH S1 Lapsed (Occ) vs. SH S1 Core OT/FF only/programfreq.csv',index=False)


programmin=allfile[['programnew','HHID_PersonID','min']]
programmin=pd.pivot_table(programmin, values='min', index=['HHID_PersonID'], columns=['programnew'], aggfunc=np.sum)
programmin=programmin.fillna(0)
programmin.reset_index(inplace=True)  
programmin=pd.merge(programmin,Cluster,on=['HHID_PersonID'])
#programmin=programmin[programmin['Clusters']!='Occasional']
programmin.to_csv('/Users/jubaplus1/Desktop/Freeform/Shadowhunters S2/SH S1 DATA/SH S1 Lapsed (Occ) vs. SH S1 Core OT/FF only/programmin.csv',index=False)


shiftview2=allfile[['HHID_PersonID','ShiftedViewCode']]
shiftview2=shiftview2.groupby(['HHID_PersonID']).mean()
shiftview2.reset_index(inplace=True)  
shiftview2['ShiftedViewCode']=np.where(shiftview2['ShiftedViewCode']==1,1,
np.where(shiftview2['ShiftedViewCode']==0,2,3))
shiftview2.columns=['HHID_PersonID','ShiftedViewindicator']

totalfre2=allfile[['HHID_PersonID','ShiftedViewCode','TelecastN']]
totalfre2=totalfre2.groupby(['HHID_PersonID','ShiftedViewCode']).count()
totalfre2.reset_index(inplace=True)  
totalfre2p1=totalfre2[totalfre2['ShiftedViewCode']==1]
totalfre2p1=totalfre2p1[['HHID_PersonID', 'TelecastN']]
totalfre2p1.columns=['HHID_PersonID', 'Totalfreq_Live']
totalfre2p2=totalfre2[totalfre2['ShiftedViewCode']!=1]
totalfre2p2=totalfre2p2[['HHID_PersonID', 'TelecastN']]
totalfre2p2.columns=['HHID_PersonID', 'Totalfreq_Shifted']
totalfre2=pd.merge(totalfre2p1,totalfre2p2,on='HHID_PersonID',how='outer')
totalfre2=totalfre2.fillna(0)

totalfre2=pd.merge(totalfre2,shiftview2,on='HHID_PersonID',how='outer')

totalfre2.to_csv('/Users/jubaplus1/Desktop/Freeform/Shadowhunters S2/SH S1 DATA/SH S1 Lapsed (Occ) vs. SH S1 Core OT/FF only/shiftedviewing.csv',index=False)


shiftnetwork=allfile[['HHID_PersonID','ShiftedViewCode','TelecastN','CableName']]
shiftnetwork=shiftnetwork.groupby(['HHID_PersonID','ShiftedViewCode','CableName']).count()
shiftnetwork.reset_index(inplace=True)  
shiftnetworkp1=shiftnetwork[shiftnetwork['ShiftedViewCode']==1]
shiftnetworkp1=shiftnetworkp1[['HHID_PersonID','CableName', 'TelecastN']]
shiftnetworkp1.columns=['HHID_PersonID', 'CableName','Totalfreq_Live']
shiftnetwork2=shiftnetwork[shiftnetwork['ShiftedViewCode']!=1]
shiftnetwork2=shiftnetwork2[['HHID_PersonID', 'CableName','TelecastN']]
shiftnetwork2.columns=['HHID_PersonID', 'CableName','Totalfreq_Shifted']
shiftnetwork=pd.merge(shiftnetworkp1,shiftnetwork2,on=['HHID_PersonID','CableName'],how='outer')
shiftnetwork=shiftnetwork.fillna(0)
shiftnetwork=pd.merge(shiftnetwork,Cluster,on=['HHID_PersonID'])
shiftnetwork=shiftnetwork[['CableName', 'Totalfreq_Live', 'Totalfreq_Shifted', 'Clusters']]
shiftnetwork=shiftnetwork.groupby(['Clusters','CableName']).sum()
shiftnetwork.reset_index(inplace=True)  
shiftnetwork=pd.pivot_table(shiftnetwork, values=[ 'Totalfreq_Live', 'Totalfreq_Shifted'], index=['CableName'], columns=['Clusters'], aggfunc=np.sum)
shiftnetwork.to_csv('/Users/jubaplus1/Desktop/Freeform/Shadowhunters S2/SH S1 DATA/SH S1 Lapsed (Occ) vs. SH S1 Core OT/FF only/shiftedviewingnetwork.csv')



del (shiftnetwork,shiftnetworkp1,shiftnetwork2)
import gc
gc.collect()

shiftnetwork=allfile[['HHID_PersonID','ShiftedViewCode','TelecastN','programnew']]
shiftnetwork=shiftnetwork.groupby(['HHID_PersonID','ShiftedViewCode','programnew']).count()
shiftnetwork.reset_index(inplace=True)  
shiftnetworkp1=shiftnetwork[shiftnetwork['ShiftedViewCode']==1]
shiftnetworkp1=shiftnetworkp1[['HHID_PersonID','programnew', 'TelecastN']]
shiftnetworkp1.columns=['HHID_PersonID', 'programnew','Totalfreq_Live']
shiftnetwork2=shiftnetwork[shiftnetwork['ShiftedViewCode']!=1]
shiftnetwork2=shiftnetwork2[['HHID_PersonID', 'programnew','TelecastN']]
shiftnetwork2.columns=['HHID_PersonID', 'programnew','Totalfreq_Shifted']
shiftnetwork=pd.merge(shiftnetworkp1,shiftnetwork2,on=['HHID_PersonID','programnew'],how='outer')
shiftnetwork=shiftnetwork.fillna(0)

shiftnetwork=pd.merge(shiftnetwork,Cluster,on=['HHID_PersonID'])
shiftnetwork=shiftnetwork[['programnew', 'Totalfreq_Live', 'Totalfreq_Shifted', 'Clusters']]
shiftnetwork=shiftnetwork.groupby(['Clusters','programnew']).sum()
shiftnetwork.reset_index(inplace=True)  
shiftnetwork=pd.pivot_table(shiftnetwork, values=[ 'Totalfreq_Live', 'Totalfreq_Shifted'], index=['programnew'], columns=['Clusters'], aggfunc=np.sum)
shiftnetwork.to_csv('/Users/jubaplus1/Desktop/Freeform/Shadowhunters S2/SH S1 DATA/SH S1 Lapsed (Occ) vs. SH S1 Core OT/FF only/shiftedviewingprogram.csv')

del (shiftnetwork,shiftnetworkp1,shiftnetwork2)
import gc
gc.collect()



  

