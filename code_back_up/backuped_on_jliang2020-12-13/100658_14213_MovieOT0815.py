# -*- coding: utf-8 -*-
"""
Created on Mon May 23 10:30:00 2016

@author: Jubaplus
"""

#####update the direcotry--all output files will be saved in this directory
foldername='/Users/jubaplus1/Desktop/Archy/Rectify on air'

###### make sure the directory file is up to date
directorypath='/Users/jubaplus1/Desktop/Code/New Code/NielsenDirectory.csv'
hhidpath='/Users/jubaplus1/Desktop/Archy/Rectify on air/Rectify hhid.csv'
####put startdate and enddate for the project here:
Startdate=20150608
Enddate=20160807


##### For Quads update Cable ID (WeTV: '007516',SUND: '007602',STARZ: '009157', WGNA: '6788')
Cable_ID=['006593','006111','008250','007516']

###put IT Shows here, put '000' if you do not want to exclude any IT shows, use to seprate
itlist=['000']








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
import gc

HHIDs=pd.read_csv(hhidpath,dtype={'Household Number':'object'})
HHIDs=HHIDs[['Household Number']]
HHIDs.columns=['HHID']
HHIDs['HHID'] = HHIDs['HHID'].apply(lambda x: x.zfill(7))

directory=pd.read_csv(directorypath)
directory['Week']=pd.to_datetime(directory['Week'])


names36=directory[(directory['Week']<=enddate)&(directory['Week']>=startdate)&(directory['Table']=='C36')]
names30=directory[(directory['Week']<=enddate)&(directory['Week']>=startdate)&(directory['Table']=='C30')]
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
    df['Date']=df['all'].str[68:76]
    df['PersonID']=df['all'].str[36:38]
    df=df[df['PersonID']!='00']
    df=pd.merge(df,HHIDs,on=['HHID'])
    df['Programcode']=df['all'].str[9:16]
    df['TelecastN']=df['all'].str[16:23]
    df['count']=1
    df['ViewStartTime']=df['all'].str[38:44].astype('int')
    df['ViewEndTime']=df['all'].str[44:50].astype('int')
    df['min']=df['ViewEndTime']-df['ViewStartTime']    
    df=df[['HHID','CableID','Programcode','TelecastN','count','min']]
    df=df.groupby(['HHID','CableID','Programcode','TelecastN']).sum()
    df.reset_index(inplace=True) 
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
    df=df[df.CableID.isin(Cable_ID)]
    type30=type30.append(df,ignore_index=True)
  

type30['CableName']=type30['all'].str[3:8]
type30['Programcode']=type30['all'].str[14:21]
type30['ProgramName']=type30['all'].str[55:80]
type30['TelecastN']=type30['all'].str[21:28]
type30['StartTime']=type30['all'].str[88:90]
type30['ProgramType']=type30['all'].str[122:126]
type30['EpisodeName']=type30['all'].str[130:170]
type30['EpisodeName']=type30.EpisodeName.str.strip()
for i in itlist:
    type30=type30[type30['Programcode']!=i]


type30.to_csv("allprograms.csv",index=False)

allfile=pd.merge(type36,type30,on=['CableID','Programcode','TelecastN'])
allfile.columns=['HHID', 'CableID', 'Programcode', 'TelecastN', 'count', 'min', 'all',
       'type', 'ProgramDate', 'CableName', 'ProgramName', 'StartTime',
       'ProgramType', 'EpisodeName']
allfile=allfile.drop_duplicates()
  
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

allfile.to_csv('alltransaction.csv',index=False)

del df
del type30
del type36
gc.collect()

all2=allfile[['CableID', 'HHID', 'Programcode',
       'TelecastN', 
       'ProgramDate', 'CableName', 'programnew', 'StartTime','ProgramDateWD','freq','ProgramType']]
all2=all2.drop_duplicates()

hourfreq=all2[['StartTime','HHID','freq']]
hourfreq=pd.pivot_table(hourfreq, values='freq', index=['HHID'], columns=['StartTime'], aggfunc=np.sum)
hourfreq=hourfreq.fillna(0)
hourfreq.to_csv('hourfreq.csv')

hourmin=allfile[['StartTime','HHID','min']]
hourmin=pd.pivot_table(hourmin, values='min', index=['HHID'], columns=['StartTime'], aggfunc=np.sum)
hourmin=hourmin.fillna(0)
hourmin.to_csv('hourmin.csv')

dayfreq=all2[['ProgramDateWD','HHID','freq']]
dayfreq=pd.pivot_table(dayfreq, values='freq', index=['HHID'], columns=['ProgramDateWD'], aggfunc=np.sum)
dayfreq=dayfreq.fillna(0)
dayfreq.to_csv('dayfreq.csv')

daymin=allfile[['ProgramDateWD','HHID','min']]
daymin=pd.pivot_table(daymin, values='min', index=['HHID'], columns=['ProgramDateWD'], aggfunc=np.sum)
daymin=daymin.fillna(0)
daymin.to_csv('daymin.csv')

programfreq=all2[all2['ProgramType']!='FF  ']
programfreq=programfreq[['programnew','HHID','freq']]
programfreq=pd.pivot_table(programfreq, values='freq', index=['HHID'], columns=['programnew'], aggfunc=np.sum)
programfreq=programfreq.fillna(0)
programfreq.to_csv('programnonfffreq.csv')

programmin=allfile[allfile['ProgramType']!='FF  ']
programmin=programmin[['programnew','HHID','min']]
programmin=pd.pivot_table(programmin, values='min', index=['HHID'], columns=['programnew'], aggfunc=np.sum)
programmin=programmin.fillna(0)
programmin.to_csv('programnonffmin.csv')

programfreq=all2[all2['ProgramType']=='FF  ']
programfreq=programfreq[['programnew','HHID','freq']]
programfreq=pd.pivot_table(programfreq, values='freq', index=['HHID'], columns=['programnew'], aggfunc=np.sum)
programfreq=programfreq.fillna(0)
programfreq.to_csv('episodefffreq.csv')

programmin=allfile[allfile['ProgramType']=='FF  ']
programmin=programmin[['programnew','HHID','min']]
programmin=pd.pivot_table(programmin, values='min', index=['HHID'], columns=['programnew'], aggfunc=np.sum)
programmin=programmin.fillna(0)
programmin.to_csv('episodeffmin.csv')




