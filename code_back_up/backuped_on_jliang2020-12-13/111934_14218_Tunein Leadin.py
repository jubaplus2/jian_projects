# -*- coding: utf-8 -*-
"""
Created on Mon May 23 10:30:00 2016

@author: Jubaplus
"""

###### make sure the directory file is up to date
directorypath='/Users/Jubaplus/Documents/R Scripts/Post Analysis/Nielsen Data/NielsenDirectory.csv'
hhintabpath='/Users/Jubaplus/Desktop/Type2526/type25_0103_0731.csv'

####put startdate and enddate for the project here:
Startdate=20160717
Enddate=20160717

##### update Cable ID (WeTV: '007516',SUND: '007602',STARZ: '009157', WGNA: '6788')
Cable_ID='009157'

#####update program ID and telecast ID
program_id=['0385703']
telecast_id=[1,2,3]

####data stream indicator: live+SD,live+3, live+7, live
livestreamindicator='live'

##### update the final file path and location
finalfiledic='/Users/Jubaplus/Desktop/power301_leadin.csv'



#####code starts here, DO NOT make any changes
import datetime
a=datetime.datetime.strptime(str(Startdate), '%Y%m%d').date()
b=6-a.weekday()
startdate=a+datetime.timedelta(days=b)
c=datetime.datetime.strptime(str(Enddate), '%Y%m%d').date()
if livestreamindicator=='live+3':
    c=c+datetime.timedelta(days=3)
elif livestreamindicator=='live+SD':
    c=c+datetime.timedelta(days=1)
elif livestreamindicator=='live+7':
    c=c+datetime.timedelta(days=7)


d=6-c.weekday()
enddate=c+datetime.timedelta(days=d)

import pandas as pd
directory=pd.read_csv(directorypath)
directory['Week']=pd.to_datetime(directory['Week'])

names36=directory[(directory['Week']<=enddate)&(directory['Week']>=startdate)&(directory['Table']=='C36')]
names36=names36['FilePath']

names30=directory[(directory['Week']<=enddate)&(directory['Week']>=startdate)&(directory['Table']=='C30')]
names30=names30['FilePath']

type36=pd.DataFrame()
for i in names36:
    df=pd.read_csv(i,sep='\t')
    df.columns=['all']
    df['type']=df['all'].str[0:2]
    df=df[df['type']=='36']
    df['HHID']=df['all'].str[29:36]
    df['CableID']=df['all'].str[3:9]
    df=df[df['CableID']==Cable_ID]
    df['ViewDate']=df['all'].str[68:76]
    df['PersonID']=df['all'].str[36:38]
    df=df[df['PersonID']!='00']
    df['Programcode']=df['all'].str[9:16]
    df=df[df.Programcode.isin(program_id)]
    #df=df[df.Programcode.isin()]
    df['TelecastN']=df['all'].str[16:23].astype(int)
    df=df[df.TelecastN.isin(telecast_id)]
    df['ViewStartTime']=df['all'].str[38:44]
    df['ViewEndTime']=df['all'].str[44:50]   
    df['ShiftedViewCode']=df['all'].str[76:77]
    df['VCRCode']=df['all'].str[55:56]
    df['PlatformCode']=df['all'].str[114:116]
    df['VODIndicator']=df['all'].str[116:118]
    df['DelayMins']=df['all'].str[109:114].astype(int)  
    type36=type36.append(df,ignore_index=True)

####data stream indicator: live+SD,live+3, live+7
if livestreamindicator=='live+3':
    type36=type36[type36['DelayMins']<=4500]
elif livestreamindicator=='live+SD':
    type36=type36[type36['DelayMins']<=1620]
elif livestreamindicator=='live':
    type36=type36[type36['ShiftedViewCode']=='1']
  
df=pd.DataFrame()
type30=pd.DataFrame()
for i in names30:
    df=pd.read_csv(i,sep='\t')
    df.columns=['all']
    df['type']=df['all'].str[0:2]
    df=df[df['type']=='30']
    df['ProgramDate']=df['all'].str[39:47]
    df['CableID']=df['all'].str[8:14]
    df=df[df['CableID']==Cable_ID]
    type30=type30.append(df,ignore_index=True)
  

type30['CableName']=type30['all'].str[3:8]
type30['Programcode']=type30['all'].str[14:21]
type30['ProgramName']=type30['all'].str[55:80]
type30['TelecastN']=type30['all'].str[21:28].astype(int)
type30['StartTime']=type30['all'].str[88:92]
type30['EpisodeName']=type30['all'].str[130:170]


allfile=pd.merge(type36,type30,on=['CableID','Programcode','TelecastN'])
allfile=allfile[[ 'HHID']]
allfile=allfile.drop_duplicates()
allfile['HHID']=allfile['HHID'].astype(int) 


import numpy as np
HHIntab=pd.read_csv(hhintabpath,dtype={'Week':'object','Weeknumber':'int'})
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

HHIntab2=HHIntab[(HHIntab['ViewDate']>=a)&(HHIntab['ViewDate']<=c)]
print(HHIntab2.groupby('ViewDate').count())
del(a,b,c,d)


HHIntab2=HHIntab2.groupby('HHID').mean()
HHIntab2.reset_index(inplace=True)

allfile=pd.merge(allfile,HHIntab2,on=['HHID'])


allfile.to_csv(finalfiledic,index=False)

del(type30,type36,allfile,names30,names36,startdate,enddate,directory,Cable_ID,df,finalfiledic,i,program_id,telecast_id,HHIntab,directorypath,Startdate,Enddate)
import gc
gc.collect()


