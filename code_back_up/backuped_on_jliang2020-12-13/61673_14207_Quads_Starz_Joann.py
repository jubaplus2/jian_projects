# -*- coding: utf-8 -*-
"""
Created on Mon Jun  6 15:02:11 2016

@author: Jubaplus
"""


###### make sure the directory file is up to date
directorypath='/Users/jubaplus1/Desktop/Code/New Code/NielsenDirectory.csv'
hhintab='/Users/jubaplus1/Desktop/Nielsen/type250717.csv'
#personintab='/Users/jubaplus1/Desktop/Nielsen/type260717.csv'
####put startdate and enddate for the project here:
Startdate=20150718
Enddate=20160625

###outputpath
outputpath='/Users/jubaplus1/Desktop/starz.csv'


##### For Quads update Cable ID (WeTV: '007516',SUND: '007602',STARZ: '009157', WGNA: '006788',CMT:'006647')
Cable_ID='009157'
####if do not want prime time for quads, update below varaible to 'F'
primetimeindicator='F'


import pandas as pd
import numpy as np

###download this file from server
HHIntab=pd.read_csv(hhintab,dtype={'Week':'object','Weeknumber':'int'})
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

import datetime
a=datetime.datetime.strptime(str(Startdate), '%Y%m%d').date()
b=6-a.weekday()
startdate=a+datetime.timedelta(days=b)
c=datetime.datetime.strptime(str(Enddate), '%Y%m%d').date()
d=6-c.weekday()
enddate=c+datetime.timedelta(days=d)

HHIntab2=HHIntab[(HHIntab['ViewDate']>=a)&(HHIntab['ViewDate']<=c)]
del(a,b,c,d)


HHIntab2=HHIntab2.groupby('HHID').mean()
HHIntab2.reset_index(inplace=True)

directory=pd.read_csv(directorypath)
directory['Week']=pd.to_datetime(directory['Week'])

names36=directory[(directory['Week']<=enddate)&(directory['Week']>=startdate)&(directory['Table']=='C36')]
names36=names36['FilePath']

names30=directory[(directory['Week']<=enddate)&(directory['Week']>=startdate)&(directory['Table']=='C30')]
names30=names30['FilePath']

df=pd.DataFrame()
type36=pd.DataFrame()
for i in names36:
    df=pd.read_csv(i,sep='\t')
    df.columns=['all']
    df['type']=df['all'].str[0:2]
    df=df[df['type']=='36']
    df['HHID']=df['all'].str[29:36]
    df['CableID']=df['all'].str[3:9]
    df=df[df['CableID']==Cable_ID]
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
    df=df[df['CableID']==Cable_ID]
    type30=type30.append(df,ignore_index=True)
  
type30['Programcode']=type30['all'].str[14:21]
type30['TelecastN']=type30['all'].str[21:28]
type30['StartTime']=type30['all'].str[88:92].astype(int)
type30['EndTime']=type30['all'].str[94:98].astype(int)
type30['Date2']=type30['ProgramDate'].astype('int')
type30=type30[(type30['Date2']>=Startdate)&(type30['Date2']<=Enddate)]

if primetimeindicator=='T':
   type30=type30[(((type30['StartTime']>2000)&(type30['StartTime']<2300))|((type30['EndTime']>2000)&(type30['EndTime']<2300)))] 
if Cable_ID=='007516':
    type30['Weekday']=pd.to_datetime(type30['ProgramDate']).dt.dayofweek
    type30=type30[type30.Weekday.isin([3,4])]

type301=type30[(type30['Date2']>=20150725)&(type30['Date2']<=20160625)]

HHIntab2=HHIntab[(HHIntab['ViewDate']>='2015-07-25')&(HHIntab['ViewDate']<='2016-06-25')]
HHIntab2=HHIntab2.groupby('HHID').mean()
HHIntab2.reset_index(inplace=True)

STARZviewers=pd.merge(type36,type301,on=['CableID','Programcode','TelecastN'])

STARZviewers=STARZviewers[['HHID']]
STARZviewers=STARZviewers.drop_duplicates()
STARZviewers['HHID']=STARZviewers['HHID'].astype(int)

df=pd.merge(STARZviewers,HHIntab2,on='HHID')

df.to_csv('/Users/jubaplus1/Desktop/starzviewer072515_062516.csv',index=False)












